//! Backfill `acceptance_datetime` for `financials.parquet` rows where
//! Massive left the field NULL (~76% of rows, mostly 2011-2022 era).
//! Source: SEC EDGAR submissions API at
//! `https://data.sec.gov/submissions/CIK{cik:010}.json`.
//!
//! Output: `data/reference/acceptance_datetime_backfill.parquet` —
//! a side table keyed by `(cik, accession_number)` that downstream
//! readers join to `financials.parquet` and COALESCE.
//!
//! Rate limit: SEC permits 10 req/sec with a User-Agent header.
//! We run at concurrency=8 with token-bucket spacing of 110ms to stay
//! safely under.
//!
//! Required env:
//!   SEC_USER_AGENT  → e.g. "sb-v0 saketh.ganji@example.com"
//!                     SEC blocks anonymous requests.
//!
//! Optional env with defaults:
//!   FINANCIALS_PATH=data/reference/financials.parquet
//!   OUT_PATH=data/reference/acceptance_datetime_backfill.parquet
//!   CONCURRENCY=1   (each task sleeps 110ms; concurrency × (1/pacing)
//!                    must stay under SEC's 10 req/s. Default is 1 →
//!                    ~9 req/sec, safe.)
//!   SMOKE=1  → use first 20 CIKs only; do not write OUT_PATH.

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate, Utc};
use duckdb::Connection;
use momentum_store::acceptance_backfill::{
    AcceptanceBackfillRow, write_acceptance_backfill,
};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Debug, Deserialize)]
struct SubmissionsResponse {
    filings: Option<Filings>,
}

#[derive(Debug, Deserialize)]
struct Filings {
    recent: Option<RecentFilings>,
    #[serde(default)]
    files: Vec<OlderFileRef>,
}

#[derive(Debug, Deserialize)]
struct RecentFilings {
    #[serde(rename = "accessionNumber")]
    accession_number: Vec<String>,
    #[serde(rename = "acceptanceDateTime")]
    acceptance_datetime: Vec<String>,
    form: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct OlderFileRef {
    name: String,
}

#[derive(Debug, Deserialize)]
struct OlderFile {
    #[serde(rename = "accessionNumber")]
    accession_number: Vec<String>,
    #[serde(rename = "acceptanceDateTime")]
    acceptance_datetime: Vec<String>,
    form: Vec<String>,
}

fn accession_from_url(url: &str) -> Option<String> {
    // URLs look like
    //   https://api.polygon.io/v1/reference/sec/filings/0001564590-23-003413
    url.rsplit('/').next().map(str::to_string)
}

fn parse_acceptance_ns(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s).ok().and_then(|dt| {
        dt.with_timezone(&Utc).timestamp_nanos_opt()
    })
}

fn pad_cik(cik: &str) -> String {
    // EDGAR expects CIK0000123 (10-digit zero-padded). Massive stores
    // already-padded values like "0000320193"; pad just in case.
    let trimmed = cik.trim_start_matches('0');
    format!("{:0>10}", trimmed)
}

/// One CIK's worth of submissions: maps accession → (acceptance, form).
type SubmissionsMap = BTreeMap<String, (i64, Option<String>)>;

async fn fetch_submissions_for_cik(
    http: &reqwest::Client,
    user_agent: &str,
    cik: &str,
) -> Result<SubmissionsMap> {
    let padded = pad_cik(cik);
    let url = format!("https://data.sec.gov/submissions/CIK{padded}.json");
    // Retry-on-429 loop. SEC returns 429 only when we exceed 10 req/s
    // — back off generously and retry up to 3 times before giving up.
    let mut backoff_ms = 1_000u64;
    let body: SubmissionsResponse = loop {
        let resp = http
            .get(&url)
            .header("User-Agent", user_agent)
            .header("Accept", "application/json")
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if status.is_success() {
            break resp.json().await.context("parse submissions json")?;
        }
        if status.as_u16() == 429 && backoff_ms <= 16_000 {
            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            backoff_ms *= 2;
            continue;
        }
        bail!("SEC non-success {status} for CIK {padded}");
    };

    let mut out: SubmissionsMap = BTreeMap::new();
    let filings = match body.filings {
        Some(f) => f,
        None => return Ok(out),
    };

    if let Some(recent) = filings.recent {
        for ((acc, ts), form) in recent
            .accession_number
            .iter()
            .zip(recent.acceptance_datetime.iter())
            .zip(recent.form.iter())
        {
            if let Some(ns) = parse_acceptance_ns(ts) {
                out.insert(acc.clone(), (ns, Some(form.clone())));
            }
        }
    }

    // Older files — referenced as supplementary JSONs we need to fetch.
    for older_ref in filings.files {
        let url = format!("https://data.sec.gov/submissions/{}", older_ref.name);
        let resp = http
            .get(&url)
            .header("User-Agent", user_agent)
            .header("Accept", "application/json")
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        if !resp.status().is_success() {
            continue;
        }
        let older: OlderFile = match resp.json().await {
            Ok(o) => o,
            Err(_) => continue,
        };
        for ((acc, ts), form) in older
            .accession_number
            .iter()
            .zip(older.acceptance_datetime.iter())
            .zip(older.form.iter())
        {
            if let Some(ns) = parse_acceptance_ns(ts) {
                out.insert(acc.clone(), (ns, Some(form.clone())));
            }
        }
    }

    Ok(out)
}

fn read_missing_targets(financials_path: &std::path::Path) -> Result<BTreeMap<String, BTreeSet<String>>> {
    // (cik → set of accession_numbers we need).
    let conn = Connection::open_in_memory()?;
    let path_str = financials_path.to_string_lossy();
    let sql = format!(
        "SELECT cik, source_filing_url
         FROM read_parquet('{path_str}')
         WHERE acceptance_datetime IS NULL
           AND cik IS NOT NULL
           AND source_filing_url IS NOT NULL"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
        ))
    })?;
    let mut by_cik: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut parsed = 0usize;
    let mut bad_url = 0usize;
    for r in rows {
        let (cik, url) = r?;
        match accession_from_url(&url) {
            Some(acc) => {
                by_cik.entry(cik).or_default().insert(acc);
                parsed += 1;
            }
            None => bad_url += 1,
        }
    }
    println!(
        "  needed accessions: {parsed} across {} CIKs ({bad_url} bad URLs)",
        by_cik.len()
    );
    Ok(by_cik)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let user_agent = std::env::var("SEC_USER_AGENT").context(
        "SEC_USER_AGENT not set (e.g. \"sb-v0 your.email@example.com\") — SEC requires this",
    )?;
    let financials_path: PathBuf = std::env::var("FINANCIALS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/reference/financials.parquet"));
    let out_path: PathBuf = std::env::var("OUT_PATH").map(PathBuf::from).unwrap_or_else(|_| {
        PathBuf::from("data/reference/acceptance_datetime_backfill.parquet")
    });
    let concurrency: usize = std::env::var("CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let smoke = std::env::var("SMOKE").ok().is_some();

    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if !financials_path.exists() {
        bail!("{} does not exist — run build-financials first", financials_path.display());
    }

    println!("scanning {} for missing-acceptance rows ...", financials_path.display());
    let mut needed = read_missing_targets(&financials_path)?;
    if smoke {
        let keep: Vec<String> = needed.keys().take(20).cloned().collect();
        needed.retain(|k, _| keep.contains(k));
        println!("  SMOKE: limited to {} CIKs", needed.len());
    }
    let cik_count = needed.len();
    if cik_count == 0 {
        println!("  nothing missing — no backfill needed");
        return Ok(());
    }

    let http = reqwest::Client::builder().gzip(true).build()?;

    // Startup probe — fail fast if SEC is already throttling us, so we
    // don't waste minutes hammering an IP-banned endpoint.
    let probe = http
        .get("https://data.sec.gov/submissions/CIK0000320193.json")
        .header("User-Agent", &user_agent)
        .send()
        .await
        .context("startup SEC probe")?;
    if !probe.status().is_success() {
        bail!(
            "startup SEC probe returned {}; wait a few minutes and re-run",
            probe.status()
        );
    }
    println!("  startup SEC probe ok");

    let sem = Arc::new(Semaphore::new(concurrency));
    println!(
        "fetching SEC submissions (n={cik_count}, concurrency={concurrency}) ...",
    );
    let started = std::time::Instant::now();
    let mut joinset: tokio::task::JoinSet<(String, Result<SubmissionsMap>)> =
        tokio::task::JoinSet::new();
    for (cik, _accessions) in needed.iter() {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let http = http.clone();
        let user_agent = user_agent.clone();
        let cik = cik.clone();
        joinset.spawn(async move {
            let _permit = permit;
            // Pacing budget: concurrency × (1/(sleep+RTT)) must stay
            // well under SEC's 10 req/s. At concurrency=1, sleep=150ms,
            // RTT≈150ms, we run at ~3.3 req/s — generous safety margin
            // that the previous SEC-ban incident showed is worth taking.
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            let r = fetch_submissions_for_cik(&http, &user_agent, &cik).await;
            (cik, r)
        });
    }

    let mut cik_maps: BTreeMap<String, SubmissionsMap> = BTreeMap::new();
    let mut errs = 0usize;
    let mut done = 0usize;
    let mut last_progress = 0usize;
    while let Some(joined) = joinset.join_next().await {
        let (cik, res) = joined?;
        match res {
            Ok(m) => {
                cik_maps.insert(cik, m);
            }
            Err(e) => {
                errs += 1;
                tracing::warn!(error = %e, "SEC fetch failed");
            }
        }
        done += 1;
        if done - last_progress >= 200 || done == cik_count {
            println!(
                "  progress: {done}/{cik_count} ({:.0}s elapsed; errs={errs})",
                started.elapsed().as_secs_f64()
            );
            last_progress = done;
        }
    }
    println!(
        "done in {:.1}s; {} CIKs fetched, {errs} errors",
        started.elapsed().as_secs_f64(),
        cik_maps.len()
    );

    // ---- Assemble backfill rows ----
    let mut out_rows: Vec<AcceptanceBackfillRow> = Vec::new();
    let mut found = 0usize;
    let mut missing = 0usize;
    for (cik, needed_accs) in &needed {
        let map = match cik_maps.get(cik) {
            Some(m) => m,
            None => {
                missing += needed_accs.len();
                continue;
            }
        };
        for acc in needed_accs {
            match map.get(acc) {
                Some((ns, form)) => {
                    out_rows.push(AcceptanceBackfillRow {
                        cik: cik.clone(),
                        accession_number: acc.clone(),
                        acceptance_datetime_ns: *ns,
                        form: form.clone(),
                        source: "sec_edgar_submissions".into(),
                    });
                    found += 1;
                }
                None => missing += 1,
            }
        }
    }

    let target_count: usize = needed.values().map(|s| s.len()).sum();
    println!(
        "backfill coverage: matched {} / {} target accessions ({:.1}%); {} unmatched",
        found,
        target_count,
        found as f64 / target_count as f64 * 100.0,
        missing
    );

    // Spot check: AAPL 0001564590-23-003413 should have a sensible
    // 2023 acceptance timestamp.
    let known = "0001564590-23-003413";
    if let Some(r) = out_rows.iter().find(|r| r.accession_number == known) {
        let dt = DateTime::<Utc>::from_timestamp_nanos(r.acceptance_datetime_ns);
        println!("  ✓ sample {known} acceptance_datetime = {}", dt.format("%Y-%m-%d %H:%M:%S UTC"));
    }

    if smoke {
        println!("SMOKE OK — not writing OUT_PATH");
        return Ok(());
    }

    let snapshot_date = Utc::now().date_naive();
    let tmp_path = out_path.with_extension("parquet.tmp");
    println!(
        "writing {} (acceptance_backfill_snapshot_date={snapshot_date}) ...",
        out_path.display()
    );
    write_acceptance_backfill(&tmp_path, &out_rows, snapshot_date)?;
    std::fs::rename(&tmp_path, &out_path)?;
    let bytes = std::fs::metadata(&out_path)?.len();
    println!(
        "  wrote {} ({} rows, {:.1} MB)",
        out_path.display(),
        out_rows.len(),
        bytes as f64 / 1e6
    );

    // Silence unused-vars warning in the simple path.
    let _ = NaiveDate::from_ymd_opt;

    Ok(())
}
