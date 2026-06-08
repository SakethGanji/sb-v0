//! Pull VIX daily close from FRED's `VIXCLS` series and write
//! `data/reference/vix_daily.parquet` with `vix_snapshot_date` stamped
//! in file-level Parquet metadata.
//!
//! Used because Massive index aggregates are not entitled on Stocks
//! Advanced (probe returned 403 for `I:VIX`). FRED's free tier provides
//! daily close only — `vix_open` in the RFC §8 schema stays NULL until
//! we have an open-price source.
//!
//! Required env:
//!   FRED_API_KEY
//!
//! Optional env with defaults:
//!   FRED_BASE=https://api.stlouisfed.org
//!   START_DATE=2016-01-01
//!   END_DATE=<today>
//!   OUT_PATH=data/reference/vix_daily.parquet
//!   SMOKE=1  → smoke window 2024-01-01 to 2024-01-31, no full pull

use anyhow::{Context, Result, bail};
use chrono::{NaiveDate, Utc};
use momentum_store::vix::{VixRow, write_vix};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
struct FredResponse {
    observations: Vec<FredObservation>,
}

#[derive(Debug, Deserialize)]
struct FredObservation {
    date: String,
    value: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let api_key = std::env::var("FRED_API_KEY")
        .context("FRED_API_KEY not set — source .env or export it")?;
    let base = std::env::var("FRED_BASE")
        .unwrap_or_else(|_| "https://api.stlouisfed.org".to_string());
    let smoke = std::env::var("SMOKE").ok().is_some();
    let (start, end) = if smoke {
        ("2024-01-01".to_string(), "2024-01-31".to_string())
    } else {
        let start = std::env::var("START_DATE").unwrap_or_else(|_| "2016-01-01".to_string());
        let end = std::env::var("END_DATE")
            .unwrap_or_else(|_| Utc::now().date_naive().to_string());
        (start, end)
    };
    let out_path: PathBuf = std::env::var("OUT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/reference/vix_daily.parquet"));

    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    println!(
        "fetching FRED VIXCLS {start} → {end} (smoke={smoke}) ...",
    );
    let client = reqwest::Client::builder().gzip(true).build()?;
    let url = format!("{base}/fred/series/observations");
    let resp = client
        .get(&url)
        .query(&[
            ("series_id", "VIXCLS"),
            ("api_key", &api_key),
            ("file_type", "json"),
            ("observation_start", &start),
            ("observation_end", &end),
        ])
        .send()
        .await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("FRED non-success {status}: {body}");
    }
    let parsed: FredResponse = resp.json().await?;
    println!("  observations returned: {}", parsed.observations.len());

    let mut rows: Vec<VixRow> = Vec::with_capacity(parsed.observations.len());
    let mut missing = 0usize;
    let mut bad_date = 0usize;
    for obs in &parsed.observations {
        if obs.value == "." {
            // FRED sentinel for missing observation (typically holidays).
            missing += 1;
            continue;
        }
        let date = match NaiveDate::parse_from_str(&obs.date, "%Y-%m-%d") {
            Ok(d) => d,
            Err(_) => {
                bad_date += 1;
                continue;
            }
        };
        let vix_close: f64 = match obs.value.parse() {
            Ok(v) => v,
            Err(_) => {
                bad_date += 1;
                continue;
            }
        };
        rows.push(VixRow { date, vix_close });
    }
    println!("  kept {} rows; missing-sentinel {} ; bad-parse {}", rows.len(), missing, bad_date);

    if rows.is_empty() {
        bail!("no usable VIX observations in window");
    }

    // -------- Validation block (always runs; loud failures) --------
    let first = rows.first().unwrap();
    let last = rows.last().unwrap();
    println!(
        "  range: {} ({:.2}) → {} ({:.2})",
        first.date, first.vix_close, last.date, last.vix_close
    );

    if smoke {
        // Expected ~21 trading days in Jan 2024; VIX 2024-01-02 ≈ 13.20.
        let n = rows.len();
        if !(18..=23).contains(&n) {
            bail!("SMOKE: expected ~21 rows for Jan 2024, got {n}");
        }
        let jan2 = rows
            .iter()
            .find(|r| r.date == NaiveDate::from_ymd_opt(2024, 1, 2).unwrap())
            .context("SMOKE: missing 2024-01-02")?;
        if (jan2.vix_close - 13.20).abs() > 0.5 {
            bail!(
                "SMOKE: 2024-01-02 VIX expected ~13.20, got {:.2}",
                jan2.vix_close
            );
        }
        println!("  SMOKE checks passed: row count + 2024-01-02 spot value");
    } else {
        // Full-pull validation: COVID peak + GME-squeeze spike + Jan-2024.
        let checks = [
            (NaiveDate::from_ymd_opt(2020, 3, 16).unwrap(), 82.69, 2.0),
            (NaiveDate::from_ymd_opt(2021, 1, 27).unwrap(), 37.21, 1.0),
            (NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), 13.20, 0.5),
        ];
        for (date, expected, tol) in checks {
            let row = rows
                .iter()
                .find(|r| r.date == date)
                .with_context(|| format!("VALIDATE: missing date {date}"))?;
            let diff = (row.vix_close - expected).abs();
            if diff > tol {
                bail!(
                    "VALIDATE: {date} VIX expected ~{expected:.2} ±{tol}, got {:.2}",
                    row.vix_close
                );
            }
            println!("  ✓ {date} = {:.2} (expected ~{expected:.2})", row.vix_close);
        }
        // Monotonic-date check.
        for w in rows.windows(2) {
            if w[1].date <= w[0].date {
                bail!("VALIDATE: dates not strictly monotonic at {}", w[1].date);
            }
        }
        println!("  ✓ dates strictly monotonic across {} rows", rows.len());
    }

    let snapshot_date = Utc::now().date_naive();
    let tmp_path = out_path.with_extension("parquet.tmp");
    println!("writing {} (vix_snapshot_date={snapshot_date}) ...", out_path.display());
    write_vix(&tmp_path, &rows, snapshot_date)?;
    std::fs::rename(&tmp_path, &out_path)?;
    println!("  wrote {}", out_path.display());

    Ok(())
}
