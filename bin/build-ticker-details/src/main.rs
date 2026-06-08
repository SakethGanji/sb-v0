//! Pull per-ticker classification details from
//! `/v3/reference/tickers/{id}` for every row in `tickers.parquet` and
//! write `data/reference/tickers_classified.parquet` with
//! `ticker_details_snapshot_date` stamped in file-level Parquet
//! metadata.
//!
//! Adds SIC industry codes, list date, description text, and current
//! snapshot of market cap + shares outstanding. These are the inputs
//! the §11.6.3 behavioral-tag rules need (is_biotech, is_semiconductor,
//! is_recent_ipo, leveraged-ETF detection via description text).
//!
//! Required env:
//!   MASSIVE_API_KEY
//!
//! Optional env with defaults:
//!   MASSIVE_REST_BASE=https://api.polygon.io
//!   TICKERS_PATH=data/reference/tickers.parquet
//!   OUT_PATH=data/reference/tickers_classified.parquet
//!   CONCURRENCY=20
//!   SMOKE=1  → fetch a hand-picked set (~10 known tickers), validate
//!              specific spot values, do NOT overwrite OUT_PATH.

use anyhow::{Context, Result, bail};
use arrow::array::{Array, AsArray};
use chrono::{NaiveDate, Utc};
use momentum_api::rest::{RestClient, RestError, TickerDetails};
use momentum_store::tickers_classified::{
    TickerClassifiedRow, parse_utc_ns, write_tickers_classified,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;

const SMOKE_SET: &[&str] = &[
    "AAPL", "NVDA", "MSFT", "GOOG", // mega-cap tech
    "BIIB", "REGN",            // biotech
    "SOXL", "TQQQ",                  // leveraged ETFs
    "BABA",                          // China ADR
    "GME",                           // meme
    "ABNB", "RIVN",                  // recent IPOs (2020, 2021)
    "JPM",                           // bank
    "XOM",                           // energy
    "SPY",                           // index ETF (locale=us, ETF type)
];

#[derive(Debug, Default)]
struct PullStats {
    requested: usize,
    ok: usize,
    err_404: usize,
    err_429: usize,
    err_5xx: usize,
    err_other: usize,
}

fn load_universe(path: &std::path::Path) -> Result<Vec<TickerClassifiedRow>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("open {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut out: Vec<TickerClassifiedRow> = Vec::new();
    for batch_res in reader {
        let batch = batch_res?;
        let n = batch.num_rows();
        let display = batch
            .column_by_name("display_symbol")
            .context("display_symbol")?
            .as_string::<i32>();
        let security_id = batch
            .column_by_name("security_id")
            .map(|c| c.as_string::<i32>());
        let composite_figi = batch
            .column_by_name("composite_figi")
            .map(|c| c.as_string::<i32>());
        let share_class_figi = batch
            .column_by_name("share_class_figi")
            .map(|c| c.as_string::<i32>());
        for i in 0..n {
            out.push(TickerClassifiedRow {
                display_symbol: display.value(i).to_string(),
                security_id: security_id
                    .as_ref()
                    .filter(|c| !c.is_null(i))
                    .map(|c| c.value(i).to_string()),
                composite_figi: composite_figi
                    .as_ref()
                    .filter(|c| !c.is_null(i))
                    .map(|c| c.value(i).to_string()),
                share_class_figi: share_class_figi
                    .as_ref()
                    .filter(|c| !c.is_null(i))
                    .map(|c| c.value(i).to_string()),
                ..Default::default()
            });
        }
    }
    Ok(out)
}

fn is_retryable(e: &RestError) -> bool {
    match e {
        RestError::Status { status, .. } => *status == 429 || *status >= 500,
        RestError::Http(_) => true,
        RestError::PaginationOverflow { .. } => false,
    }
}

fn merge_details(row: &mut TickerClassifiedRow, d: TickerDetails) {
    if row.name.is_none() {
        row.name = d.name;
    }
    if row.market.is_none() {
        row.market = d.market;
    }
    if row.locale.is_none() {
        row.locale = d.locale;
    }
    if row.primary_exchange.is_none() {
        row.primary_exchange = d.primary_exchange;
    }
    if row.ticker_type.is_none() {
        row.ticker_type = d.ticker_type;
    }
    row.active = d.active.or(row.active);
    if row.currency_name.is_none() {
        row.currency_name = d.currency_name;
    }
    if row.cik.is_none() {
        row.cik = d.cik;
    }
    if row.composite_figi.is_none() {
        row.composite_figi = d.composite_figi.clone();
    }
    if row.security_id.is_none() {
        row.security_id = d.composite_figi.or(d.share_class_figi);
    }
    if let Some(s) = d.delisted_utc.as_deref() {
        row.delisted_utc_ns = parse_utc_ns(s);
    }
    row.market_cap_snapshot = d.market_cap;
    row.sic_code = d.sic_code;
    row.sic_description = d.sic_description;
    row.ticker_root = d.ticker_root;
    row.total_employees = d.total_employees;
    if let Some(s) = d.list_date.as_deref() {
        row.list_date = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok();
    }
    row.share_class_shares_outstanding_snapshot = d.share_class_shares_outstanding;
    row.weighted_shares_outstanding_snapshot = d.weighted_shares_outstanding;
    row.round_lot = d.round_lot;
    row.description = d.description;
    if let Some(addr) = d.address {
        row.address_state = addr.state;
        row.address_city = addr.city;
    }
    row.homepage_url = d.homepage_url;
}

async fn fetch_one(
    client: &RestClient,
    ticker: &str,
    max_attempts: u32,
) -> Result<TickerDetails, RestError> {
    let mut delay_ms: u64 = 200;
    let max_delay_ms: u64 = 30_000;
    for attempt in 1..=max_attempts {
        match client.ticker_details(ticker).await {
            Ok(d) => return Ok(d),
            Err(e) => {
                if attempt == max_attempts || !is_retryable(&e) {
                    return Err(e);
                }
                let jitter = fastrand::u64(0..=delay_ms / 2);
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms + jitter)).await;
                delay_ms = (delay_ms * 2).min(max_delay_ms);
            }
        }
    }
    unreachable!()
}

async fn run_pull(
    client: Arc<RestClient>,
    mut rows: Vec<TickerClassifiedRow>,
    concurrency: usize,
) -> Result<(Vec<TickerClassifiedRow>, PullStats)> {
    let sem = Arc::new(Semaphore::new(concurrency));
    let mut joinset: tokio::task::JoinSet<(usize, Result<TickerDetails, RestError>)> =
        tokio::task::JoinSet::new();
    for (i, row) in rows.iter().enumerate() {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let client = client.clone();
        let ticker = row.display_symbol.clone();
        joinset.spawn(async move {
            let _permit = permit;
            let r = fetch_one(&client, &ticker, 5).await;
            (i, r)
        });
    }
    let mut stats = PullStats {
        requested: rows.len(),
        ..Default::default()
    };
    let mut progress_at: usize = 0;
    while let Some(joined) = joinset.join_next().await {
        let (i, res) = joined?;
        match res {
            Ok(d) => {
                merge_details(&mut rows[i], d);
                stats.ok += 1;
            }
            Err(RestError::Status { status, .. }) => {
                match status {
                    404 => stats.err_404 += 1,
                    429 => stats.err_429 += 1,
                    s if s >= 500 => stats.err_5xx += 1,
                    _ => stats.err_other += 1,
                }
            }
            Err(_) => stats.err_other += 1,
        }
        let done = stats.ok
            + stats.err_404
            + stats.err_429
            + stats.err_5xx
            + stats.err_other;
        if done - progress_at >= 500 || done == stats.requested {
            println!(
                "  progress: {done}/{} (ok={} 404={} 429={} 5xx={} other={})",
                stats.requested, stats.ok, stats.err_404, stats.err_429, stats.err_5xx, stats.err_other
            );
            progress_at = done;
        }
    }
    Ok((rows, stats))
}

fn smoke_validate(rows: &[TickerClassifiedRow]) -> Result<()> {
    use std::collections::HashMap;
    let by_sym: HashMap<&str, &TickerClassifiedRow> =
        rows.iter().map(|r| (r.display_symbol.as_str(), r)).collect();

    let mut fail: Vec<String> = Vec::new();
    let record = |sym: &str, pred: bool, msg: &str, fail: &mut Vec<String>| {
        if !pred {
            fail.push(format!("  ✗ {sym}: {msg}"));
        } else {
            println!("  ✓ {sym}: {msg}");
        }
    };

    if let Some(r) = by_sym.get("AAPL") {
        record("AAPL", r.sic_code.as_deref() == Some("3571"), "sic_code == 3571", &mut fail);
        record(
            "AAPL",
            r.list_date == NaiveDate::from_ymd_opt(1980, 12, 12),
            "list_date == 1980-12-12",
            &mut fail,
        );
        record(
            "AAPL",
            r.market_cap_snapshot.map(|v| v > 1e12).unwrap_or(false),
            "market_cap > $1T",
            &mut fail,
        );
    } else {
        fail.push("  ✗ AAPL: not in result set".into());
    }

    if let Some(r) = by_sym.get("ABNB") {
        record(
            "ABNB",
            r.list_date == NaiveDate::from_ymd_opt(2020, 12, 10),
            "list_date == 2020-12-10",
            &mut fail,
        );
    }

    if let Some(r) = by_sym.get("SOXL") {
        // For ETFs the `description` field is usually empty; the leverage
        // hint lives in `name` ("Direxion Daily Semiconductor Bull 3X ETF").
        let blob = format!(
            "{} {}",
            r.name.as_deref().unwrap_or(""),
            r.description.as_deref().unwrap_or("")
        )
        .to_lowercase();
        record(
            "SOXL",
            blob.contains("3x")
                || blob.contains("leverag")
                || blob.contains("bull")
                || blob.contains("bear"),
            "name/description hints leveraged",
            &mut fail,
        );
    }

    if let Some(r) = by_sym.get("BABA") {
        record(
            "BABA",
            r.ticker_type.as_deref() == Some("ADRC")
                || r.ticker_type.as_deref() == Some("ADR")
                || r.sic_description
                    .as_deref()
                    .map(|s| s.contains("CHINA") || s.contains("INTERNATIONAL"))
                    .unwrap_or(false),
            "ADR-like or international SIC",
            &mut fail,
        );
    }

    if let Some(r) = by_sym.get("SPY") {
        record(
            "SPY",
            r.ticker_type.as_deref() == Some("ETF"),
            "ticker_type == ETF",
            &mut fail,
        );
    }

    let with_sic = rows.iter().filter(|r| r.sic_code.is_some()).count();
    let with_list_date = rows.iter().filter(|r| r.list_date.is_some()).count();
    println!(
        "  smoke coverage: sic_code {}/{}, list_date {}/{}",
        with_sic,
        rows.len(),
        with_list_date,
        rows.len()
    );

    if !fail.is_empty() {
        for line in &fail {
            println!("{line}");
        }
        bail!("SMOKE validation failed ({} checks)", fail.len());
    }
    Ok(())
}

fn full_validate(rows: &[TickerClassifiedRow], stats: &PullStats) -> Result<()> {
    let total = rows.len();
    // Active universe = the rows we actually got details for (success).
    // Delisted tickers commonly 404 on /v3/reference/tickers/{id} because
    // the endpoint only serves the current snapshot; classification for
    // delisted names is sparse by design.
    let with_sic = rows.iter().filter(|r| r.sic_code.is_some()).count();
    let with_list_date = rows.iter().filter(|r| r.list_date.is_some()).count();
    let with_mcap = rows.iter().filter(|r| r.market_cap_snapshot.is_some()).count();
    let pct_sic_total = with_sic as f64 / total as f64 * 100.0;
    let pct_list_total = with_list_date as f64 / total as f64 * 100.0;
    let pct_mcap_total = with_mcap as f64 / total as f64 * 100.0;
    println!(
        "  coverage (of all {total}): sic_code {with_sic} ({pct_sic_total:.1}%) | list_date {with_list_date} ({pct_list_total:.1}%) | market_cap {with_mcap} ({pct_mcap_total:.1}%)"
    );
    if stats.ok > 0 {
        let pct_sic_ok = with_sic as f64 / stats.ok as f64 * 100.0;
        let pct_list_ok = with_list_date as f64 / stats.ok as f64 * 100.0;
        let pct_mcap_ok = with_mcap as f64 / stats.ok as f64 * 100.0;
        println!(
            "  coverage (of {} active): sic_code {pct_sic_ok:.1}% | list_date {pct_list_ok:.1}% | market_cap {pct_mcap_ok:.1}%",
            stats.ok
        );
        if pct_list_ok < 80.0 {
            bail!(
                "VALIDATE: list_date coverage among successful pulls is {pct_list_ok:.1}%, below 80% threshold"
            );
        }
        if pct_sic_ok < 60.0 {
            bail!(
                "VALIDATE: sic_code coverage among successful pulls is {pct_sic_ok:.1}%, below 60% threshold (many ETFs/ETNs have no SIC; threshold reflects expected mix)"
            );
        }
    }
    // The 404 rate is informational, not fatal — delisted-heavy universes
    // can easily exceed 50% 404s and that's fine.
    let pct_404 = stats.err_404 as f64 / stats.requested as f64 * 100.0;
    println!(
        "  pull mix: ok {} ({:.1}%) | 404 {} ({pct_404:.1}%) | other-error {}",
        stats.ok,
        stats.ok as f64 / stats.requested as f64 * 100.0,
        stats.err_404,
        stats.err_429 + stats.err_5xx + stats.err_other,
    );
    if stats.err_429 > stats.requested / 100 {
        bail!(
            "VALIDATE: 429 rate {} > 1% — rate-limit hit, lower CONCURRENCY",
            stats.err_429
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let api_key = std::env::var("MASSIVE_API_KEY")
        .context("MASSIVE_API_KEY not set — source .env or export it")?;
    let base = std::env::var("MASSIVE_REST_BASE")
        .unwrap_or_else(|_| "https://api.polygon.io".to_string());
    let tickers_path: PathBuf = std::env::var("TICKERS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/reference/tickers.parquet"));
    let out_path: PathBuf = std::env::var("OUT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/reference/tickers_classified.parquet"));
    let concurrency: usize = std::env::var("CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let smoke = std::env::var("SMOKE").ok().is_some();

    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let client = Arc::new(RestClient::new(base, api_key));

    let rows = if smoke {
        SMOKE_SET
            .iter()
            .map(|t| TickerClassifiedRow {
                display_symbol: (*t).to_string(),
                ..Default::default()
            })
            .collect()
    } else {
        if !tickers_path.exists() {
            bail!("{} does not exist — run build-universe first", tickers_path.display());
        }
        println!("loading universe from {} ...", tickers_path.display());
        let r = load_universe(&tickers_path)?;
        println!("  loaded {} rows", r.len());
        r
    };

    println!(
        "fetching ticker details (n={}, concurrency={concurrency}, smoke={smoke}) ...",
        rows.len()
    );
    let started = std::time::Instant::now();
    let (rows, stats) = run_pull(client, rows, concurrency).await?;
    println!(
        "done in {:.1}s; ok={} 404={} 429={} 5xx={} other={}",
        started.elapsed().as_secs_f64(),
        stats.ok,
        stats.err_404,
        stats.err_429,
        stats.err_5xx,
        stats.err_other
    );

    if smoke {
        println!("== smoke validation ==");
        smoke_validate(&rows)?;
        println!("SMOKE OK — not writing OUT_PATH (set SMOKE='' to do a full pull)");
        return Ok(());
    }

    println!("== full-pull validation ==");
    full_validate(&rows, &stats)?;

    let snapshot_date = Utc::now().date_naive();
    let tmp_path = out_path.with_extension("parquet.tmp");
    println!(
        "writing {} (ticker_details_snapshot_date={snapshot_date}) ...",
        out_path.display()
    );
    write_tickers_classified(&tmp_path, &rows, snapshot_date)?;
    std::fs::rename(&tmp_path, &out_path)?;
    println!("  wrote {} ({} rows)", out_path.display(), rows.len());

    Ok(())
}
