//! Pull rename events for every FIGI in the universe and write
//! `ticker_events.parquet`.
//!
//! Scope: iterates `tickers.parquet`, picks every row with a non-null
//! `security_id` (composite FIGI), and calls
//! `/vX/reference/tickers/{id}/events` per row. Rows without FIGI cannot
//! be queried this way; they carry no rename chain. This means renamed
//! rows missing FIGI (~21% of active per the 2026-06-07 coverage
//! measurement) are an unfixable blind spot at the data layer — the
//! engine logs fallback joins for those.
//!
//! Bounded concurrency (default 10 inflight, same starting point as
//! universe enrichment). 404 is a common and expected outcome here —
//! many FIGIs simply don't have an events record on Massive.
//!
//! Required env:  MASSIVE_API_KEY
//! Optional env:
//!   MASSIVE_REST_BASE=https://api.polygon.io
//!   TICKERS_PATH=data/reference/tickers.parquet
//!   OUT_PATH=data/reference/ticker_events.parquet
//!   EVENTS_CONCURRENCY=10
//!   EVENTS_MAX_ATTEMPTS=3

use anyhow::{Context, Result};
use arrow::array::{Array, AsArray};
use chrono::{NaiveDate, Utc};
use momentum_api::rest::{EnrichConfig, RestClient, fetch_ticker_events_concurrent};
use momentum_store::ticker_events::{TickerEventRow, write_ticker_events};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn load_universe_figis(path: &std::path::Path) -> Result<Vec<String>> {
    let file = std::fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for batch_res in reader {
        let batch = batch_res?;
        let sids = batch
            .column_by_name("security_id")
            .context("security_id column missing")?
            .as_string::<i32>();
        let actives = batch
            .column_by_name("active")
            .context("active column missing")?
            .as_boolean();
        for i in 0..batch.num_rows() {
            // Rename chain only matters for active tickers (delisted can't rename).
            if !actives.value(i) {
                continue;
            }
            if sids.is_null(i) {
                continue;
            }
            let s = sids.value(i).to_string();
            if seen.insert(s.clone()) {
                out.push(s);
            }
        }
    }
    Ok(out)
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
        .unwrap_or_else(|_| PathBuf::from("data/reference/ticker_events.parquet"));

    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if !tickers_path.exists() {
        anyhow::bail!("{} does not exist — run build-universe first", tickers_path.display());
    }

    let cfg = EnrichConfig {
        concurrency: env_usize("EVENTS_CONCURRENCY", 10),
        max_attempts: env_u32("EVENTS_MAX_ATTEMPTS", 3),
        base_backoff_ms: 200,
        max_backoff_ms: 30_000,
    };

    println!("loading active+FIGI ids from {} ...", tickers_path.display());
    let ids = load_universe_figis(&tickers_path)?;
    println!("  active+FIGI id count: {}", ids.len());

    println!("phase 1/2 — fetch /vX/reference/tickers/{{id}}/events (concurrency={}) ...", cfg.concurrency);
    let client = Arc::new(RestClient::new(base, api_key));
    let (results, stats) = fetch_ticker_events_concurrent(client, ids, cfg).await?;
    println!(
        "  queried={} ok={} (with_events={}) err={} (404={} 429={} 5xx={} other={})",
        stats.queried,
        stats.ok,
        stats.ok_with_events,
        stats.err,
        stats.err_404,
        stats.err_429,
        stats.err_5xx,
        stats.err_other,
    );

    // Flatten to one row per (security_id, event).
    let mut rows: Vec<TickerEventRow> = Vec::new();
    let mut skipped = 0;
    for r in &results {
        for e in &r.events {
            let date = match NaiveDate::parse_from_str(&e.date, "%Y-%m-%d") {
                Ok(d) => d,
                Err(_) => {
                    skipped += 1;
                    continue;
                }
            };
            rows.push(TickerEventRow {
                security_id: r.queried_id.clone(),
                name: r.name.clone(),
                event_date: date,
                event_type: e.event_type.clone(),
                new_ticker: e.new_ticker.clone(),
            });
        }
    }
    if skipped > 0 {
        println!("  skipped {skipped} rows with unparseable event date");
    }
    println!("  flattened: {} event rows", rows.len());

    println!("phase 2/2 — write {} ...", out_path.display());
    let snap = Utc::now().date_naive();
    write_ticker_events(&out_path, &rows, snap)?;
    println!("  wrote {} (snapshot={})", out_path.display(), snap);

    Ok(())
}
