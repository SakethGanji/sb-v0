//! Bulk-download historical minute-aggregate flat files.
//!
//! Driver for `momentum_store::bulk_download::run_bulk_download`. Iterates
//! weekdays in `[START_DATE, END_DATE]` and downloads every Massive S3
//! flat file into `STAGING_DIR`. Resume cursor at `CURSOR_DB` (SQLite)
//! makes restarts O(1).
//!
//! REQUIRED args:
//!   START_DATE=YYYY-MM-DD
//!   END_DATE=YYYY-MM-DD
//!
//! Required env (or .env): MASSIVE_S3_KEY_ID, MASSIVE_S3_SECRET.
//!
//! Optional env with defaults:
//!   MASSIVE_S3_ENDPOINT=https://files.massive.com
//!   MASSIVE_S3_BUCKET=flatfiles
//!   STAGING_DIR=data/_staging/flat_files
//!   CURSOR_DB=data/_ingest_state.sqlite
//!   S3_CONCURRENCY=16
//!   MAX_ATTEMPTS=5
//!
//! Required start/end date envs forces the operator to think about scope.
//! A bare `bulk-download` does nothing — no accidental 85 GB pulls.

use anyhow::{Context, Result};
use chrono::NaiveDate;
use momentum_api::s3::FlatFileClient;
use momentum_store::bulk_download::{
    BulkConfig, DownloadCursor, run_bulk_download, weekday_range,
};
use std::path::PathBuf;
use std::sync::Arc;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn env_date(name: &str) -> Result<NaiveDate> {
    let s = std::env::var(name).with_context(|| format!("{name} not set"))?;
    NaiveDate::parse_from_str(&s, "%Y-%m-%d")
        .with_context(|| format!("{name}={s:?} not parseable as YYYY-MM-DD"))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let start = env_date("START_DATE")?;
    let end = env_date("END_DATE")?;

    let endpoint = std::env::var("MASSIVE_S3_ENDPOINT")
        .unwrap_or_else(|_| "https://files.massive.com".to_string());
    let bucket =
        std::env::var("MASSIVE_S3_BUCKET").unwrap_or_else(|_| "flatfiles".to_string());
    let key_id =
        std::env::var("MASSIVE_S3_KEY_ID").context("MASSIVE_S3_KEY_ID not set")?;
    let secret =
        std::env::var("MASSIVE_S3_SECRET").context("MASSIVE_S3_SECRET not set")?;

    let staging_dir: PathBuf = std::env::var("STAGING_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/_staging/flat_files"));
    let cursor_db: PathBuf = std::env::var("CURSOR_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/_ingest_state.sqlite"));

    let cfg = BulkConfig {
        concurrency: env_usize("S3_CONCURRENCY", 16),
        max_attempts: env_u32("MAX_ATTEMPTS", 5),
        ..Default::default()
    };

    let dates = weekday_range(start, end)?;
    println!("range        : {start} → {end}");
    println!("weekdays     : {} (holidays included; resolved as empty-success)", dates.len());
    println!("staging dir  : {}", staging_dir.display());
    println!("cursor db    : {}", cursor_db.display());
    println!("concurrency  : {}", cfg.concurrency);
    println!("max attempts : {}", cfg.max_attempts);

    let client = Arc::new(FlatFileClient::new(endpoint, bucket, key_id, secret));
    let cursor = Arc::new(DownloadCursor::open(&cursor_db)?);

    let pre = cursor.summary()?;
    println!(
        "cursor before: pending={} done={} failed={}",
        pre.pending, pre.done, pre.failed,
    );

    let stats = run_bulk_download(client, Arc::clone(&cursor), &staging_dir, dates, cfg).await?;
    let post = cursor.summary()?;
    println!("--- run complete ---");
    println!(
        "attempted    : {} (done_with_bytes={}, done_empty={}, failed={})",
        stats.attempted, stats.done_with_bytes, stats.done_empty, stats.failed,
    );
    println!(
        "bytes downloaded this run: {} ({:.2} MiB)",
        stats.total_bytes,
        stats.total_bytes as f64 / (1024.0 * 1024.0),
    );
    println!(
        "cursor after : pending={} done={} failed={}",
        post.pending, post.done, post.failed,
    );

    if post.failed > 0 {
        eprintln!(
            "WARNING: {} days remained in 'failed' state. Re-run with the same env to retry them.",
            post.failed
        );
    }

    Ok(())
}
