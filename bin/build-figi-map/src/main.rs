//! Build `figi_map.parquet` from `ticker_events.parquet` + the current
//! universe in `tickers.parquet`. Pure derivation — no API calls.
//!
//! One row per (security_id, display_symbol, validity-window):
//!   - `valid_from = NULL` → open-start (no prior rename observed)
//!   - `valid_to   = NULL` → currently in effect
//!
//! Read path: `MaterializedBarReader` uses this to resolve `(sid, day) →
//! display_symbol` so a trade entered pre-rename (e.g. FB) continues
//! reading the post-rename file (META) without losing bars after
//! 2022-06-09.
//!
//! Optional env (with defaults):
//!   TICKER_EVENTS_PATH=data/_smoke/reference/ticker_events.parquet
//!   TICKERS_PATH=data/_smoke/reference/tickers_enriched.parquet
//!   OUT_PATH=data/_smoke/reference/figi_map.parquet

use anyhow::{Context, Result};
use chrono::Utc;
use momentum_store::figi_map::{build_figi_map, write_figi_map};
use std::path::PathBuf;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let events_path: PathBuf = std::env::var("TICKER_EVENTS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/_smoke/reference/ticker_events.parquet"));
    let tickers_path: PathBuf = std::env::var("TICKERS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/_smoke/reference/tickers_enriched.parquet"));
    let out_path: PathBuf = std::env::var("OUT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/_smoke/reference/figi_map.parquet"));

    if !events_path.exists() {
        anyhow::bail!(
            "{} does not exist — run build-ticker-events first",
            events_path.display()
        );
    }
    if !tickers_path.exists() {
        anyhow::bail!(
            "{} does not exist — run build-universe first",
            tickers_path.display()
        );
    }
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    println!(
        "reading {} + {} ...",
        events_path.display(),
        tickers_path.display()
    );
    let rows = build_figi_map(&events_path, &tickers_path)
        .with_context(|| format!("build_figi_map({})", events_path.display()))?;

    // Cheap shape report so the operator can see what landed before the
    // next stage consumes it.
    let total = rows.len();
    let with_renames = rows.iter().filter(|r| r.valid_from.is_some()).count();
    let current_only = rows
        .iter()
        .filter(|r| r.valid_from.is_none() && r.valid_to.is_none())
        .count();
    let distinct_sids = rows
        .iter()
        .map(|r| r.security_id.as_str())
        .collect::<std::collections::HashSet<_>>()
        .len();
    let renamed_sids = {
        let mut counts: std::collections::HashMap<&str, usize> =
            std::collections::HashMap::new();
        for r in &rows {
            *counts.entry(r.security_id.as_str()).or_insert(0) += 1;
        }
        counts.values().filter(|&&n| n > 1).count()
    };

    println!(
        "  built {total} rows ({distinct_sids} distinct security_ids; {renamed_sids} with rename history)"
    );
    println!("  current-only rows (no events): {current_only}");
    println!("  rename-window rows (valid_from set): {with_renames}");

    let snap = Utc::now().date_naive();
    println!(
        "writing {} (figi_map_snapshot_date={snap}) ...",
        out_path.display()
    );
    write_figi_map(&out_path, &rows, snap)
        .with_context(|| format!("write_figi_map({})", out_path.display()))?;
    println!("  wrote {}", out_path.display());

    Ok(())
}
