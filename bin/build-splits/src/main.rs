//! Pull every split row from Massive and write `splits.parquet` with
//! `splits_snapshot_date` stamped in file-level Parquet metadata.
//!
//! The stamp is the **pin source** for the read-time split multiply
//! per README §5.3 (amended 2026-06-06). A backtest run reads it at
//! startup and refuses to apply any split row with
//! `execution_date > pin`.
//!
//! Required env:
//!   MASSIVE_API_KEY
//!
//! Optional env with defaults:
//!   MASSIVE_REST_BASE=https://api.polygon.io
//!   TICKERS_PATH=data/reference/tickers.parquet   (universe for FIGI lookup)
//!   OUT_PATH=data/reference/splits.parquet

use anyhow::{Context, Result};
use chrono::{NaiveDate, Utc};
use momentum_api::rest::{RestClient, Split};
use momentum_store::splits::{SplitRow, write_splits};
use momentum_store::tickers::read_security_id_map;
use std::path::PathBuf;

fn to_row(s: &Split) -> Result<SplitRow> {
    let execution_date = NaiveDate::parse_from_str(&s.execution_date, "%Y-%m-%d")
        .with_context(|| format!("split {} has unparseable date {:?}", s.id, s.execution_date))?;
    Ok(SplitRow {
        id: s.id.clone(),
        display_symbol: s.ticker.clone(),
        execution_date,
        split_from: s.split_from,
        split_to: s.split_to,
        adjustment_type: s.adjustment_type.clone(),
        historical_adjustment_factor: s.historical_adjustment_factor,
    })
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
        .unwrap_or_else(|_| PathBuf::from("data/reference/splits.parquet"));

    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    if !tickers_path.exists() {
        anyhow::bail!(
            "{} does not exist — run build-universe first",
            tickers_path.display()
        );
    }
    println!("loading FIGI map from {} ...", tickers_path.display());
    let figi = read_security_id_map(&tickers_path)?;
    println!("  FIGI map size: {}", figi.len());

    println!("paginate /stocks/v1/splits ...");
    let client = RestClient::new(base, api_key);
    let splits = client.list_splits().await.context("list_splits")?;
    println!("  got {} split rows", splits.len());

    let mut rows: Vec<SplitRow> = Vec::with_capacity(splits.len());
    let mut skip_bad_date = 0;
    for s in &splits {
        match to_row(s) {
            Ok(r) => rows.push(r),
            Err(e) => {
                skip_bad_date += 1;
                tracing::warn!(error = %e, "skip split row");
            }
        }
    }
    if skip_bad_date > 0 {
        println!("  skipped {skip_bad_date} rows with unparseable execution_date");
    }

    let with_figi = rows.iter().filter(|r| figi.contains_key(&r.display_symbol)).count();
    let forward = rows
        .iter()
        .filter(|r| r.adjustment_type.as_deref() == Some("forward_split"))
        .count();
    let reverse = rows
        .iter()
        .filter(|r| r.adjustment_type.as_deref() == Some("reverse_split"))
        .count();
    let stock_div = rows
        .iter()
        .filter(|r| r.adjustment_type.as_deref() == Some("stock_dividend"))
        .count();
    println!("  symbol→FIGI coverage: {with_figi}/{}", rows.len());
    println!(
        "  adjustment_type: forward_split={forward} reverse_split={reverse} stock_dividend={stock_div}"
    );

    let snapshot_date = Utc::now().date_naive();
    println!(
        "writing {} (splits_snapshot_date={snapshot_date}) ...",
        out_path.display()
    );
    write_splits(&out_path, &rows, snapshot_date, &figi)?;
    println!("  wrote {}", out_path.display());

    Ok(())
}
