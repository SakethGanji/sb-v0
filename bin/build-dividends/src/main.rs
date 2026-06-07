//! Pull every dividend row from Massive and write `dividends.parquet`.
//!
//! Dividends are stored as events joined to per-trade summaries per
//! §5.3.1 — NOT folded into bars. The engine's price line stays
//! unadjusted for ex-dividend drops, which is the intended Phase 0
//! behavior (a real stop order WOULD fire on the drop).
//!
//! Required env:  MASSIVE_API_KEY
//! Optional env:
//!   MASSIVE_REST_BASE=https://api.polygon.io
//!   TICKERS_PATH=data/reference/tickers.parquet
//!   OUT_PATH=data/reference/dividends.parquet

use anyhow::{Context, Result};
use chrono::{NaiveDate, Utc};
use momentum_api::rest::{Dividend, RestClient};
use momentum_store::dividends::{DividendRow, write_dividends};
use momentum_store::tickers::read_security_id_map;
use std::path::PathBuf;

fn parse_date_opt(s: &Option<String>) -> Option<NaiveDate> {
    s.as_deref().and_then(|raw| NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok())
}

fn to_row(d: &Dividend) -> Result<DividendRow> {
    let ex = NaiveDate::parse_from_str(&d.ex_dividend_date, "%Y-%m-%d").with_context(|| {
        format!("dividend {} has unparseable ex_dividend_date {:?}", d.id, d.ex_dividend_date)
    })?;
    Ok(DividendRow {
        id: d.id.clone(),
        display_symbol: d.ticker.clone(),
        ex_dividend_date: ex,
        pay_date: parse_date_opt(&d.pay_date),
        record_date: parse_date_opt(&d.record_date),
        declaration_date: parse_date_opt(&d.declaration_date),
        cash_amount: d.cash_amount,
        split_adjusted_cash_amount: d.split_adjusted_cash_amount,
        historical_adjustment_factor: d.historical_adjustment_factor,
        currency: d.currency.clone(),
        distribution_type: d.distribution_type.clone(),
        frequency: d.frequency,
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
        .unwrap_or_else(|_| PathBuf::from("data/reference/dividends.parquet"));

    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    if !tickers_path.exists() {
        anyhow::bail!("{} does not exist — run build-universe first", tickers_path.display());
    }
    println!("loading FIGI map from {} ...", tickers_path.display());
    let figi = read_security_id_map(&tickers_path)?;
    println!("  FIGI map size: {}", figi.len());

    println!("paginate /stocks/v1/dividends ...");
    let client = RestClient::new(base, api_key);
    let divs = client.list_dividends().await.context("list_dividends")?;
    println!("  got {} dividend rows", divs.len());

    let mut rows: Vec<DividendRow> = Vec::with_capacity(divs.len());
    let mut skipped = 0;
    for d in &divs {
        match to_row(d) {
            Ok(r) => rows.push(r),
            Err(e) => {
                skipped += 1;
                tracing::warn!(error = %e, "skip dividend row");
            }
        }
    }
    if skipped > 0 {
        println!("  skipped {skipped} rows with unparseable ex_dividend_date");
    }

    let with_figi = rows.iter().filter(|r| figi.contains_key(&r.display_symbol)).count();
    let recurring = rows
        .iter()
        .filter(|r| r.distribution_type.as_deref() == Some("recurring"))
        .count();
    let special = rows
        .iter()
        .filter(|r| r.distribution_type.as_deref() == Some("special"))
        .count();
    println!("  symbol→FIGI coverage: {with_figi}/{}", rows.len());
    println!("  distribution_type: recurring={recurring} special={special} other={}", rows.len() - recurring - special);

    let snapshot_date = Utc::now().date_naive();
    println!(
        "writing {} (dividends_snapshot_date={snapshot_date}) ...",
        out_path.display()
    );
    write_dividends(&out_path, &rows, snapshot_date, &figi)?;
    println!("  wrote {}", out_path.display());

    Ok(())
}
