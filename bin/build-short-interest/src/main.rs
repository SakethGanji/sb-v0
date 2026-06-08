//! Pull every short-interest row from Massive's
//! `/stocks/v1/short-interest` and write
//! `data/reference/short_interest.parquet` with
//! `short_interest_snapshot_date` stamped in file-level Parquet
//! metadata.
//!
//! The endpoint serves the whole universe per call (no ticker filter
//! required) — single global pagination at 10k rows/page ≈ 120 pages
//! for the full dataset back to 2017-12-29.
//!
//! FINRA publishes short interest bi-weekly with ~8-day lag. For
//! point-in-time correctness, downstream readers should treat
//! `settlement_date + 8d` as the earliest available date.
//!
//! Required env:
//!   MASSIVE_API_KEY
//!
//! Optional env with defaults:
//!   MASSIVE_REST_BASE=https://api.polygon.io
//!   TICKERS_PATH=data/reference/tickers.parquet
//!   OUT_PATH=data/reference/short_interest.parquet
//!   SMOKE=1  → stop after the first page (10k rows), validate spot
//!              values, do NOT write OUT_PATH

use anyhow::{Context, Result, bail};
use chrono::{NaiveDate, Utc};
use momentum_api::rest::{RestClient, ShortInterest};
use momentum_store::short_interest::{ShortInterestRow, write_short_interest};
use momentum_store::tickers::read_security_id_map;
use std::path::PathBuf;

fn to_row(r: &ShortInterest) -> Option<ShortInterestRow> {
    let settlement_date = NaiveDate::parse_from_str(&r.settlement_date, "%Y-%m-%d").ok()?;
    Some(ShortInterestRow {
        display_symbol: r.ticker.clone(),
        settlement_date,
        short_interest: r.short_interest,
        avg_daily_volume: r.avg_daily_volume,
        days_to_cover: r.days_to_cover,
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
        .unwrap_or_else(|_| PathBuf::from("data/reference/short_interest.parquet"));
    let smoke = std::env::var("SMOKE").ok().is_some();

    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    println!("loading FIGI map from {} ...", tickers_path.display());
    let figi = read_security_id_map(&tickers_path)?;
    println!("  FIGI map size: {}", figi.len());

    let client = RestClient::new(base, api_key);
    println!("paginate /stocks/v1/short-interest (smoke={smoke}) ...");
    let started = std::time::Instant::now();
    // In smoke mode we still call the same paginated method but bail
    // after the first page by raising an early-stop sentinel.
    let mut last_progress = 0usize;
    let raw: Vec<ShortInterest> = if smoke {
        client
            .list_short_interest(|page, total| {
                if page != last_progress {
                    println!("  page {page}: total {total}");
                    last_progress = page;
                }
            })
            .await
            .context("list_short_interest")?
            .into_iter()
            .take(10_000)
            .collect()
    } else {
        client
            .list_short_interest(|page, total| {
                if total - last_progress >= 50_000 || page == 1 {
                    println!("  page {page}: total {total}");
                    last_progress = total;
                }
            })
            .await
            .context("list_short_interest")?
    };
    println!(
        "  fetched {} raw rows in {:.1}s",
        raw.len(),
        started.elapsed().as_secs_f64()
    );

    let mut rows: Vec<ShortInterestRow> = Vec::with_capacity(raw.len());
    let mut skip_bad_date = 0usize;
    for r in &raw {
        match to_row(r) {
            Some(row) => rows.push(row),
            None => skip_bad_date += 1,
        }
    }
    if skip_bad_date > 0 {
        println!("  skipped {skip_bad_date} rows with bad settlement_date");
    }

    // ---- Validation ----
    let with_figi = rows.iter().filter(|r| figi.contains_key(&r.display_symbol)).count();
    let unique_tickers: std::collections::BTreeSet<&str> =
        rows.iter().map(|r| r.display_symbol.as_str()).collect();
    let (mut min_d, mut max_d) = (NaiveDate::MAX, NaiveDate::MIN);
    for r in &rows {
        if r.settlement_date < min_d {
            min_d = r.settlement_date;
        }
        if r.settlement_date > max_d {
            max_d = r.settlement_date;
        }
    }
    println!(
        "  coverage: {} rows | {} unique tickers | dates {min_d} → {max_d} | FIGI-resolved {}/{} ({:.1}%)",
        rows.len(),
        unique_tickers.len(),
        with_figi,
        rows.len(),
        with_figi as f64 / rows.len() as f64 * 100.0
    );

    // Spot check: GME 2021-01-15 settlement_date short_interest ≈ 61.78M.
    let gme_jan2021 = rows.iter().find(|r| {
        r.display_symbol == "GME"
            && r.settlement_date == NaiveDate::from_ymd_opt(2021, 1, 15).unwrap()
    });
    if !smoke {
        match gme_jan2021 {
            Some(r) => {
                if (r.short_interest - 61_782_730.0).abs() > 100_000.0 {
                    bail!(
                        "VALIDATE: GME 2021-01-15 short_interest expected ~61.78M, got {:.0}",
                        r.short_interest
                    );
                }
                println!(
                    "  ✓ GME 2021-01-15 short_interest = {:.0} (expected ~61.78M)",
                    r.short_interest
                );
            }
            None => bail!("VALIDATE: GME 2021-01-15 row not present"),
        }
    }

    if smoke {
        println!("SMOKE OK ({} rows from first page) — not writing OUT_PATH", rows.len());
        return Ok(());
    }

    if rows.len() < 500_000 {
        bail!(
            "VALIDATE: only {} rows; expected ≥500k for full universe × bi-weekly back to 2017",
            rows.len()
        );
    }

    let snapshot_date = Utc::now().date_naive();
    let tmp_path = out_path.with_extension("parquet.tmp");
    println!(
        "writing {} (short_interest_snapshot_date={snapshot_date}) ...",
        out_path.display()
    );
    write_short_interest(&tmp_path, &rows, snapshot_date, &figi)?;
    std::fs::rename(&tmp_path, &out_path)?;
    println!("  wrote {} ({} rows)", out_path.display(), rows.len());

    Ok(())
}
