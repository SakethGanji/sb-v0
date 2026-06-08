//! Pull raw SEC filings from Massive's `/vX/reference/financials` and
//! write `data/reference/financials.parquet` with
//! `financials_snapshot_date` stamped in file-level Parquet metadata.
//!
//! Schema choice (RFC §9 + comment in `momentum_core::schema`):
//! - Top-level filing fields (ticker, dates, fiscal period,
//!   acceptance_datetime, etc.) as native columns.
//! - A handful of high-value metrics extracted (shares outstanding,
//!   EPS, revenue, net income, equity, assets).
//! - The **full** nested `financials` struct preserved as a JSON
//!   string in `financials_json` — anything we didn't extract is
//!   recoverable later without re-pulling.
//!
//! TTM rollups (filing_date == None) are dropped — they're derived,
//! not real filings, and have no point-in-time stamp.
//!
//! Required env:
//!   MASSIVE_API_KEY
//!
//! Optional env with defaults:
//!   MASSIVE_REST_BASE=https://api.polygon.io
//!   TICKERS_PATH=data/reference/tickers.parquet
//!   OUT_PATH=data/reference/financials.parquet
//!   SMOKE=1  → stop after 200 pages (~20k rows), validate spot
//!              values, do NOT write OUT_PATH

use anyhow::{Context, Result, bail};
use chrono::{NaiveDate, Utc};
use momentum_api::rest::{Financial, RestClient};
use momentum_store::financials::{FinancialRow, parse_acceptance_ns, write_financials};
use momentum_store::tickers::read_security_id_map;
use std::path::PathBuf;

fn pick_value(v: &serde_json::Value, section: &str, key: &str) -> Option<f64> {
    v.get(section)?.get(key)?.get("value")?.as_f64()
}

fn to_row(f: &Financial) -> Option<FinancialRow> {
    // Skip TTM rollups (no real filing date). Acceptance datetime is
    // optional — many older filings lack it; we still keep them with a
    // filing-date-only point-in-time stamp.
    let filing_date_s = f.filing_date.as_deref()?;
    let filing_date = NaiveDate::parse_from_str(filing_date_s, "%Y-%m-%d").ok()?;
    let acceptance_datetime_ns = f
        .acceptance_datetime
        .as_deref()
        .and_then(parse_acceptance_ns);
    // Take the first ticker from the array.
    let ticker = f.tickers.as_ref().and_then(|v| v.first().cloned())?;

    let start_date = f
        .start_date
        .as_deref()
        .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
    let end_date = f
        .end_date
        .as_deref()
        .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());

    let basic_average_shares = pick_value(&f.financials, "income_statement", "basic_average_shares");
    let diluted_average_shares =
        pick_value(&f.financials, "income_statement", "diluted_average_shares");
    let basic_eps = pick_value(&f.financials, "income_statement", "basic_earnings_per_share");
    let diluted_eps = pick_value(&f.financials, "income_statement", "diluted_earnings_per_share");
    let revenues = pick_value(&f.financials, "income_statement", "revenues");
    let net_income_loss = pick_value(&f.financials, "income_statement", "net_income_loss");
    let equity = pick_value(&f.financials, "balance_sheet", "equity");
    let assets = pick_value(&f.financials, "balance_sheet", "assets");

    // Serialize the entire `financials` object as a JSON string for
    // later mining of anything we didn't extract above.
    let financials_json = if f.financials.is_null() {
        None
    } else {
        serde_json::to_string(&f.financials).ok()
    };

    Some(FinancialRow {
        ticker,
        cik: f.cik.clone(),
        sic_from_filing: f.sic.clone(),
        company_name: f.company_name.clone(),
        start_date,
        end_date,
        filing_date,
        acceptance_datetime_ns,
        timeframe: f.timeframe.clone(),
        fiscal_period: f.fiscal_period.clone(),
        fiscal_year: f.fiscal_year.clone(),
        source_filing_url: f.source_filing_url.clone(),
        basic_average_shares,
        diluted_average_shares,
        basic_earnings_per_share: basic_eps,
        diluted_earnings_per_share: diluted_eps,
        revenues,
        net_income_loss,
        equity,
        assets,
        financials_json,
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
        .unwrap_or_else(|_| PathBuf::from("data/reference/financials.parquet"));
    let smoke = std::env::var("SMOKE").ok().is_some();

    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    println!("loading FIGI map from {} ...", tickers_path.display());
    let figi = read_security_id_map(&tickers_path)?;
    println!("  FIGI map size: {}", figi.len());

    let client = RestClient::new(base, api_key);

    println!("paginate /vX/reference/financials (smoke={smoke}) ...");
    let started = std::time::Instant::now();
    let mut last_progress_total = 0usize;
    let raw: Vec<Financial> = client
        .list_financials(|page, total| {
            if total - last_progress_total >= 2000 || page == 1 {
                println!(
                    "  page {page}: total {total} ({:.0}s elapsed)",
                    started.elapsed().as_secs_f64()
                );
                last_progress_total = total;
            }
        })
        .await
        .context("list_financials")?;
    println!(
        "  fetched {} raw rows in {:.1}s",
        raw.len(),
        started.elapsed().as_secs_f64()
    );

    // Convert + filter TTM rollups in one pass.
    let mut rows: Vec<FinancialRow> = Vec::with_capacity(raw.len());
    let mut skip_ttm = 0usize;
    let mut skip_bad = 0usize;
    for f in &raw {
        if f.filing_date.is_none() {
            skip_ttm += 1;
            continue;
        }
        match to_row(f) {
            Some(r) => rows.push(r),
            None => skip_bad += 1,
        }
    }
    println!(
        "  kept {} filings; dropped {} TTM rollups; {} bad-parse",
        rows.len(),
        skip_ttm,
        skip_bad
    );

    // ---- Validation ----
    let total = rows.len();
    if total == 0 {
        bail!("VALIDATE: no rows after filtering");
    }
    let with_eps = rows
        .iter()
        .filter(|r| r.diluted_earnings_per_share.is_some())
        .count();
    let with_rev = rows.iter().filter(|r| r.revenues.is_some()).count();
    let with_shares = rows
        .iter()
        .filter(|r| r.diluted_average_shares.is_some())
        .count();
    let with_figi = rows.iter().filter(|r| figi.contains_key(&r.ticker)).count();
    let pct_eps = with_eps as f64 / total as f64 * 100.0;
    let pct_rev = with_rev as f64 / total as f64 * 100.0;
    let pct_shares = with_shares as f64 / total as f64 * 100.0;
    let pct_figi = with_figi as f64 / total as f64 * 100.0;

    let mut unique_tickers = std::collections::BTreeSet::new();
    let (mut min_d, mut max_d) = (NaiveDate::MAX, NaiveDate::MIN);
    for r in &rows {
        unique_tickers.insert(r.ticker.as_str());
        if r.filing_date < min_d {
            min_d = r.filing_date;
        }
        if r.filing_date > max_d {
            max_d = r.filing_date;
        }
    }
    println!(
        "  coverage: {total} rows | {} unique tickers | filing_date {min_d} → {max_d}",
        unique_tickers.len()
    );
    println!(
        "  metrics: eps {with_eps} ({pct_eps:.1}%) | revenues {with_rev} ({pct_rev:.1}%) | diluted_shares {with_shares} ({pct_shares:.1}%) | FIGI-resolved {with_figi} ({pct_figi:.1}%)"
    );

    // Sanity: AAPL should have many filings; the most recent Q's
    // diluted_average_shares should be ~14.7B.
    let aapl_rows: Vec<&FinancialRow> = rows.iter().filter(|r| r.ticker == "AAPL").collect();
    if !smoke {
        // Each ticker has roughly ~4 quarterly + 1 annual per year. AAPL
        // since 2009 ≈ 17 years × 5 ≈ 85 filings max. We pad downward
        // because Massive may not carry every filing.
        if aapl_rows.len() < 40 {
            bail!(
                "VALIDATE: only {} AAPL filings; expected ≥40 quarterly+annual since 2009",
                aapl_rows.len()
            );
        }
        let latest = aapl_rows.iter().max_by_key(|r| r.filing_date).unwrap();
        if let Some(das) = latest.diluted_average_shares {
            if !(10e9..=20e9).contains(&das) {
                bail!(
                    "VALIDATE: AAPL latest diluted_average_shares {das} out of plausible 10-20B range"
                );
            }
            println!(
                "  ✓ AAPL latest filing {} diluted_average_shares = {:.2e}",
                latest.filing_date, das
            );
        } else {
            bail!("VALIDATE: AAPL latest filing missing diluted_average_shares");
        }
    } else {
        println!("  smoke AAPL filings present: {}", aapl_rows.len());
    }

    if smoke {
        println!("SMOKE OK — not writing OUT_PATH");
        return Ok(());
    }

    let snapshot_date = Utc::now().date_naive();
    let tmp_path = out_path.with_extension("parquet.tmp");
    println!(
        "writing {} (financials_snapshot_date={snapshot_date}) ...",
        out_path.display()
    );
    write_financials(&tmp_path, &rows, snapshot_date, &figi)?;
    std::fs::rename(&tmp_path, &out_path)?;
    let bytes = std::fs::metadata(&out_path)?.len();
    println!(
        "  wrote {} ({} rows, {:.1} MB)",
        out_path.display(),
        rows.len(),
        bytes as f64 / 1e6
    );

    Ok(())
}
