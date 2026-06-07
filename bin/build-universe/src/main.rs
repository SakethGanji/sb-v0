//! Production universe builder.
//!
//! Pipeline:
//!   1. Page through `/v3/reference/tickers?type=CS&market=stocks` for
//!      `active=true` and `active=false`.
//!   2. Identify rows missing `composite_figi` (engine identity key) or
//!      missing `delisted_utc` on inactive rows (eligibility filter input).
//!   3. Hit `/v3/reference/tickers/{T}` per-ticker for those gap rows only,
//!      bounded by `ENRICH_CONCURRENCY` (default 10), with retry+jitter.
//!   4. Convert to `momentum_store::TickerRow` and write to `OUT_PATH` as
//!      Parquet with `as_of_date` = today.
//!
//! Defaults write to the **real** `data/reference/tickers.parquet` — this
//! is the production bin. For throwaway runs use `smoke-universe`, which
//! defaults to `data/_smoke/`.
//!
//! Env vars:
//!   MASSIVE_API_KEY (required)
//!   MASSIVE_REST_BASE (default: https://api.polygon.io)
//!   OUT_PATH (default: data/reference/tickers.parquet)
//!   ENRICH_CONCURRENCY (default: 10)
//!   ENRICH_MAX_ATTEMPTS (default: 5)
//!   SKIP_ENRICH=1 to disable the per-ticker pass entirely (smoke-equivalent
//!     output without the second-pass coverage)

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use momentum_api::rest::{
    EnrichConfig, RestClient, TickerListItem, enrich_universe_gap, needs_enrichment,
};
use momentum_store::tickers::{TickerRow, write_tickers};
use std::path::PathBuf;
use std::sync::Arc;

fn parse_ts(s: &Option<String>) -> Option<DateTime<Utc>> {
    s.as_ref()
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|d| d.with_timezone(&Utc))
}

fn to_row(it: &TickerListItem) -> TickerRow {
    TickerRow {
        display_symbol: it.ticker.clone(),
        name: it.name.clone(),
        market: it.market.clone(),
        locale: it.locale.clone(),
        primary_exchange: it.primary_exchange.clone(),
        ticker_type: it.ticker_type.clone(),
        active: it.active,
        currency_name: it.currency_name.clone(),
        cik: it.cik.clone(),
        composite_figi: it.composite_figi.clone(),
        share_class_figi: it.share_class_figi.clone(),
        last_updated_utc: parse_ts(&it.last_updated_utc),
        delisted_utc: parse_ts(&it.delisted_utc),
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
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
    let out_path: PathBuf = std::env::var("OUT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/reference/tickers.parquet"));
    let skip_enrich = std::env::var("SKIP_ENRICH").ok().as_deref() == Some("1");

    let enrich_cfg = EnrichConfig {
        concurrency: env_usize("ENRICH_CONCURRENCY", 10),
        max_attempts: env_u32("ENRICH_MAX_ATTEMPTS", 5),
        base_backoff_ms: 200,
        max_backoff_ms: 30_000,
    };

    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let client = Arc::new(RestClient::new(base, api_key));

    println!("phase 1/3 — list active tickers (type=CS, market=stocks) ...");
    let active = client.list_tickers(true).await.context("list_tickers(true)")?;
    println!("  got {} active", active.len());

    println!("phase 1/3 — list delisted tickers ...");
    let delisted = client
        .list_tickers(false)
        .await
        .context("list_tickers(false)")?;
    println!("  got {} delisted", delisted.len());

    let mut rows: Vec<TickerListItem> = active.into_iter().chain(delisted.into_iter()).collect();
    let total = rows.len();
    let gap_size = rows.iter().filter(|r| needs_enrichment(r)).count();
    println!("  total rows: {total}");
    println!("  enrichment gap size: {gap_size} ({}%)", gap_size * 100 / total.max(1));

    if !skip_enrich && gap_size > 0 {
        println!(
            "phase 2/3 — enrich gap via /v3/reference/tickers/{{T}} (concurrency={}, max_attempts={}) ...",
            enrich_cfg.concurrency, enrich_cfg.max_attempts,
        );
        let (enriched, stats) =
            enrich_universe_gap(Arc::clone(&client), rows, enrich_cfg).await?;
        rows = enriched;
        println!(
            "  enrichment: gap={} ok={} err={} (404={} 429={} 5xx={} other={})",
            stats.gap,
            stats.ok,
            stats.err,
            stats.err_404,
            stats.err_429,
            stats.err_5xx,
            stats.err_other,
        );
        let active_rows = rows.iter().filter(|r| r.active).count();
        let inactive_rows = rows.iter().filter(|r| !r.active).count();
        let figi_active = rows
            .iter()
            .filter(|r| r.active && r.composite_figi.is_some())
            .count();
        let figi_inactive = rows
            .iter()
            .filter(|r| !r.active && r.composite_figi.is_some())
            .count();
        let delist_after = rows
            .iter()
            .filter(|r| !r.active && r.delisted_utc.is_some())
            .count();
        println!(
            "  post-enrich FIGI coverage: active {}/{} = {}% | delisted {}/{} = {}%",
            figi_active,
            active_rows,
            figi_active * 100 / active_rows.max(1),
            figi_inactive,
            inactive_rows,
            figi_inactive * 100 / inactive_rows.max(1),
        );
        println!(
            "  post-enrich delisted_utc on inactive: {}/{} = {}%",
            delist_after,
            inactive_rows,
            delist_after * 100 / inactive_rows.max(1),
        );
    } else if skip_enrich {
        println!("phase 2/3 — SKIP_ENRICH=1, skipping per-ticker pass");
    } else {
        println!("phase 2/3 — gap is empty, no per-ticker calls needed");
    }

    println!("phase 3/3 — write {} ...", out_path.display());
    let trows: Vec<TickerRow> = rows.iter().map(to_row).collect();
    let as_of = Utc::now().date_naive();
    write_tickers(&out_path, &trows, as_of)?;
    println!("  wrote {} (as_of_date={as_of})", out_path.display());

    Ok(())
}
