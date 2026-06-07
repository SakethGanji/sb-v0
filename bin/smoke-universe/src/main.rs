use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use momentum_api::rest::{RestClient, TickerListItem};
use momentum_store::tickers::{TickerRow, write_tickers};
use std::path::PathBuf;

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
        .unwrap_or_else(|_| PathBuf::from("data/_smoke/reference/tickers.parquet"));

    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let client = RestClient::new(base, api_key);

    println!("fetching active tickers (type=CS, market=stocks) ...");
    let active = client.list_tickers(true).await.context("list_tickers(true)")?;
    println!("  got {} active", active.len());

    println!("fetching delisted tickers (type=CS, market=stocks) ...");
    let delisted = client
        .list_tickers(false)
        .await
        .context("list_tickers(false)")?;
    println!("  got {} delisted", delisted.len());

    let rows: Vec<TickerRow> = active.iter().chain(delisted.iter()).map(to_row).collect();
    let n = rows.len();

    let with_composite = rows.iter().filter(|r| r.composite_figi.is_some()).count();
    let with_shareclass = rows.iter().filter(|r| r.share_class_figi.is_some()).count();
    let with_any_figi = rows.iter().filter(|r| r.security_id().is_some()).count();
    let with_delist = rows.iter().filter(|r| r.delisted_utc.is_some()).count();
    let with_delist_among_inactive = rows
        .iter()
        .filter(|r| !r.active && r.delisted_utc.is_some())
        .count();
    let inactive = rows.iter().filter(|r| !r.active).count();
    let with_exchange = rows.iter().filter(|r| r.primary_exchange.is_some()).count();

    println!("---");
    println!("total rows                  : {n}");
    println!("  composite_figi populated  : {with_composite}");
    println!("  share_class_figi populated: {with_shareclass}");
    println!("  any FIGI (security_id ok) : {with_any_figi}");
    println!("  primary_exchange populated: {with_exchange}");
    println!("  delisted_utc populated    : {with_delist}");
    println!("    among active=false ({inactive}) : {with_delist_among_inactive}");
    println!("---");
    println!(
        "delisted_utc coverage on inactive: {}%",
        if inactive == 0 {
            0
        } else {
            with_delist_among_inactive * 100 / inactive
        }
    );
    println!("  (if < 100%, the list endpoint omits delisted_utc and per-ticker enrichment via /v3/reference/tickers/{{T}} is required)");

    let as_of = Utc::now().date_naive();
    write_tickers(&out_path, &rows, as_of)?;
    println!("wrote {} (as_of_date={as_of})", out_path.display());

    Ok(())
}
