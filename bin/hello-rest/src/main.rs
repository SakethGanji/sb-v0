use anyhow::{Context, Result};
use momentum_api::rest::RestClient;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let api_key = std::env::var("MASSIVE_API_KEY")
        .context("MASSIVE_API_KEY not set — source .env or export it")?;
    let base = std::env::var("MASSIVE_REST_BASE")
        .unwrap_or_else(|_| "https://api.polygon.io".to_string());

    let ticker = std::env::args().nth(1).unwrap_or_else(|| "AAPL".to_string());

    let client = RestClient::new(base, api_key);
    let details = client
        .ticker_details(&ticker)
        .await
        .with_context(|| format!("ticker_details({ticker})"))?;

    println!("--- {} ---", details.ticker);
    println!("name             : {}", details.name.as_deref().unwrap_or("-"));
    println!("market           : {}", details.market.as_deref().unwrap_or("-"));
    println!("locale           : {}", details.locale.as_deref().unwrap_or("-"));
    println!("primary_exchange : {}", details.primary_exchange.as_deref().unwrap_or("-"));
    println!("type             : {}", details.ticker_type.as_deref().unwrap_or("-"));
    println!("active           : {:?}", details.active);
    println!("composite_figi   : {}", details.composite_figi.as_deref().unwrap_or("-"));
    println!("share_class_figi : {}", details.share_class_figi.as_deref().unwrap_or("-"));
    println!("delisted_utc     : {}", details.delisted_utc.as_deref().unwrap_or("-"));

    Ok(())
}
