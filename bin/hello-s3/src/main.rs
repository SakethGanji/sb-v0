use anyhow::{Context, Result};
use chrono::{Datelike, Duration, Local, NaiveDate, Weekday};
use momentum_api::s3::FlatFileClient;

fn most_recent_weekday(today: NaiveDate) -> NaiveDate {
    // Last-published flat file is for the prior trading day (~11am ET T+1).
    // Walk back from yesterday to the most recent Mon-Fri. (Holidays may still
    // miss; the hello-world is fine with one retry on a different date.)
    let mut d = today - Duration::days(1);
    while matches!(d.weekday(), Weekday::Sat | Weekday::Sun) {
        d -= Duration::days(1);
    }
    // Step back one more day to be safe — the previous day's file may not have
    // been published yet if it's morning ET. T-2 is always safely published.
    d -= Duration::days(1);
    while matches!(d.weekday(), Weekday::Sat | Weekday::Sun) {
        d -= Duration::days(1);
    }
    d
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let endpoint = std::env::var("MASSIVE_S3_ENDPOINT")
        .unwrap_or_else(|_| "https://files.massive.com".to_string());
    let bucket =
        std::env::var("MASSIVE_S3_BUCKET").unwrap_or_else(|_| "flatfiles".to_string());
    let key_id =
        std::env::var("MASSIVE_S3_KEY_ID").context("MASSIVE_S3_KEY_ID not set")?;
    let secret =
        std::env::var("MASSIVE_S3_SECRET").context("MASSIVE_S3_SECRET not set")?;

    let client = FlatFileClient::new(endpoint, bucket, key_id, secret);

    let target = most_recent_weekday(Local::now().date_naive());
    let key = FlatFileClient::minute_aggs_key(target);
    println!("target date : {target}");
    println!("s3 key      : {key}");

    let bytes = client
        .get_object_bytes(&key)
        .await
        .with_context(|| format!("get_object_bytes({key})"))?;
    println!("downloaded  : {} bytes gzipped", bytes.len());

    let lines = FlatFileClient::gunzip_first_lines(&bytes, 5)?;
    println!("--- first 5 CSV lines ---");
    for line in lines {
        println!("{line}");
    }

    Ok(())
}
