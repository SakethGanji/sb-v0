use anyhow::{Context, Result};
use arrow::array::{Array, AsArray, TimestampNanosecondArray};
use chrono::{Datelike, Duration, Local, NaiveDate, Weekday};
use momentum_api::s3::FlatFileClient;
use momentum_store::flat_file::ingest_day_throwaway;
use momentum_store::tickers::read_security_id_map;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::PathBuf;

fn most_recent_safe_weekday(today: NaiveDate) -> NaiveDate {
    let mut d = today - Duration::days(2);
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

    let date = match std::env::args().nth(1) {
        Some(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
            .with_context(|| format!("parse date arg {s:?} as YYYY-MM-DD"))?,
        None => most_recent_safe_weekday(Local::now().date_naive()),
    };

    let endpoint = std::env::var("MASSIVE_S3_ENDPOINT")
        .unwrap_or_else(|_| "https://files.massive.com".to_string());
    let bucket =
        std::env::var("MASSIVE_S3_BUCKET").unwrap_or_else(|_| "flatfiles".to_string());
    let key_id =
        std::env::var("MASSIVE_S3_KEY_ID").context("MASSIVE_S3_KEY_ID not set")?;
    let secret =
        std::env::var("MASSIVE_S3_SECRET").context("MASSIVE_S3_SECRET not set")?;

    let tickers_path: PathBuf = std::env::var("TICKERS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/_smoke/reference/tickers.parquet"));
    let out_dir: PathBuf = std::env::var("OUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/_smoke/bars_1m_raw"));

    if !tickers_path.exists() {
        anyhow::bail!(
            "{} does not exist — run smoke-universe first",
            tickers_path.display()
        );
    }

    println!("loading FIGI map from {} ...", tickers_path.display());
    let figi = read_security_id_map(&tickers_path)?;
    println!("  FIGI map size: {}", figi.len());

    println!("target date  : {date}");
    let key = FlatFileClient::minute_aggs_key(date);
    println!("s3 key       : {key}");

    let client = FlatFileClient::new(endpoint, bucket, key_id, secret);
    let bytes = client
        .get_object_bytes(&key)
        .await
        .with_context(|| format!("get_object_bytes({key})"))?;
    println!("downloaded   : {} bytes gzipped", bytes.len());

    std::fs::create_dir_all(&out_dir)?;
    let n_files = ingest_day_throwaway(&bytes[..], &out_dir, &figi)
        .context("ingest_day_throwaway")?;
    println!("wrote        : {} per-ticker files under {}", n_files, out_dir.display());

    // Spot checks: AAPL row count + t range, plus how many output files had a null security_id
    // (= tickers present in the flat file but missing from the universe table — coverage gap).
    let mut null_sid_files = 0usize;
    for entry in std::fs::read_dir(&out_dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.extension().and_then(|e| e.to_str()) != Some("parquet") {
            continue;
        }
        let file = std::fs::File::open(&p)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        if let Some(batch) = reader.next() {
            let batch = batch?;
            let sids = batch
                .column_by_name("security_id")
                .unwrap()
                .as_string::<i32>();
            if sids.is_null(0) {
                null_sid_files += 1;
            }
        }
    }
    println!(
        "tickers with null security_id (universe miss): {} / {} ({}%)",
        null_sid_files,
        n_files,
        if n_files == 0 {
            0
        } else {
            null_sid_files * 100 / n_files
        }
    );

    let aapl = out_dir.join("AAPL.parquet");
    if aapl.exists() {
        let file = std::fs::File::open(&aapl)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let mut total = 0usize;
        let mut min_t: Option<i64> = None;
        let mut max_t: Option<i64> = None;
        for batch_res in reader {
            let batch = batch_res?;
            total += batch.num_rows();
            let t = batch
                .column_by_name("t")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            for i in 0..t.len() {
                let v = t.value(i);
                min_t = Some(min_t.map_or(v, |m| m.min(v)));
                max_t = Some(max_t.map_or(v, |m| m.max(v)));
            }
        }
        let to_str = |ns: i64| {
            chrono::DateTime::<Utc>::from_timestamp_nanos(ns)
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string()
        };
        use chrono::Utc;
        println!(
            "AAPL         : {} rows, t in [{}, {}]",
            total,
            min_t.map(to_str).unwrap_or_else(|| "-".into()),
            max_t.map(to_str).unwrap_or_else(|| "-".into()),
        );
    } else {
        println!("AAPL.parquet not present (unexpected for any 2015+ trading day)");
    }

    Ok(())
}
