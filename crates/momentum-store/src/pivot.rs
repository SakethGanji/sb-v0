//! Single end-of-bulk pivot (M3 stage 2): scan every gzipped CSV under
//! `_staging/flat_files/**/*.csv.gz`, globally sort by `(ticker,
//! window_start)`, and write one `bars_1m_raw/{ticker}.parquet` per ticker
//! present — each per-ticker file written exactly once. Replaces the M2
//! throwaway `flat_file::ingest_day_throwaway` shape.
//!
//! Memory profile: DuckDB does the external sort (spills to disk as
//! needed); Rust accumulates one ticker's rows at a time before flushing
//! to Parquet, so peak Rust-side memory is ~one ticker's bars
//! (~25 MB at f64×8 cols × 980k rows for a 10-year ticker).

use crate::WriteError;
use arrow::array::{ArrayRef, AsArray, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::Int64Type;
use duckdb::Connection;
use momentum_core::schema::bars_1m_raw_schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum PivotError {
    #[error("duckdb: {0}")]
    Duck(#[from] duckdb::Error),
    #[error("write: {0}")]
    Write(#[from] WriteError),
}

impl From<arrow::error::ArrowError> for PivotError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Write(WriteError::Arrow(e))
    }
}

impl From<parquet::errors::ParquetError> for PivotError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::Write(WriteError::Parquet(e))
    }
}

impl From<std::io::Error> for PivotError {
    fn from(e: std::io::Error) -> Self {
        Self::Write(WriteError::Io(e))
    }
}

pub fn bulk_pivot(
    staging_dir: &Path,
    out_dir: &Path,
    figi_lookup: &HashMap<String, String>,
) -> Result<usize, PivotError> {
    std::fs::create_dir_all(out_dir)?;

    let glob = staging_dir.join("**").join("*.csv.gz");
    let glob_str = glob.to_string_lossy().replace('\'', "''");

    let sql = format!(
        "SELECT ticker, window_start, open, high, low, close, volume, transactions \
         FROM read_csv( \
            '{glob_str}', \
            columns={{ \
                'ticker': 'VARCHAR', \
                'volume': 'DOUBLE', \
                'open': 'DOUBLE', \
                'close': 'DOUBLE', \
                'high': 'DOUBLE', \
                'low': 'DOUBLE', \
                'window_start': 'BIGINT', \
                'transactions': 'BIGINT' \
            }}, \
            header=true \
         ) \
         ORDER BY ticker, window_start"
    );

    let conn = Connection::open_in_memory()?;
    let mut stmt = conn.prepare(&sql)?;
    let arrow_iter = stmt.query_arrow([])?;

    let mut current_ticker: Option<String> = None;
    let mut accumulated: Vec<RecordBatch> = Vec::new();
    let mut written = 0;

    for batch in arrow_iter {
        let n = batch.num_rows();
        if n == 0 {
            continue;
        }
        let tickers = batch.column(0).as_string::<i32>().clone();

        let mut start = 0;
        while start < n {
            let cur = tickers.value(start);
            let mut end = start + 1;
            while end < n && tickers.value(end) == cur {
                end += 1;
            }
            let slice = batch.slice(start, end - start);

            match &current_ticker {
                Some(t) if t == cur => {
                    accumulated.push(slice);
                }
                _ => {
                    if let Some(prev) = current_ticker.take() {
                        write_partition(out_dir, &prev, &accumulated, figi_lookup)?;
                        accumulated.clear();
                        written += 1;
                    }
                    current_ticker = Some(cur.to_string());
                    accumulated.push(slice);
                }
            }
            start = end;
        }
    }

    if let Some(prev) = current_ticker.take() {
        write_partition(out_dir, &prev, &accumulated, figi_lookup)?;
        written += 1;
    }

    Ok(written)
}

fn write_partition(
    out_dir: &Path,
    ticker: &str,
    csv_batches: &[RecordBatch],
    figi_lookup: &HashMap<String, String>,
) -> Result<(), WriteError> {
    let in_schema = csv_batches[0].schema();
    let concat = arrow::compute::concat_batches(&in_schema, csv_batches)?;
    let n = concat.num_rows();

    let out_schema = bars_1m_raw_schema();

    let security_id: ArrayRef = match figi_lookup.get(ticker) {
        Some(sid) => Arc::new(StringArray::from(vec![sid.as_str(); n])),
        None => {
            tracing::warn!(ticker, "no security_id (figi) in universe; writing nulls");
            Arc::new(StringArray::from_iter(
                std::iter::repeat(None::<&str>).take(n),
            ))
        }
    };
    let display_symbol: ArrayRef = Arc::new(StringArray::from(vec![ticker; n]));

    let ws = concat
        .column_by_name("window_start")
        .expect("window_start")
        .as_primitive::<Int64Type>();
    let t: ArrayRef = Arc::new(
        TimestampNanosecondArray::from_iter_values(ws.values().iter().copied())
            .with_timezone("UTC"),
    );

    let open = concat.column_by_name("open").expect("open").clone();
    let high = concat.column_by_name("high").expect("high").clone();
    let low = concat.column_by_name("low").expect("low").clone();
    let close = concat.column_by_name("close").expect("close").clone();
    let volume = concat.column_by_name("volume").expect("volume").clone();
    let transactions = concat
        .column_by_name("transactions")
        .expect("transactions")
        .clone();

    let out = RecordBatch::try_new(
        out_schema.clone(),
        vec![
            security_id,
            display_symbol,
            t,
            open,
            high,
            low,
            close,
            volume,
            transactions,
        ],
    )?;

    let path = out_dir.join(format!("{ticker}.parquet"));
    let file = File::create(&path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_max_row_group_row_count(Some(128 * 1024))
        .build();
    let mut writer = ArrowWriter::try_new(file, out_schema, Some(props))?;
    writer.write(&out)?;
    let _ = writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, TimestampNanosecondArray};
    use flate2::Compression as GzC;
    use flate2::write::GzEncoder;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::io::Write;
    use tempfile::tempdir;

    fn gz_to(dir: &Path, name: &str, csv: &str) {
        let path = dir.join(name);
        let f = File::create(&path).unwrap();
        let mut enc = GzEncoder::new(f, GzC::default());
        enc.write_all(csv.as_bytes()).unwrap();
        enc.finish().unwrap();
    }

    #[test]
    fn pivots_across_days_into_per_ticker_files_globally_sorted() {
        let dir = tempdir().unwrap();
        let staging = dir.path().join("_staging").join("flat_files").join("2025").join("01");
        std::fs::create_dir_all(&staging).unwrap();

        // Day 1: AAPL × 2, MSFT × 1
        gz_to(
            &staging,
            "2025-01-02.csv.gz",
            "ticker,volume,open,close,high,low,window_start,transactions\n\
             AAPL,200,151.0,152.0,152.5,150.5,1735822800000000000,15\n\
             AAPL,100,150.0,151.0,151.5,149.5,1735822740000000000,10\n\
             MSFT,300,250.0,251.0,251.5,249.5,1735822740000000000,20\n",
        );
        // Day 2: AAPL × 1 (earlier in calendar order? No — day 2's t is later than day 1's),
        // GOOG × 1. NOTE: deliberately place AAPL's day-2 row first in the CSV to test
        // that the *global* ORDER BY (ticker, window_start) sorts across files.
        gz_to(
            &staging,
            "2025-01-03.csv.gz",
            "ticker,volume,open,close,high,low,window_start,transactions\n\
             AAPL,400,153.0,154.0,154.5,152.5,1735909140000000000,18\n\
             GOOG,500,2800.0,2810.0,2812.0,2799.0,1735909140000000000,30\n",
        );

        let mut figi = HashMap::new();
        figi.insert("AAPL".into(), "BBG000B9XRY4".into());
        figi.insert("GOOG".into(), "BBG009S39JX6".into());
        // MSFT intentionally missing → null security_id

        let out_dir = dir.path().join("bars_1m_raw");
        let written = bulk_pivot(
            &dir.path().join("_staging").join("flat_files"),
            &out_dir,
            &figi,
        )
        .unwrap();
        assert_eq!(written, 3);

        // AAPL: 3 rows across both days, sorted by t
        let f = File::open(out_dir.join("AAPL.parquet")).unwrap();
        let batches: Vec<_> = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let b = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(b.num_rows(), 3);
        assert_eq!(b.schema(), bars_1m_raw_schema());

        let t = b
            .column_by_name("t")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        // Verify monotonic non-decreasing across the day boundary.
        for i in 1..t.len() {
            assert!(t.value(i) >= t.value(i - 1), "t not sorted at row {i}");
        }
        assert_eq!(t.value(0), 1735822740000000000);
        assert_eq!(t.value(2), 1735909140000000000);

        let opens = b
            .column_by_name("open")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(opens.value(0), 150.0);
        assert_eq!(opens.value(2), 153.0);

        let sids = b.column_by_name("security_id").unwrap().as_string::<i32>();
        assert_eq!(sids.value(0), "BBG000B9XRY4");

        // MSFT: 1 row, null security_id (not in figi map)
        let f = File::open(out_dir.join("MSFT.parquet")).unwrap();
        let batches: Vec<_> = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let sids = batches[0]
            .column_by_name("security_id")
            .unwrap()
            .as_string::<i32>();
        assert!(sids.is_null(0));

        // GOOG: 1 row
        let f = File::open(out_dir.join("GOOG.parquet")).unwrap();
        let batches: Vec<_> = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn empty_staging_writes_nothing() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("_staging")).unwrap();
        // Use a deeper nested path with no matching files.
        let staging = dir.path().join("_staging");
        let out_dir = dir.path().join("bars_1m_raw");

        // Put a single non-matching file so the glob has no .csv.gz hits.
        std::fs::write(staging.join("README"), b"not data").unwrap();

        let res = bulk_pivot(&staging, &out_dir, &HashMap::new());
        // DuckDB raises an error on an empty glob; either way no per-ticker file is created.
        match res {
            Ok(0) => {}
            Err(_) => {}
            Ok(n) => panic!("expected 0 files written, got {n}"),
        }
        if out_dir.exists() {
            let n = std::fs::read_dir(&out_dir).unwrap().count();
            assert_eq!(n, 0);
        }
    }
}
