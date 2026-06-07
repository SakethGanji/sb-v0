//! Single-day flat-file ingest — **throwaway shape**.
//!
//! This module exists for M2 only: prove the byte-stream → gunzip → CSV →
//! per-ticker Parquet pipeline against one day. The M3 bulk-ingest path
//! (`_staging/` + single end-of-bulk DuckDB pivot, README §5.1 stage 2)
//! replaces it wholesale. Nothing here should graduate into the bulk path —
//! per-day per-ticker writes don't compose, because Parquet is write-once.
//!
//! Visibility is `pub(crate)` (plus the public M2 entry point) on purpose;
//! the surface should not grow.

use crate::WriteError;
use arrow::array::{ArrayRef, AsArray, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::compute::{sort_to_indices, take};
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use flate2::read::GzDecoder;
use momentum_core::schema::bars_1m_raw_schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

fn csv_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::Utf8, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("open", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("window_start", DataType::Int64, false),
        Field::new("transactions", DataType::Int64, true),
    ]))
}

/// Decode a single day's gzipped flat-file CSV and write one
/// `{out_dir}/{ticker}.parquet` per ticker present in the file, fully
/// overwriting each per-ticker file. Returns the number of ticker files
/// written.
///
/// **M2 only — see module docs.** The M3 path does not call this.
pub fn ingest_day_throwaway<R: Read>(
    gz_bytes: R,
    out_dir: &Path,
    figi_lookup: &HashMap<String, String>,
) -> Result<usize, WriteError> {
    std::fs::create_dir_all(out_dir)?;
    let decoder = GzDecoder::new(gz_bytes);
    let csv_s = csv_schema();
    let reader = ReaderBuilder::new(csv_s.clone())
        .with_header(true)
        .build(decoder)?;

    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
    if batches.is_empty() {
        return Ok(0);
    }
    let all = arrow::compute::concat_batches(&csv_s, &batches)?;

    let ticker_col = all
        .column_by_name("ticker")
        .expect("ticker column")
        .clone();
    let order = sort_to_indices(ticker_col.as_ref(), None, None)?;
    let sorted = take_record_batch(&all, &order)?;

    let tickers = sorted
        .column_by_name("ticker")
        .expect("ticker column")
        .as_string::<i32>()
        .clone();
    let n = sorted.num_rows();

    let mut written = 0;
    let mut start = 0;
    while start < n {
        let cur = tickers.value(start);
        let mut end = start + 1;
        while end < n && tickers.value(end) == cur {
            end += 1;
        }
        let slice = sorted.slice(start, end - start);
        write_ticker_partition(out_dir, cur, &slice, figi_lookup)?;
        written += 1;
        start = end;
    }
    Ok(written)
}

fn take_record_batch(
    batch: &RecordBatch,
    indices: &arrow::array::UInt32Array,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let cols = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), cols)
}

fn write_ticker_partition(
    out_dir: &Path,
    ticker: &str,
    rows: &RecordBatch,
    figi_lookup: &HashMap<String, String>,
) -> Result<(), WriteError> {
    let n = rows.num_rows();
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

    let ws = rows
        .column_by_name("window_start")
        .expect("window_start column")
        .as_primitive::<Int64Type>();
    let t: ArrayRef = Arc::new(
        TimestampNanosecondArray::from_iter_values(ws.values().iter().copied())
            .with_timezone("UTC"),
    );

    let open = rows.column_by_name("open").expect("open").clone();
    let high = rows.column_by_name("high").expect("high").clone();
    let low = rows.column_by_name("low").expect("low").clone();
    let close = rows.column_by_name("close").expect("close").clone();
    let volume = rows.column_by_name("volume").expect("volume").clone();
    let transactions = rows
        .column_by_name("transactions")
        .expect("transactions")
        .clone();

    let unsorted = RecordBatch::try_new(
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

    let t_col = unsorted.column_by_name("t").expect("t column");
    let order = sort_to_indices(t_col.as_ref(), None, None)?;
    let sorted = take_record_batch(&unsorted, &order)?;

    let out_path = out_dir.join(format!("{ticker}.parquet"));
    let file = File::create(&out_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_max_row_group_row_count(Some(128 * 1024))
        .build();
    let mut writer = ArrowWriter::try_new(file, out_schema, Some(props))?;
    writer.write(&sorted)?;
    let _ = writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array};
    use flate2::Compression as GzC;
    use flate2::write::GzEncoder;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::io::Write;
    use tempfile::tempdir;

    fn gz(s: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut enc = GzEncoder::new(&mut buf, GzC::default());
        enc.write_all(s.as_bytes()).unwrap();
        enc.finish().unwrap();
        buf
    }

    #[test]
    fn partitions_by_ticker_and_sorts_within_ticker() {
        // Two tickers, with rows deliberately out of (ticker, t) order on input.
        let csv = "ticker,volume,open,close,high,low,window_start,transactions\n\
            MSFT,300,250.0,251.0,251.5,249.5,1672531200000000000,20\n\
            AAPL,200,151.0,152.0,152.5,150.5,1672531260000000000,15\n\
            AAPL,100,150.0,151.0,151.5,149.5,1672531200000000000,10\n";
        let bytes = gz(csv);

        let mut figi = HashMap::new();
        figi.insert("AAPL".into(), "BBG000B9XRY4".into());

        let dir = tempdir().unwrap();
        let written = ingest_day_throwaway(&bytes[..], dir.path(), &figi).unwrap();
        assert_eq!(written, 2);

        // AAPL file: 2 rows, schema matches, sorted by t, security_id populated
        let aapl = dir.path().join("AAPL.parquet");
        assert!(aapl.exists());
        let file = File::open(&aapl).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 2);
        assert_eq!(b.schema(), bars_1m_raw_schema());

        let sids = b.column_by_name("security_id").unwrap().as_string::<i32>();
        assert_eq!(sids.value(0), "BBG000B9XRY4");
        assert_eq!(sids.value(1), "BBG000B9XRY4");

        let opens = b
            .column_by_name("open")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Sorted by t: 1672531200... first (open=150), 1672531260... second (open=151).
        assert_eq!(opens.value(0), 150.0);
        assert_eq!(opens.value(1), 151.0);

        // MSFT file: 1 row, security_id null (not in figi map)
        let msft = dir.path().join("MSFT.parquet");
        let file = File::open(&msft).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 1);
        let sids = b.column_by_name("security_id").unwrap().as_string::<i32>();
        assert!(sids.is_null(0));
    }

    #[test]
    fn empty_file_writes_nothing() {
        let csv = "ticker,volume,open,close,high,low,window_start,transactions\n";
        let bytes = gz(csv);
        let dir = tempdir().unwrap();
        let written = ingest_day_throwaway(&bytes[..], dir.path(), &HashMap::new()).unwrap();
        assert_eq!(written, 0);
        // No stray files
        let n = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(n, 0);
    }
}
