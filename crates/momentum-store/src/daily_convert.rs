//! Daily CSV.gz → daily Parquet, 1:1. No pivot, no global sort.
//!
//! One input: `_staging/flat_files/YYYY/MM/YYYY-MM-DD.csv.gz`
//! One output: `bars_1m_raw/YYYY-MM-DD.parquet`
//!
//! Per-day storage matches the engine's day-major read pattern
//! (`BarReader::session_bars(sid, day)` opens one file per iteration of
//! the daily loop; predicate-pushdown on `display_symbol` finds the
//! right rows). Replaces the earlier global-sort pivot that tried to
//! materialize per-ticker files via DuckDB external sort.
//!
//! Rows are sorted by `(display_symbol, t)` so DuckDB / arrow row-group
//! min/max stats prune effectively on the ticker column.

use crate::WriteError;
use arrow::array::{ArrayRef, AsArray, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::compute::{lexsort_to_indices, SortColumn, SortOptions, take};
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

/// Decode one gzipped daily flat-file CSV and write **one** Parquet
/// containing every ticker's rows for that day, sorted by
/// `(display_symbol, t)`. Returns the row count written.
pub fn convert_one_day<R: Read>(
    gz_bytes: R,
    out_path: &Path,
    figi_lookup: &HashMap<String, String>,
) -> Result<usize, WriteError> {
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

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
    let n = all.num_rows();
    if n == 0 {
        return Ok(0);
    }

    let tickers = all
        .column_by_name("ticker")
        .expect("ticker column")
        .as_string::<i32>()
        .clone();

    // Build security_id column via FIGI lookup. Nulls when not in the map.
    let mut sids: Vec<Option<&str>> = Vec::with_capacity(n);
    for i in 0..n {
        let t = tickers.value(i);
        sids.push(figi_lookup.get(t).map(|s| s.as_str()));
    }
    let security_id: ArrayRef = Arc::new(StringArray::from(sids));

    let display_symbol: ArrayRef =
        Arc::new(arrow::array::StringArray::from(tickers.clone()));

    let ws = all
        .column_by_name("window_start")
        .expect("window_start column")
        .as_primitive::<Int64Type>();
    let t: ArrayRef = Arc::new(
        TimestampNanosecondArray::from_iter_values(ws.values().iter().copied())
            .with_timezone("UTC"),
    );

    let open = all.column_by_name("open").expect("open").clone();
    let high = all.column_by_name("high").expect("high").clone();
    let low = all.column_by_name("low").expect("low").clone();
    let close = all.column_by_name("close").expect("close").clone();
    let volume = all.column_by_name("volume").expect("volume").clone();
    let transactions = all
        .column_by_name("transactions")
        .expect("transactions")
        .clone();

    let out_schema = bars_1m_raw_schema();
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

    // Sort by (display_symbol, t). Stable per-ticker clustering enables
    // row-group min/max pruning on display_symbol at read time.
    let cols = vec![
        SortColumn {
            values: unsorted.column_by_name("display_symbol").unwrap().clone(),
            options: Some(SortOptions::default()),
        },
        SortColumn {
            values: unsorted.column_by_name("t").unwrap().clone(),
            options: Some(SortOptions::default()),
        },
    ];
    let order = lexsort_to_indices(&cols, None)?;
    let sorted_cols = unsorted
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &order, None))
        .collect::<Result<Vec<_>, _>>()?;
    let sorted = RecordBatch::try_new(out_schema.clone(), sorted_cols)?;

    let file = File::create(out_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_max_row_group_row_count(Some(128 * 1024))
        .build();
    let mut writer = ArrowWriter::try_new(file, out_schema, Some(props))?;
    writer.write(&sorted)?;
    let _ = writer.close()?;
    Ok(n)
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
    fn one_day_one_file_multiple_tickers_sorted_by_symbol_then_t() {
        // Three tickers, rows interleaved + out of t-order on purpose.
        let csv = "ticker,volume,open,close,high,low,window_start,transactions\n\
            MSFT,300,250.0,251.0,251.5,249.5,1672531260000000000,20\n\
            AAPL,200,151.0,152.0,152.5,150.5,1672531260000000000,15\n\
            AAPL,100,150.0,151.0,151.5,149.5,1672531200000000000,10\n\
            ZZZ,50,1.0,1.1,1.2,0.9,1672531200000000000,5\n";
        let bytes = gz(csv);

        let mut figi = HashMap::new();
        figi.insert("AAPL".into(), "BBG000B9XRY4".into());
        figi.insert("MSFT".into(), "BBG000BPH459".into());
        // ZZZ deliberately not in map → null security_id row.

        let dir = tempdir().unwrap();
        let out = dir.path().join("2023-01-01.parquet");
        let n = convert_one_day(&bytes[..], &out, &figi).unwrap();
        assert_eq!(n, 4);

        let file = File::open(&out).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let b = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(b.num_rows(), 4);
        assert_eq!(b.schema(), bars_1m_raw_schema());

        // Sort assertion: display_symbol then t.
        let symbols = b
            .column_by_name("display_symbol")
            .unwrap()
            .as_string::<i32>();
        let t = b
            .column_by_name("t")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        // Expected order: AAPL@t1, AAPL@t2, MSFT@t2, ZZZ@t1.
        assert_eq!(symbols.value(0), "AAPL");
        assert_eq!(symbols.value(1), "AAPL");
        assert_eq!(symbols.value(2), "MSFT");
        assert_eq!(symbols.value(3), "ZZZ");
        assert!(t.value(0) < t.value(1)); // AAPL: t-ascending within ticker
        assert_eq!(t.value(2), 1672531260000000000);
        assert_eq!(t.value(3), 1672531200000000000);

        // FIGI map applied.
        let sids = b.column_by_name("security_id").unwrap().as_string::<i32>();
        assert_eq!(sids.value(0), "BBG000B9XRY4");
        assert_eq!(sids.value(2), "BBG000BPH459");
        assert!(sids.is_null(3)); // ZZZ wasn't in the map

        // OHLCV preserved.
        let opens = b
            .column_by_name("open")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(opens.value(0), 150.0); // AAPL earliest
        assert_eq!(opens.value(1), 151.0); // AAPL later
        assert_eq!(opens.value(2), 250.0); // MSFT
    }

    #[test]
    fn empty_csv_writes_nothing() {
        let csv = "ticker,volume,open,close,high,low,window_start,transactions\n";
        let bytes = gz(csv);
        let dir = tempdir().unwrap();
        let out = dir.path().join("2023-01-01.parquet");
        let n = convert_one_day(&bytes[..], &out, &HashMap::new()).unwrap();
        assert_eq!(n, 0);
        assert!(!out.exists());
    }
}
