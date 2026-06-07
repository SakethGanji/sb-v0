//! `splits.parquet` writer (corporate-actions ingest, M4).
//!
//! Writes Massive's `/stocks/v1/splits` rows as one Parquet file with
//! the schema defined in `momentum_core::schema::splits_schema`. The
//! `splits_snapshot_date` lands in **file-level Parquet metadata** as
//! the pin source for the read-time multiply (§5.3 amended 2026-06-06).
//!
//! `security_id` is resolved by looking up `display_symbol` in the
//! tickers map (the `figi_lookup` used elsewhere in the crate). Rows
//! whose ticker isn't in the universe land with `security_id` null —
//! the engine will fall back to display-symbol matching for those.

use crate::WriteError;
use arrow::array::{
    ArrayRef, Date32Array, Float64Array, RecordBatch, StringArray, StringDictionaryBuilder,
};
use arrow::datatypes::Int32Type;
use chrono::NaiveDate;
use momentum_core::schema::{SPLITS_SNAPSHOT_DATE_META, splits_schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SplitRow {
    pub id: String,
    pub display_symbol: String,
    pub execution_date: NaiveDate,
    pub split_from: f64,
    pub split_to: f64,
    pub adjustment_type: Option<String>,
    pub historical_adjustment_factor: Option<f64>,
}

fn date32_value(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date");
    (d - epoch).num_days() as i32
}

fn build_batch(
    rows: &[SplitRow],
    figi_lookup: &HashMap<String, String>,
) -> Result<RecordBatch, WriteError> {
    let schema = splits_schema();

    let security_id: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| figi_lookup.get(&r.display_symbol).map(String::as_str)),
    ));
    let display_symbol: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.display_symbol.as_str()),
    ));
    let execution_date: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| date32_value(r.execution_date)).collect::<Vec<_>>(),
    ));
    let split_from: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.split_from).collect::<Vec<_>>(),
    ));
    let split_to: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.split_to).collect::<Vec<_>>(),
    ));

    let mut typ_b = StringDictionaryBuilder::<Int32Type>::new();
    for r in rows {
        match &r.adjustment_type {
            Some(s) => typ_b.append_value(s),
            None => typ_b.append_null(),
        }
    }
    let adjustment_type: ArrayRef = Arc::new(typ_b.finish());

    let hist: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.historical_adjustment_factor).collect::<Vec<_>>(),
    ));
    let id: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.id.as_str()),
    ));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            security_id,
            display_symbol,
            execution_date,
            split_from,
            split_to,
            adjustment_type,
            hist,
            id,
        ],
    )?)
}

/// Write `rows` to `path` and stamp the `splits_snapshot_date` into
/// file-level Parquet metadata so the run-pin enforcement can read it
/// cheaply at engine startup.
pub fn write_splits(
    path: &Path,
    rows: &[SplitRow],
    snapshot_date: NaiveDate,
    figi_lookup: &HashMap<String, String>,
) -> Result<(), WriteError> {
    let batch = build_batch(rows, figi_lookup)?;
    let schema = splits_schema();
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_key_value_metadata(Some(vec![KeyValue {
            key: SPLITS_SNAPSHOT_DATE_META.to_string(),
            value: Some(snapshot_date.to_string()),
        }]))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    let _ = writer.close()?;
    Ok(())
}

/// Read the `splits_snapshot_date` stamped at file-level metadata.
/// Returns `None` if the file lacks the key — older builds, or non-
/// snapshot files.
pub fn read_snapshot_date(path: &Path) -> Result<Option<NaiveDate>, WriteError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let meta = builder.metadata().file_metadata();
    let kvs = meta.key_value_metadata();
    if let Some(kvs) = kvs {
        for kv in kvs {
            if kv.key == SPLITS_SNAPSHOT_DATE_META {
                if let Some(v) = &kv.value {
                    return Ok(NaiveDate::parse_from_str(v, "%Y-%m-%d").ok());
                }
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    fn row(id: &str, sym: &str, date: &str, from: f64, to: f64) -> SplitRow {
        SplitRow {
            id: id.into(),
            display_symbol: sym.into(),
            execution_date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            split_from: from,
            split_to: to,
            adjustment_type: Some("forward_split".into()),
            historical_adjustment_factor: Some(from / to),
        }
    }

    #[test]
    fn write_roundtrip_and_metadata_pin() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("splits.parquet");
        let rows = vec![
            row("S1", "AAPL", "2020-08-31", 1.0, 4.0),
            row("S2", "NVDA", "2021-07-20", 1.0, 4.0),
            row("S3", "UNKN", "2010-01-04", 1.0, 2.0), // no FIGI
        ];
        let mut figi = HashMap::new();
        figi.insert("AAPL".into(), "BBG000B9XRY4".into());
        figi.insert("NVDA".into(), "BBG000BBJQV0".into());

        let snap = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();
        write_splits(&path, &rows, snap, &figi).unwrap();

        // Pin readable from file metadata.
        let pin = read_snapshot_date(&path).unwrap();
        assert_eq!(pin, Some(snap));

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 3);
        assert_eq!(b.schema(), splits_schema());

        let sids = b.column_by_name("security_id").unwrap().as_string::<i32>();
        assert_eq!(sids.value(0), "BBG000B9XRY4");
        assert_eq!(sids.value(1), "BBG000BBJQV0");
        assert!(sids.is_null(2)); // UNKN not in universe

        let dates = b
            .column_by_name("execution_date")
            .unwrap()
            .as_primitive::<arrow::datatypes::Date32Type>();
        // 2020-08-31 → days since 1970-01-01
        let expected = (NaiveDate::from_ymd_opt(2020, 8, 31).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        assert_eq!(dates.value(0), expected);
    }
}
