//! `vix_daily.parquet` writer (FRED VIXCLS).
//!
//! VIX is needed for the `vix_level` regime taxonomy and as a per-day
//! feature in `market_context_daily`. Massive's index endpoints are not
//! entitled on Stocks Advanced (probe returned 403), so the daily-close
//! series comes from FRED's free `VIXCLS` series.
//!
//! Tradeoff: FRED gives only the daily close. The RFC §8 schema lists
//! `vix_open` + `vix_close`; with this source `vix_open` is NULL until
//! Massive indices are entitled or another source is wired in.

use crate::WriteError;
use arrow::array::{ArrayRef, Date32Array, Float64Array, RecordBatch};
use chrono::NaiveDate;
use momentum_core::schema::{VIX_SNAPSHOT_DATE_META, vix_daily_schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct VixRow {
    pub date: NaiveDate,
    pub vix_close: f64,
}

fn date32_value(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    (d - epoch).num_days() as i32
}

fn build_batch(rows: &[VixRow]) -> Result<RecordBatch, WriteError> {
    let schema = vix_daily_schema();
    let date: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| date32_value(r.date)).collect::<Vec<_>>(),
    ));
    let vix_close: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.vix_close).collect::<Vec<_>>(),
    ));
    Ok(RecordBatch::try_new(schema, vec![date, vix_close])?)
}

pub fn write_vix(
    path: &Path,
    rows: &[VixRow],
    snapshot_date: NaiveDate,
) -> Result<(), WriteError> {
    let batch = build_batch(rows)?;
    let schema = vix_daily_schema();
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_key_value_metadata(Some(vec![KeyValue {
            key: VIX_SNAPSHOT_DATE_META.to_string(),
            value: Some(snapshot_date.to_string()),
        }]))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    let _ = writer.close()?;
    Ok(())
}

pub fn read_snapshot_date(path: &Path) -> Result<Option<NaiveDate>, WriteError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let meta = builder.metadata().file_metadata();
    if let Some(kvs) = meta.key_value_metadata() {
        for kv in kvs {
            if kv.key == VIX_SNAPSHOT_DATE_META {
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
    use arrow::array::AsArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    #[test]
    fn write_roundtrip_and_metadata_pin() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("vix.parquet");
        let rows = vec![
            VixRow {
                date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                vix_close: 13.20,
            },
            VixRow {
                date: NaiveDate::from_ymd_opt(2021, 1, 27).unwrap(),
                vix_close: 37.21,
            },
            VixRow {
                date: NaiveDate::from_ymd_opt(2020, 3, 16).unwrap(),
                vix_close: 82.69,
            },
        ];
        let snap = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();
        write_vix(&path, &rows, snap).unwrap();

        assert_eq!(read_snapshot_date(&path).unwrap(), Some(snap));

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 3);
        let vc = b
            .column_by_name("vix_close")
            .unwrap()
            .as_primitive::<arrow::datatypes::Float64Type>();
        assert!((vc.value(2) - 82.69).abs() < 1e-9);
    }
}
