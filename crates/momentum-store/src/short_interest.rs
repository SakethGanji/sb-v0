//! `short_interest.parquet` writer.

use crate::WriteError;
use arrow::array::{ArrayRef, Date32Array, Float64Array, RecordBatch, StringArray};
use chrono::NaiveDate;
use momentum_core::schema::{SHORT_INTEREST_SNAPSHOT_DATE_META, short_interest_schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ShortInterestRow {
    pub display_symbol: String,
    pub settlement_date: NaiveDate,
    pub short_interest: f64,
    pub avg_daily_volume: Option<f64>,
    pub days_to_cover: Option<f64>,
}

fn date32_value(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    (d - epoch).num_days() as i32
}

fn build_batch(
    rows: &[ShortInterestRow],
    figi_lookup: &HashMap<String, String>,
) -> Result<RecordBatch, WriteError> {
    let schema = short_interest_schema();

    let security_id: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| figi_lookup.get(&r.display_symbol).map(String::as_str)),
    ));
    let display_symbol: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.display_symbol.as_str()),
    ));
    let settlement_date: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| date32_value(r.settlement_date)).collect::<Vec<_>>(),
    ));
    let short_interest: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.short_interest).collect::<Vec<_>>(),
    ));
    let avg_daily_volume: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.avg_daily_volume).collect::<Vec<_>>(),
    ));
    let days_to_cover: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.days_to_cover).collect::<Vec<_>>(),
    ));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            security_id,
            display_symbol,
            settlement_date,
            short_interest,
            avg_daily_volume,
            days_to_cover,
        ],
    )?)
}

pub fn write_short_interest(
    path: &Path,
    rows: &[ShortInterestRow],
    snapshot_date: NaiveDate,
    figi_lookup: &HashMap<String, String>,
) -> Result<(), WriteError> {
    let batch = build_batch(rows, figi_lookup)?;
    let schema = short_interest_schema();
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_key_value_metadata(Some(vec![KeyValue {
            key: SHORT_INTEREST_SNAPSHOT_DATE_META.to_string(),
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
    if let Some(kvs) = builder.metadata().file_metadata().key_value_metadata() {
        for kv in kvs {
            if kv.key == SHORT_INTEREST_SNAPSHOT_DATE_META {
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
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    #[test]
    fn write_roundtrip_and_metadata_pin() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("si.parquet");
        let rows = vec![
            ShortInterestRow {
                display_symbol: "GME".into(),
                settlement_date: NaiveDate::from_ymd_opt(2021, 1, 15).unwrap(),
                short_interest: 61_782_730.0,
                avg_daily_volume: Some(29_363_915.0),
                days_to_cover: Some(2.1),
            },
            ShortInterestRow {
                display_symbol: "AAPL".into(),
                settlement_date: NaiveDate::from_ymd_opt(2024, 1, 12).unwrap(),
                short_interest: 101_263_039.0,
                avg_daily_volume: Some(50_000_000.0),
                days_to_cover: Some(2.0),
            },
        ];
        let mut figi = HashMap::new();
        figi.insert("AAPL".into(), "BBG000B9XRY4".into());
        let snap = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();
        write_short_interest(&path, &rows, snap, &figi).unwrap();
        assert_eq!(read_snapshot_date(&path).unwrap(), Some(snap));

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let b = &reader.collect::<Result<Vec<_>, _>>().unwrap()[0];
        assert_eq!(b.num_rows(), 2);
    }
}
