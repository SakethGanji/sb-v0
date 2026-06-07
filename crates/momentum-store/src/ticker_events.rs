//! `ticker_events.parquet` writer (corporate-actions ingest, M4).
//!
//! One row per (security_id, event_date, new_ticker). The full rename
//! chain for a ticker can be reconstructed by joining on `security_id`
//! and sorting by `event_date`. Currently the only event type Massive
//! returns is `ticker_change`; the column is dictionary-encoded to
//! anticipate other event types later.
//!
//! `figi_map.parquet` (the downstream `(display_symbol, t) → security_id`
//! resolver) is derived from this file; it does not exist as a separate
//! ingest stage.

use crate::WriteError;
use arrow::array::{
    ArrayRef, Date32Array, RecordBatch, StringArray, StringDictionaryBuilder,
};
use arrow::datatypes::Int32Type;
use chrono::NaiveDate;
use momentum_core::schema::{TICKER_EVENTS_SNAPSHOT_DATE_META, ticker_events_schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TickerEventRow {
    pub security_id: String,
    pub name: Option<String>,
    pub event_date: NaiveDate,
    pub event_type: String,
    pub new_ticker: String,
}

fn date32_value(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date");
    (d - epoch).num_days() as i32
}

fn build_batch(rows: &[TickerEventRow]) -> Result<RecordBatch, WriteError> {
    let schema = ticker_events_schema();

    let security_id: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.security_id.as_str()),
    ));
    let name: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.name.as_deref()),
    ));
    let event_date: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| date32_value(r.event_date)).collect::<Vec<_>>(),
    ));
    let mut typ_b = StringDictionaryBuilder::<Int32Type>::new();
    for r in rows {
        typ_b.append_value(&r.event_type);
    }
    let event_type: ArrayRef = Arc::new(typ_b.finish());
    let new_ticker: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.new_ticker.as_str()),
    ));

    Ok(RecordBatch::try_new(
        schema,
        vec![security_id, name, event_date, event_type, new_ticker],
    )?)
}

pub fn write_ticker_events(
    path: &Path,
    rows: &[TickerEventRow],
    snapshot_date: NaiveDate,
) -> Result<(), WriteError> {
    let batch = build_batch(rows)?;
    let schema = ticker_events_schema();
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_key_value_metadata(Some(vec![KeyValue {
            key: TICKER_EVENTS_SNAPSHOT_DATE_META.to_string(),
            value: Some(snapshot_date.to_string()),
        }]))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    let _ = writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    fn row(sid: &str, date: &str, new_t: &str) -> TickerEventRow {
        TickerEventRow {
            security_id: sid.into(),
            name: Some("Meta Platforms, Inc. Class A Common Stock".into()),
            event_date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            event_type: "ticker_change".into(),
            new_ticker: new_t.into(),
        }
    }

    #[test]
    fn write_roundtrip_handles_multiple_renames_per_security() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ticker_events.parquet");
        let rows = vec![
            row("BBG000MM2P62", "2012-05-18", "FB"),
            row("BBG000MM2P62", "2022-06-09", "META"),
        ];
        let snap = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();
        write_ticker_events(&path, &rows, snap).unwrap();

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let meta = builder.metadata().file_metadata();
        let kvs = meta.key_value_metadata().expect("kv metadata present");
        let snap_kv = kvs.iter().find(|kv| kv.key == TICKER_EVENTS_SNAPSHOT_DATE_META);
        assert!(snap_kv.is_some());

        let batches: Vec<_> = builder.build().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 2);
        assert_eq!(b.schema(), ticker_events_schema());
    }
}
