//! `acceptance_datetime_backfill.parquet` writer.
//!
//! Side table that joins to `financials.parquet` on `accession_number`
//! and supplies acceptance timestamps where Massive's feed left them
//! NULL.

use crate::WriteError;
use arrow::array::{
    ArrayRef, RecordBatch, StringArray, StringDictionaryBuilder, TimestampNanosecondArray,
};
use arrow::datatypes::Int32Type;
use chrono::NaiveDate;
use momentum_core::schema::{
    ACCEPTANCE_BACKFILL_SNAPSHOT_DATE_META, acceptance_backfill_schema,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct AcceptanceBackfillRow {
    pub cik: String,
    pub accession_number: String,
    pub acceptance_datetime_ns: i64,
    pub form: Option<String>,
    pub source: String,
}

fn build_batch(rows: &[AcceptanceBackfillRow]) -> Result<RecordBatch, WriteError> {
    let schema = acceptance_backfill_schema();

    let cik: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.cik.as_str()),
    ));
    let accession: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.accession_number.as_str()),
    ));
    let ts: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(
            rows.iter().map(|r| r.acceptance_datetime_ns).collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    );
    let form: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.form.as_deref()),
    ));
    let mut sb = StringDictionaryBuilder::<Int32Type>::new();
    for r in rows {
        sb.append_value(&r.source);
    }
    let source: ArrayRef = Arc::new(sb.finish());

    Ok(RecordBatch::try_new(
        schema,
        vec![cik, accession, ts, form, source],
    )?)
}

pub fn write_acceptance_backfill(
    path: &Path,
    rows: &[AcceptanceBackfillRow],
    snapshot_date: NaiveDate,
) -> Result<(), WriteError> {
    let batch = build_batch(rows)?;
    let schema = acceptance_backfill_schema();
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_key_value_metadata(Some(vec![KeyValue {
            key: ACCEPTANCE_BACKFILL_SNAPSHOT_DATE_META.to_string(),
            value: Some(snapshot_date.to_string()),
        }]))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    let _ = writer.close()?;
    Ok(())
}

/// Read the backfill as `accession_number → acceptance_datetime_ns`.
/// Join key matches `validate_reference_data.py` §F: the accession is the
/// last path segment of `financials.source_filing_url`.
pub fn read_acceptance_by_accession(
    path: &Path,
) -> Result<std::collections::HashMap<String, i64>, WriteError> {
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::TimestampNanosecondType;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut out = std::collections::HashMap::new();
    for batch_res in reader {
        let batch = batch_res?;
        let acc = batch
            .column_by_name("accession_number")
            .expect("accession_number")
            .as_string::<i32>();
        let ts = batch
            .column_by_name("acceptance_datetime")
            .expect("acceptance_datetime")
            .as_primitive::<TimestampNanosecondType>();
        for i in 0..batch.num_rows() {
            if !ts.is_null(i) {
                out.insert(acc.value(i).to_string(), ts.value(i));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    #[test]
    fn write_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ab.parquet");
        let rows = vec![AcceptanceBackfillRow {
            cik: "0000320193".into(),
            accession_number: "0001564590-23-003413".into(),
            acceptance_datetime_ns: 1_700_000_000_000_000_000,
            form: Some("10-Q".into()),
            source: "sec_edgar_submissions".into(),
        }];
        let snap = NaiveDate::from_ymd_opt(2026, 6, 8).unwrap();
        write_acceptance_backfill(&path, &rows, snap).unwrap();
        let f = File::open(&path).unwrap();
        let b = &ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()[0];
        assert_eq!(b.num_rows(), 1);
    }
}
