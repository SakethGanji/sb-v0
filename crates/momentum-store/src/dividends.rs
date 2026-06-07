//! `dividends.parquet` writer (corporate-actions ingest, M4).
//!
//! Writes Massive's `/stocks/v1/dividends` rows. Dividends are stored as
//! events joined to per-trade summaries per §5.3.1 — they are NOT folded
//! into bars. The engine's price line stays unadjusted for ex-dividend
//! drops, which is the intended Phase 0 behavior.
//!
//! `dividends_snapshot_date` lands in file-level Parquet metadata for
//! cache-invalidation diagnostics. (Unlike `splits.parquet`, this stamp
//! does not gate any read-time math.)

use crate::WriteError;
use arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int32Array, RecordBatch, StringArray,
    StringDictionaryBuilder,
};
use arrow::datatypes::Int32Type;
use chrono::NaiveDate;
use momentum_core::schema::{DIVIDENDS_SNAPSHOT_DATE_META, dividends_schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DividendRow {
    pub id: String,
    pub display_symbol: String,
    pub ex_dividend_date: NaiveDate,
    pub pay_date: Option<NaiveDate>,
    pub record_date: Option<NaiveDate>,
    pub declaration_date: Option<NaiveDate>,
    pub cash_amount: f64,
    pub split_adjusted_cash_amount: Option<f64>,
    pub historical_adjustment_factor: Option<f64>,
    pub currency: Option<String>,
    pub distribution_type: Option<String>,
    pub frequency: Option<i32>,
}

fn date32_value(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date");
    (d - epoch).num_days() as i32
}

fn date32_opt(d: Option<NaiveDate>) -> Option<i32> {
    d.map(date32_value)
}

fn build_batch(
    rows: &[DividendRow],
    figi_lookup: &HashMap<String, String>,
) -> Result<RecordBatch, WriteError> {
    let schema = dividends_schema();

    let security_id: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| figi_lookup.get(&r.display_symbol).map(String::as_str)),
    ));
    let display_symbol: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.display_symbol.as_str()),
    ));
    let ex_dividend_date: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| date32_value(r.ex_dividend_date)).collect::<Vec<_>>(),
    ));
    let pay_date: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| date32_opt(r.pay_date)).collect::<Vec<_>>(),
    ));
    let record_date: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| date32_opt(r.record_date)).collect::<Vec<_>>(),
    ));
    let declaration_date: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| date32_opt(r.declaration_date)).collect::<Vec<_>>(),
    ));
    let cash_amount: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.cash_amount).collect::<Vec<_>>(),
    ));
    let split_adjusted: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.split_adjusted_cash_amount).collect::<Vec<_>>(),
    ));
    let hist: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.historical_adjustment_factor).collect::<Vec<_>>(),
    ));
    let currency: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.currency.as_deref()),
    ));

    let mut typ_b = StringDictionaryBuilder::<Int32Type>::new();
    for r in rows {
        match &r.distribution_type {
            Some(s) => typ_b.append_value(s),
            None => typ_b.append_null(),
        }
    }
    let distribution_type: ArrayRef = Arc::new(typ_b.finish());

    let frequency: ArrayRef = Arc::new(Int32Array::from(
        rows.iter().map(|r| r.frequency).collect::<Vec<_>>(),
    ));
    let id: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.id.as_str()),
    ));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            security_id,
            display_symbol,
            ex_dividend_date,
            pay_date,
            record_date,
            declaration_date,
            cash_amount,
            split_adjusted,
            hist,
            currency,
            distribution_type,
            frequency,
            id,
        ],
    )?)
}

pub fn write_dividends(
    path: &Path,
    rows: &[DividendRow],
    snapshot_date: NaiveDate,
    figi_lookup: &HashMap<String, String>,
) -> Result<(), WriteError> {
    let batch = build_batch(rows, figi_lookup)?;
    let schema = dividends_schema();
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_key_value_metadata(Some(vec![KeyValue {
            key: DIVIDENDS_SNAPSHOT_DATE_META.to_string(),
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
    use arrow::array::{Array, AsArray, Float64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    fn row(id: &str, sym: &str, ex: &str, cash: f64) -> DividendRow {
        DividendRow {
            id: id.into(),
            display_symbol: sym.into(),
            ex_dividend_date: NaiveDate::parse_from_str(ex, "%Y-%m-%d").unwrap(),
            pay_date: NaiveDate::parse_from_str("2025-08-14", "%Y-%m-%d").ok(),
            record_date: NaiveDate::parse_from_str(ex, "%Y-%m-%d").ok(),
            declaration_date: None,
            cash_amount: cash,
            split_adjusted_cash_amount: Some(cash),
            historical_adjustment_factor: Some(1.0),
            currency: Some("USD".into()),
            distribution_type: Some("recurring".into()),
            frequency: Some(4),
        }
    }

    #[test]
    fn write_roundtrip_round_trips_fields_and_nulls() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dividends.parquet");
        let rows = vec![
            row("D1", "AAPL", "2025-08-11", 0.26),
            row("D2", "UNKN", "2025-08-12", 0.10), // no FIGI
        ];
        let mut figi = HashMap::new();
        figi.insert("AAPL".into(), "BBG000B9XRY4".into());
        let snap = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();
        write_dividends(&path, &rows, snap, &figi).unwrap();

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 2);
        assert_eq!(b.schema(), dividends_schema());

        let sids = b.column_by_name("security_id").unwrap().as_string::<i32>();
        assert_eq!(sids.value(0), "BBG000B9XRY4");
        assert!(sids.is_null(1));

        let cash = b
            .column_by_name("cash_amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(cash.value(0), 0.26);

        // declaration_date is None on both rows → all null.
        let decl = b
            .column_by_name("declaration_date")
            .unwrap()
            .as_primitive::<arrow::datatypes::Date32Type>();
        assert!(decl.is_null(0));
        assert!(decl.is_null(1));
    }
}
