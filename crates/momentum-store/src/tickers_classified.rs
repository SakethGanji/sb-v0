//! `tickers_classified.parquet` writer.
//!
//! One row per (security_id, snapshot_date) — current-snapshot
//! classification fields pulled from Massive's
//! `/v3/reference/tickers/{id}` endpoint. The snapshot date is stamped
//! in file-level Parquet metadata under
//! `ticker_details_snapshot_date`.
//!
//! Downstream uses:
//! - `sic_code` / `sic_description` → behavioral tags
//!   (is_biotech, is_semiconductor, is_energy in RFC §11.6.3).
//! - `list_date` → `is_recent_ipo` and accurate
//!   `days_since_ipo_or_first_bar`.
//! - `market_cap_snapshot` + `share_class_shares_outstanding_snapshot`
//!   → current market_cap_bucket; for historical series we'll derive
//!   from financials filings.
//! - `description` → heuristic leveraged/inverse ETF detection
//!   (e.g. text matches "2x", "3x", "ultra", "inverse").

use crate::WriteError;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, StringDictionaryBuilder, TimestampNanosecondArray,
};
use arrow::datatypes::Int32Type;
use chrono::{DateTime, NaiveDate};
use momentum_core::schema::{
    TICKERS_CLASSIFIED_SNAPSHOT_DATE_META, tickers_classified_schema,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct TickerClassifiedRow {
    pub security_id: Option<String>,
    pub display_symbol: String,
    pub name: Option<String>,
    pub market: Option<String>,
    pub locale: Option<String>,
    pub primary_exchange: Option<String>,
    pub ticker_type: Option<String>,
    pub active: Option<bool>,
    pub currency_name: Option<String>,
    pub cik: Option<String>,
    pub composite_figi: Option<String>,
    pub share_class_figi: Option<String>,
    pub delisted_utc_ns: Option<i64>,
    pub list_date: Option<NaiveDate>,
    pub sic_code: Option<String>,
    pub sic_description: Option<String>,
    pub ticker_root: Option<String>,
    pub total_employees: Option<i64>,
    pub market_cap_snapshot: Option<f64>,
    pub share_class_shares_outstanding_snapshot: Option<f64>,
    pub weighted_shares_outstanding_snapshot: Option<f64>,
    pub round_lot: Option<i32>,
    pub description: Option<String>,
    pub address_state: Option<String>,
    pub address_city: Option<String>,
    pub homepage_url: Option<String>,
}

/// Parse a `last_updated_utc` / `delisted_utc` ISO8601 string to nanos.
pub fn parse_utc_ns(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s).ok().map(|dt| {
        dt.with_timezone(&chrono::Utc)
            .timestamp_nanos_opt()
            .unwrap_or(0)
    })
}

fn date32_value(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    (d - epoch).num_days() as i32
}

fn build_batch(rows: &[TickerClassifiedRow]) -> Result<RecordBatch, WriteError> {
    let schema = tickers_classified_schema();

    let security_id: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.security_id.as_deref()),
    ));
    let display_symbol: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.display_symbol.as_str()),
    ));
    let name: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.name.as_deref()),
    ));
    let market: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.market.as_deref()),
    ));
    let locale: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.locale.as_deref()),
    ));

    let mut pex = StringDictionaryBuilder::<Int32Type>::new();
    for r in rows {
        match &r.primary_exchange {
            Some(s) => pex.append_value(s),
            None => pex.append_null(),
        }
    }
    let primary_exchange: ArrayRef = Arc::new(pex.finish());

    let mut tt = StringDictionaryBuilder::<Int32Type>::new();
    for r in rows {
        match &r.ticker_type {
            Some(s) => tt.append_value(s),
            None => tt.append_null(),
        }
    }
    let ticker_type: ArrayRef = Arc::new(tt.finish());

    let active: ArrayRef = Arc::new(BooleanArray::from(
        rows.iter().map(|r| r.active).collect::<Vec<_>>(),
    ));
    let currency_name: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.currency_name.as_deref()),
    ));
    let cik: ArrayRef = Arc::new(StringArray::from_iter(rows.iter().map(|r| r.cik.as_deref())));
    let composite_figi: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.composite_figi.as_deref()),
    ));
    let share_class_figi: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.share_class_figi.as_deref()),
    ));

    let delisted_utc: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(
            rows.iter().map(|r| r.delisted_utc_ns).collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    );

    let list_date: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| r.list_date.map(date32_value)).collect::<Vec<_>>(),
    ));
    let sic_code: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.sic_code.as_deref()),
    ));
    let sic_description: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.sic_description.as_deref()),
    ));
    let ticker_root: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.ticker_root.as_deref()),
    ));
    let total_employees: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.total_employees).collect::<Vec<_>>(),
    ));
    let market_cap: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.market_cap_snapshot).collect::<Vec<_>>(),
    ));
    let sc_so: ArrayRef = Arc::new(Float64Array::from(
        rows.iter()
            .map(|r| r.share_class_shares_outstanding_snapshot)
            .collect::<Vec<_>>(),
    ));
    let w_so: ArrayRef = Arc::new(Float64Array::from(
        rows.iter()
            .map(|r| r.weighted_shares_outstanding_snapshot)
            .collect::<Vec<_>>(),
    ));
    let round_lot: ArrayRef = Arc::new(Int32Array::from(
        rows.iter().map(|r| r.round_lot).collect::<Vec<_>>(),
    ));
    let description: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.description.as_deref()),
    ));
    let address_state: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.address_state.as_deref()),
    ));
    let address_city: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.address_city.as_deref()),
    ));
    let homepage_url: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.homepage_url.as_deref()),
    ));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            security_id,
            display_symbol,
            name,
            market,
            locale,
            primary_exchange,
            ticker_type,
            active,
            currency_name,
            cik,
            composite_figi,
            share_class_figi,
            delisted_utc,
            list_date,
            sic_code,
            sic_description,
            ticker_root,
            total_employees,
            market_cap,
            sc_so,
            w_so,
            round_lot,
            description,
            address_state,
            address_city,
            homepage_url,
        ],
    )?)
}

pub fn write_tickers_classified(
    path: &Path,
    rows: &[TickerClassifiedRow],
    snapshot_date: NaiveDate,
) -> Result<(), WriteError> {
    let batch = build_batch(rows)?;
    let schema = tickers_classified_schema();
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_key_value_metadata(Some(vec![KeyValue {
            key: TICKERS_CLASSIFIED_SNAPSHOT_DATE_META.to_string(),
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
            if kv.key == TICKERS_CLASSIFIED_SNAPSHOT_DATE_META {
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
        let path = dir.path().join("c.parquet");
        let rows = vec![TickerClassifiedRow {
            security_id: Some("BBG000B9XRY4".into()),
            display_symbol: "AAPL".into(),
            name: Some("Apple Inc.".into()),
            sic_code: Some("3571".into()),
            sic_description: Some("ELECTRONIC COMPUTERS".into()),
            list_date: NaiveDate::parse_from_str("1980-12-12", "%Y-%m-%d").ok(),
            market_cap_snapshot: Some(4_514_011_993_040.0),
            ticker_type: Some("CS".into()),
            primary_exchange: Some("XNAS".into()),
            active: Some(true),
            ..Default::default()
        }];
        let snap = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();
        write_tickers_classified(&path, &rows, snap).unwrap();
        assert_eq!(read_snapshot_date(&path).unwrap(), Some(snap));

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].schema(), tickers_classified_schema());
    }
}
