//! `financials.parquet` writer.

use crate::WriteError;
use arrow::array::{
    ArrayRef, Date32Array, Float64Array, RecordBatch, StringArray, StringDictionaryBuilder,
    TimestampNanosecondArray,
};
use arrow::datatypes::Int32Type;
use chrono::{DateTime, NaiveDate};
use momentum_core::schema::{FINANCIALS_SNAPSHOT_DATE_META, financials_schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct FinancialRow {
    pub ticker: String,
    pub cik: Option<String>,
    pub sic_from_filing: Option<String>,
    pub company_name: Option<String>,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub filing_date: NaiveDate,
    pub acceptance_datetime_ns: Option<i64>,
    pub timeframe: Option<String>,
    pub fiscal_period: Option<String>,
    pub fiscal_year: Option<String>,
    pub source_filing_url: Option<String>,
    pub basic_average_shares: Option<f64>,
    pub diluted_average_shares: Option<f64>,
    pub basic_earnings_per_share: Option<f64>,
    pub diluted_earnings_per_share: Option<f64>,
    pub revenues: Option<f64>,
    pub net_income_loss: Option<f64>,
    pub equity: Option<f64>,
    pub assets: Option<f64>,
    pub financials_json: Option<String>,
}

/// Parse `"2026-05-01T10:01:00Z"` style strings to nanoseconds.
pub fn parse_acceptance_ns(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s).ok().and_then(|dt| {
        dt.with_timezone(&chrono::Utc).timestamp_nanos_opt()
    })
}

fn date32_value(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    (d - epoch).num_days() as i32
}

fn build_batch(
    rows: &[FinancialRow],
    figi_lookup: &HashMap<String, String>,
) -> Result<RecordBatch, WriteError> {
    let schema = financials_schema();

    let security_id: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| figi_lookup.get(&r.ticker).map(String::as_str)),
    ));
    let ticker: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.ticker.as_str()),
    ));
    let cik: ArrayRef = Arc::new(StringArray::from_iter(rows.iter().map(|r| r.cik.as_deref())));
    let sic: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.sic_from_filing.as_deref()),
    ));
    let company_name: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.company_name.as_deref()),
    ));
    let start_date: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| r.start_date.map(date32_value)).collect::<Vec<_>>(),
    ));
    let end_date: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| r.end_date.map(date32_value)).collect::<Vec<_>>(),
    ));
    let filing_date: ArrayRef = Arc::new(Date32Array::from(
        rows.iter().map(|r| date32_value(r.filing_date)).collect::<Vec<_>>(),
    ));
    let acceptance: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(
            rows.iter().map(|r| r.acceptance_datetime_ns).collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    );
    // Note: TimestampNanosecondArray::from(Vec<Option<i64>>) yields a
    // nullable array, matching schema.acceptance_datetime nullable=true.

    let mut tf = StringDictionaryBuilder::<Int32Type>::new();
    for r in rows {
        match &r.timeframe {
            Some(s) => tf.append_value(s),
            None => tf.append_null(),
        }
    }
    let timeframe: ArrayRef = Arc::new(tf.finish());

    let mut fp = StringDictionaryBuilder::<Int32Type>::new();
    for r in rows {
        match &r.fiscal_period {
            Some(s) => fp.append_value(s),
            None => fp.append_null(),
        }
    }
    let fiscal_period: ArrayRef = Arc::new(fp.finish());

    let fiscal_year: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.fiscal_year.as_deref()),
    ));
    let source: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.source_filing_url.as_deref()),
    ));
    let bas: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.basic_average_shares).collect::<Vec<_>>(),
    ));
    let das: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.diluted_average_shares).collect::<Vec<_>>(),
    ));
    let beps: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.basic_earnings_per_share).collect::<Vec<_>>(),
    ));
    let deps: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.diluted_earnings_per_share).collect::<Vec<_>>(),
    ));
    let rev: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.revenues).collect::<Vec<_>>(),
    ));
    let ni: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.net_income_loss).collect::<Vec<_>>(),
    ));
    let eq: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.equity).collect::<Vec<_>>(),
    ));
    let as_: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.assets).collect::<Vec<_>>(),
    ));
    let fj: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.financials_json.as_deref()),
    ));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            security_id,
            ticker,
            cik,
            sic,
            company_name,
            start_date,
            end_date,
            filing_date,
            acceptance,
            timeframe,
            fiscal_period,
            fiscal_year,
            source,
            bas,
            das,
            beps,
            deps,
            rev,
            ni,
            eq,
            as_,
            fj,
        ],
    )?)
}

pub fn write_financials(
    path: &Path,
    rows: &[FinancialRow],
    snapshot_date: NaiveDate,
    figi_lookup: &HashMap<String, String>,
) -> Result<(), WriteError> {
    let batch = build_batch(rows, figi_lookup)?;
    let schema = financials_schema();
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_key_value_metadata(Some(vec![KeyValue {
            key: FINANCIALS_SNAPSHOT_DATE_META.to_string(),
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
            if kv.key == FINANCIALS_SNAPSHOT_DATE_META {
                if let Some(v) = &kv.value {
                    return Ok(NaiveDate::parse_from_str(v, "%Y-%m-%d").ok());
                }
            }
        }
    }
    Ok(None)
}

/// The filing-identity slice of one financials row — everything the
/// earnings-calendar derivation needs, nothing else.
#[derive(Debug, Clone)]
pub struct FilingLite {
    pub security_id: Option<String>,
    pub ticker: String,
    pub filing_date: NaiveDate,
    pub acceptance_datetime_ns: Option<i64>,
    /// Last path segment of `source_filing_url` — the EDGAR accession
    /// number, the join key for `acceptance_datetime_backfill.parquet`
    /// (same rule as `validate_reference_data.py` §F).
    pub accession_number: Option<String>,
    pub timeframe: Option<String>,
    pub fiscal_period: Option<String>,
    pub fiscal_year: Option<String>,
}

/// Read the filing-identity columns from `financials.parquet`.
pub fn read_filings_lite(path: &Path) -> Result<Vec<FilingLite>, WriteError> {
    use arrow::array::{Array, AsArray};
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Date32Type, TimestampNanosecondType};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut out = Vec::new();
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");

    for batch_res in reader {
        let batch = batch_res?;
        let utf8 = |name: &str| -> Result<arrow::array::ArrayRef, WriteError> {
            Ok(cast(batch.column_by_name(name).expect(name), &DataType::Utf8)?)
        };
        let sid = utf8("security_id")?;
        let sid = sid.as_string::<i32>();
        let ticker = utf8("ticker")?;
        let ticker = ticker.as_string::<i32>();
        let timeframe = utf8("timeframe")?;
        let timeframe = timeframe.as_string::<i32>();
        let fiscal_period = utf8("fiscal_period")?;
        let fiscal_period = fiscal_period.as_string::<i32>();
        let fiscal_year = utf8("fiscal_year")?;
        let fiscal_year = fiscal_year.as_string::<i32>();
        let url = utf8("source_filing_url")?;
        let url = url.as_string::<i32>();
        let filing_date = batch
            .column_by_name("filing_date")
            .expect("filing_date")
            .as_primitive::<Date32Type>();
        let acceptance = batch
            .column_by_name("acceptance_datetime")
            .expect("acceptance_datetime")
            .as_primitive::<TimestampNanosecondType>();

        let opt_str = |arr: &arrow::array::StringArray, i: usize| {
            (!arr.is_null(i)).then(|| arr.value(i).to_string())
        };
        for i in 0..batch.num_rows() {
            out.push(FilingLite {
                security_id: opt_str(sid, i),
                ticker: ticker.value(i).to_string(),
                filing_date: epoch + chrono::Duration::days(filing_date.value(i) as i64),
                acceptance_datetime_ns: (!acceptance.is_null(i)).then(|| acceptance.value(i)),
                accession_number: opt_str(url, i)
                    .map(|u| u.rsplit('/').next().unwrap_or("").to_string())
                    .filter(|a| !a.is_empty()),
                timeframe: opt_str(timeframe, i),
                fiscal_period: opt_str(fiscal_period, i),
                fiscal_year: opt_str(fiscal_year, i),
            });
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
    fn write_roundtrip_and_metadata_pin() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("f.parquet");
        let rows = vec![FinancialRow {
            ticker: "AAPL".into(),
            cik: Some("0000320193".into()),
            sic_from_filing: Some("3571".into()),
            company_name: Some("Apple Inc.".into()),
            start_date: NaiveDate::from_ymd_opt(2025, 12, 28),
            end_date: NaiveDate::from_ymd_opt(2026, 3, 28),
            filing_date: NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),
            acceptance_datetime_ns: parse_acceptance_ns("2026-05-01T10:01:00Z"),
            timeframe: Some("quarterly".into()),
            fiscal_period: Some("Q2".into()),
            fiscal_year: Some("2026".into()),
            source_filing_url: Some("https://example".into()),
            basic_average_shares: Some(14_673_278_000.0),
            diluted_average_shares: Some(14_725_873_000.0),
            basic_earnings_per_share: Some(2.02),
            diluted_earnings_per_share: Some(2.01),
            revenues: Some(111_184_000_000.0),
            net_income_loss: Some(29_578_000_000.0),
            equity: Some(106_491_000_000.0),
            assets: Some(371_082_000_000.0),
            financials_json: Some(r#"{"income_statement":{}}"#.into()),
        }];
        let mut figi = HashMap::new();
        figi.insert("AAPL".into(), "BBG000B9XRY4".into());
        let snap = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();
        write_financials(&path, &rows, snap, &figi).unwrap();
        assert_eq!(read_snapshot_date(&path).unwrap(), Some(snap));

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let b = &reader.collect::<Result<Vec<_>, _>>().unwrap()[0];
        assert_eq!(b.num_rows(), 1);
    }
}
