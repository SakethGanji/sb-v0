use crate::WriteError;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, RecordBatch, StringArray, StringDictionaryBuilder,
    TimestampNanosecondArray,
};
use arrow::datatypes::Int32Type;
use chrono::{DateTime, NaiveDate, Utc};
use momentum_core::schema::tickers_schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TickerRow {
    pub display_symbol: String,
    pub name: Option<String>,
    pub market: Option<String>,
    pub locale: Option<String>,
    pub primary_exchange: Option<String>,
    pub ticker_type: Option<String>,
    pub active: bool,
    pub currency_name: Option<String>,
    pub cik: Option<String>,
    pub composite_figi: Option<String>,
    pub share_class_figi: Option<String>,
    pub last_updated_utc: Option<DateTime<Utc>>,
    pub delisted_utc: Option<DateTime<Utc>>,
}

impl TickerRow {
    pub fn security_id(&self) -> Option<&str> {
        self.composite_figi
            .as_deref()
            .or(self.share_class_figi.as_deref())
    }
}

const EPOCH_DAYS_OFFSET: i32 = 0;

fn date32_value(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date");
    (d - epoch).num_days() as i32 + EPOCH_DAYS_OFFSET
}

fn ts_ns(ts: Option<DateTime<Utc>>) -> Option<i64> {
    ts.and_then(|t| t.timestamp_nanos_opt())
}

fn build_batch(rows: &[TickerRow], as_of: NaiveDate) -> Result<RecordBatch, WriteError> {
    let schema = tickers_schema();
    let n = rows.len();

    let security_id: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(TickerRow::security_id),
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

    let mut exch_b = StringDictionaryBuilder::<Int32Type>::new();
    for r in rows {
        match &r.primary_exchange {
            Some(s) => exch_b.append_value(s),
            None => exch_b.append_null(),
        }
    }
    let primary_exchange: ArrayRef = Arc::new(exch_b.finish());

    let mut type_b = StringDictionaryBuilder::<Int32Type>::new();
    for r in rows {
        match &r.ticker_type {
            Some(s) => type_b.append_value(s),
            None => type_b.append_null(),
        }
    }
    let ticker_type: ArrayRef = Arc::new(type_b.finish());

    let active: ArrayRef = Arc::new(BooleanArray::from_iter(
        rows.iter().map(|r| Some(r.active)),
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

    let last_updated: ArrayRef = Arc::new(
        TimestampNanosecondArray::from_iter(rows.iter().map(|r| ts_ns(r.last_updated_utc)))
            .with_timezone("UTC"),
    );
    let delisted: ArrayRef = Arc::new(
        TimestampNanosecondArray::from_iter(rows.iter().map(|r| ts_ns(r.delisted_utc)))
            .with_timezone("UTC"),
    );

    let as_of_v = date32_value(as_of);
    let as_of_date: ArrayRef = Arc::new(Date32Array::from(vec![as_of_v; n]));

    let batch = RecordBatch::try_new(
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
            last_updated,
            delisted,
            as_of_date,
        ],
    )?;
    Ok(batch)
}

pub fn read_security_id_map(
    path: &Path,
) -> Result<std::collections::HashMap<String, String>, WriteError> {
    use arrow::array::{Array, AsArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut map = std::collections::HashMap::new();
    for batch_res in reader {
        let batch = batch_res?;
        let symbols = batch
            .column_by_name("display_symbol")
            .expect("display_symbol column")
            .as_string::<i32>();
        let sids = batch
            .column_by_name("security_id")
            .expect("security_id column")
            .as_string::<i32>();
        for i in 0..batch.num_rows() {
            if sids.is_null(i) {
                continue;
            }
            map.insert(symbols.value(i).to_string(), sids.value(i).to_string());
        }
    }
    Ok(map)
}

pub fn write_tickers(
    path: &Path,
    rows: &[TickerRow],
    as_of: NaiveDate,
) -> Result<(), WriteError> {
    let batch = build_batch(rows, as_of)?;
    let schema = tickers_schema();
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    let _ = writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use chrono::TimeZone;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    fn sample_row(sym: &str, active: bool, figi: Option<&str>) -> TickerRow {
        TickerRow {
            display_symbol: sym.into(),
            name: Some(format!("{sym} Inc.")),
            market: Some("stocks".into()),
            locale: Some("us".into()),
            primary_exchange: Some("XNYS".into()),
            ticker_type: Some("CS".into()),
            active,
            currency_name: Some("usd".into()),
            cik: Some("0000123".into()),
            composite_figi: figi.map(String::from),
            share_class_figi: figi.map(String::from),
            last_updated_utc: Some(Utc.with_ymd_and_hms(2025, 1, 2, 3, 4, 5).unwrap()),
            delisted_utc: if !active {
                Some(Utc.with_ymd_and_hms(2022, 6, 15, 0, 0, 0).unwrap())
            } else {
                None
            },
        }
    }

    #[test]
    fn security_id_fallback() {
        let mut r = sample_row("X", true, None);
        assert_eq!(r.security_id(), None);
        r.share_class_figi = Some("SHARE".into());
        assert_eq!(r.security_id(), Some("SHARE"));
        r.composite_figi = Some("COMP".into());
        assert_eq!(r.security_id(), Some("COMP"));
    }

    #[test]
    fn write_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("tickers.parquet");
        let rows = vec![
            sample_row("AAPL", true, Some("BBG000B9XRY4")),
            sample_row("DEAD", false, Some("BBG000DEAD00")),
            sample_row("NOFIG", true, None),
        ];
        let as_of = NaiveDate::from_ymd_opt(2026, 6, 6).unwrap();
        write_tickers(&path, &rows, as_of).unwrap();

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b.num_rows(), 3);
        assert_eq!(b.schema(), tickers_schema());

        let symbols = b
            .column_by_name("display_symbol")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(symbols.value(0), "AAPL");
        assert_eq!(symbols.value(2), "NOFIG");

        let sid = b.column_by_name("security_id").unwrap().as_string::<i32>();
        assert_eq!(sid.value(0), "BBG000B9XRY4");
        assert!(sid.is_null(2));

        let as_of_col = b
            .column_by_name("as_of_date")
            .unwrap()
            .as_primitive::<arrow::datatypes::Date32Type>();
        assert_eq!(as_of_col.value(0), date32_value(as_of));
    }
}
