use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::sync::{Arc, OnceLock};

fn utc_ns() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
}

fn dict_str() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

pub fn bars_1m_raw_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("security_id", DataType::Utf8, true),
                Field::new("display_symbol", DataType::Utf8, false),
                Field::new("t", utc_ns(), false),
                Field::new("open", DataType::Float64, false),
                Field::new("high", DataType::Float64, false),
                Field::new("low", DataType::Float64, false),
                Field::new("close", DataType::Float64, false),
                Field::new("volume", DataType::Float64, false),
                Field::new("transactions", DataType::Int64, true),
            ]))
        })
        .clone()
}

/// `splits.parquet` — historical stock-split events from
/// `GET /stocks/v1/splits`. The file-level Parquet metadata carries
/// `splits_snapshot_date` = ISO date string; that's the pin source for the
/// read-time multiply (§5.3, amended 2026-06-06).
pub fn splits_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("security_id", DataType::Utf8, true),
                Field::new("display_symbol", DataType::Utf8, false),
                Field::new("execution_date", DataType::Date32, false),
                Field::new("split_from", DataType::Float64, false),
                Field::new("split_to", DataType::Float64, false),
                Field::new("adjustment_type", dict_str(), true),
                Field::new("historical_adjustment_factor", DataType::Float64, true),
                Field::new("id", DataType::Utf8, false),
            ]))
        })
        .clone()
}

/// File-level Parquet metadata key under which the splits snapshot date
/// lives. Used by the BarReader to pin the run (refuse `execution_date`
/// values newer than this date — see §5.3).
pub const SPLITS_SNAPSHOT_DATE_META: &str = "splits_snapshot_date";

/// `dividends.parquet` — cash dividend events from
/// `GET /stocks/v1/dividends`. Stored as events joined to per-trade
/// summaries (§5.3.1); NOT folded into bars. The file-level metadata
/// key `dividends_snapshot_date` records when this snapshot was pulled
/// — does not gate any read-time math (the engine doesn't apply
/// dividends), but is useful for cache-invalidation diagnostics.
pub fn dividends_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("security_id", DataType::Utf8, true),
                Field::new("display_symbol", DataType::Utf8, false),
                Field::new("ex_dividend_date", DataType::Date32, false),
                Field::new("pay_date", DataType::Date32, true),
                Field::new("record_date", DataType::Date32, true),
                Field::new("declaration_date", DataType::Date32, true),
                Field::new("cash_amount", DataType::Float64, false),
                Field::new("split_adjusted_cash_amount", DataType::Float64, true),
                Field::new("historical_adjustment_factor", DataType::Float64, true),
                Field::new("currency", DataType::Utf8, true),
                Field::new("distribution_type", dict_str(), true),
                Field::new("frequency", DataType::Int32, true),
                Field::new("id", DataType::Utf8, false),
            ]))
        })
        .clone()
}

pub const DIVIDENDS_SNAPSHOT_DATE_META: &str = "dividends_snapshot_date";

/// `ticker_events.parquet` — rename events from
/// `GET /vX/reference/tickers/{id}/events`. One row per
/// (security_id, event_date, new_ticker). Used downstream to derive
/// `figi_map.parquet` and resolve `(display_symbol, t) → security_id`
/// at engine read time so paths survive renames (FB → META, etc.).
pub fn ticker_events_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("security_id", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("event_date", DataType::Date32, false),
                Field::new("event_type", dict_str(), false),
                Field::new("new_ticker", DataType::Utf8, false),
            ]))
        })
        .clone()
}

pub const TICKER_EVENTS_SNAPSHOT_DATE_META: &str = "ticker_events_snapshot_date";

/// `figi_map.parquet` — derived from `ticker_events.parquet` + the
/// current universe. One row per (security_id, display_symbol,
/// validity-window) so the engine can resolve `SecurityId → ticker` at
/// any historical day. `valid_from = NULL` means open-start (no prior
/// rename observed); `valid_to = NULL` means current.
pub fn figi_map_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("security_id", DataType::Utf8, false),
                Field::new("display_symbol", DataType::Utf8, false),
                Field::new("valid_from", DataType::Date32, true),
                Field::new("valid_to", DataType::Date32, true),
            ]))
        })
        .clone()
}

pub const FIGI_MAP_SNAPSHOT_DATE_META: &str = "figi_map_snapshot_date";

pub fn tickers_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("security_id", DataType::Utf8, true),
                Field::new("display_symbol", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("market", DataType::Utf8, true),
                Field::new("locale", DataType::Utf8, true),
                Field::new("primary_exchange", dict_str(), true),
                Field::new("ticker_type", dict_str(), true),
                Field::new("active", DataType::Boolean, false),
                Field::new("currency_name", DataType::Utf8, true),
                Field::new("cik", DataType::Utf8, true),
                Field::new("composite_figi", DataType::Utf8, true),
                Field::new("share_class_figi", DataType::Utf8, true),
                Field::new("last_updated_utc", utc_ns(), true),
                Field::new("delisted_utc", utc_ns(), true),
                Field::new("as_of_date", DataType::Date32, false),
            ]))
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tickers_schema_is_stable() {
        let a = tickers_schema();
        let b = tickers_schema();
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(a.fields().len(), 15);
    }

    #[test]
    fn bars_1m_raw_schema_is_stable() {
        let a = bars_1m_raw_schema();
        let b = bars_1m_raw_schema();
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(a.fields().len(), 9);
        assert_eq!(
            a.field_with_name("t").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }

    #[test]
    fn splits_schema_is_stable_and_shaped() {
        let a = splits_schema();
        let b = splits_schema();
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(a.fields().len(), 8);
        assert_eq!(
            a.field_with_name("execution_date").unwrap().data_type(),
            &DataType::Date32
        );
        assert_eq!(
            a.field_with_name("security_id").unwrap().is_nullable(),
            true
        );
        assert_eq!(
            a.field_with_name("display_symbol").unwrap().is_nullable(),
            false
        );
    }

    #[test]
    fn timestamps_carry_utc_zone() {
        let s = tickers_schema();
        for name in ["last_updated_utc", "delisted_utc"] {
            let f = s.field_with_name(name).unwrap();
            assert_eq!(
                f.data_type(),
                &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                "field {name} must be timestamp[ns, UTC]"
            );
        }
    }
}
