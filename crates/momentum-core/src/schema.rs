use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::sync::{Arc, OnceLock};

pub(crate) fn utc_ns() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
}

pub(crate) fn dict_str() -> DataType {
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

/// `vix_daily.parquet` — daily VIX close from FRED's `VIXCLS` series.
/// Used because Massive indices are not entitled on the Stocks Advanced
/// tier. One row per trading day; FRED missing-value rows (`value == "."`)
/// are filtered before write so `vix_close` is non-null.
pub fn vix_daily_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("date", DataType::Date32, false),
                Field::new("vix_close", DataType::Float64, false),
            ]))
        })
        .clone()
}

pub const VIX_SNAPSHOT_DATE_META: &str = "vix_snapshot_date";

/// `tickers_classified.parquet` — per-ticker classification snapshot
/// pulled from Massive's `/v3/reference/tickers/{id}`. Adds SIC
/// industry codes, list date, current-snapshot market cap and shares
/// outstanding, and description text (used downstream to detect
/// leveraged/inverse ETFs heuristically). One row per (security_id,
/// snapshot_date); the snapshot date lives in file-level Parquet
/// metadata under `ticker_details_snapshot_date`.
///
/// **Naming note vs. the RFC.** The observation-pivot RFC (v6) §6.6 / §11.6
/// references "`tickers_enriched.parquet`" as the source of point-in-time
/// classification inputs (`is_etf`, `is_adr`, `sic`-derived sector, etc.).
/// In this repo that role is split across two files:
///
/// - `tickers_enriched.parquet` — minimal universe enrichment (FIGI fill-in
///   + `delisted_utc`). Used as the join key surface (`figi_map` source).
/// - `tickers_classified.parquet` — the richer classification snapshot
///   (this schema). This is the file that feeds the §11.6 behavioral-tag
///   inputs and the `momentum-classify` crate.
///
/// Downstream readers that want "RFC's tickers_enriched" should read this
/// file; the slimmer `tickers_enriched.parquet` is only for the join layer.
///
/// Most fields are nullable because the Massive details endpoint
/// returns sparse data for thinly-covered or long-delisted tickers.
pub fn tickers_classified_schema() -> SchemaRef {
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
                Field::new("active", DataType::Boolean, true),
                Field::new("currency_name", DataType::Utf8, true),
                Field::new("cik", DataType::Utf8, true),
                Field::new("composite_figi", DataType::Utf8, true),
                Field::new("share_class_figi", DataType::Utf8, true),
                Field::new("delisted_utc", utc_ns(), true),
                Field::new("list_date", DataType::Date32, true),
                Field::new("sic_code", DataType::Utf8, true),
                Field::new("sic_description", DataType::Utf8, true),
                Field::new("ticker_root", DataType::Utf8, true),
                Field::new("total_employees", DataType::Int64, true),
                Field::new("market_cap_snapshot", DataType::Float64, true),
                Field::new("share_class_shares_outstanding_snapshot", DataType::Float64, true),
                Field::new("weighted_shares_outstanding_snapshot", DataType::Float64, true),
                Field::new("round_lot", DataType::Int32, true),
                Field::new("description", DataType::Utf8, true),
                Field::new("address_state", DataType::Utf8, true),
                Field::new("address_city", DataType::Utf8, true),
                Field::new("homepage_url", DataType::Utf8, true),
            ]))
        })
        .clone()
}

pub const TICKERS_CLASSIFIED_SNAPSHOT_DATE_META: &str = "ticker_details_snapshot_date";

/// `short_interest.parquet` — FINRA bi-weekly short interest from
/// Massive's `/stocks/v1/short-interest`. One row per
/// (security_id, settlement_date). Used downstream to refine
/// `is_meme_candidate` and `is_low_float_candidate` heuristics
/// (RFC §11.6.3).
///
/// FINRA settlement dates are the 15th and last day of each month (or
/// the preceding trading day). The publish-to-public lag is typically
/// 8 days; for strict point-in-time correctness, downstream readers
/// should use `settlement_date + 8d` as the "available date."
pub fn short_interest_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("security_id", DataType::Utf8, true),
                Field::new("display_symbol", DataType::Utf8, false),
                Field::new("settlement_date", DataType::Date32, false),
                Field::new("short_interest", DataType::Float64, false),
                Field::new("avg_daily_volume", DataType::Float64, true),
                Field::new("days_to_cover", DataType::Float64, true),
            ]))
        })
        .clone()
}

pub const SHORT_INTEREST_SNAPSHOT_DATE_META: &str = "short_interest_snapshot_date";

/// `financials.parquet` — raw SEC filings from Massive's
/// `/vX/reference/financials`. One row per (security_id, fiscal_period,
/// filing_date). The `acceptance_datetime` column is the
/// minute-granularity point-in-time stamp for "when this information
/// became public," and is what downstream code must use to gate any
/// feature derived from a filing.
///
/// Derived tables (built in a later pass, not this one):
/// - `earnings_calendar.parquet` ← project (security_id,
///   acceptance_datetime, fiscal_period). The filing date is a
///   conservative-but-correct point-in-time stamp for the earnings
///   event; press-release date (typically 1-30 days earlier) is not
///   available without a partner add-on.
/// - `shares_outstanding_history.parquet` ← project (security_id,
///   filing_date, diluted_average_shares).
///
/// The `financials_json` column carries the entire nested financials
/// struct as a JSON string so all extractable metrics — not just the
/// ~10 we surface as native columns — are recoverable later without
/// re-pulling.
pub fn financials_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("security_id", DataType::Utf8, true),
                Field::new("ticker", DataType::Utf8, false),
                Field::new("cik", DataType::Utf8, true),
                Field::new("sic_from_filing", DataType::Utf8, true),
                Field::new("company_name", DataType::Utf8, true),
                Field::new("start_date", DataType::Date32, true),
                Field::new("end_date", DataType::Date32, true),
                Field::new("filing_date", DataType::Date32, false),
                // Many older filings lack `acceptance_datetime`; downstream
                // uses `filing_date` end-of-day as the fallback point-in-time stamp.
                Field::new("acceptance_datetime", utc_ns(), true),
                Field::new("timeframe", dict_str(), true),
                Field::new("fiscal_period", dict_str(), true),
                Field::new("fiscal_year", DataType::Utf8, true),
                Field::new("source_filing_url", DataType::Utf8, true),
                Field::new("basic_average_shares", DataType::Float64, true),
                Field::new("diluted_average_shares", DataType::Float64, true),
                Field::new("basic_earnings_per_share", DataType::Float64, true),
                Field::new("diluted_earnings_per_share", DataType::Float64, true),
                Field::new("revenues", DataType::Float64, true),
                Field::new("net_income_loss", DataType::Float64, true),
                Field::new("equity", DataType::Float64, true),
                Field::new("assets", DataType::Float64, true),
                Field::new("financials_json", DataType::Utf8, true),
            ]))
        })
        .clone()
}

pub const FINANCIALS_SNAPSHOT_DATE_META: &str = "financials_snapshot_date";

/// `acceptance_datetime_backfill.parquet` — minute-precision filing
/// acceptance timestamps backfilled from SEC EDGAR for rows where
/// Massive's financials feed left `acceptance_datetime` NULL (~75% of
/// 2011-2022 filings).
///
/// Join pattern: `financials.parquet LEFT JOIN this ON accession_number`.
/// Downstream readers should COALESCE the two columns; SEC is the
/// authoritative source where available.
///
/// `source` is hard-coded "sec_edgar_submissions" today but kept as a
/// column so future backfills from other sources (Polygon partners,
/// proprietary feeds) can land in the same table with provenance.
pub fn acceptance_backfill_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("cik", DataType::Utf8, false),
                Field::new("accession_number", DataType::Utf8, false),
                Field::new("acceptance_datetime", utc_ns(), false),
                Field::new("form", DataType::Utf8, true),
                Field::new("source", dict_str(), false),
            ]))
        })
        .clone()
}

pub const ACCEPTANCE_BACKFILL_SNAPSHOT_DATE_META: &str = "acceptance_backfill_snapshot_date";

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
    fn vix_daily_schema_is_stable_and_shaped() {
        let a = vix_daily_schema();
        let b = vix_daily_schema();
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(a.fields().len(), 2);
        assert_eq!(
            a.field_with_name("date").unwrap().data_type(),
            &DataType::Date32
        );
        assert_eq!(
            a.field_with_name("vix_close").unwrap().data_type(),
            &DataType::Float64
        );
        assert!(!a.field_with_name("vix_close").unwrap().is_nullable());
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
