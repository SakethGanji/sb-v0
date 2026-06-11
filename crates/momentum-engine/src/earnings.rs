//! `earnings_calendar` derivation (RFC §11) — from SEC filings already
//! on disk (`financials.parquet` + `acceptance_datetime_backfill.parquet`).
//! No new ingest.
//!
//! ## Semantics
//!
//! - `earnings_date` = the date (ET) the period's results became public:
//!   the EARLIEST acceptance timestamp among the period's filings
//!   (original + amendments), falling back to `filing_date` when no
//!   acceptance timestamp exists on either source (~0.2%).
//! - `report_timing` = ET wall-clock of that acceptance:
//!   `before_open` (< 09:30) / `during_market` (09:30–16:00) /
//!   `after_close` / `unknown` (filing-date fallback).
//! - `announced_at_date` = same date (kept as its own column per the
//!   point-in-time contract; identical to `earnings_date` under this
//!   source — a scheduled-earnings feed could later make them differ).
//! - `earnings_window` = fiscal period label (`Q1`…`Q4`, `FY`).
//! - `revision_count` = filings for the period beyond the first
//!   (amendments).
//! - `source` = `acceptance` / `edgar_backfill` / `filing_date`.
//!
//! The SEC acceptance timestamp is the moment the 10-Q/10-K hit EDGAR —
//! a *conservative proxy* for the earnings announcement (press releases
//! usually precede the filing by minutes-to-days). Documented limitation,
//! carried by the `source` column.
//!
//! ## What this source can NOT provide
//!
//! `days_to_next_known_earnings` requires a forward-looking *scheduled*
//! earnings feed; SEC data only records announcements after the fact.
//! That column stays null in Phase 0 (deferred-ingest list) rather than
//! being faked with lookahead.

use arrow::array::{ArrayRef, Date32Array, Int32Array, RecordBatch, StringArray};
use arrow::error::ArrowError;
use chrono::{NaiveDate, TimeZone, Timelike, Utc};
use chrono_tz::America::New_York;
use momentum_core::phase0_outputs::earnings_calendar_schema;
use momentum_store::financials::FilingLite;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct EarningsEvent {
    pub security_id: String,
    pub earnings_date: NaiveDate,
    pub report_timing: String,
    pub announced_at_date: NaiveDate,
    pub earnings_window: String,
    pub revision_count: i32,
    pub source: String,
}

fn timing_of(ns: i64) -> &'static str {
    let et = Utc.timestamp_nanos(ns).with_timezone(&New_York);
    let mins = et.hour() * 60 + et.minute();
    if mins < 9 * 60 + 30 {
        "before_open"
    } else if mins < 16 * 60 {
        "during_market"
    } else {
        "after_close"
    }
}

fn et_date(ns: i64) -> NaiveDate {
    Utc.timestamp_nanos(ns).with_timezone(&New_York).date_naive()
}

/// Derive the calendar. `backfill` maps EDGAR accession numbers to
/// acceptance timestamps for rows whose own `acceptance_datetime` is null.
pub fn derive(filings: &[FilingLite], backfill: &HashMap<String, i64>) -> Vec<EarningsEvent> {
    // One announcement candidate per filing row.
    struct Cand {
        date: NaiveDate,
        ns: Option<i64>,
        timing: &'static str,
        source: &'static str,
        window: String,
    }

    // Group filings by (sid, fiscal identity). Rows without a fiscal
    // identity can't be grouped into a period — each becomes its own
    // single-filing event (keyed uniquely by index).
    let mut groups: HashMap<(String, String), Vec<Cand>> = HashMap::new();
    for (i, f) in filings.iter().enumerate() {
        // Quarterly + annual report filings only; TTM rows are
        // derived aggregates, not announcement events.
        match f.timeframe.as_deref() {
            Some("quarterly") | Some("annual") => {}
            _ => continue,
        }
        let sid = f
            .security_id
            .clone()
            .unwrap_or_else(|| f.ticker.clone());
        let period_key = match (&f.fiscal_year, &f.fiscal_period) {
            (Some(y), Some(p)) => format!("{y}-{p}"),
            _ => format!("row-{i}"),
        };
        let ns = f.acceptance_datetime_ns.or_else(|| {
            f.accession_number.as_ref().and_then(|a| backfill.get(a)).copied()
        });
        let source = if f.acceptance_datetime_ns.is_some() {
            "acceptance"
        } else if ns.is_some() {
            "edgar_backfill"
        } else {
            "filing_date"
        };
        let (date, timing) = match ns {
            Some(ns) => (et_date(ns), timing_of(ns)),
            None => (f.filing_date, "unknown"),
        };
        groups.entry((sid, period_key)).or_default().push(Cand {
            date,
            ns,
            timing,
            source,
            window: f.fiscal_period.clone().unwrap_or_else(|| "unknown".into()),
        });
    }

    // First announcement per period; later filings are revisions.
    let mut events: Vec<EarningsEvent> = Vec::with_capacity(groups.len());
    for ((sid, _), mut cands) in groups {
        cands.sort_by_key(|c| (c.date, c.ns));
        let n = cands.len();
        let first = cands.into_iter().next().expect("non-empty group");
        events.push(EarningsEvent {
            security_id: sid,
            earnings_date: first.date,
            report_timing: first.timing.to_string(),
            announced_at_date: first.date,
            earnings_window: first.window,
            revision_count: (n - 1) as i32,
            source: first.source.to_string(),
        });
    }

    // Enforce the (security_id, earnings_date) grain: a 10-K announces
    // Q4 and FY together. Prefer the quarterly label; keep the larger
    // revision count.
    events.sort_by(|a, b| {
        (&a.security_id, a.earnings_date, &a.earnings_window)
            .cmp(&(&b.security_id, b.earnings_date, &b.earnings_window))
    });
    let mut deduped: Vec<EarningsEvent> = Vec::with_capacity(events.len());
    for e in events {
        match deduped.last_mut() {
            Some(last)
                if last.security_id == e.security_id
                    && last.earnings_date == e.earnings_date =>
            {
                // "FY" < "Q1" lexically, so the quarterly variant arrives
                // second and wins the window label.
                if e.earnings_window.starts_with('Q') {
                    last.earnings_window = e.earnings_window;
                }
                last.revision_count = last.revision_count.max(e.revision_count);
            }
            _ => deduped.push(e),
        }
    }
    deduped
}

/// Build the `earnings_calendar` RecordBatch (events must be sorted by
/// (security_id, earnings_date), which [`derive`] guarantees).
pub fn to_batch(events: &[EarningsEvent]) -> Result<RecordBatch, ArrowError> {
    use arrow::array::StringDictionaryBuilder;
    use arrow::datatypes::Int32Type;

    let schema = earnings_calendar_schema();
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let d32 = |d: NaiveDate| (d - epoch).num_days() as i32;

    let mut timing = StringDictionaryBuilder::<Int32Type>::new();
    let mut window = StringDictionaryBuilder::<Int32Type>::new();
    let mut source = StringDictionaryBuilder::<Int32Type>::new();
    for e in events {
        timing.append_value(&e.report_timing);
        window.append_value(&e.earnings_window);
        source.append_value(&e.source);
    }

    let cols: Vec<ArrayRef> = vec![
        std::sync::Arc::new(StringArray::from(
            events.iter().map(|e| e.security_id.as_str()).collect::<Vec<_>>(),
        )),
        std::sync::Arc::new(Date32Array::from(
            events.iter().map(|e| d32(e.earnings_date)).collect::<Vec<_>>(),
        )),
        std::sync::Arc::new(timing.finish()),
        std::sync::Arc::new(Date32Array::from(
            events.iter().map(|e| Some(d32(e.announced_at_date))).collect::<Vec<_>>(),
        )),
        std::sync::Arc::new(window.finish()),
        std::sync::Arc::new(Int32Array::from(
            events.iter().map(|e| Some(e.revision_count)).collect::<Vec<_>>(),
        )),
        std::sync::Arc::new(source.finish()),
    ];
    RecordBatch::try_new(schema, cols)
}

/// In-memory lookup for the sweep's earnings-proximity columns.
pub struct EarningsLookup {
    /// sid → (earnings_date, report_timing), sorted by date.
    map: HashMap<String, Vec<(NaiveDate, String)>>,
}

impl EarningsLookup {
    pub fn from_events(events: &[EarningsEvent]) -> Self {
        let mut map: HashMap<String, Vec<(NaiveDate, String)>> = HashMap::new();
        for e in events {
            map.entry(e.security_id.clone())
                .or_default()
                .push((e.earnings_date, e.report_timing.clone()));
        }
        for v in map.values_mut() {
            v.sort_by_key(|(d, _)| *d);
        }
        Self { map }
    }

    /// Read the calendar back from the written parquet file.
    pub fn open(path: &std::path::Path) -> Result<Self, ArrowError> {
        use arrow::array::{Array, AsArray};
        use arrow::compute::cast;
        use arrow::datatypes::{DataType, Date32Type};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = std::fs::File::open(path)
            .map_err(|e| ArrowError::IoError(e.to_string(), e))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?
            .build()
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        let mut events = Vec::new();
        for batch_res in reader {
            let batch = batch_res?;
            let sid = batch.column_by_name("security_id").expect("security_id");
            let sid = sid.as_string::<i32>();
            let date = batch
                .column_by_name("earnings_date")
                .expect("earnings_date")
                .as_primitive::<Date32Type>();
            let timing = cast(
                batch.column_by_name("report_timing").expect("report_timing"),
                &DataType::Utf8,
            )?;
            let timing = timing.as_string::<i32>();
            for i in 0..batch.num_rows() {
                events.push(EarningsEvent {
                    security_id: sid.value(i).to_string(),
                    earnings_date: epoch + chrono::Duration::days(date.value(i) as i64),
                    report_timing: if timing.is_null(i) {
                        "unknown".into()
                    } else {
                        timing.value(i).to_string()
                    },
                    announced_at_date: epoch + chrono::Duration::days(date.value(i) as i64),
                    earnings_window: String::new(),
                    revision_count: 0,
                    source: String::new(),
                });
            }
        }
        Ok(Self::from_events(&events))
    }

    /// Calendar days since the most recent earnings on record with
    /// `earnings_date <= day` (0 = announced today; see `report_timing`
    /// for intra-day safety). None = no earnings on record yet.
    pub fn days_since_last(&self, sid: &str, day: NaiveDate) -> Option<i32> {
        let v = self.map.get(sid)?;
        let idx = v.partition_point(|(d, _)| *d <= day);
        (idx > 0).then(|| (day - v[idx - 1].0).num_days() as i32)
    }

    /// (is_earnings_day, report_timing) for `day`.
    pub fn on_day(&self, sid: &str, day: NaiveDate) -> (bool, Option<&str>) {
        match self.map.get(sid) {
            Some(v) => match v.binary_search_by_key(&day, |(d, _)| *d) {
                Ok(i) => (true, Some(v[i].1.as_str())),
                Err(_) => (false, None),
            },
            None => (false, None),
        }
    }

    pub fn securities(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use momentum_engine_test_helpers::*;

    mod momentum_engine_test_helpers {
        use super::FilingLite;
        use chrono::NaiveDate;

        pub fn filing(
            sid: &str,
            filing_date: (i32, u32, u32),
            acceptance_ns: Option<i64>,
            accession: Option<&str>,
            timeframe: &str,
            fy: &str,
            fp: &str,
        ) -> FilingLite {
            FilingLite {
                security_id: Some(sid.into()),
                ticker: sid.into(),
                filing_date: NaiveDate::from_ymd_opt(filing_date.0, filing_date.1, filing_date.2)
                    .unwrap(),
                acceptance_datetime_ns: acceptance_ns,
                accession_number: accession.map(|s| s.to_string()),
                timeframe: Some(timeframe.into()),
                fiscal_period: Some(fp.into()),
                fiscal_year: Some(fy.into()),
            }
        }
    }

    fn ns_at_et(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> i64 {
        use chrono_tz::America::New_York;
        chrono::TimeZone::with_ymd_and_hms(&New_York, y, mo, d, h, mi, 0)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
    }

    #[test]
    fn derive_groups_revisions_and_classifies_timing() {
        // Original 10-Q accepted 2021-07-27 16:32 ET, amended a week later.
        let filings = vec![
            filing("BBG1", (2021, 7, 27), Some(ns_at_et(2021, 7, 27, 16, 32)), None, "quarterly", "2021", "Q2"),
            filing("BBG1", (2021, 8, 3), Some(ns_at_et(2021, 8, 3, 9, 1)), None, "quarterly", "2021", "Q2"),
        ];
        let events = derive(&filings, &HashMap::new());
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.earnings_date, NaiveDate::from_ymd_opt(2021, 7, 27).unwrap());
        assert_eq!(e.report_timing, "after_close");
        assert_eq!(e.revision_count, 1);
        assert_eq!(e.earnings_window, "Q2");
        assert_eq!(e.source, "acceptance");
    }

    #[test]
    fn derive_uses_backfill_then_filing_date_fallback_and_skips_ttm() {
        let mut backfill = HashMap::new();
        backfill.insert("acc-123".to_string(), ns_at_et(2019, 2, 21, 7, 45));
        let filings = vec![
            // acceptance null → backfill hit → before_open
            filing("BBG2", (2019, 2, 21), None, Some("acc-123"), "annual", "2018", "FY"),
            // acceptance null, no backfill → filing_date + unknown
            filing("BBG3", (2020, 5, 11), None, Some("acc-999"), "quarterly", "2020", "Q1"),
            // ttm rows never become events
            filing("BBG3", (2020, 5, 11), None, None, "ttm", "2020", "Q1"),
        ];
        let events = derive(&filings, &backfill);
        assert_eq!(events.len(), 2);
        let b2 = events.iter().find(|e| e.security_id == "BBG2").unwrap();
        assert_eq!(b2.report_timing, "before_open");
        assert_eq!(b2.source, "edgar_backfill");
        let b3 = events.iter().find(|e| e.security_id == "BBG3").unwrap();
        assert_eq!(b3.report_timing, "unknown");
        assert_eq!(b3.source, "filing_date");
        assert_eq!(b3.earnings_date, NaiveDate::from_ymd_opt(2020, 5, 11).unwrap());
    }

    #[test]
    fn q4_and_fy_on_same_date_collapse_to_quarterly_label() {
        let ns = Some(ns_at_et(2022, 2, 1, 17, 0));
        let filings = vec![
            filing("BBG4", (2022, 2, 1), ns, None, "annual", "2021", "FY"),
            filing("BBG4", (2022, 2, 1), ns, None, "quarterly", "2021", "Q4"),
        ];
        let events = derive(&filings, &HashMap::new());
        assert_eq!(events.len(), 1, "grain is (sid, earnings_date)");
        assert_eq!(events[0].earnings_window, "Q4");
    }

    #[test]
    fn batch_matches_schema_and_lookup_answers_proximity() {
        let filings = vec![
            filing("BBG1", (2021, 4, 28), Some(ns_at_et(2021, 4, 28, 16, 31)), None, "quarterly", "2021", "Q1"),
            filing("BBG1", (2021, 7, 27), Some(ns_at_et(2021, 7, 27, 16, 32)), None, "quarterly", "2021", "Q2"),
        ];
        let events = derive(&filings, &HashMap::new());
        let batch = to_batch(&events).unwrap();
        assert_eq!(batch.schema(), earnings_calendar_schema());
        assert_eq!(batch.num_rows(), 2);

        let lk = EarningsLookup::from_events(&events);
        let day = NaiveDate::from_ymd_opt(2021, 8, 6).unwrap();
        assert_eq!(lk.days_since_last("BBG1", day), Some(10));
        let (is_day, timing) =
            lk.on_day("BBG1", NaiveDate::from_ymd_opt(2021, 7, 27).unwrap());
        assert!(is_day);
        assert_eq!(timing, Some("after_close"));
        assert_eq!(lk.days_since_last("BBG1", NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()), None);
        assert_eq!(lk.days_since_last("NOPE", day), None);
    }
}
