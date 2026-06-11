//! `figi_map.parquet` — derived rename-aware resolver. Given a
//! `SecurityId` and a query date, returns the `display_symbol` that was
//! valid at that point in time.
//!
//! Built from `ticker_events.parquet` (Massive's rename chain) + the
//! current universe (`tickers.parquet`). One row per validity window:
//! `(security_id, display_symbol, valid_from, valid_to)` with
//! `valid_from = NULL` meaning open-start and `valid_to = NULL` meaning
//! current.
//!
//! ## Why this is M-A2, not part of M4 ingest
//!
//! `ticker_events.parquet` is the raw API response (one row per rename
//! event). The bar reader wants the *inverse* shape — given a sid and a
//! day, which ticker was it called then? This file is the inverted
//! index. It's pure derivation, so it builds locally without hitting
//! Massive again.
//!
//! ## Resolution semantics
//!
//! - Security has NO events: emit one (NULL, NULL) row with current
//!   display_symbol. Resolver returns that symbol for any query day.
//! - Security has K events: emit K rows. Row i covers
//!   `[events[i].date, events[i+1].date)` with
//!   `display_symbol = events[i].new_ticker`. The last row has
//!   `valid_to = NULL` (current). Query days strictly before
//!   `events[0].date` resolve to `None` — the security didn't carry a
//!   ticker yet under Massive's history.

use crate::WriteError;
use arrow::array::{Array, ArrayRef, AsArray, Date32Array, RecordBatch, StringArray};
use arrow::datatypes::Date32Type;
use chrono::NaiveDate;
use momentum_core::ids::SecurityId;
use momentum_core::schema::{FIGI_MAP_SNAPSHOT_DATE_META, figi_map_schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FigiMapRow {
    pub security_id: String,
    pub display_symbol: String,
    pub valid_from: Option<NaiveDate>,
    pub valid_to: Option<NaiveDate>,
}

fn date32_value(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date");
    (d - epoch).num_days() as i32
}

fn date32_to_naive(v: i32) -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch") + chrono::Duration::days(v as i64)
}

/// One rename event from `ticker_events.parquet`, post-grouping.
#[derive(Debug, Clone)]
struct EventEntry {
    date: NaiveDate,
    new_ticker: String,
}

/// Read `ticker_events.parquet` and group events by `security_id`.
fn load_events_grouped(
    path: &Path,
) -> Result<HashMap<String, Vec<EventEntry>>, WriteError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut by_sid: HashMap<String, Vec<EventEntry>> = HashMap::new();
    for batch_res in reader {
        let batch = batch_res?;
        let sids = batch
            .column_by_name("security_id")
            .expect("security_id")
            .as_string::<i32>();
        let dates = batch
            .column_by_name("event_date")
            .expect("event_date")
            .as_primitive::<Date32Type>();
        let new_tickers = batch
            .column_by_name("new_ticker")
            .expect("new_ticker")
            .as_string::<i32>();
        for i in 0..batch.num_rows() {
            let sid = sids.value(i).to_string();
            let date = date32_to_naive(dates.value(i));
            let new_ticker = new_tickers.value(i).to_string();
            by_sid
                .entry(sid)
                .or_default()
                .push(EventEntry { date, new_ticker });
        }
    }
    for v in by_sid.values_mut() {
        v.sort_by(|a, b| a.date.cmp(&b.date).then_with(|| a.new_ticker.cmp(&b.new_ticker)));
    }
    Ok(by_sid)
}

/// Read `tickers.parquet` and gather the current `display_symbol` for
/// every row carrying a non-null `security_id`.
fn load_current_symbols(path: &Path) -> Result<HashMap<String, String>, WriteError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut out: HashMap<String, String> = HashMap::new();
    for batch_res in reader {
        let batch = batch_res?;
        let sids = batch
            .column_by_name("security_id")
            .expect("security_id")
            .as_string::<i32>();
        let symbols = batch
            .column_by_name("display_symbol")
            .expect("display_symbol")
            .as_string::<i32>();
        for i in 0..batch.num_rows() {
            if sids.is_null(i) {
                continue;
            }
            // Last-write-wins on duplicate sids; universe build should
            // already be deduped by then.
            out.insert(sids.value(i).to_string(), symbols.value(i).to_string());
        }
    }
    Ok(out)
}

/// Build the full `figi_map` row set from the two inputs.
///
/// Every security_id present in the universe (with FIGI) gets at least
/// one row. Securities that appear in events but not in the universe
/// also get rows — defensive, since an event without a universe entry
/// still describes a valid historical naming.
pub fn build_figi_map(
    ticker_events_path: &Path,
    tickers_path: &Path,
) -> Result<Vec<FigiMapRow>, WriteError> {
    let events = load_events_grouped(ticker_events_path)?;
    let current = load_current_symbols(tickers_path)?;

    let mut rows: Vec<FigiMapRow> = Vec::new();

    // Pass 1: every sid in the universe.
    for (sid, current_symbol) in &current {
        if let Some(chain) = events.get(sid) {
            emit_chain(&mut rows, sid, chain);
        } else {
            rows.push(FigiMapRow {
                security_id: sid.clone(),
                display_symbol: current_symbol.clone(),
                valid_from: None,
                valid_to: None,
            });
        }
    }

    // Pass 2: any sid only present in events (no universe row).
    for (sid, chain) in &events {
        if !current.contains_key(sid) {
            emit_chain(&mut rows, sid, chain);
        }
    }

    // Deterministic order — makes the file byte-stable across builds for
    // the same inputs, which is convenient for audit and CI.
    rows.sort_by(|a, b| {
        a.security_id
            .cmp(&b.security_id)
            .then_with(|| match (a.valid_from, b.valid_from) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (Some(x), Some(y)) => x.cmp(&y),
            })
    });

    Ok(rows)
}

fn emit_chain(out: &mut Vec<FigiMapRow>, sid: &str, chain: &[EventEntry]) {
    for (i, ev) in chain.iter().enumerate() {
        let valid_to = chain.get(i + 1).map(|next| next.date);
        out.push(FigiMapRow {
            security_id: sid.to_string(),
            display_symbol: ev.new_ticker.clone(),
            valid_from: Some(ev.date),
            valid_to,
        });
    }
}

fn build_batch(rows: &[FigiMapRow]) -> Result<RecordBatch, WriteError> {
    let schema = figi_map_schema();

    let security_id: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.security_id.as_str()),
    ));
    let display_symbol: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.display_symbol.as_str()),
    ));
    let valid_from: ArrayRef = Arc::new(Date32Array::from_iter(
        rows.iter().map(|r| r.valid_from.map(date32_value)),
    ));
    let valid_to: ArrayRef = Arc::new(Date32Array::from_iter(
        rows.iter().map(|r| r.valid_to.map(date32_value)),
    ));

    Ok(RecordBatch::try_new(
        schema,
        vec![security_id, display_symbol, valid_from, valid_to],
    )?)
}

pub fn write_figi_map(
    path: &Path,
    rows: &[FigiMapRow],
    snapshot_date: NaiveDate,
) -> Result<(), WriteError> {
    let batch = build_batch(rows)?;
    let schema = figi_map_schema();
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_key_value_metadata(Some(vec![KeyValue {
            key: FIGI_MAP_SNAPSHOT_DATE_META.to_string(),
            value: Some(snapshot_date.to_string()),
        }]))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    let _ = writer.close()?;
    Ok(())
}

/// One window within a security_id's resolution chain. Pre-sorted by
/// `valid_from` (None first).
#[derive(Debug, Clone)]
struct Window {
    valid_from: Option<NaiveDate>,
    valid_to: Option<NaiveDate>,
    display_symbol: String,
}

/// In-memory resolver. Construct via `FigiMap::open` (reads parquet) or
/// `FigiMap::from_rows` (tests).
#[derive(Debug, Clone, Default)]
pub struct FigiMap {
    by_sid: HashMap<String, Vec<Window>>,
    snapshot_date: Option<NaiveDate>,
}

impl FigiMap {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_rows(rows: Vec<FigiMapRow>) -> Self {
        let mut by_sid: HashMap<String, Vec<Window>> = HashMap::new();
        for r in rows {
            by_sid
                .entry(r.security_id)
                .or_default()
                .push(Window {
                    valid_from: r.valid_from,
                    valid_to: r.valid_to,
                    display_symbol: r.display_symbol,
                });
        }
        for v in by_sid.values_mut() {
            v.sort_by(|a, b| match (a.valid_from, b.valid_from) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (Some(x), Some(y)) => x.cmp(&y),
            });
        }
        Self {
            by_sid,
            snapshot_date: None,
        }
    }

    pub fn open(path: &Path) -> Result<Self, WriteError> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let snapshot_date = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == FIGI_MAP_SNAPSHOT_DATE_META)
                    .and_then(|kv| kv.value.as_deref())
                    .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
            });

        let reader = builder.build()?;
        let mut rows: Vec<FigiMapRow> = Vec::new();
        for batch_res in reader {
            let batch = batch_res?;
            let sids = batch
                .column_by_name("security_id")
                .expect("security_id")
                .as_string::<i32>();
            let symbols = batch
                .column_by_name("display_symbol")
                .expect("display_symbol")
                .as_string::<i32>();
            let from = batch
                .column_by_name("valid_from")
                .expect("valid_from")
                .as_primitive::<Date32Type>();
            let to = batch
                .column_by_name("valid_to")
                .expect("valid_to")
                .as_primitive::<Date32Type>();
            for i in 0..batch.num_rows() {
                rows.push(FigiMapRow {
                    security_id: sids.value(i).to_string(),
                    display_symbol: symbols.value(i).to_string(),
                    valid_from: if from.is_null(i) {
                        None
                    } else {
                        Some(date32_to_naive(from.value(i)))
                    },
                    valid_to: if to.is_null(i) {
                        None
                    } else {
                        Some(date32_to_naive(to.value(i)))
                    },
                });
            }
        }
        let mut s = Self::from_rows(rows);
        s.snapshot_date = snapshot_date;
        Ok(s)
    }

    pub fn snapshot_date(&self) -> Option<NaiveDate> {
        self.snapshot_date
    }

    pub fn is_empty(&self) -> bool {
        self.by_sid.is_empty()
    }

    pub fn len(&self) -> usize {
        self.by_sid.values().map(Vec::len).sum()
    }

    /// Return the `display_symbol` that was valid for this `SecurityId`
    /// on `day`. `valid_from` is inclusive; `valid_to` is exclusive.
    /// Returns `None` if no window covers the day — either the
    /// security_id is unknown, or `day` falls before the earliest
    /// observed rename event for that sid.
    pub fn resolve(&self, sid: &SecurityId, day: NaiveDate) -> Option<&str> {
        let windows = self.by_sid.get(sid.as_str())?;
        for w in windows {
            let after_start = w.valid_from.map(|d| day >= d).unwrap_or(true);
            let before_end = w.valid_to.map(|d| day < d).unwrap_or(true);
            if after_start && before_end {
                return Some(w.display_symbol.as_str());
            }
        }
        None
    }

    /// True when a new display symbol became effective for this
    /// `SecurityId` exactly on `day` — i.e. some window has
    /// `valid_from == Some(day)`. Open-start windows (`valid_from =
    /// NULL`) never match: they carry no rename event, only the
    /// security's original/current name.
    pub fn renamed_on(&self, sid: &SecurityId, day: NaiveDate) -> bool {
        self.by_sid
            .get(sid.as_str())
            .is_some_and(|windows| windows.iter().any(|w| w.valid_from == Some(day)))
    }

    /// Current display symbol — the one in the open-ended `valid_to =
    /// NULL` window. Useful for callers that don't have a specific date.
    pub fn current(&self, sid: &SecurityId) -> Option<&str> {
        let windows = self.by_sid.get(sid.as_str())?;
        windows
            .iter()
            .rev()
            .find(|w| w.valid_to.is_none())
            .map(|w| w.display_symbol.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ticker_events::{TickerEventRow, write_ticker_events};
    use crate::tickers::{TickerRow, write_tickers};
    use chrono::{TimeZone, Utc};
    use tempfile::tempdir;

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    fn ev(sid: &str, date: &str, new_t: &str) -> TickerEventRow {
        TickerEventRow {
            security_id: sid.into(),
            name: None,
            event_date: d(date),
            event_type: "ticker_change".into(),
            new_ticker: new_t.into(),
        }
    }

    fn ticker(sym: &str, figi: Option<&str>, active: bool) -> TickerRow {
        TickerRow {
            display_symbol: sym.into(),
            name: Some(format!("{sym} Inc.")),
            market: Some("stocks".into()),
            locale: Some("us".into()),
            primary_exchange: Some("XNYS".into()),
            ticker_type: Some("CS".into()),
            active,
            currency_name: Some("usd".into()),
            cik: None,
            composite_figi: figi.map(String::from),
            share_class_figi: None,
            last_updated_utc: Some(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()),
            delisted_utc: None,
        }
    }

    fn build_inputs(
        dir: &Path,
        events: &[TickerEventRow],
        tickers: &[TickerRow],
    ) -> (std::path::PathBuf, std::path::PathBuf) {
        let ev_path = dir.join("ticker_events.parquet");
        let tk_path = dir.join("tickers.parquet");
        write_ticker_events(&ev_path, events, d("2026-06-07")).unwrap();
        write_tickers(&tk_path, tickers, d("2026-06-07")).unwrap();
        (ev_path, tk_path)
    }

    #[test]
    fn security_with_no_events_gets_one_open_row() {
        let dir = tempdir().unwrap();
        let (ev, tk) = build_inputs(
            dir.path(),
            &[],
            &[ticker("AAPL", Some("BBG000B9XRY4"), true)],
        );
        let rows = build_figi_map(&ev, &tk).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].security_id, "BBG000B9XRY4");
        assert_eq!(rows[0].display_symbol, "AAPL");
        assert_eq!(rows[0].valid_from, None);
        assert_eq!(rows[0].valid_to, None);
    }

    #[test]
    fn rename_chain_emits_consecutive_windows() {
        // FB→META: two events, expect two rows. First covers
        // [2012-05-18, 2022-06-09), second [2022-06-09, NULL).
        let dir = tempdir().unwrap();
        let (ev, tk) = build_inputs(
            dir.path(),
            &[
                ev("BBG000MM2P62", "2012-05-18", "FB"),
                ev("BBG000MM2P62", "2022-06-09", "META"),
            ],
            &[ticker("META", Some("BBG000MM2P62"), true)],
        );
        let rows = build_figi_map(&ev, &tk).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].display_symbol, "FB");
        assert_eq!(rows[0].valid_from, Some(d("2012-05-18")));
        assert_eq!(rows[0].valid_to, Some(d("2022-06-09")));
        assert_eq!(rows[1].display_symbol, "META");
        assert_eq!(rows[1].valid_from, Some(d("2022-06-09")));
        assert_eq!(rows[1].valid_to, None);
    }

    #[test]
    fn resolver_picks_window_by_query_date() {
        let rows = vec![
            FigiMapRow {
                security_id: "BBG000MM2P62".into(),
                display_symbol: "FB".into(),
                valid_from: Some(d("2012-05-18")),
                valid_to: Some(d("2022-06-09")),
            },
            FigiMapRow {
                security_id: "BBG000MM2P62".into(),
                display_symbol: "META".into(),
                valid_from: Some(d("2022-06-09")),
                valid_to: None,
            },
        ];
        let m = FigiMap::from_rows(rows);
        let sid = SecurityId::new("BBG000MM2P62");
        // Day-of rename: rename inclusive at start, exclusive at end.
        assert_eq!(m.resolve(&sid, d("2022-06-08")), Some("FB"));
        assert_eq!(m.resolve(&sid, d("2022-06-09")), Some("META"));
        assert_eq!(m.resolve(&sid, d("2026-06-05")), Some("META"));
        // Before earliest event: None — Massive has no name for it.
        assert_eq!(m.resolve(&sid, d("2010-01-01")), None);
        // Unknown sid.
        assert_eq!(m.resolve(&SecurityId::new("UNKNOWN"), d("2022-06-09")), None);
    }

    #[test]
    fn resolver_handles_open_ended_single_row() {
        let rows = vec![FigiMapRow {
            security_id: "BBG000B9XRY4".into(),
            display_symbol: "AAPL".into(),
            valid_from: None,
            valid_to: None,
        }];
        let m = FigiMap::from_rows(rows);
        let sid = SecurityId::new("BBG000B9XRY4");
        assert_eq!(m.resolve(&sid, d("1990-01-01")), Some("AAPL"));
        assert_eq!(m.resolve(&sid, d("2026-06-05")), Some("AAPL"));
    }

    #[test]
    fn write_roundtrip_preserves_rows_and_pin() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("figi_map.parquet");
        let rows = vec![
            FigiMapRow {
                security_id: "BBG000MM2P62".into(),
                display_symbol: "FB".into(),
                valid_from: Some(d("2012-05-18")),
                valid_to: Some(d("2022-06-09")),
            },
            FigiMapRow {
                security_id: "BBG000MM2P62".into(),
                display_symbol: "META".into(),
                valid_from: Some(d("2022-06-09")),
                valid_to: None,
            },
            FigiMapRow {
                security_id: "BBG000B9XRY4".into(),
                display_symbol: "AAPL".into(),
                valid_from: None,
                valid_to: None,
            },
        ];
        let snap = d("2026-06-07");
        write_figi_map(&path, &rows, snap).unwrap();

        let m = FigiMap::open(&path).unwrap();
        assert_eq!(m.snapshot_date(), Some(snap));
        assert_eq!(m.len(), 3);
        assert_eq!(
            m.resolve(&SecurityId::new("BBG000MM2P62"), d("2018-01-01")),
            Some("FB")
        );
        assert_eq!(
            m.resolve(&SecurityId::new("BBG000MM2P62"), d("2023-01-01")),
            Some("META")
        );
        assert_eq!(
            m.resolve(&SecurityId::new("BBG000B9XRY4"), d("2018-01-01")),
            Some("AAPL")
        );
    }

    #[test]
    fn renamed_on_matches_only_window_start_days() {
        let rows = vec![
            FigiMapRow {
                security_id: "BBG000MM2P62".into(),
                display_symbol: "FB".into(),
                valid_from: Some(d("2012-05-18")),
                valid_to: Some(d("2022-06-09")),
            },
            FigiMapRow {
                security_id: "BBG000MM2P62".into(),
                display_symbol: "META".into(),
                valid_from: Some(d("2022-06-09")),
                valid_to: None,
            },
            FigiMapRow {
                security_id: "BBG000B9XRY4".into(),
                display_symbol: "AAPL".into(),
                valid_from: None,
                valid_to: None,
            },
        ];
        let m = FigiMap::from_rows(rows);
        let meta = SecurityId::new("BBG000MM2P62");
        // Both event-effective days hit.
        assert!(m.renamed_on(&meta, d("2012-05-18")));
        assert!(m.renamed_on(&meta, d("2022-06-09")));
        // A day inside a window (not its start) does not.
        assert!(!m.renamed_on(&meta, d("2022-06-08")));
        assert!(!m.renamed_on(&meta, d("2022-06-10")));
        // Open-start row: no rename event, never matches.
        assert!(!m.renamed_on(&SecurityId::new("BBG000B9XRY4"), d("2012-05-18")));
        // Unknown sid.
        assert!(!m.renamed_on(&SecurityId::new("UNKNOWN"), d("2022-06-09")));
    }

    #[test]
    fn current_returns_only_open_ended_window() {
        let rows = vec![
            FigiMapRow {
                security_id: "X".into(),
                display_symbol: "OLD".into(),
                valid_from: Some(d("2010-01-01")),
                valid_to: Some(d("2020-01-01")),
            },
            FigiMapRow {
                security_id: "X".into(),
                display_symbol: "NEW".into(),
                valid_from: Some(d("2020-01-01")),
                valid_to: None,
            },
        ];
        let m = FigiMap::from_rows(rows);
        assert_eq!(m.current(&SecurityId::new("X")), Some("NEW"));
    }
}
