//! `MaterializedBarReader` — the `BarReader` impl that the engine reads
//! through. Applies split adjustment **at read time** against a pinned
//! `splits.parquet` snapshot (§5.3 amended 2026-06-06).
//!
//! ## Storage shape
//!
//! Per-day Parquet files at `bars_dir/YYYY-MM-DD.parquet`, each
//! containing every ticker's 1-min bars for that day, sorted by
//! `(display_symbol, t)`. Matches the engine's day-major loop:
//! `session_bars(sid, day)` opens one file per day, filters on
//! `display_symbol`. Replaces an earlier per-ticker layout whose
//! global-sort pivot was rejected.
//!
//! ## Contract
//!
//! Construction reads `splits.parquet`, extracts `splits_snapshot_date`
//! from file-level Parquet metadata, and **excludes** any split row whose
//! `execution_date > snapshot_pin` from the factor tables (with a warn
//! log). Vendor snapshots legitimately contain announced-but-not-yet-
//! executed splits; those are not in the tape as of the pin basis, so
//! letting them into `factor_at` would mis-adjust every historical bar.
//! Exclusion is the only treatment consistent with the single-adjustment-
//! baseline decision. This is the only place that enforcement lives —
//! once `MaterializedBarReader` exists, the invariant holds for the
//! lifetime of the run.
//!
//! `session_bars(sid, day)` returns the day's bars **already adjusted**:
//!
//! ```text
//! adjusted_price  = raw_price  × split_factor_at(t)
//! adjusted_volume = raw_volume ÷ split_factor_at(t)
//! ```
//!
//! `split_factor_at(t)` = product of `(split_from / split_to)` for every
//! split for this ticker whose `execution_date > t.date()`. For the
//! most-recent bars this is `1.0` (no future splits). For a bar before
//! Apple's 2020-08-31 4-for-1, it's `0.25`, so the pre-split $400 raw
//! price reads back as $100 adjusted.
//!
//! ## Audit roundtrip
//!
//! `adjusted_price ÷ factor_at(t) == raw_price` within f64 epsilon —
//! tested as a property below.
//!
//! ## Symbol resolution
//!
//! Time-varying via `FigiMap` (built from `ticker_events.parquet`).
//! `session_bars(sid, day)` resolves the sid → display_symbol for
//! `day`, so a trade in a security that renamed (FB → META on
//! 2022-06-09) reads the pre-rename file under "FB" and the post-rename
//! file under "META" without the caller knowing. Sids missing from the
//! map (~21% of active per measured coverage) fall back to using the
//! sid string as the symbol so unconfigured runs don't 404 silently.

use crate::WriteError;
use crate::figi_map::FigiMap;
use crate::splits::read_snapshot_date;
use arrow::array::{Array, AsArray};
use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use momentum_core::bar::Bar;
use momentum_core::error::StoreError;
use momentum_core::ids::SecurityId;
use momentum_core::store::{BarReader, Session};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum BarReaderError {
    #[error("write: {0}")]
    Write(#[from] WriteError),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("splits.parquet missing `splits_snapshot_date` metadata key — refusing to build a reader with no pin")]
    MissingPin,
    #[error("missing day file: {0}")]
    MissingDay(NaiveDate),
}

/// One security's full session for a day, as returned by the bulk
/// [`MaterializedBarReader::day_sessions`] read.
#[derive(Debug)]
pub struct DaySession {
    pub security_id: SecurityId,
    pub display_symbol: String,
    pub session: Session,
}

/// One split's adjustment effect, pre-extracted from `splits.parquet`.
/// `factor = split_from / split_to` per the amended §5.3 math
/// (4-for-1 → 1/4 = 0.25; reverse 1-for-10 → 10/1 = 10).
#[derive(Debug, Clone, Copy)]
struct SplitFactor {
    execution_date: NaiveDate,
    factor: f64,
}

/// Materialized bar reader. Backed by per-day `bars_1m_raw/YYYY-MM-DD.parquet`
/// files, a once-loaded splits map, and a time-varying FigiMap for
/// rename-aware sid → display_symbol resolution.
pub struct MaterializedBarReader {
    bars_dir: PathBuf,
    snapshot_pin: NaiveDate,
    // Primary: split factors keyed by composite FIGI.
    splits_by_sid: HashMap<String, Vec<SplitFactor>>,
    // Fallback: split factors keyed by current display symbol — for
    // rows in splits.parquet whose security_id was null at write time.
    splits_by_symbol: HashMap<String, Vec<SplitFactor>>,
    // sid → time-varying display_symbol resolver.
    figi_map: FigiMap,
}

impl MaterializedBarReader {
    /// Open a reader. Reads `splits.parquet`, pins the snapshot, and
    /// pre-computes per-ticker split factor tables.
    pub fn open(
        bars_dir: &Path,
        splits_parquet: &Path,
        figi_map: FigiMap,
    ) -> Result<Self, BarReaderError> {
        let snapshot_pin = read_snapshot_date(splits_parquet)?
            .ok_or(BarReaderError::MissingPin)?;

        let file = File::open(splits_parquet)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

        let mut splits_by_sid: HashMap<String, Vec<SplitFactor>> = HashMap::new();
        let mut splits_by_symbol: HashMap<String, Vec<SplitFactor>> = HashMap::new();

        for batch_res in reader {
            let batch = batch_res?;
            let sids = batch.column_by_name("security_id").expect("security_id").as_string::<i32>();
            let symbols = batch.column_by_name("display_symbol").expect("display_symbol").as_string::<i32>();
            let dates = batch
                .column_by_name("execution_date")
                .expect("execution_date")
                .as_primitive::<arrow::datatypes::Date32Type>();
            let from = batch
                .column_by_name("split_from")
                .expect("split_from")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("split_from f64");
            let to = batch
                .column_by_name("split_to")
                .expect("split_to")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("split_to f64");

            for i in 0..batch.num_rows() {
                let exec = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                    + chrono::Duration::days(dates.value(i) as i64);
                if exec > snapshot_pin {
                    // Announced-but-not-executed split (vendor snapshots
                    // include these). Not in the tape as of the pin basis
                    // — must NOT contribute to adjustment.
                    tracing::warn!(
                        ticker = symbols.value(i),
                        execution_date = %exec,
                        pin = %snapshot_pin,
                        "future-dated split excluded from adjustment"
                    );
                    continue;
                }
                let f_from = from.value(i);
                let f_to = to.value(i);
                if f_to == 0.0 {
                    // Defensive: malformed row, skip.
                    tracing::warn!(
                        ticker = symbols.value(i),
                        "split_to=0 — skipping"
                    );
                    continue;
                }
                let factor = SplitFactor {
                    execution_date: exec,
                    factor: f_from / f_to,
                };
                if !sids.is_null(i) {
                    splits_by_sid
                        .entry(sids.value(i).to_string())
                        .or_default()
                        .push(factor);
                }
                splits_by_symbol
                    .entry(symbols.value(i).to_string())
                    .or_default()
                    .push(factor);
            }
        }

        // Sort each ticker's factors by execution_date ascending so the
        // factor_at lookup is a single linear scan from the right.
        for v in splits_by_sid.values_mut() {
            v.sort_by_key(|s| s.execution_date);
        }
        for v in splits_by_symbol.values_mut() {
            v.sort_by_key(|s| s.execution_date);
        }

        Ok(Self {
            bars_dir: bars_dir.to_path_buf(),
            snapshot_pin,
            splits_by_sid,
            splits_by_symbol,
            figi_map,
        })
    }

    /// Resolve a security_id to the display symbol that was valid on
    /// `day`. Falls back to the sid string itself if the figi_map has
    /// no entry — keeps unconfigured runs from 404'ing silently, and
    /// matches the engine's logged-fallback contract for the ~21% of
    /// active sids missing FIGI coverage.
    fn resolve_symbol<'a>(&'a self, sid: &'a SecurityId, day: NaiveDate) -> &'a str {
        self.figi_map.resolve(sid, day).unwrap_or(sid.as_str())
    }

    fn day_path(&self, day: NaiveDate) -> PathBuf {
        self.bars_dir.join(format!("{day}.parquet"))
    }

    /// Cumulative split factor that converts raw price at time t to the
    /// pin-basis adjusted price. Factor at the latest bar = 1.0.
    fn factor_at(factors: &[SplitFactor], t_date: NaiveDate) -> f64 {
        let mut f = 1.0f64;
        for s in factors {
            if s.execution_date > t_date {
                f *= s.factor;
            }
        }
        f
    }

    /// Bulk read: every security's session for `day` in ONE scan of the
    /// per-day file, split-adjusted. This is the engine's read path —
    /// `session_bars` re-scans the whole file per sid, which is fine for
    /// audits/backfills but O(names²) inside a day-major engine loop.
    ///
    /// Rows with a null `security_id` fall back to the display symbol as
    /// the sid, mirroring `resolve_symbol`'s logged-fallback contract.
    /// Output is sorted by display_symbol (the file's native order).
    pub fn day_sessions(&self, day: NaiveDate) -> Result<Vec<DaySession>, BarReaderError> {
        let path = self.day_path(day);
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(BarReaderError::MissingDay(day));
            }
            Err(e) => return Err(BarReaderError::Io(e)),
        };
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

        // (display_symbol, sid) → bars. BTreeMap keeps the output in the
        // file's (display_symbol, t) order without a separate sort; the
        // map also tolerates a symbol's rows spanning batch boundaries.
        let mut by_key: std::collections::BTreeMap<(String, String), Vec<Bar>> =
            std::collections::BTreeMap::new();

        for batch_res in reader {
            let batch = batch_res?;
            let sids = batch.column_by_name("security_id").expect("security_id").as_string::<i32>();
            let symbols = batch
                .column_by_name("display_symbol")
                .expect("display_symbol")
                .as_string::<i32>();
            let t = batch
                .column_by_name("t")
                .expect("t column")
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .expect("t timestamp");
            let open = batch
                .column_by_name("open")
                .expect("open")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("open f64");
            let high = batch
                .column_by_name("high")
                .expect("high")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("high f64");
            let low = batch
                .column_by_name("low")
                .expect("low")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("low f64");
            let close = batch
                .column_by_name("close")
                .expect("close")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("close f64");
            let volume = batch
                .column_by_name("volume")
                .expect("volume")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("volume f64");

            for i in 0..batch.num_rows() {
                let ts_ns = t.value(i);
                let dt = Utc.timestamp_nanos(ts_ns);
                let d = NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day()).expect("valid");
                // Same UTC-midnight defense as session_bars.
                if d != day {
                    continue;
                }
                let symbol = symbols.value(i);
                let sid = if sids.is_null(i) { symbol } else { sids.value(i) };
                by_key
                    .entry((symbol.to_string(), sid.to_string()))
                    .or_default()
                    .push(Bar {
                        t: dt,
                        open: open.value(i),
                        high: high.value(i),
                        low: low.value(i),
                        close: close.value(i),
                        volume: volume.value(i),
                    });
            }
        }

        // Apply split adjustment once per security — the factor depends
        // only on the bar's date, which is `day` for every surviving row.
        let mut out = Vec::with_capacity(by_key.len());
        for ((symbol, sid), mut bars) in by_key {
            let factors: &[SplitFactor] = self
                .splits_by_sid
                .get(&sid)
                .or_else(|| self.splits_by_symbol.get(&symbol))
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let f = Self::factor_at(factors, day);
            if f != 1.0 {
                for b in &mut bars {
                    b.open *= f;
                    b.high *= f;
                    b.low *= f;
                    b.close *= f;
                    b.volume /= f;
                }
            }
            out.push(DaySession {
                security_id: SecurityId::new(&sid),
                display_symbol: symbol,
                session: Session::new(day, bars),
            });
        }
        Ok(out)
    }
}

impl BarReader for MaterializedBarReader {
    fn snapshot_pin(&self) -> NaiveDate {
        self.snapshot_pin
    }

    fn session_bars(
        &self,
        sid: &SecurityId,
        day: NaiveDate,
    ) -> Result<Session, StoreError> {
        let symbol = self.resolve_symbol(sid, day).to_string();
        let path = self.day_path(day);
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(StoreError::MissingBars {
                    security_id: sid.clone(),
                    date: day,
                });
            }
            Err(e) => return Err(StoreError::Io(e)),
        };
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| StoreError::Io(std::io::Error::other(e.to_string())))?
            .build()
            .map_err(|e| StoreError::Io(std::io::Error::other(e.to_string())))?;

        // Pre-resolve which split-factor table applies. Prefer
        // security_id; fall back to display_symbol.
        let factors: &[SplitFactor] = self
            .splits_by_sid
            .get(sid.as_str())
            .or_else(|| self.splits_by_symbol.get(&symbol))
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        let mut bars: Vec<Bar> = Vec::new();
        for batch_res in reader {
            let batch = batch_res
                .map_err(|e| StoreError::Io(std::io::Error::other(e.to_string())))?;
            let symbols = batch
                .column_by_name("display_symbol")
                .expect("display_symbol")
                .as_string::<i32>();
            let t = batch
                .column_by_name("t")
                .expect("t column")
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .expect("t timestamp");
            let open = batch
                .column_by_name("open")
                .expect("open")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("open f64");
            let high = batch
                .column_by_name("high")
                .expect("high")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("high f64");
            let low = batch
                .column_by_name("low")
                .expect("low")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("low f64");
            let close = batch
                .column_by_name("close")
                .expect("close")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("close f64");
            let volume = batch
                .column_by_name("volume")
                .expect("volume")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("volume f64");

            for i in 0..batch.num_rows() {
                if symbols.value(i) != symbol {
                    continue;
                }
                let ts_ns = t.value(i);
                let dt = Utc.timestamp_nanos(ts_ns);
                let d = NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day()).expect("valid");
                // Defense-in-depth: per-day file should only contain `day`'s rows,
                // but a row spanning UTC midnight could land in the adjacent file.
                if d != day {
                    continue;
                }
                let f = Self::factor_at(factors, d);
                bars.push(Bar {
                    t: dt,
                    open: open.value(i) * f,
                    high: high.value(i) * f,
                    low: low.value(i) * f,
                    close: close.value(i) * f,
                    volume: volume.value(i) / f,
                });
            }
        }

        if bars.is_empty() {
            return Err(StoreError::MissingBars {
                security_id: sid.clone(),
                date: day,
            });
        }
        Ok(Session::new(day, bars))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::figi_map::FigiMapRow;
    use crate::splits::{SplitRow, write_splits};
    use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
    use chrono::TimeZone;
    use momentum_core::schema::bars_1m_raw_schema;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn single_symbol_map(sid: &str, symbol: &str) -> FigiMap {
        FigiMap::from_rows(vec![FigiMapRow {
            security_id: sid.into(),
            display_symbol: symbol.into(),
            valid_from: None,
            valid_to: None,
        }])
    }

    fn write_one_day_bars(
        dir: &Path,
        symbol: &str,
        sid: &str,
        day_str: &str,
        prices: &[(f64, f64, f64, f64, f64)], // open, high, low, close, volume
    ) {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(format!("{day_str}.parquet"));
        let day = NaiveDate::parse_from_str(day_str, "%Y-%m-%d").unwrap();
        let schema = bars_1m_raw_schema();
        let n = prices.len();

        let security_id: ArrayRef = Arc::new(StringArray::from(vec![sid; n]));
        let display_symbol: ArrayRef = Arc::new(StringArray::from(vec![symbol; n]));
        // 09:30, 09:31, ... in UTC.
        let t: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(
                (0..n as i64)
                    .map(|i| {
                        Utc.with_ymd_and_hms(day.year(), day.month(), day.day(), 14, 30 + i as u32, 0)
                            .unwrap()
                            .timestamp_nanos_opt()
                            .unwrap()
                    })
                    .collect::<Vec<i64>>(),
            )
            .with_timezone("UTC"),
        );
        let open: ArrayRef = Arc::new(Float64Array::from(prices.iter().map(|p| p.0).collect::<Vec<_>>()));
        let high: ArrayRef = Arc::new(Float64Array::from(prices.iter().map(|p| p.1).collect::<Vec<_>>()));
        let low: ArrayRef = Arc::new(Float64Array::from(prices.iter().map(|p| p.2).collect::<Vec<_>>()));
        let close: ArrayRef = Arc::new(Float64Array::from(prices.iter().map(|p| p.3).collect::<Vec<_>>()));
        let volume: ArrayRef = Arc::new(Float64Array::from(prices.iter().map(|p| p.4).collect::<Vec<_>>()));
        let transactions: ArrayRef =
            Arc::new(arrow::array::Int64Array::from(vec![Some(1i64); n]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![security_id, display_symbol, t, open, high, low, close, volume, transactions],
        )
        .unwrap();

        let file = File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, Some(WriterProperties::builder().build())).unwrap();
        w.write(&batch).unwrap();
        let _ = w.close().unwrap();
    }

    fn write_splits_for_aapl(path: &Path, pin: NaiveDate) {
        let mut figi = HashMap::new();
        figi.insert("AAPL".to_string(), "BBG000B9XRY4".to_string());
        let rows = vec![
            SplitRow {
                id: "S1".into(),
                display_symbol: "AAPL".into(),
                execution_date: NaiveDate::from_ymd_opt(2020, 8, 31).unwrap(),
                split_from: 1.0,
                split_to: 4.0,
                adjustment_type: Some("forward_split".into()),
                historical_adjustment_factor: Some(0.25),
            },
        ];
        write_splits(path, &rows, pin, &figi).unwrap();
    }

    #[test]
    fn reader_pin_matches_metadata() {
        let dir = tempdir().unwrap();
        let splits = dir.path().join("splits.parquet");
        let pin = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();
        write_splits_for_aapl(&splits, pin);

        let reader = MaterializedBarReader::open(
            dir.path(),
            &splits,
            FigiMap::empty(),
        )
        .unwrap();
        assert_eq!(reader.snapshot_pin(), pin);
    }

    #[test]
    fn factor_at_drops_to_quarter_before_a_4_for_1_split() {
        let dir = tempdir().unwrap();
        let splits = dir.path().join("splits.parquet");
        let pin = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();
        write_splits_for_aapl(&splits, pin);

        let reader = MaterializedBarReader::open(
            dir.path(),
            &splits,
            FigiMap::empty(),
        )
        .unwrap();
        let factors = reader.splits_by_symbol.get("AAPL").unwrap();
        let before = NaiveDate::from_ymd_opt(2020, 8, 30).unwrap();
        let after = NaiveDate::from_ymd_opt(2020, 9, 1).unwrap();
        // Pre-split day: f = 1/4 (so raw $400 reads as $100 adjusted).
        assert!((MaterializedBarReader::factor_at(factors, before) - 0.25).abs() < 1e-12);
        // Post-split: f = 1.0.
        assert!((MaterializedBarReader::factor_at(factors, after) - 1.0).abs() < 1e-12);
    }

    #[test]
    fn session_bars_applies_split_adjustment() {
        // Pre-split day → expect raw × 0.25.
        let dir = tempdir().unwrap();
        let splits = dir.path().join("splits.parquet");
        let pin = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();
        write_splits_for_aapl(&splits, pin);

        let bars_dir = dir.path().join("bars");
        write_one_day_bars(
            &bars_dir,
            "AAPL",
            "BBG000B9XRY4",
            "2020-08-28",
            &[(400.0, 405.0, 398.0, 402.0, 1000.0)],
        );

        let reader = MaterializedBarReader::open(
            &bars_dir,
            &splits,
            single_symbol_map("BBG000B9XRY4", "AAPL"),
        )
        .unwrap();

        let day = NaiveDate::from_ymd_opt(2020, 8, 28).unwrap();
        let sid = SecurityId::new("BBG000B9XRY4");
        let session = reader.session_bars(&sid, day).unwrap();
        assert_eq!(session.len(), 1);
        let b = &session.bars[0];
        assert!((b.open - 100.0).abs() < 1e-9, "expected open=100, got {}", b.open);
        assert!((b.close - 100.5).abs() < 1e-9);
        assert!((b.volume - 4000.0).abs() < 1e-9);
    }

    #[test]
    fn audit_roundtrip_within_epsilon() {
        // For every bar returned, adjusted / factor_at(t) == raw.
        let dir = tempdir().unwrap();
        let splits = dir.path().join("splits.parquet");
        let pin = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();
        write_splits_for_aapl(&splits, pin);

        let bars_dir = dir.path().join("bars");
        let raw = (400.0, 405.5, 398.25, 402.875, 1234.0);
        write_one_day_bars(
            &bars_dir,
            "AAPL",
            "BBG000B9XRY4",
            "2020-08-28",
            &[raw],
        );

        let reader = MaterializedBarReader::open(
            &bars_dir,
            &splits,
            single_symbol_map("BBG000B9XRY4", "AAPL"),
        )
        .unwrap();

        let day = NaiveDate::from_ymd_opt(2020, 8, 28).unwrap();
        let factors = reader.splits_by_sid.get("BBG000B9XRY4").unwrap();
        let f = MaterializedBarReader::factor_at(factors, day);
        let s = reader.session_bars(&SecurityId::new("BBG000B9XRY4"), day).unwrap();
        let b = &s.bars[0];
        let eps = 1e-9;
        assert!((b.open / f - raw.0).abs() < eps);
        assert!((b.high / f - raw.1).abs() < eps);
        assert!((b.low / f - raw.2).abs() < eps);
        assert!((b.close / f - raw.3).abs() < eps);
        assert!((b.volume * f - raw.4).abs() < eps);
    }

    #[test]
    fn future_dated_split_is_excluded_from_adjustment() {
        // Vendor snapshots contain announced-but-not-yet-executed splits
        // (execution_date > pin). They are not in the tape as of the pin
        // basis, so the reader must EXCLUDE them from the factor tables —
        // letting one in would mis-adjust every historical bar.
        let dir = tempdir().unwrap();
        let splits_path = dir.path().join("splits.parquet");
        let bars_dir = dir.path().join("bars");
        std::fs::create_dir_all(&bars_dir).unwrap();

        // Pin = 2020-08-30 but row.execution_date = 2020-08-31 (future).
        let pin = NaiveDate::from_ymd_opt(2020, 8, 30).unwrap();
        let rows = vec![SplitRow {
            id: "S1".into(),
            display_symbol: "AAPL".into(),
            execution_date: NaiveDate::from_ymd_opt(2020, 8, 31).unwrap(),
            split_from: 1.0,
            split_to: 4.0,
            adjustment_type: Some("forward_split".into()),
            historical_adjustment_factor: Some(0.25),
        }];
        let mut figi = HashMap::new();
        figi.insert("AAPL".to_string(), "BBG000B9XRY4".to_string());
        write_splits(&splits_path, &rows, pin, &figi).unwrap();

        let reader =
            MaterializedBarReader::open(&bars_dir, &splits_path, FigiMap::empty()).unwrap();
        // The future split must not appear in either factor table…
        assert!(reader.splits_by_sid.get("BBG000B9XRY4").is_none());
        assert!(reader.splits_by_symbol.get("AAPL").is_none());
        // …so the adjustment factor for any historical date stays 1.0.
        let before = NaiveDate::from_ymd_opt(2020, 8, 28).unwrap();
        assert_eq!(MaterializedBarReader::factor_at(&[], before), 1.0);
    }

    #[test]
    fn missing_pin_metadata_is_a_hard_error() {
        // splits.parquet without `splits_snapshot_date` in metadata.
        let dir = tempdir().unwrap();
        let splits_path = dir.path().join("splits.parquet");
        let bars_dir = dir.path().join("bars");
        std::fs::create_dir_all(&bars_dir).unwrap();

        // Write a bare parquet without the snapshot key.
        let schema = momentum_core::schema::splits_schema();
        let file = File::create(&splits_path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema.clone(), Some(WriterProperties::builder().build())).unwrap();
        // Empty batch: matches schema, no rows.
        let empty = RecordBatch::new_empty(schema);
        w.write(&empty).unwrap();
        let _ = w.close().unwrap();

        let res = MaterializedBarReader::open(&bars_dir, &splits_path, FigiMap::empty());
        assert!(matches!(res, Err(BarReaderError::MissingPin)));
    }

    #[test]
    fn day_sessions_bulk_reads_every_security_split_adjusted() {
        // Two securities in one day file; AAPL is pre-split so its bars
        // must come back adjusted, MSFT untouched. One scan, both back.
        let dir = tempdir().unwrap();
        let splits = dir.path().join("splits.parquet");
        let pin = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();
        write_splits_for_aapl(&splits, pin);

        let bars_dir = dir.path().join("bars");
        write_one_day_bars(
            &bars_dir,
            "AAPL",
            "BBG000B9XRY4",
            "2020-08-28",
            &[(400.0, 405.0, 398.0, 402.0, 1000.0)],
        );
        // Append MSFT rows to the same day by writing a second file is not
        // possible (one file per day) — write both in one go instead.
        // write_one_day_bars overwrites, so build a combined file manually.
        std::fs::remove_file(bars_dir.join("2020-08-28.parquet")).unwrap();
        {
            let schema = bars_1m_raw_schema();
            let day = NaiveDate::from_ymd_opt(2020, 8, 28).unwrap();
            let mk_t = |min: u32| {
                Utc.with_ymd_and_hms(day.year(), day.month(), day.day(), 14, 30 + min, 0)
                    .unwrap()
                    .timestamp_nanos_opt()
                    .unwrap()
            };
            let security_id: ArrayRef = Arc::new(StringArray::from(vec![
                "BBG000B9XRY4",
                "BBG000BPH459",
            ]));
            let display_symbol: ArrayRef =
                Arc::new(StringArray::from(vec!["AAPL", "MSFT"]));
            let t: ArrayRef = Arc::new(
                TimestampNanosecondArray::from(vec![mk_t(0), mk_t(0)]).with_timezone("UTC"),
            );
            let open: ArrayRef = Arc::new(Float64Array::from(vec![400.0, 210.0]));
            let high: ArrayRef = Arc::new(Float64Array::from(vec![405.0, 211.0]));
            let low: ArrayRef = Arc::new(Float64Array::from(vec![398.0, 209.0]));
            let close: ArrayRef = Arc::new(Float64Array::from(vec![402.0, 210.5]));
            let volume: ArrayRef = Arc::new(Float64Array::from(vec![1000.0, 2000.0]));
            let transactions: ArrayRef =
                Arc::new(arrow::array::Int64Array::from(vec![Some(1i64), Some(1)]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![security_id, display_symbol, t, open, high, low, close, volume, transactions],
            )
            .unwrap();
            let file = File::create(bars_dir.join("2020-08-28.parquet")).unwrap();
            let mut w =
                ArrowWriter::try_new(file, schema, Some(WriterProperties::builder().build()))
                    .unwrap();
            w.write(&batch).unwrap();
            let _ = w.close().unwrap();
        }

        let reader =
            MaterializedBarReader::open(&bars_dir, &splits, FigiMap::empty()).unwrap();
        let day = NaiveDate::from_ymd_opt(2020, 8, 28).unwrap();
        let sessions = reader.day_sessions(day).unwrap();
        assert_eq!(sessions.len(), 2);

        // BTreeMap order: AAPL first.
        let aapl = &sessions[0];
        assert_eq!(aapl.display_symbol, "AAPL");
        assert_eq!(aapl.security_id.as_str(), "BBG000B9XRY4");
        assert!((aapl.session.bars[0].open - 100.0).abs() < 1e-9, "split-adjusted");
        assert!((aapl.session.bars[0].volume - 4000.0).abs() < 1e-9);

        let msft = &sessions[1];
        assert_eq!(msft.display_symbol, "MSFT");
        assert!((msft.session.bars[0].open - 210.0).abs() < 1e-9, "no split: raw");

        // Missing day is a typed error.
        assert!(matches!(
            reader.day_sessions(NaiveDate::from_ymd_opt(2020, 8, 29).unwrap()),
            Err(BarReaderError::MissingDay(_))
        ));
    }

    #[test]
    fn session_bars_resolves_pre_and_post_rename() {
        // FB→META on 2022-06-09. The same security_id has bars under
        // "FB" on 2022-06-08 and under "META" on 2022-06-09. Both
        // session_bars calls must succeed and return the right ticker's
        // rows from each per-day file.
        let dir = tempdir().unwrap();
        let splits = dir.path().join("splits.parquet");
        let bars_dir = dir.path().join("bars");
        let pin = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap();

        // splits.parquet — none, but the writer still stamps the pin.
        let figi = HashMap::new();
        write_splits(&splits, &[], pin, &figi).unwrap();

        write_one_day_bars(
            &bars_dir,
            "FB",
            "BBG000MM2P62",
            "2022-06-08",
            &[(200.0, 201.0, 199.0, 200.5, 1000.0)],
        );
        write_one_day_bars(
            &bars_dir,
            "META",
            "BBG000MM2P62",
            "2022-06-09",
            &[(210.0, 212.0, 209.0, 211.0, 1200.0)],
        );

        let map = FigiMap::from_rows(vec![
            FigiMapRow {
                security_id: "BBG000MM2P62".into(),
                display_symbol: "FB".into(),
                valid_from: Some(NaiveDate::from_ymd_opt(2012, 5, 18).unwrap()),
                valid_to: Some(NaiveDate::from_ymd_opt(2022, 6, 9).unwrap()),
            },
            FigiMapRow {
                security_id: "BBG000MM2P62".into(),
                display_symbol: "META".into(),
                valid_from: Some(NaiveDate::from_ymd_opt(2022, 6, 9).unwrap()),
                valid_to: None,
            },
        ]);
        let reader = MaterializedBarReader::open(&bars_dir, &splits, map).unwrap();

        let sid = SecurityId::new("BBG000MM2P62");
        let pre = reader
            .session_bars(&sid, NaiveDate::from_ymd_opt(2022, 6, 8).unwrap())
            .expect("pre-rename day");
        assert_eq!(pre.len(), 1);
        assert!((pre.bars[0].open - 200.0).abs() < 1e-9);

        let post = reader
            .session_bars(&sid, NaiveDate::from_ymd_opt(2022, 6, 9).unwrap())
            .expect("post-rename day");
        assert_eq!(post.len(), 1);
        assert!((post.bars[0].open - 210.0).abs() < 1e-9);
    }
}
