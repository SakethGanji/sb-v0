//! `Calendar` — the engine's only source of truth for "when did the
//! market trade" and "when did each session close".
//!
//! ## Why this exists
//!
//! Frozen decisions #8: half-day sessions (~6/yr at 13:00 ET) are NOT
//! hardcoded. Path-row count per trade-day is variable. Nothing in the
//! engine may hardcode "39 bars" or "16:00 close" — instead, the
//! session-close time per date comes from the *actual* tape.
//!
//! Concretely: `session_close(day)` = `max(t)` over SPY rows in
//! `bars_1m_raw/{day}.parquet`, **restricted to regular trading hours
//! (ET ≤ 16:00)**. SPY is the canary because it trades every session a
//! US-listed equity does, including the early-close half-days. Massive
//! flat files include pre-market and after-hours bars, so naïve
//! `max(t)` lands in after-hours; the RTH filter is what makes this
//! return the actual session close.
//!
//! ## What's in the file vs. derived
//!
//! - `trading_days()` is derived from the filenames in `bars_1m_raw/`,
//!   not from a hand-curated holiday list. If a date has a Parquet
//!   file, the tape recorded trading on it; if it doesn't, no trading.
//!   This makes the calendar invariant: it can't lie about what data
//!   the engine has.
//! - `session_close(day)` is read on demand and memoized in a Mutex.
//!   The engine's daily loop calls it once per trading day, so the
//!   cache pays for itself.

use arrow::array::{Array, AsArray, TimestampNanosecondArray};
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum CalendarError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("bars_dir {0} contains no YYYY-MM-DD.parquet files")]
    EmptyBarsDir(PathBuf),
    #[error("no bar file for {date} at {path}")]
    MissingDay { date: NaiveDate, path: PathBuf },
    #[error("no SPY rows in {path} — calendar canary is gone")]
    NoSpyRows { path: PathBuf },
}

/// SPY is the session-close canary per frozen decisions #8. If it ever
/// stops being a reliable proxy (corp action removing it from listed
/// status, ETF complex restructuring, etc.) this constant is the one
/// place to change.
const SESSION_ANCHOR_SYMBOL: &str = "SPY";

/// Last ET time we accept as a "session" bar. 16:00 ET is the equity
/// closing-cross instant; bars after that are after-hours and must be
/// excluded so the close on a half-day (~13:00 ET) doesn't get
/// dominated by the 16:00–20:00 ET after-hours session.
fn rth_close_et() -> NaiveTime {
    NaiveTime::from_hms_opt(16, 0, 0).expect("static 16:00:00")
}

pub struct Calendar {
    bars_dir: PathBuf,
    trading_days: Vec<NaiveDate>,
    closes: Mutex<HashMap<NaiveDate, DateTime<Utc>>>,
}

impl Calendar {
    /// Open the calendar against `bars_dir` (typically
    /// `data/bars_1m_raw`). Enumerates `YYYY-MM-DD.parquet` files at
    /// the top level and sorts the dates ascending. Does **not** read
    /// any file contents at open time — session closes are pulled
    /// lazily on first access per day.
    pub fn open(bars_dir: &Path) -> Result<Self, CalendarError> {
        let mut days: Vec<NaiveDate> = Vec::new();
        for entry in std::fs::read_dir(bars_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let name = match name.to_str() {
                Some(s) => s,
                None => continue,
            };
            if let Some(stem) = name.strip_suffix(".parquet")
                && let Ok(d) = NaiveDate::parse_from_str(stem, "%Y-%m-%d")
            {
                days.push(d);
            }
        }
        if days.is_empty() {
            return Err(CalendarError::EmptyBarsDir(bars_dir.to_path_buf()));
        }
        days.sort_unstable();
        days.dedup();
        Ok(Self {
            bars_dir: bars_dir.to_path_buf(),
            trading_days: days,
            closes: Mutex::new(HashMap::new()),
        })
    }

    /// All trading days the engine has bars for, sorted ascending.
    pub fn trading_days(&self) -> &[NaiveDate] {
        &self.trading_days
    }

    /// True if the engine has bars for `day`.
    pub fn has_day(&self, day: NaiveDate) -> bool {
        self.trading_days.binary_search(&day).is_ok()
    }

    /// First trading day on or after `day`. Useful for clamping a query
    /// range onto known-trading dates.
    pub fn next_trading_day(&self, day: NaiveDate) -> Option<NaiveDate> {
        let idx = self.trading_days.partition_point(|d| *d < day);
        self.trading_days.get(idx).copied()
    }

    /// Last trading day on or before `day`.
    pub fn prev_trading_day(&self, day: NaiveDate) -> Option<NaiveDate> {
        let idx = self.trading_days.partition_point(|d| *d <= day);
        if idx == 0 {
            None
        } else {
            self.trading_days.get(idx - 1).copied()
        }
    }

    /// `max(t)` over `display_symbol == SPY` rows in `bars_1m_raw/{day}.parquet`,
    /// memoized. The engine's EOD mark-to-market reads this — frozen
    /// decisions #8 forbid hardcoded "16:00 close".
    pub fn session_close(&self, day: NaiveDate) -> Result<DateTime<Utc>, CalendarError> {
        if let Some(t) = self.closes.lock().unwrap().get(&day) {
            return Ok(*t);
        }
        let close = self.read_session_close(day)?;
        self.closes.lock().unwrap().insert(day, close);
        Ok(close)
    }

    fn day_path(&self, day: NaiveDate) -> PathBuf {
        self.bars_dir.join(format!("{day}.parquet"))
    }

    fn read_session_close(&self, day: NaiveDate) -> Result<DateTime<Utc>, CalendarError> {
        let path = self.day_path(day);
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(CalendarError::MissingDay { date: day, path });
            }
            Err(e) => return Err(CalendarError::Io(e)),
        };

        // Project only the columns we need. Filtering on display_symbol
        // happens row-by-row below — the file is sorted by
        // (display_symbol, t) but Arrow's predicate pushdown isn't free
        // to wire here, and SPY's row block is a small slice of the
        // file so the scan terminates quickly in practice.
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema();
        let mut projection: Vec<usize> = Vec::with_capacity(2);
        for (i, f) in schema.fields().iter().enumerate() {
            if f.name() == "display_symbol" || f.name() == "t" {
                projection.push(i);
            }
        }
        let mask = parquet::arrow::ProjectionMask::leaves(
            builder.parquet_schema(),
            projection.iter().copied(),
        );
        let reader = builder.with_projection(mask).build()?;

        let cutoff = rth_close_et();
        let mut max_ns: Option<i64> = None;
        for batch_res in reader {
            let batch = batch_res?;
            let symbols = batch
                .column_by_name("display_symbol")
                .expect("display_symbol")
                .as_string::<i32>();
            let t = batch
                .column_by_name("t")
                .expect("t")
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("t timestamp[ns,UTC]");
            for i in 0..batch.num_rows() {
                if symbols.value(i) != SESSION_ANCHOR_SYMBOL {
                    continue;
                }
                let v = t.value(i);
                let dt_et = Utc.timestamp_nanos(v).with_timezone(&New_York);
                if dt_et.time() > cutoff {
                    continue;
                }
                max_ns = Some(max_ns.map_or(v, |m| m.max(v)));
            }
        }

        match max_ns {
            Some(ns) => Ok(Utc.timestamp_nanos(ns)),
            None => Err(CalendarError::NoSpyRows { path }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
    use chrono::Timelike;
    use momentum_core::schema::bars_1m_raw_schema;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    /// Write a synthetic per-day parquet file with the given SPY close
    /// (last minute, UTC). Other tickers are added so the scan path is
    /// realistic (must skip non-SPY rows).
    fn write_day(dir: &Path, day_str: &str, spy_close_utc_h: u32, spy_close_utc_m: u32) {
        let day = d(day_str);
        let path = dir.join(format!("{day_str}.parquet"));
        let schema = bars_1m_raw_schema();

        // 3 SPY rows including the close-marker, plus 2 AAPL rows
        // before it. Sort order: (display_symbol, t).
        let sids = vec!["BBG_AAPL"; 2]
            .into_iter()
            .chain(vec!["BBG_SPY"; 3])
            .collect::<Vec<_>>();
        let syms = vec!["AAPL"; 2]
            .into_iter()
            .chain(vec!["SPY"; 3])
            .collect::<Vec<_>>();
        use chrono::Datelike;
        let ts = |h, m| {
            Utc.with_ymd_and_hms(day.year(), day.month(), day.day(), h, m, 0)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap()
        };
        let t: ArrayRef = Arc::new(
            arrow::array::TimestampNanosecondArray::from(vec![
                ts(14, 30),
                ts(14, 35),
                ts(14, 30),
                ts(15, 0),
                ts(spy_close_utc_h, spy_close_utc_m),
            ])
            .with_timezone("UTC"),
        );

        let n = 5;
        let security_id: ArrayRef = Arc::new(StringArray::from(sids));
        let display_symbol: ArrayRef = Arc::new(StringArray::from(syms));
        let mk_f64 = |v: f64| -> ArrayRef { Arc::new(Float64Array::from(vec![v; n])) };
        let open = mk_f64(100.0);
        let high = mk_f64(101.0);
        let low = mk_f64(99.0);
        let close = mk_f64(100.5);
        let volume = mk_f64(1000.0);
        let transactions: ArrayRef = Arc::new(Int64Array::from(vec![Some(1i64); n]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                security_id,
                display_symbol,
                t,
                open,
                high,
                low,
                close,
                volume,
                transactions,
            ],
        )
        .unwrap();

        let file = File::create(&path).unwrap();
        let mut w =
            ArrowWriter::try_new(file, schema, Some(WriterProperties::builder().build())).unwrap();
        w.write(&batch).unwrap();
        let _ = w.close().unwrap();
    }

    #[test]
    fn empty_dir_errors() {
        let dir = tempdir().unwrap();
        let res = Calendar::open(dir.path());
        assert!(matches!(res, Err(CalendarError::EmptyBarsDir(_))));
    }

    #[test]
    fn trading_days_are_sorted_and_filtered() {
        let dir = tempdir().unwrap();
        // Out-of-order writes + one non-conforming filename.
        write_day(dir.path(), "2024-01-04", 21, 0);
        write_day(dir.path(), "2024-01-02", 21, 0);
        write_day(dir.path(), "2024-01-03", 21, 0);
        std::fs::write(dir.path().join("README.txt"), "not a date").unwrap();
        std::fs::create_dir_all(dir.path().join("nested")).unwrap();

        let cal = Calendar::open(dir.path()).unwrap();
        assert_eq!(
            cal.trading_days(),
            &[d("2024-01-02"), d("2024-01-03"), d("2024-01-04")]
        );
        assert!(cal.has_day(d("2024-01-03")));
        assert!(!cal.has_day(d("2024-01-01")));
    }

    #[test]
    fn next_and_prev_trading_day() {
        let dir = tempdir().unwrap();
        write_day(dir.path(), "2024-01-02", 21, 0);
        write_day(dir.path(), "2024-01-03", 21, 0);
        write_day(dir.path(), "2024-01-05", 21, 0);
        let cal = Calendar::open(dir.path()).unwrap();

        // Weekend → next is Monday-equivalent.
        assert_eq!(cal.next_trading_day(d("2024-01-04")), Some(d("2024-01-05")));
        assert_eq!(cal.next_trading_day(d("2024-01-03")), Some(d("2024-01-03")));
        assert_eq!(cal.next_trading_day(d("2024-01-06")), None);

        assert_eq!(cal.prev_trading_day(d("2024-01-04")), Some(d("2024-01-03")));
        assert_eq!(cal.prev_trading_day(d("2024-01-03")), Some(d("2024-01-03")));
        assert_eq!(cal.prev_trading_day(d("2024-01-01")), None);
    }

    #[test]
    fn session_close_reads_spy_max_t_full_day() {
        // Full-session close at 21:00 UTC (16:00 ET pre-DST).
        let dir = tempdir().unwrap();
        write_day(dir.path(), "2024-01-02", 21, 0);
        let cal = Calendar::open(dir.path()).unwrap();
        let close = cal.session_close(d("2024-01-02")).unwrap();
        assert_eq!(close.hour(), 21);
        assert_eq!(close.minute(), 0);
    }

    #[test]
    fn session_close_picks_up_half_day() {
        // Half-day at 18:00 UTC (13:00 ET pre-DST) — frozen-decisions
        // #8 case. The function must reflect what's on the tape, not a
        // hardcoded 21:00.
        let dir = tempdir().unwrap();
        write_day(dir.path(), "2024-11-29", 18, 0); // Black Friday
        write_day(dir.path(), "2024-11-25", 21, 0); // Normal
        let cal = Calendar::open(dir.path()).unwrap();
        assert_eq!(
            cal.session_close(d("2024-11-29")).unwrap().hour(),
            18,
            "Black Friday half-day must close at 18:00 UTC per tape"
        );
        assert_eq!(cal.session_close(d("2024-11-25")).unwrap().hour(), 21);
    }

    #[test]
    fn session_close_caches() {
        // Call twice; second call hits the cache (no file IO). We can't
        // observe the cache directly, but we can delete the file
        // after the first call and confirm the second still returns.
        let dir = tempdir().unwrap();
        write_day(dir.path(), "2024-01-02", 21, 0);
        let cal = Calendar::open(dir.path()).unwrap();
        let _first = cal.session_close(d("2024-01-02")).unwrap();
        std::fs::remove_file(dir.path().join("2024-01-02.parquet")).unwrap();
        let second = cal.session_close(d("2024-01-02")).unwrap();
        assert_eq!(second.hour(), 21);
    }

    #[test]
    fn session_close_ignores_after_hours_bars() {
        // SPY rows at 14:30 / 15:00 UTC (pre-RTH and RTH morning), a
        // proper close at 21:00 UTC (= 16:00 ET, full session), AND an
        // after-hours bar at 23:30 UTC (= 18:30 ET). max(t) without
        // the RTH filter would return the after-hours bar; with the
        // filter we must return the 21:00 UTC close.
        use chrono::Datelike;

        let dir = tempdir().unwrap();
        let day_str = "2024-01-02";
        let day = d(day_str);
        let path = dir.path().join(format!("{day_str}.parquet"));
        let schema = bars_1m_raw_schema();
        let ts = |h, m| {
            Utc.with_ymd_and_hms(day.year(), day.month(), day.day(), h, m, 0)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap()
        };
        let sids = vec!["BBG_SPY"; 4];
        let syms = vec!["SPY"; 4];
        let t: ArrayRef = Arc::new(
            arrow::array::TimestampNanosecondArray::from(vec![
                ts(14, 30), // 09:30 ET — RTH open
                ts(15, 0),  // 10:00 ET — RTH
                ts(21, 0),  // 16:00 ET — RTH close
                ts(23, 30), // 18:30 ET — AFTER HOURS
            ])
            .with_timezone("UTC"),
        );
        let n = 4;
        let mk = |v: f64| -> ArrayRef { Arc::new(Float64Array::from(vec![v; n])) };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(sids)),
                Arc::new(StringArray::from(syms)),
                t,
                mk(100.0),
                mk(101.0),
                mk(99.0),
                mk(100.5),
                mk(1000.0),
                Arc::new(Int64Array::from(vec![Some(1i64); n])),
            ],
        )
        .unwrap();
        let file = File::create(&path).unwrap();
        let mut w =
            ArrowWriter::try_new(file, schema, Some(WriterProperties::builder().build())).unwrap();
        w.write(&batch).unwrap();
        let _ = w.close().unwrap();

        let cal = Calendar::open(dir.path()).unwrap();
        let close = cal.session_close(day).unwrap();
        assert_eq!(
            close.hour(),
            21,
            "RTH close should win over after-hours bar"
        );
        assert_eq!(close.minute(), 0);
    }

    #[test]
    fn missing_day_errors() {
        let dir = tempdir().unwrap();
        write_day(dir.path(), "2024-01-02", 21, 0);
        let cal = Calendar::open(dir.path()).unwrap();
        let res = cal.session_close(d("2024-01-03"));
        assert!(matches!(res, Err(CalendarError::MissingDay { .. })));
    }
}
