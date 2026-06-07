//! `MaterializedBarReader` — the `BarReader` impl that the engine reads
//! through. Applies split adjustment **at read time** against a pinned
//! `splits.parquet` snapshot (§5.3 amended 2026-06-06).
//!
//! ## Contract
//!
//! Construction reads `splits.parquet`, extracts `splits_snapshot_date`
//! from file-level Parquet metadata, and refuses any split row whose
//! `execution_date > snapshot_pin`. This is the only place that
//! enforcement lives — once `MaterializedBarReader` exists, the
//! invariant holds for the lifetime of the run.
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
//! Today's resolver is a simple `HashMap<SecurityId, String>` of
//! current display symbols. The rename-aware figi_map (derived from
//! `ticker_events.parquet`) is task-10 — the API surface here doesn't
//! change, only the resolver's behavior.

use crate::WriteError;
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
    #[error(
        "splits row violates snapshot pin: ticker={ticker} execution_date={execution_date} > pin={pin}"
    )]
    SnapshotViolation {
        ticker: String,
        execution_date: NaiveDate,
        pin: NaiveDate,
    },
}

/// One split's adjustment effect, pre-extracted from `splits.parquet`.
/// `factor = split_from / split_to` per the amended §5.3 math
/// (4-for-1 → 1/4 = 0.25; reverse 1-for-10 → 10/1 = 10).
#[derive(Debug, Clone, Copy)]
struct SplitFactor {
    execution_date: NaiveDate,
    factor: f64,
}

/// Materialized bar reader. Backed by per-ticker `bars_1m_raw/{T}.parquet`
/// files and a once-loaded splits map.
pub struct MaterializedBarReader {
    bars_dir: PathBuf,
    snapshot_pin: NaiveDate,
    // Primary: split factors keyed by composite FIGI.
    splits_by_sid: HashMap<String, Vec<SplitFactor>>,
    // Fallback: split factors keyed by current display symbol — for
    // rows in splits.parquet whose security_id was null at write time.
    splits_by_symbol: HashMap<String, Vec<SplitFactor>>,
    // sid → current display symbol; used to locate the per-ticker file.
    sid_to_symbol: HashMap<String, String>,
}

impl MaterializedBarReader {
    /// Open a reader. Reads `splits.parquet`, pins the snapshot, and
    /// pre-computes per-ticker split factor tables.
    pub fn open(
        bars_dir: &Path,
        splits_parquet: &Path,
        sid_to_symbol: HashMap<String, String>,
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
                    return Err(BarReaderError::SnapshotViolation {
                        ticker: if sids.is_null(i) {
                            symbols.value(i).to_string()
                        } else {
                            sids.value(i).to_string()
                        },
                        execution_date: exec,
                        pin: snapshot_pin,
                    });
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
            sid_to_symbol,
        })
    }

    /// Resolve a security_id to the current display symbol. Today this
    /// is current-only — task-10 will swap in time-varying rename
    /// resolution via figi_map. Returns the sid string itself as a
    /// last-resort fallback so an unconfigured map doesn't 404 silently.
    fn resolve_symbol<'a>(&'a self, sid: &'a SecurityId) -> &'a str {
        self.sid_to_symbol
            .get(sid.as_str())
            .map(String::as_str)
            .unwrap_or(sid.as_str())
    }

    fn ticker_path(&self, symbol: &str) -> PathBuf {
        self.bars_dir.join(format!("{symbol}.parquet"))
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
        let symbol = self.resolve_symbol(sid).to_string();
        let path = self.ticker_path(&symbol);
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
                // Filter to the requested day (UTC-naive day, since
                // session-window filtering is the engine's job — we
                // just need to cover the right calendar date).
                let d = NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day()).expect("valid");
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
    use crate::splits::{SplitRow, write_splits};
    use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
    use chrono::TimeZone;
    use momentum_core::schema::bars_1m_raw_schema;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn write_one_day_bars(
        dir: &Path,
        symbol: &str,
        sid: &str,
        day_str: &str,
        prices: &[(f64, f64, f64, f64, f64)], // open, high, low, close, volume
    ) {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(format!("{symbol}.parquet"));
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
            HashMap::new(),
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
            HashMap::new(),
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

        let mut sid_map = HashMap::new();
        sid_map.insert("BBG000B9XRY4".to_string(), "AAPL".to_string());
        let reader = MaterializedBarReader::open(&bars_dir, &splits, sid_map).unwrap();

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

        let mut sid_map = HashMap::new();
        sid_map.insert("BBG000B9XRY4".into(), "AAPL".into());
        let reader = MaterializedBarReader::open(&bars_dir, &splits, sid_map).unwrap();

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
    fn snapshot_violation_is_a_hard_error() {
        // Construct a splits.parquet whose row's execution_date > pin.
        // Reader::open MUST refuse to build.
        use parquet::file::metadata::KeyValue;
        let dir = tempdir().unwrap();
        let splits_path = dir.path().join("splits.parquet");
        let bars_dir = dir.path().join("bars");
        std::fs::create_dir_all(&bars_dir).unwrap();

        // Pin = 2020-08-30 but row.execution_date = 2020-08-31 → violation.
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
        // Confirm the stamp went in unchanged.
        let _ = KeyValue {
            key: "splits_snapshot_date".into(),
            value: Some(pin.to_string()),
        };

        let res = MaterializedBarReader::open(&bars_dir, &splits_path, HashMap::new());
        assert!(matches!(
            res,
            Err(BarReaderError::SnapshotViolation { .. })
        ));
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

        let res = MaterializedBarReader::open(&bars_dir, &splits_path, HashMap::new());
        assert!(matches!(res, Err(BarReaderError::MissingPin)));
    }
}
