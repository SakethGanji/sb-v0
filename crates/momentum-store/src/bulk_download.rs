//! M3 stage 1 — bulk download of historical flat files.
//!
//! Pulls every weekday's gzipped CSV from `us_stocks_sip/minute_aggs_v1/`
//! into `data/_staging/flat_files/YYYY/MM/YYYY-MM-DD.csv.gz`. Resume is
//! O(1) via a SQLite cursor; weekends are skipped before they enter the
//! cursor (Massive doesn't publish files for them); holidays present as
//! `NoSuchKey` 404s on S3 and are recorded as `done` with `bytes=0` so
//! a future run doesn't re-try them.
//!
//! ## Cursor schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS download_state (
//!     date         TEXT PRIMARY KEY,  -- YYYY-MM-DD
//!     status       TEXT NOT NULL,     -- 'pending' | 'done' | 'failed'
//!     attempts     INTEGER NOT NULL DEFAULT 0,
//!     bytes        INTEGER,           -- NULL until done
//!     last_error   TEXT,              -- NULL on done
//!     completed_at TEXT               -- ISO-8601 UTC, NULL on pending
//! );
//! ```
//!
//! Concurrency is bounded by `tokio::sync::Semaphore` (default 16 per the
//! frozen S3-pool guidance in §5.2). Retry uses exponential backoff with
//! jitter, classifying NoSuchKey as a terminal "no file for this date"
//! rather than a failure.
//!
//! Resume contract: re-run with the same `(start, end, staging_dir,
//! cursor_db)` and only rows whose status is not `done` will be touched.
//! Partial writes are detected by status — a row only flips to `done`
//! AFTER the bytes are flushed to disk.

use crate::WriteError;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};
use momentum_api::s3::{FlatFileClient, S3Error};
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::time::{Duration as TokioDuration, sleep};

#[derive(Debug, thiserror::Error)]
pub enum BulkError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("s3: {0}")]
    S3(#[from] S3Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("write: {0}")]
    Write(#[from] WriteError),
    #[error("invalid date range: {start} > {end}")]
    InvalidRange { start: NaiveDate, end: NaiveDate },
}

/// Status flag persisted per date in the cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DayStatus {
    Pending,
    Done,
    Failed,
}

impl DayStatus {
    fn from_str(s: &str) -> Self {
        match s {
            "done" => Self::Done,
            "failed" => Self::Failed,
            _ => Self::Pending,
        }
    }
}

/// One row of the cursor table.
#[derive(Debug, Clone)]
pub struct DayRecord {
    pub date: NaiveDate,
    pub status: DayStatus,
    pub attempts: u32,
    pub bytes: Option<u64>,
    pub last_error: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Resume cursor backed by a single SQLite file. All mutating ops take a
/// `&self` lock on an internal `Mutex<Connection>` — safe to clone across
/// spawned tasks via `Arc<DownloadCursor>`.
pub struct DownloadCursor {
    conn: Mutex<Connection>,
}

impl DownloadCursor {
    /// Open or create the cursor at `path`. Creates parent directories.
    pub fn open(path: &Path) -> Result<Self, BulkError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        Self::init(&conn)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    /// In-memory cursor — used by unit tests.
    pub fn in_memory() -> Result<Self, BulkError> {
        let conn = Connection::open_in_memory()?;
        Self::init(&conn)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    fn init(conn: &Connection) -> Result<(), BulkError> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS download_state (
                date         TEXT PRIMARY KEY,
                status       TEXT NOT NULL,
                attempts     INTEGER NOT NULL DEFAULT 0,
                bytes        INTEGER,
                last_error   TEXT,
                completed_at TEXT
            )",
            [],
        )?;
        Ok(())
    }

    /// Insert `pending` rows for any date not already in the table. Dates
    /// already in `done` are left alone (no re-download).
    pub fn upsert_pending(
        &self,
        dates: impl IntoIterator<Item = NaiveDate>,
    ) -> Result<usize, BulkError> {
        let mut conn = self.conn.lock().expect("cursor mutex");
        let tx = conn.transaction()?;
        let mut inserted = 0;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO download_state (date, status, attempts) \
                 VALUES (?1, 'pending', 0)",
            )?;
            for d in dates {
                inserted += stmt.execute(params![d.to_string()])?;
            }
        }
        tx.commit()?;
        Ok(inserted)
    }

    /// Return all rows whose status is NOT `done`. These are what the
    /// downloader will attempt on this run.
    pub fn list_undone(&self) -> Result<Vec<DayRecord>, BulkError> {
        let conn = self.conn.lock().expect("cursor mutex");
        let mut stmt = conn.prepare(
            "SELECT date, status, attempts, bytes, last_error, completed_at \
             FROM download_state WHERE status != 'done' ORDER BY date",
        )?;
        let rows = stmt
            .query_map([], |r| {
                let date_str: String = r.get(0)?;
                let status_str: String = r.get(1)?;
                let attempts: i64 = r.get(2)?;
                let bytes: Option<i64> = r.get(3)?;
                let last_error: Option<String> = r.get(4)?;
                let completed_at: Option<String> = r.get(5)?;
                Ok(DayRecord {
                    date: NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")
                        .expect("cursor date parse"),
                    status: DayStatus::from_str(&status_str),
                    attempts: attempts as u32,
                    bytes: bytes.map(|b| b as u64),
                    last_error,
                    completed_at: completed_at.and_then(|s| {
                        DateTime::parse_from_rfc3339(&s)
                            .ok()
                            .map(|d| d.with_timezone(&Utc))
                    }),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Mark a date as successfully downloaded.
    pub fn mark_done(&self, date: NaiveDate, bytes: u64) -> Result<(), BulkError> {
        let conn = self.conn.lock().expect("cursor mutex");
        conn.execute(
            "UPDATE download_state SET status='done', bytes=?1, \
             last_error=NULL, completed_at=?2 WHERE date=?3",
            params![bytes as i64, Utc::now().to_rfc3339(), date.to_string()],
        )?;
        Ok(())
    }

    /// Mark a date as failed after exhausting retries. Bumps `attempts`
    /// and records the last error.
    pub fn mark_failed(
        &self,
        date: NaiveDate,
        attempts: u32,
        error: &str,
    ) -> Result<(), BulkError> {
        let conn = self.conn.lock().expect("cursor mutex");
        conn.execute(
            "UPDATE download_state SET status='failed', attempts=?1, last_error=?2 \
             WHERE date=?3",
            params![attempts as i64, error, date.to_string()],
        )?;
        Ok(())
    }

    /// Aggregated row counts grouped by status. Cheap summary for
    /// progress reporting.
    pub fn summary(&self) -> Result<CursorSummary, BulkError> {
        let conn = self.conn.lock().expect("cursor mutex");
        let mut s = CursorSummary::default();
        let mut stmt =
            conn.prepare("SELECT status, COUNT(*) FROM download_state GROUP BY status")?;
        let rows = stmt.query_map([], |r| {
            let status: String = r.get(0)?;
            let n: i64 = r.get(1)?;
            Ok((status, n as usize))
        })?;
        for r in rows {
            let (status, n) = r?;
            match status.as_str() {
                "pending" => s.pending = n,
                "done" => s.done = n,
                "failed" => s.failed = n,
                _ => {}
            }
        }
        Ok(s)
    }
}

/// Status histogram over the cursor.
#[derive(Debug, Default, Clone, Copy)]
pub struct CursorSummary {
    pub pending: usize,
    pub done: usize,
    pub failed: usize,
}

/// Knobs for a bulk-download run.
#[derive(Debug, Clone, Copy)]
pub struct BulkConfig {
    /// Max simultaneous S3 GETs. README §5.2 starts at 16.
    pub concurrency: usize,
    /// Per-day retry ceiling. Includes the first attempt.
    pub max_attempts: u32,
    /// Initial backoff on transient failures (429/5xx/transport).
    pub base_backoff_ms: u64,
    /// Hard ceiling so a string of 5xx doesn't wedge for hours.
    pub max_backoff_ms: u64,
}

impl Default for BulkConfig {
    fn default() -> Self {
        Self {
            concurrency: 16,
            max_attempts: 5,
            base_backoff_ms: 500,
            max_backoff_ms: 60_000,
        }
    }
}

/// Run summary printed by `bin/bulk-download`.
#[derive(Debug, Default, Clone, Copy)]
pub struct BulkRunStats {
    /// Dates attempted on this run (i.e. rows not in `done` at start).
    pub attempted: usize,
    /// Dates that ended `done` with bytes > 0.
    pub done_with_bytes: usize,
    /// Dates that ended `done` with bytes == 0 (holiday / NoSuchKey).
    pub done_empty: usize,
    /// Dates that ended `failed` after `max_attempts`.
    pub failed: usize,
    /// Total bytes downloaded on this run (excluding empty days).
    pub total_bytes: u64,
}

/// Compute the inclusive weekday-only date list between `start` and `end`.
/// Holidays remain — they enter the cursor and present as NoSuchKey on
/// S3, which the downloader records as empty-success.
pub fn weekday_range(start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>, BulkError> {
    if start > end {
        return Err(BulkError::InvalidRange { start, end });
    }
    let mut out = Vec::new();
    let mut d = start;
    while d <= end {
        if !matches!(d.weekday(), Weekday::Sat | Weekday::Sun) {
            out.push(d);
        }
        d += Duration::days(1);
    }
    Ok(out)
}

/// Where each day's staged file lives. Mirrors §5.4.
pub fn staging_path(staging_dir: &Path, date: NaiveDate) -> PathBuf {
    staging_dir
        .join(format!("{:04}", date.year()))
        .join(format!("{:02}", date.month()))
        .join(format!("{}.csv.gz", date))
}

/// Classify an S3 error into either a "no file for this date" terminal or
/// a transient. NoSuchKey ⇒ terminal (record as empty-success). All other
/// SdkErrors and IO errors are retried as transient.
fn classify_s3_error(e: &S3Error) -> ErrorClass {
    if e.is_no_such_key() {
        ErrorClass::NoFile
    } else {
        ErrorClass::Transient
    }
}

#[derive(Debug)]
enum ErrorClass {
    /// Terminal — Massive doesn't publish a file for this date. Record
    /// as `done` with `bytes=0`.
    NoFile,
    /// Retryable.
    Transient,
}

/// Drive the bulk download. Inserts pending rows for everything in
/// `dates` that's not already in the cursor, then attempts each undone
/// row with bounded concurrency + retry. Atomic per-day update: the row
/// only flips to `done` after the file bytes are flushed to disk.
pub async fn run_bulk_download(
    client: Arc<FlatFileClient>,
    cursor: Arc<DownloadCursor>,
    staging_dir: &Path,
    dates: Vec<NaiveDate>,
    config: BulkConfig,
) -> Result<BulkRunStats, BulkError> {
    cursor.upsert_pending(dates)?;
    let undone = cursor.list_undone()?;
    let attempted = undone.len();
    let staging_dir = staging_dir.to_path_buf();
    let sem = Arc::new(Semaphore::new(config.concurrency));

    let mut joinset: tokio::task::JoinSet<(NaiveDate, DayOutcome)> =
        tokio::task::JoinSet::new();

    for rec in undone {
        let client = Arc::clone(&client);
        let sem = Arc::clone(&sem);
        let cfg = config;
        let dir = staging_dir.clone();
        joinset.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            let outcome = download_one_day(&client, &dir, rec.date, cfg).await;
            (rec.date, outcome)
        });
    }

    let mut stats = BulkRunStats { attempted, ..Default::default() };
    while let Some(join_res) = joinset.join_next().await {
        let (date, outcome) = join_res.expect("download task panicked");
        match outcome {
            DayOutcome::DoneBytes(n) => {
                cursor.mark_done(date, n)?;
                stats.done_with_bytes += 1;
                stats.total_bytes += n;
            }
            DayOutcome::DoneEmpty => {
                cursor.mark_done(date, 0)?;
                stats.done_empty += 1;
            }
            DayOutcome::Failed { attempts, last_error } => {
                cursor.mark_failed(date, attempts, &last_error)?;
                stats.failed += 1;
                tracing::warn!(%date, attempts, error = %last_error, "bulk download failed");
            }
        }
    }
    Ok(stats)
}

#[derive(Debug)]
enum DayOutcome {
    DoneBytes(u64),
    DoneEmpty,
    Failed { attempts: u32, last_error: String },
}

async fn download_one_day(
    client: &FlatFileClient,
    staging_dir: &Path,
    date: NaiveDate,
    cfg: BulkConfig,
) -> DayOutcome {
    let key = FlatFileClient::minute_aggs_key(date);
    let out_path = staging_path(staging_dir, date);
    if let Some(parent) = out_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return DayOutcome::Failed {
                attempts: 0,
                last_error: format!("create_dir_all({}): {e}", parent.display()),
            };
        }
    }

    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match client.get_object_bytes(&key).await {
            Ok(bytes) => {
                if let Err(e) = std::fs::write(&out_path, &bytes) {
                    return DayOutcome::Failed {
                        attempts: attempt,
                        last_error: format!("write({}): {e}", out_path.display()),
                    };
                }
                return DayOutcome::DoneBytes(bytes.len() as u64);
            }
            Err(e) => match classify_s3_error(&e) {
                ErrorClass::NoFile => return DayOutcome::DoneEmpty,
                ErrorClass::Transient => {
                    if attempt >= cfg.max_attempts {
                        return DayOutcome::Failed {
                            attempts: attempt,
                            last_error: format!("{e}"),
                        };
                    }
                    let exp = (cfg.base_backoff_ms.saturating_mul(1u64 << attempt))
                        .min(cfg.max_backoff_ms);
                    let jitter = fastrand::u64(0..=(exp / 2 + 1));
                    sleep(TokioDuration::from_millis(exp + jitter)).await;
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weekday_range_excludes_sat_sun() {
        // Mon 2025-01-06 .. Sun 2025-01-12 → 5 weekdays
        let start = NaiveDate::from_ymd_opt(2025, 1, 6).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 12).unwrap();
        let v = weekday_range(start, end).unwrap();
        assert_eq!(v.len(), 5);
        assert_eq!(v.first().unwrap().weekday(), Weekday::Mon);
        assert_eq!(v.last().unwrap().weekday(), Weekday::Fri);
        for d in &v {
            assert!(!matches!(d.weekday(), Weekday::Sat | Weekday::Sun));
        }
    }

    #[test]
    fn weekday_range_rejects_inverted() {
        let a = NaiveDate::from_ymd_opt(2025, 1, 6).unwrap();
        let b = NaiveDate::from_ymd_opt(2025, 1, 5).unwrap();
        assert!(matches!(
            weekday_range(a, b),
            Err(BulkError::InvalidRange { .. })
        ));
    }

    #[test]
    fn staging_path_matches_5_4_layout() {
        let p = staging_path(
            Path::new("/data/_staging/flat_files"),
            NaiveDate::from_ymd_opt(2026, 6, 4).unwrap(),
        );
        assert_eq!(
            p,
            PathBuf::from("/data/_staging/flat_files/2026/06/2026-06-04.csv.gz")
        );
    }

    #[test]
    fn cursor_upsert_idempotent_and_done_sticks() {
        let cursor = DownloadCursor::in_memory().unwrap();
        let d1 = NaiveDate::from_ymd_opt(2025, 1, 6).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2025, 1, 7).unwrap();
        let n = cursor.upsert_pending([d1, d2]).unwrap();
        assert_eq!(n, 2);

        cursor.mark_done(d1, 12345).unwrap();
        // Re-upsert: d1 must stay done, d2 stays pending.
        let n2 = cursor.upsert_pending([d1, d2]).unwrap();
        assert_eq!(n2, 0, "INSERT OR IGNORE — no rows added");

        let undone = cursor.list_undone().unwrap();
        assert_eq!(undone.len(), 1);
        assert_eq!(undone[0].date, d2);
        assert_eq!(undone[0].status, DayStatus::Pending);

        let s = cursor.summary().unwrap();
        assert_eq!(s.done, 1);
        assert_eq!(s.pending, 1);
        assert_eq!(s.failed, 0);
    }

    #[test]
    fn cursor_failed_can_be_retried_on_next_run() {
        let cursor = DownloadCursor::in_memory().unwrap();
        let d = NaiveDate::from_ymd_opt(2025, 1, 6).unwrap();
        cursor.upsert_pending([d]).unwrap();
        cursor.mark_failed(d, 5, "boom").unwrap();

        // list_undone includes failed rows so the next run retries them.
        let undone = cursor.list_undone().unwrap();
        assert_eq!(undone.len(), 1);
        assert_eq!(undone[0].status, DayStatus::Failed);
        assert_eq!(undone[0].attempts, 5);
        assert_eq!(undone[0].last_error.as_deref(), Some("boom"));

        // Then a successful retry flips it to done.
        cursor.mark_done(d, 999).unwrap();
        let undone2 = cursor.list_undone().unwrap();
        assert!(undone2.is_empty());
    }
}
