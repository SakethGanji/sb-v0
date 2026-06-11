//! SQLite resume cursor — same pattern as the ingest layer's
//! `_ingest_state.sqlite` (frozen decision: SQLite for tiny mutable state,
//! Parquet for data). One row per (table, day) marks that day's output file
//! as completely written, making engine re-runs idempotent: finished days
//! are skipped, an interrupted day is rewritten from scratch.

use chrono::NaiveDate;
use rusqlite::Connection;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum CursorError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

pub struct EngineCursor {
    conn: Connection,
}

impl EngineCursor {
    pub fn open(path: &Path) -> Result<Self, CursorError> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS day_done (
                table_name   TEXT NOT NULL,
                day          TEXT NOT NULL,
                completed_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (table_name, day)
            );",
        )?;
        Ok(Self { conn })
    }

    /// In-memory cursor for tests.
    pub fn in_memory() -> Result<Self, CursorError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS day_done (
                table_name   TEXT NOT NULL,
                day          TEXT NOT NULL,
                completed_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (table_name, day)
            );",
        )?;
        Ok(Self { conn })
    }

    pub fn is_done(&self, table: &str, day: NaiveDate) -> Result<bool, CursorError> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT 1 FROM day_done WHERE table_name = ?1 AND day = ?2")?;
        Ok(stmt.exists((table, day.to_string()))?)
    }

    /// Mark AFTER the output file is fully written and closed — the write
    /// is the commit point, this is just the bookmark.
    pub fn mark_done(&self, table: &str, day: NaiveDate) -> Result<(), CursorError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO day_done (table_name, day) VALUES (?1, ?2)",
            (table, day.to_string()),
        )?;
        Ok(())
    }

    /// Clear a day (e.g. `--force` rewrite).
    pub fn clear(&self, table: &str, day: NaiveDate) -> Result<(), CursorError> {
        self.conn.execute(
            "DELETE FROM day_done WHERE table_name = ?1 AND day = ?2",
            (table, day.to_string()),
        )?;
        Ok(())
    }

    pub fn done_count(&self, table: &str) -> Result<u64, CursorError> {
        let n: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM day_done WHERE table_name = ?1",
            (table,),
            |r| r.get(0),
        )?;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_roundtrip_and_idempotence() {
        let c = EngineCursor::in_memory().unwrap();
        let day = NaiveDate::from_ymd_opt(2021, 3, 15).unwrap();

        assert!(!c.is_done("daily_observation", day).unwrap());
        c.mark_done("daily_observation", day).unwrap();
        assert!(c.is_done("daily_observation", day).unwrap());
        // Same (table, day) twice is fine (INSERT OR REPLACE).
        c.mark_done("daily_observation", day).unwrap();
        assert_eq!(c.done_count("daily_observation").unwrap(), 1);

        // Different table tracks independently.
        assert!(!c.is_done("forward_outcomes", day).unwrap());

        c.clear("daily_observation", day).unwrap();
        assert!(!c.is_done("daily_observation", day).unwrap());
    }

    #[test]
    fn cursor_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("engine_state.sqlite");
        let day = NaiveDate::from_ymd_opt(2021, 3, 15).unwrap();
        {
            let c = EngineCursor::open(&path).unwrap();
            c.mark_done("daily_observation", day).unwrap();
        }
        let c = EngineCursor::open(&path).unwrap();
        assert!(c.is_done("daily_observation", day).unwrap());
    }
}
