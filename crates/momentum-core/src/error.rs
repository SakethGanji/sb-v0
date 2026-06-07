use crate::ids::SecurityId;
use chrono::NaiveDate;

#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("store error: {0}")]
    Store(#[from] StoreError),
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("no bars found for {security_id} on {date}")]
    MissingBars {
        security_id: SecurityId,
        date: NaiveDate,
    },
    #[error(
        "splits snapshot mismatch for {security_id}: file stamped {file_stamp}, run pinned {run_pin}"
    )]
    SnapshotMismatch {
        security_id: SecurityId,
        file_stamp: NaiveDate,
        run_pin: NaiveDate,
    },
    #[error("could not resolve {display_symbol} at {at} to a security_id")]
    RenameUnresolved {
        display_symbol: String,
        at: chrono::DateTime<chrono::Utc>,
    },
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
