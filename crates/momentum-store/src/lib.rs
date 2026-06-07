pub mod bar_reader;
pub mod bulk_download;
pub mod dividends;
pub mod flat_file;
pub mod pivot;
pub mod splits;
pub mod ticker_events;
pub mod tickers;

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
