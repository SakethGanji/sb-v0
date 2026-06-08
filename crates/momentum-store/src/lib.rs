pub mod bar_reader;
pub mod bulk_download;
pub mod daily_convert;
pub mod dividends;
pub mod figi_map;
pub mod flat_file;
pub mod acceptance_backfill;
pub mod financials;
pub mod short_interest;
pub mod splits;
pub mod ticker_events;
pub mod tickers;
pub mod tickers_classified;
pub mod vix;

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
