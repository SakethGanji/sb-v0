use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bar {
    pub t: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}
