use crate::bar::Bar;
use crate::error::StoreError;
use crate::ids::SecurityId;
use chrono::NaiveDate;

#[derive(Debug, Clone)]
pub struct Session {
    pub day: NaiveDate,
    pub bars: Vec<Bar>,
}

impl Session {
    pub fn new(day: NaiveDate, bars: Vec<Bar>) -> Self {
        Self { day, bars }
    }

    pub fn len(&self) -> usize {
        self.bars.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bars.is_empty()
    }

    pub fn first(&self) -> Option<&Bar> {
        self.bars.first()
    }

    pub fn last(&self) -> Option<&Bar> {
        self.bars.last()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Bar> {
        self.bars.iter()
    }
}

pub trait BarReader {
    fn snapshot_pin(&self) -> NaiveDate;

    fn session_bars(
        &self,
        sid: &SecurityId,
        day: NaiveDate,
    ) -> Result<Session, StoreError>;
}
