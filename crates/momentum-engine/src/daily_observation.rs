//! `daily_observation` builder — B0 partial coverage.
//!
//! Fills the identity columns, the two `[SRC]` bar-list columns
//! (`intraday_1m_first_hour` 09:30–10:30 ET, `intraday_10m_rest`
//! 10:30→session close, RFC §7), the bar-count data-quality fields, and
//! the calendar fields. Every other column is written as a typed null
//! array — B1 fills them. The one knowingly-placeholder non-null value is
//! `days_since_first_bar = 0` (needs the full-history first-bar map that
//! lands with B1's daily-aggregate preload); files are stamped
//! `engine_milestone = B0-skeleton` so this can't be mistaken for output.

use crate::slices::{aggregate, et, slice};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, ListArray,
    RecordBatch, StringArray, StructArray, TimestampNanosecondArray, new_null_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use chrono_tz::America::New_York;
use momentum_core::bar::Bar;
use momentum_core::phase0_outputs::daily_observation_schema;
use momentum_store::bar_reader::DaySession;
use std::collections::HashMap;
use std::sync::Arc;

/// Build the day's `daily_observation` batch (B0 coverage — see module doc).
/// `session_close` is the calendar's data-derived close (max SPY bar `t`),
/// NOT a hardcoded 16:00 — frozen decision #8.
pub fn build_b0(
    day: NaiveDate,
    sessions: &[DaySession],
    session_close: DateTime<Utc>,
) -> Result<RecordBatch, ArrowError> {
    let schema = daily_observation_schema();
    let n = sessions.len();

    let premarket_start = et(day, 4, 0);
    let rth_open = et(day, 9, 30);
    let first_30m_end = et(day, 10, 0);
    let first_hour_end = et(day, 10, 30);
    // session_close is the last bar's open timestamp; +1min closes the
    // half-open window over that final bar.
    let rth_end = session_close + chrono::Duration::minutes(1);

    // Half day iff the session closes before 14:00 ET (half-day close is
    // 13:00 ET; full-day last bar is 15:59/16:00 ET).
    let close_et = session_close.with_timezone(&New_York);
    let is_half_day = close_et.hour() < 14;

    let mut first_hour_slices: Vec<&[Bar]> = Vec::with_capacity(n);
    let mut rest_10m: Vec<Vec<Bar>> = Vec::with_capacity(n);
    let mut bar_count_rth = Vec::with_capacity(n);
    let mut bar_count_first_hour = Vec::with_capacity(n);
    let mut bar_count_first_30m = Vec::with_capacity(n);
    let mut bar_count_premarket = Vec::with_capacity(n);
    let mut first_rth_bar_time: Vec<Option<String>> = Vec::with_capacity(n);
    let mut minutes_after_open_first_bar: Vec<Option<i32>> = Vec::with_capacity(n);

    for s in sessions {
        let bars = &s.session.bars;
        let first_hour = slice(bars, rth_open, first_hour_end);
        let rth = slice(bars, rth_open, rth_end);

        bar_count_rth.push(rth.len() as i32);
        bar_count_first_hour.push(first_hour.len() as i32);
        bar_count_first_30m.push(slice(bars, rth_open, first_30m_end).len() as i32);
        bar_count_premarket.push(slice(bars, premarket_start, rth_open).len() as i32);

        match rth.first() {
            Some(b) => {
                let t_et = b.t.with_timezone(&New_York);
                first_rth_bar_time.push(Some(format!("{:02}:{:02}", t_et.hour(), t_et.minute())));
                minutes_after_open_first_bar.push(Some((b.t - rth_open).num_minutes() as i32));
            }
            None => {
                first_rth_bar_time.push(None);
                minutes_after_open_first_bar.push(None);
            }
        }

        first_hour_slices.push(first_hour);
        rest_10m.push(aggregate(bars, first_hour_end, rth_end, 10));
    }

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let day32 = (day - epoch).num_days() as i32;

    // Filled columns, keyed by name; everything else becomes a typed null.
    let mut filled: HashMap<&str, ArrayRef> = HashMap::new();
    filled.insert("day", Arc::new(Date32Array::from(vec![day32; n])));
    filled.insert(
        "security_id",
        Arc::new(StringArray::from(
            sessions.iter().map(|s| s.security_id.as_str()).collect::<Vec<_>>(),
        )),
    );
    filled.insert(
        "display_symbol_on_day",
        Arc::new(StringArray::from(
            sessions.iter().map(|s| s.display_symbol.as_str()).collect::<Vec<_>>(),
        )),
    );
    filled.insert(
        "intraday_1m_first_hour",
        bars_list_array(
            schema.field_with_name("intraday_1m_first_hour").expect("field").data_type(),
            &first_hour_slices,
        ),
    );
    let rest_slices: Vec<&[Bar]> = rest_10m.iter().map(Vec::as_slice).collect();
    filled.insert(
        "intraday_10m_rest",
        bars_list_array(
            schema.field_with_name("intraday_10m_rest").expect("field").data_type(),
            &rest_slices,
        ),
    );
    filled.insert("bar_count_rth", Arc::new(Int32Array::from(bar_count_rth)));
    filled.insert("bar_count_first_hour", Arc::new(Int32Array::from(bar_count_first_hour)));
    filled.insert("bar_count_first_30m", Arc::new(Int32Array::from(bar_count_first_30m)));
    filled.insert("bar_count_premarket", Arc::new(Int32Array::from(bar_count_premarket)));
    filled.insert("first_rth_bar_time", Arc::new(StringArray::from(first_rth_bar_time)));
    filled.insert(
        "minutes_after_open_first_bar",
        Arc::new(Int32Array::from(minutes_after_open_first_bar)),
    );
    filled.insert(
        "day_of_week",
        Arc::new(Int32Array::from(vec![
            chrono::Datelike::weekday(&day).num_days_from_monday() as i32;
            n
        ])),
    );
    // B0 placeholder (non-nullable field) — real value needs the
    // full-history first-bar map (B1).
    filled.insert("days_since_first_bar", Arc::new(Int32Array::from(vec![0; n])));
    filled.insert("is_half_day", Arc::new(BooleanArray::from(vec![is_half_day; n])));

    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| {
            filled
                .remove(f.name().as_str())
                .unwrap_or_else(|| new_null_array(f.data_type(), n))
        })
        .collect();

    RecordBatch::try_new(schema, columns)
}

/// Build a `List<Struct<t,o,h,l,c,v>>` array (one list per row) matching
/// the schema's exact field layout, from per-row bar slices.
fn bars_list_array(list_dt: &DataType, per_row: &[&[Bar]]) -> ArrayRef {
    let DataType::List(item_field) = list_dt else {
        panic!("bars column must be List, got {list_dt:?}");
    };
    let DataType::Struct(struct_fields) = item_field.data_type() else {
        panic!("bars list item must be Struct");
    };

    let total: usize = per_row.iter().map(|b| b.len()).sum();
    let mut t: Vec<i64> = Vec::with_capacity(total);
    let mut open = Vec::with_capacity(total);
    let mut high = Vec::with_capacity(total);
    let mut low = Vec::with_capacity(total);
    let mut close = Vec::with_capacity(total);
    let mut volume = Vec::with_capacity(total);
    let mut offsets: Vec<i32> = Vec::with_capacity(per_row.len() + 1);
    offsets.push(0);
    for bars in per_row {
        for b in *bars {
            t.push(b.t.timestamp_nanos_opt().expect("in-range timestamp"));
            open.push(b.open);
            high.push(b.high);
            low.push(b.low);
            close.push(b.close);
            volume.push(b.volume);
        }
        offsets.push(t.len() as i32);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(t).with_timezone("UTC")),
        Arc::new(Float64Array::from(open)),
        Arc::new(Float64Array::from(high)),
        Arc::new(Float64Array::from(low)),
        Arc::new(Float64Array::from(close)),
        Arc::new(Float64Array::from(volume)),
    ];
    let child = StructArray::new(struct_fields.clone(), arrays, None);
    Arc::new(ListArray::new(
        item_field.clone(),
        OffsetBuffer::new(offsets.into()),
        Arc::new(child),
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use momentum_core::ids::SecurityId;
    use momentum_core::store::Session;

    fn mk_session(day: NaiveDate, sid: &str, symbol: &str, minutes_et: &[(u32, u32)]) -> DaySession {
        let bars = minutes_et
            .iter()
            .map(|&(h, m)| Bar {
                t: et(day, h, m),
                open: 10.0,
                high: 11.0,
                low: 9.0,
                close: 10.5,
                volume: 100.0,
            })
            .collect();
        DaySession {
            security_id: SecurityId::new(sid),
            display_symbol: symbol.to_string(),
            session: Session::new(day, bars),
        }
    }

    #[test]
    fn b0_batch_matches_schema_and_counts() {
        let day = NaiveDate::from_ymd_opt(2021, 3, 15).unwrap();
        let close = et(day, 15, 59);
        let sessions = vec![
            // Premarket bar, two first-30m bars, one 10:15 bar, one 14:00 bar.
            mk_session(day, "BBG000TEST01", "AAA", &[(8, 0), (9, 30), (9, 45), (10, 15), (14, 0)]),
            // First bar late at 9:42 (gappy microcap).
            mk_session(day, "BBG000TEST02", "BBB", &[(9, 42), (10, 45)]),
        ];

        let batch = build_b0(day, &sessions, close).unwrap();
        assert_eq!(batch.schema(), daily_observation_schema());
        assert_eq!(batch.num_rows(), 2);

        let col = |name: &str| batch.column_by_name(name).unwrap().clone();

        let pm = col("bar_count_premarket");
        let pm = pm.as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(pm.value(0), 1);
        assert_eq!(pm.value(1), 0);

        let f30 = col("bar_count_first_30m");
        let f30 = f30.as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(f30.value(0), 2);
        assert_eq!(f30.value(1), 1);

        let rth = col("bar_count_rth");
        let rth = rth.as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(rth.value(0), 4, "premarket bar excluded");

        let mins = col("minutes_after_open_first_bar");
        let mins = mins.as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(mins.value(0), 0);
        assert_eq!(mins.value(1), 12);

        let fb = col("first_rth_bar_time");
        let fb = fb.as_string::<i32>();
        assert_eq!(fb.value(0), "09:30");
        assert_eq!(fb.value(1), "09:42");

        // First-hour list: row 0 carries 9:30, 9:45, 10:15 (3 bars).
        let fh = col("intraday_1m_first_hour");
        let fh = fh.as_list::<i32>();
        assert_eq!(fh.value(0).len(), 3);
        assert_eq!(fh.value(1).len(), 1, "only the 9:42 bar; 10:45 is in rest");

        // 10m rest: row 0 has one bar (14:00 bucket); 10:15 is first-hour.
        let rest = col("intraday_10m_rest");
        let rest = rest.as_list::<i32>();
        assert_eq!(rest.value(0).len(), 1);
        assert_eq!(rest.value(1).len(), 1, "10:45 bar lands in rest");

        // Unfilled v2 column is a typed null array, not absent.
        let sig = col("signal_concentration_hhi_today");
        assert_eq!(sig.null_count(), 2);

        // Full-day close → not a half day.
        let hd = col("is_half_day");
        let hd = hd.as_boolean();
        assert!(!hd.value(0));
    }

    #[test]
    fn half_day_close_sets_flag() {
        let day = NaiveDate::from_ymd_opt(2021, 11, 26).unwrap(); // post-Thanksgiving half day
        let close = et(day, 12, 59);
        let sessions = vec![mk_session(day, "BBG000TEST01", "AAA", &[(9, 30)])];
        let batch = build_b0(day, &sessions, close).unwrap();
        let hd = batch.column_by_name("is_half_day").unwrap();
        assert!(hd.as_boolean().value(0));
    }
}
