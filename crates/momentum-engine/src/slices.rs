//! Session window slicing over a security's sorted 1-minute bars.
//!
//! All session boundaries are expressed in **ET wall-clock** and converted
//! to UTC per-day via `America/New_York` (DST-correct: 09:30 ET is 14:30
//! UTC in winter, 13:30 UTC in summer). Bars are sorted by `t` (the
//! per-day files are sorted `(display_symbol, t)`), so every slice is two
//! binary searches.

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use momentum_core::bar::Bar;

/// `day` HH:MM ET → UTC. Panics only on times that don't exist on a DST
/// transition day — session boundaries (04:00/09:30/10:00/10:30/16:00)
/// never fall inside the 02:00–03:00 ET transition window.
pub fn et(day: NaiveDate, hour: u32, minute: u32) -> DateTime<Utc> {
    New_York
        .from_local_datetime(&day.and_hms_opt(hour, minute, 0).expect("valid hh:mm"))
        .single()
        .expect("session boundaries never fall inside a DST gap")
        .with_timezone(&Utc)
}

/// Half-open window `[start, end)` over bars sorted by `t`.
pub fn slice(bars: &[Bar], start: DateTime<Utc>, end: DateTime<Utc>) -> &[Bar] {
    let lo = bars.partition_point(|b| b.t < start);
    let hi = bars.partition_point(|b| b.t < end);
    &bars[lo..hi]
}

/// Aggregate 1m bars in `[start, end)` into fixed `width_min`-minute bars
/// anchored at `start`. Empty buckets produce no bar (matches the raw
/// tape, which has no rows for no-trade minutes). Output bar `t` = bucket
/// start.
pub fn aggregate(bars: &[Bar], start: DateTime<Utc>, end: DateTime<Utc>, width_min: i64) -> Vec<Bar> {
    let window = slice(bars, start, end);
    let mut out: Vec<Bar> = Vec::new();
    let mut current_bucket: Option<i64> = None;
    for b in window {
        let bucket = (b.t - start).num_minutes() / width_min;
        match current_bucket {
            Some(cb) if cb == bucket => {
                let last = out.last_mut().expect("bucket open implies bar exists");
                last.high = last.high.max(b.high);
                last.low = last.low.min(b.low);
                last.close = b.close;
                last.volume += b.volume;
            }
            _ => {
                out.push(Bar {
                    t: start + chrono::Duration::minutes(bucket * width_min),
                    open: b.open,
                    high: b.high,
                    low: b.low,
                    close: b.close,
                    volume: b.volume,
                });
                current_bucket = Some(bucket);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(t: DateTime<Utc>, o: f64, h: f64, l: f64, c: f64, v: f64) -> Bar {
        Bar { t, open: o, high: h, low: l, close: c, volume: v }
    }

    #[test]
    fn et_conversion_is_dst_correct() {
        // Winter (EST, UTC-5): 09:30 ET = 14:30 UTC.
        let winter = NaiveDate::from_ymd_opt(2021, 1, 15).unwrap();
        assert_eq!(et(winter, 9, 30), Utc.with_ymd_and_hms(2021, 1, 15, 14, 30, 0).unwrap());
        // Summer (EDT, UTC-4): 09:30 ET = 13:30 UTC.
        let summer = NaiveDate::from_ymd_opt(2021, 7, 15).unwrap();
        assert_eq!(et(summer, 9, 30), Utc.with_ymd_and_hms(2021, 7, 15, 13, 30, 0).unwrap());
    }

    #[test]
    fn slice_is_half_open() {
        let day = NaiveDate::from_ymd_opt(2021, 1, 15).unwrap();
        let bars: Vec<Bar> = (0..5)
            .map(|i| bar(et(day, 9, 30 + i), 1.0, 1.0, 1.0, 1.0, 1.0))
            .collect();
        let s = slice(&bars, et(day, 9, 31), et(day, 9, 33));
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].t, et(day, 9, 31));
        assert_eq!(s[1].t, et(day, 9, 32));
    }

    #[test]
    fn aggregate_10m_ohlcv_semantics_with_gaps() {
        let day = NaiveDate::from_ymd_opt(2021, 1, 15).unwrap();
        // Two bars in the first 10m bucket, none in the second, one in the third.
        let bars = vec![
            bar(et(day, 10, 31), 10.0, 12.0, 9.0, 11.0, 100.0),
            bar(et(day, 10, 38), 11.0, 15.0, 10.5, 14.0, 200.0),
            bar(et(day, 10, 55), 14.0, 14.5, 13.0, 13.5, 50.0),
        ];
        let agg = aggregate(&bars, et(day, 10, 30), et(day, 16, 0), 10);
        assert_eq!(agg.len(), 2, "empty bucket produces no bar");

        let b0 = &agg[0];
        assert_eq!(b0.t, et(day, 10, 30));
        assert_eq!(b0.open, 10.0);
        assert_eq!(b0.high, 15.0);
        assert_eq!(b0.low, 9.0);
        assert_eq!(b0.close, 14.0);
        assert_eq!(b0.volume, 300.0);

        let b1 = &agg[1];
        assert_eq!(b1.t, et(day, 10, 50));
        assert_eq!(b1.volume, 50.0);
    }
}
