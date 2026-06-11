//! Per-(security, day) daily aggregates, computed from the day's session
//! during the chronological sweep. These feed two places:
//!
//! 1. Day D's own `eod_*` / `premarket_*` columns (RES-only).
//! 2. The rolling per-security history ([`crate::rolling`]) that serves
//!    every trailing-window column on later days.
//!
//! Window boundaries use the calendar's data-derived `session_close`
//! (frozen decision #8: never a hardcoded 16:00), so half days produce
//! correct aggregates without special-casing.
//!
//! Prices arrive pin-basis split-adjusted from `day_sessions`, so
//! cross-day comparisons (trailing windows, overnight gaps) are on one
//! consistent basis. Dollar volume per bar is `close × volume`; vwap is
//! `Σ(close×vol) / Σvol` — documented proxy, the 1m tape has no per-bar
//! vwap field.

use crate::slices::{et, slice};
use chrono::{DateTime, NaiveDate, Utc};
use momentum_core::store::Session;

#[derive(Debug, Clone, Copy)]
pub struct DailyAgg {
    pub day: NaiveDate,
    // RTH (09:30 ET → session close)
    pub rth_open: f64,
    pub rth_high: f64,
    pub rth_low: f64,
    pub rth_close: f64,
    pub rth_volume: f64,
    pub rth_dollar_volume: f64,
    pub rth_vwap: f64,
    // Pre-market (04:00–09:30 ET); None when no premarket prints.
    pub premarket_volume: f64,
    pub premarket_dollar_volume: f64,
    pub premarket_high: Option<f64>,
    pub premarket_low: Option<f64>,
    pub premarket_vwap: Option<f64>,
    // Last 30 minutes of RTH (prior-day shape inputs).
    pub last_30m_return: Option<f64>,
    pub last_30m_volume_share: Option<f64>,
}

/// Compute the day's aggregates. Returns `None` when the session has no
/// RTH bars (premarket-only prints happen on halted/expiring names).
pub fn compute(session: &Session, day: NaiveDate, session_close: DateTime<Utc>) -> Option<DailyAgg> {
    let bars = &session.bars;
    let premarket_start = et(day, 4, 0);
    let rth_open_t = et(day, 9, 30);
    let rth_end = session_close + chrono::Duration::minutes(1);

    let rth = slice(bars, rth_open_t, rth_end);
    let first = rth.first()?;
    let last = rth.last().expect("non-empty");

    let mut high = f64::MIN;
    let mut low = f64::MAX;
    let mut vol = 0.0;
    let mut dvol = 0.0;
    for b in rth {
        high = high.max(b.high);
        low = low.min(b.low);
        vol += b.volume;
        dvol += b.close * b.volume;
    }

    let pm = slice(bars, premarket_start, rth_open_t);
    let mut pm_vol = 0.0;
    let mut pm_dvol = 0.0;
    let mut pm_high = f64::MIN;
    let mut pm_low = f64::MAX;
    for b in pm {
        pm_vol += b.volume;
        pm_dvol += b.close * b.volume;
        pm_high = pm_high.max(b.high);
        pm_low = pm_low.min(b.low);
    }

    // Last 30 minutes of the session, half-day correct by construction.
    let last_30m = slice(bars, rth_end - chrono::Duration::minutes(30), rth_end);
    let (last_30m_return, last_30m_volume_share) = match last_30m.first() {
        Some(w_first) if vol > 0.0 => {
            let w_vol: f64 = last_30m.iter().map(|b| b.volume).sum();
            let w_last = last_30m.last().expect("non-empty");
            (
                Some(w_last.close / w_first.open - 1.0),
                Some(w_vol / vol),
            )
        }
        _ => (None, None),
    };

    Some(DailyAgg {
        day,
        rth_open: first.open,
        rth_high: high,
        rth_low: low,
        rth_close: last.close,
        rth_volume: vol,
        rth_dollar_volume: dvol,
        rth_vwap: if vol > 0.0 { dvol / vol } else { last.close },
        premarket_volume: pm_vol,
        premarket_dollar_volume: pm_dvol,
        premarket_high: (!pm.is_empty()).then_some(pm_high),
        premarket_low: (!pm.is_empty()).then_some(pm_low),
        premarket_vwap: (pm_vol > 0.0).then(|| pm_dvol / pm_vol),
        last_30m_return,
        last_30m_volume_share,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use momentum_core::bar::Bar;

    fn bar(t: DateTime<Utc>, o: f64, h: f64, l: f64, c: f64, v: f64) -> Bar {
        Bar { t, open: o, high: h, low: l, close: c, volume: v }
    }

    #[test]
    fn aggregates_split_premarket_from_rth_and_compute_last_30m() {
        let day = NaiveDate::from_ymd_opt(2021, 3, 15).unwrap();
        let close_t = et(day, 15, 59);
        let bars = vec![
            bar(et(day, 8, 0), 99.0, 99.5, 98.5, 99.2, 500.0), // premarket
            bar(et(day, 9, 30), 100.0, 101.0, 99.5, 100.5, 1000.0),
            bar(et(day, 12, 0), 100.5, 102.0, 100.0, 101.5, 800.0),
            bar(et(day, 15, 31), 101.0, 101.5, 100.8, 101.2, 200.0), // last 30m
            bar(et(day, 15, 59), 101.2, 101.4, 101.0, 101.3, 300.0), // last 30m
        ];
        let s = Session::new(day, bars);
        let a = compute(&s, day, close_t).unwrap();

        assert_eq!(a.rth_open, 100.0);
        assert_eq!(a.rth_high, 102.0);
        assert_eq!(a.rth_low, 99.5);
        assert_eq!(a.rth_close, 101.3);
        assert_eq!(a.rth_volume, 2300.0);
        assert_eq!(a.premarket_volume, 500.0);
        assert_eq!(a.premarket_high, Some(99.5));

        // last 30m: [15:30, 16:00) → the 15:31 and 15:59 bars.
        assert!((a.last_30m_return.unwrap() - (101.3 / 101.0 - 1.0)).abs() < 1e-12);
        assert!((a.last_30m_volume_share.unwrap() - 500.0 / 2300.0).abs() < 1e-12);
    }

    #[test]
    fn premarket_only_session_yields_none() {
        let day = NaiveDate::from_ymd_opt(2021, 3, 15).unwrap();
        let s = Session::new(day, vec![bar(et(day, 7, 0), 1.0, 1.0, 1.0, 1.0, 10.0)]);
        assert!(compute(&s, day, et(day, 15, 59)).is_none());
    }
}
