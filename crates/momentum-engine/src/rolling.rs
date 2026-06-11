//! Rolling per-security trailing state, accumulated during the
//! chronological sweep.
//!
//! Contract (RFC §6.2): every value served by this module is computed
//! over `[D-N, D-1]` — strictly BEFORE the current day. The sweep
//! therefore reads features for day D first, then pushes D's aggregates.
//! `SecurityHistory` keeps a 252-trading-day window of [`DailyAgg`]
//! (enough for every trailing column incl. 52w range) plus O(1)
//! incremental counters for the unbounded stats (first-bar date,
//! consecutive up days, last ≥X% move).
//!
//! Resume note: trailing state is rebuilt by sweeping from dataset
//! start. A persisted snapshot cache is a later optimization; at the
//! measured ~0.36 s/day read cost a cold rebuild over the full window
//! is ~15 minutes.

use crate::aggregates::DailyAgg;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::collections::VecDeque;

const WINDOW: usize = 252;

/// Thresholds tracked incrementally for `days_since_last_X_pct_move`.
pub const MOVE_THRESHOLDS: [f64; 3] = [0.05, 0.10, 0.20];

#[derive(Debug)]
pub struct SecurityHistory {
    pub first_bar_day: NaiveDate,
    /// Total days pushed, 1-based (first day on record = 1). With the
    /// read-then-push sweep ordering, while day D is being processed this
    /// equals D's 0-based index among the security's trading days.
    total_days: u32,
    /// Up to the last 252 daily aggregates, chronological.
    days: VecDeque<DailyAgg>,
    /// `total_days` at the push whose close completed the last
    /// close-to-close |move| ≥ threshold.
    last_move_at: [Option<u32>; 3],
    consecutive_up: i32,
}

impl SecurityHistory {
    fn new(first_bar_day: NaiveDate) -> Self {
        Self {
            first_bar_day,
            total_days: 0,
            days: VecDeque::with_capacity(WINDOW),
            last_move_at: [None; 3],
            consecutive_up: 0,
        }
    }

    /// Push day D's aggregates AFTER day D's features were read.
    fn push(&mut self, agg: DailyAgg) {
        self.total_days += 1;
        if let Some(prev) = self.days.back() {
            let ret = agg.rth_close / prev.rth_close - 1.0;
            for (i, thr) in MOVE_THRESHOLDS.iter().enumerate() {
                if ret.abs() >= *thr {
                    self.last_move_at[i] = Some(self.total_days);
                }
            }
            self.consecutive_up = if ret > 0.0 { self.consecutive_up + 1 } else { 0 };
        }
        if self.days.len() == WINDOW {
            self.days.pop_front();
        }
        self.days.push_back(agg);
    }

    /// Most recent prior day's aggregates.
    pub fn prior(&self) -> Option<&DailyAgg> {
        self.days.back()
    }

    /// Trading days between the first bar and the day being processed.
    /// (The builder writes 0 itself for a never-seen sid's first day.)
    pub fn days_since_first_bar(&self) -> i32 {
        self.total_days as i32
    }

    /// ATR over the last `n` days (Wilder true range, simple mean).
    /// None until `n` prior days exist.
    pub fn atr(&self, n: usize) -> Option<f64> {
        if self.days.len() < n + 1 {
            return None; // need n ranges, each vs a previous close
        }
        let s = self.days.len() - n;
        let mut sum = 0.0;
        for i in s..self.days.len() {
            let d = &self.days[i];
            let prev_close = self.days[i - 1].rth_close;
            let tr = (d.rth_high - d.rth_low)
                .max((d.rth_high - prev_close).abs())
                .max((d.rth_low - prev_close).abs());
            sum += tr;
        }
        Some(sum / n as f64)
    }

    /// Annualized close-to-close realized volatility over `n` days.
    pub fn realized_vol(&self, n: usize) -> Option<f64> {
        if self.days.len() < n + 1 {
            return None;
        }
        let s = self.days.len() - n;
        let rets: Vec<f64> = (s..self.days.len())
            .map(|i| (self.days[i].rth_close / self.days[i - 1].rth_close).ln())
            .collect();
        let mean = rets.iter().sum::<f64>() / n as f64;
        let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n as f64 - 1.0);
        Some(var.sqrt() * (252.0f64).sqrt())
    }

    /// Average daily RTH share volume over the last `n` days.
    pub fn adv(&self, n: usize) -> Option<f64> {
        self.window_mean(n, |d| d.rth_volume)
    }

    /// Average daily RTH dollar volume over the last `n` days.
    pub fn addv(&self, n: usize) -> Option<f64> {
        self.window_mean(n, |d| d.rth_dollar_volume)
    }

    /// Trailing 252-day high/low (split-adjusted). Partial windows are
    /// served as-is — a 100-day-old IPO's "52w high" is its life high,
    /// which is the point-in-time-correct answer.
    pub fn high_52w(&self) -> Option<f64> {
        self.days.iter().map(|d| d.rth_high).fold(None, |m, v| Some(m.map_or(v, |x: f64| x.max(v))))
    }

    pub fn low_52w(&self) -> Option<f64> {
        self.days.iter().map(|d| d.rth_low).fold(None, |m, v| Some(m.map_or(v, |x: f64| x.min(v))))
    }

    /// Yang-Zhang volatility over the last `n` days, annualized (√252).
    /// σ²_YZ = σ²_overnight + k·σ²_open-close + (1−k)·σ²_RS,
    /// k = 0.34 / (1.34 + (n+1)/(n−1)) (Yang & Zhang 2000).
    pub fn yang_zhang_vol(&self, n: usize) -> Option<f64> {
        if self.days.len() < n + 1 || n < 2 {
            return None;
        }
        let s = self.days.len() - n;
        let mut o_rets = Vec::with_capacity(n);
        let mut c_rets = Vec::with_capacity(n);
        let mut rs_sum = 0.0;
        for i in s..self.days.len() {
            let d = &self.days[i];
            let prev_close = self.days[i - 1].rth_close;
            if prev_close <= 0.0 || d.rth_open <= 0.0 || d.rth_low <= 0.0 {
                return None; // degenerate prices; refuse rather than emit junk
            }
            o_rets.push((d.rth_open / prev_close).ln());
            let c = (d.rth_close / d.rth_open).ln();
            c_rets.push(c);
            let u = (d.rth_high / d.rth_open).ln();
            let l = (d.rth_low / d.rth_open).ln();
            rs_sum += u * (u - c) + l * (l - c);
        }
        let sample_var = |v: &[f64]| {
            let m = v.iter().sum::<f64>() / v.len() as f64;
            v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (v.len() as f64 - 1.0)
        };
        let k = 0.34 / (1.34 + (n as f64 + 1.0) / (n as f64 - 1.0));
        let yz = sample_var(&o_rets) + k * sample_var(&c_rets) + (1.0 - k) * rs_sum / n as f64;
        (yz >= 0.0).then(|| (yz * 252.0).sqrt())
    }

    /// Beta vs an index over the last `n` daily log returns. Stock and
    /// index returns are aligned by date through the index's
    /// cumulative-log-return map, so a stock with missing days matches a
    /// compounded index return over the same span. Requires ≥75% of the
    /// window matched.
    pub fn beta(&self, index_cumlog: &HashMap<NaiveDate, f64>, n: usize) -> Option<f64> {
        if self.days.len() < n + 1 {
            return None;
        }
        let s = self.days.len() - n;
        let mut xs = Vec::with_capacity(n);
        let mut ys = Vec::with_capacity(n);
        for i in s..self.days.len() {
            let prev = &self.days[i - 1];
            let cur = &self.days[i];
            if let (Some(a), Some(b)) =
                (index_cumlog.get(&prev.day), index_cumlog.get(&cur.day))
            {
                if prev.rth_close > 0.0 && cur.rth_close > 0.0 {
                    xs.push(b - a);
                    ys.push((cur.rth_close / prev.rth_close).ln());
                }
            }
        }
        if xs.len() < n * 3 / 4 {
            return None;
        }
        let m = xs.len() as f64;
        let mx = xs.iter().sum::<f64>() / m;
        let my = ys.iter().sum::<f64>() / m;
        let mut cov = 0.0;
        let mut var = 0.0;
        for (x, y) in xs.iter().zip(&ys) {
            cov += (x - mx) * (y - my);
            var += (x - mx).powi(2);
        }
        (var > 0.0).then(|| cov / var)
    }

    /// Did the default signal (`snapshot_ret_1000 > 0`) fire on any of
    /// the last `n` recorded days? None if fewer than `n` days on record
    /// (can't verify "none of the prior n").
    pub fn signal_fired_in_last(&self, n: usize) -> Option<bool> {
        if self.days.len() < n {
            return None;
        }
        Some(
            self.days
                .iter()
                .rev()
                .take(n)
                .any(|d| d.snapshot_ret_1000.is_some_and(|r| r > 0.0)),
        )
    }

    /// Median premarket volume over the last `n` days.
    pub fn premarket_volume_median(&self, n: usize) -> Option<f64> {
        if self.days.len() < n {
            return None;
        }
        let mut v: Vec<f64> = self.days.iter().rev().take(n).map(|d| d.premarket_volume).collect();
        v.sort_by(|a, b| a.partial_cmp(b).expect("no NaN volumes"));
        Some((v[n / 2] + v[(n - 1) / 2]) / 2.0)
    }

    /// Trading days between the close that completed the last
    /// |move| ≥ `MOVE_THRESHOLDS[idx]` and the day being processed
    /// (move completed yesterday → 1). None if no move on record.
    pub fn days_since_move(&self, idx: usize) -> Option<i32> {
        self.last_move_at[idx].map(|at| (self.total_days - at) as i32 + 1)
    }

    pub fn consecutive_up_days(&self) -> i32 {
        self.consecutive_up
    }

    fn window_mean(&self, n: usize, f: impl Fn(&DailyAgg) -> f64) -> Option<f64> {
        if self.days.len() < n {
            return None;
        }
        Some(self.days.iter().rev().take(n).map(f).sum::<f64>() / n as f64)
    }
}

/// All securities' trailing state, keyed by security_id string.
#[derive(Default)]
pub struct RollingState {
    map: HashMap<String, SecurityHistory>,
}

impl RollingState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read-side: day D's features. Returns None for a never-seen sid
    /// (its first day — every trailing column is null by construction).
    pub fn get(&self, sid: &str) -> Option<&SecurityHistory> {
        self.map.get(sid)
    }

    /// Write-side: push day D's aggregates AFTER features were read.
    pub fn update(&mut self, sid: &str, day: NaiveDate, agg: DailyAgg) {
        self.map
            .entry(sid.to_string())
            .or_insert_with(|| SecurityHistory::new(day))
            .push(agg);
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agg(day: NaiveDate, o: f64, h: f64, l: f64, c: f64, v: f64) -> DailyAgg {
        DailyAgg {
            day,
            rth_open: o,
            rth_high: h,
            rth_low: l,
            rth_close: c,
            rth_close_unadjusted: c,
            rth_volume: v,
            rth_dollar_volume: c * v,
            rth_vwap: c,
            snapshot_ret_1000: None,
            premarket_volume: 0.0,
            premarket_dollar_volume: 0.0,
            premarket_high: None,
            premarket_low: None,
            premarket_vwap: None,
            last_30m_return: None,
            last_30m_volume_share: None,
        }
    }

    fn d(n: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap() + chrono::Duration::days(n as i64)
    }

    #[test]
    fn counters_track_moves_and_streaks() {
        let mut st = RollingState::new();
        let sid = "X";
        // closes: 100, 101 (+1%), 112 (+10.9%), 113, 114, 113
        for (i, c) in [100.0, 101.0, 112.0, 113.0, 114.0, 113.0].iter().enumerate() {
            st.update(sid, d(i as u32), agg(d(i as u32), *c, *c, *c, *c, 1000.0));
        }
        let h = st.get(sid).unwrap();
        // Closes were pushed for sweep days 0..=5; the "current" day being
        // processed is day 6. The ≥5%/≥10% move completed at day 2's close
        // (101→112) → 6 − 2 = 4 trading days ago.
        assert_eq!(h.days_since_move(0), Some(4));
        assert_eq!(h.days_since_move(1), Some(4));
        assert_eq!(h.days_since_move(2), None, "no 20% move on record");
        // 113 < 114 → streak reset.
        assert_eq!(h.consecutive_up_days(), 0);
        // First bar was sweep day 0, current day is 6 → 6.
        assert_eq!(h.days_since_first_bar(), 6);
        assert_eq!(h.prior().unwrap().rth_close, 113.0);
    }

    #[test]
    fn consecutive_up_counts_streak() {
        let mut st = RollingState::new();
        for (i, c) in [100.0, 101.0, 102.0, 103.0].iter().enumerate() {
            st.update("X", d(i as u32), agg(d(i as u32), *c, *c, *c, *c, 1.0));
        }
        assert_eq!(st.get("X").unwrap().consecutive_up_days(), 3);
    }

    #[test]
    fn atr_and_adv_and_52w_windows() {
        let mut st = RollingState::new();
        // 15 days: close drifts 100→114, range 2.0 every day, volume 1000.
        for i in 0..15u32 {
            let c = 100.0 + i as f64;
            st.update("X", d(i), agg(d(i), c, c + 1.0, c - 1.0, c, 1000.0));
        }
        let h = st.get("X").unwrap();
        // True range each day: max(2.0, |h-pc|=2, |l-pc|=0) = 2.0.
        assert!((h.atr(14).unwrap() - 2.0).abs() < 1e-12);
        assert_eq!(h.adv(5), Some(1000.0));
        assert_eq!(h.addv(5).unwrap(), (110.0 + 111.0 + 112.0 + 113.0 + 114.0) / 5.0 * 1000.0);
        assert_eq!(h.high_52w(), Some(115.0));
        assert_eq!(h.low_52w(), Some(99.0));
        // Not enough days for a 21d vol or 42d ATR yet.
        assert!(h.realized_vol(21).is_none());
        assert!(h.atr(42).is_none());
        assert!(h.realized_vol(5).is_some());
    }

    #[test]
    fn beta_of_perfectly_correlated_2x_stock_is_2() {
        let mut st = RollingState::new();
        let mut index_cumlog: HashMap<NaiveDate, f64> = HashMap::new();
        let mut idx_close = 100.0f64;
        let mut stock_close = 50.0f64;
        let mut cum = 0.0;
        index_cumlog.insert(d(0), 0.0);
        st.update("X", d(0), agg(d(0), stock_close, stock_close, stock_close, stock_close, 1.0));
        for i in 1..70u32 {
            // Index alternates ±1%; stock moves exactly 2× in log space.
            let r: f64 = if i % 2 == 0 { 0.01 } else { -0.01 };
            idx_close *= r.exp();
            stock_close *= (2.0 * r).exp();
            cum += r;
            index_cumlog.insert(d(i), cum);
            st.update("X", d(i), agg(d(i), stock_close, stock_close, stock_close, stock_close, 1.0));
        }
        let h = st.get("X").unwrap();
        let _ = idx_close;
        assert!((h.beta(&index_cumlog, 60).unwrap() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn yang_zhang_is_positive_and_scales_with_range() {
        let mut st = RollingState::new();
        for i in 0..30u32 {
            let c = 100.0 + (i % 5) as f64;
            // Wide intraday range relative to close-to-close moves.
            st.update("W", d(i), agg(d(i), c, c + 5.0, c - 5.0, c, 1.0));
            st.update("N", d(i), agg(d(i), c, c + 0.5, c - 0.5, c, 1.0));
        }
        let wide = st.get("W").unwrap().yang_zhang_vol(21).unwrap();
        let narrow = st.get("N").unwrap().yang_zhang_vol(21).unwrap();
        assert!(wide > narrow, "wider ranges must imply higher YZ vol");
        assert!(narrow > 0.0);
    }

    #[test]
    fn signal_fired_in_last_handles_short_history() {
        let mut st = RollingState::new();
        let mut a = agg(d(0), 1.0, 1.0, 1.0, 1.0, 1.0);
        a.snapshot_ret_1000 = Some(0.02); // fired
        st.update("X", d(0), a);
        let h = st.get("X").unwrap();
        assert_eq!(h.signal_fired_in_last(1), Some(true));
        assert_eq!(h.signal_fired_in_last(5), None, "only 1 day on record");
    }

    #[test]
    fn window_caps_at_252_days() {
        let mut st = RollingState::new();
        for i in 0..300u32 {
            let c = 100.0 + (i % 7) as f64;
            st.update("X", d(i), agg(d(i), c, c + 0.5, c - 0.5, c, 1.0));
        }
        let h = st.get("X").unwrap();
        assert_eq!(h.days.len(), 252);
        // 300 days pushed; the day under processing is sweep index 300.
        assert_eq!(h.days_since_first_bar(), 300);
    }
}
