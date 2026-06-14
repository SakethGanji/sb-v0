//! `forward_outcomes` builder — B3 coverage (short horizons).
//!
//! Grain: one row per `(day, security_id, entry_offset)` — RFC §9. The
//! entry day D's outcomes look FORWARD into D+1…D+5, so this table is
//! produced by a separate forward pass (`bin/write-forward-outcomes`)
//! with a ~6-day ring buffer of full 1-minute sessions, NOT by the
//! chronological trailing sweep that writes B1/B2.
//!
//! ## What B3 fills (this module)
//! - Identity + entry pricing (`entry_price`, `entry_unadjusted_price`).
//! - Pre-entry parametric features (state at the entry decision).
//! - Entry-quality / fill-realism proxies (the entry 1m bar).
//! - Per-horizon outcomes for the **8 short horizons**
//!   (`10min,30min,60min,EOD,1d,2d,3d,5d`): `ret`, `max_drawdown`,
//!   `max_runup`, `close_max_ret`, `close_min_ret`, and the four
//!   `bars_to_*` extreme-locators.
//! - Market-relative `ret_<H>_excess_{spy,qqq,iwm}` for those horizons.
//!
//! ## Deferred to later B3 increments / B5 (left null here)
//! threshold crossings, day-0 segment + session-shape, time-underwater,
//! next-day, gap-vs-RTH days 1-5, `hit_`/`first_event_` labels (next B3
//! increments); 10d–252d horizons, `ret_<H>_total` + dividend flags,
//! `bar_gap_minutes_max`, terminal events (B5). Every deferred column is
//! a typed null via `new_null_array`, so the file is always schema-valid.
//!
//! ## Adjudicated conventions (encoded in the L2 validator)
//! - `entry_price` = **open of the 1m bar at `entry_offset`** (D-basis
//!   adjusted); `entry_unadjusted_price` = that bar's raw open.
//!   `is_halted_at_entry` = no bar exists exactly at the entry minute
//!   (the fill uses the first bar at/after the entry minute).
//! - Forward path is **RTH bars only**, entry bar inclusive as bar 0.
//!   Intraday horizons are capped at D's session close; `EOD` ends at D's
//!   last RTH bar; `1d…5d` end at the close of D+1…D+5.
//! - All forward-day bars are rescaled to **D's split basis** before any
//!   return is computed (see `ForwardDay::scale`).
//! - Returns are simple (`p/entry_price - 1`); drawdown ≤ 0, runup ≥ 0.

use crate::daily_observation::ranks_desc;
use crate::slices::et;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, DictionaryArray, Float64Array, Int32Array,
    RecordBatch, StringArray, UInt32Array, new_null_array,
};
use arrow::datatypes::Int32Type;
use chrono::{DateTime, NaiveDate, Utc};
use momentum_core::bar::Bar;
use momentum_core::phase0_outputs::{
    ATR_THRESHOLDS_V2, ENTRY_OFFSETS_V1, FORWARD_HORIZONS_V2, PCT_THRESHOLDS_V2,
    TARGET_STOP_PAIRS_V6, forward_outcomes_schema,
};
use std::sync::Arc;

/// The 8 short horizons B3 fills, with their kind. Index horizons are
/// clock-minute offsets from entry on day D; day horizons end at the
/// close of D+k.
#[derive(Clone, Copy)]
enum Horizon {
    /// minutes from entry, capped at D's session close
    Intraday(&'static str, i64),
    /// last RTH bar of day D
    Eod,
    /// close of D+k (k ≥ 1)
    Day(&'static str, usize),
}

const SHORT_HORIZONS: &[Horizon] = &[
    Horizon::Intraday("10min", 10),
    Horizon::Intraday("30min", 30),
    Horizon::Intraday("60min", 60),
    Horizon::Eod,
    Horizon::Day("1d", 1),
    Horizon::Day("2d", 2),
    Horizon::Day("3d", 3),
    Horizon::Day("5d", 5),
];

const INDEX_SIDS: [&str; 3] = ["SPY", "QQQ", "IWM"];

/// Trailing/entry context for one security as of day D, read back from the
/// already-validated `daily_observation[D]` output (B1). Drives the
/// entry-quality vol/liquidity normalizers — no need to recompute trailing
/// state in the forward pass.
#[derive(Clone, Copy, Default)]
pub struct EntryCtx {
    pub atr_14d: Option<f64>,
    pub yang_zhang_vol_14d: Option<f64>,
    pub adv_20d: Option<f64>,
    pub addv_20d: Option<f64>,
    /// Day-D premarket (04:00–09:30 ET) volume / dollar volume, read back
    /// from `daily_observation[D]` (pin-adjusted) — the 04:00 base for
    /// `cumulative_volume_to_entry`.
    pub premarket_volume: Option<f64>,
    pub premarket_dollar_volume: Option<f64>,
}

/// One forward day's RTH bars on D's split basis, plus the day's RTH
/// close time (half-day aware). `bars` are already rescaled.
pub struct ForwardDay {
    pub day: NaiveDate,
    pub rth_bars: Vec<Bar>,
    pub session_close: DateTime<Utc>,
}

impl ForwardDay {
    /// Build from a day's full session bars by keeping RTH
    /// `[09:30, session_close]`. **No cross-day rescaling is needed:**
    /// `MaterializedBarReader::day_sessions` already adjusts every day to
    /// the same PIN basis (`factor_at(day)` folds in all splits after
    /// `day` up to the pin), so the entry day D and every forward day
    /// D+k are already on one common, mutually-consistent price basis —
    /// including windows that straddle a split. Returns therefore use the
    /// bars as-is. Volumes are likewise on the uniform pin basis
    /// (build-state §6).
    pub fn from_session(day: NaiveDate, session_bars: &[Bar], session_close: DateTime<Utc>) -> Self {
        let open = et(day, 9, 30);
        let rth_bars = session_bars
            .iter()
            .filter(|b| b.t >= open && b.t <= session_close)
            .copied()
            .collect();
        ForwardDay { day, rth_bars, session_close }
    }
}

/// One security's forward window: entry day D (`days[0]`) plus up to five
/// following trading days, all on D's split basis.
pub struct ForwardInput<'a> {
    pub security_id: &'a str,
    /// Display symbol on day D — index ETFs are matched on this (the
    /// `security_id` is a composite FIGI, not the ticker).
    pub display_symbol: &'a str,
    pub days: &'a [ForwardDay],
    pub entry_ctx: EntryCtx,
    /// `factor_at(D)` for this security — used to recover the raw
    /// (unadjusted) entry price from the D-basis adjusted open.
    pub entry_day_factor: f64,
    /// (B5) This security's pin-adjusted daily bars for D+1, D+2, … (sorted),
    /// up to ~252 trading days ahead. Drives the 10d–252d horizons (daily
    /// resolution) and `ret_<H>_total`. Empty until B5.
    pub forward_daily: &'a [DailyBar],
    /// (B5) Pin-adjusted cash dividends with ex-date in (D, …], sorted by
    /// ex-date, for `ret_<H>_total` + `dividend_ex_date_within_<H>`.
    pub dividends_fwd: &'a [(NaiveDate, f64)],
    /// (B5b) Terminal-event detection for this (security, day D). Computed in
    /// the bin (needs the delisting lookup + calendar); the engine just emits
    /// it and derives `terminal_event_return` per entry price.
    pub terminal: TerminalInfo,
    /// Vendor sid collision: this composite FIGI appears under two display
    /// symbols on day D (two distinct securities). Forward tracking can't
    /// attribute bars to either listing, so ALL outcome columns are emitted
    /// null — honest ambiguity, matching `daily_observation`'s purged trailing
    /// state for collided sids (build-state §6). The row is still emitted.
    pub ambiguous: bool,
}

/// (B5b) Per-(security, day D) terminal-event info. `event_type` is `"none"`
/// or `"delisted_unknown"` — no delisting REASON is on disk (build-state §8),
/// so merger/bankruptcy are never claimed.
#[derive(Clone, Copy)]
pub struct TerminalInfo {
    pub event_type: &'static str,
    pub date: Option<NaiveDate>,
    /// Last pin-adjusted close on/before the terminal date (for the return).
    pub last_valid_close: Option<f64>,
    pub last_valid_trade_date: Option<NaiveDate>,
    pub days_with_missing_forward_bars: Option<i32>,
    pub confidence: Option<&'static str>,
}

impl Default for TerminalInfo {
    fn default() -> Self {
        TerminalInfo {
            event_type: "none",
            date: None,
            last_valid_close: None,
            last_valid_trade_date: None,
            days_with_missing_forward_bars: None,
            confidence: None,
        }
    }
}

/// One security's pin-adjusted daily aggregate (RTH) for a forward day.
#[derive(Clone, Copy, Debug)]
pub struct DailyBar {
    pub day: NaiveDate,
    pub close: f64,
    pub high: f64,
    pub low: f64,
}

/// Long horizons (10d–252d) resolved at DAILY resolution (label, trading-day
/// count). The ≤5d horizons stay 1m (B3).
const LONG_HORIZONS: &[(&str, usize)] =
    &[("10d", 10), ("21d", 21), ("42d", 42), ("63d", 63), ("252d", 252)];

/// Per-horizon path statistics, all measured from `entry_price` over the
/// RTH bars in `[entry_bar, horizon_end]`.
#[derive(Default)]
struct HorizonStat {
    ret: Option<f64>,
    max_drawdown: Option<f64>,
    max_runup: Option<f64>,
    close_max_ret: Option<f64>,
    close_min_ret: Option<f64>,
    bars_to_max_drawdown: Option<u32>,
    bars_to_max_runup: Option<u32>,
    bars_to_close_min: Option<u32>,
    bars_to_close_max: Option<u32>,
}

/// Slice of one entry's resolved forward state, per entry_offset.
struct EntryRow {
    entry_price: Option<f64>,
    entry_unadjusted_price: Option<f64>,
    is_halted_at_entry: Option<bool>,
    // pre-entry
    pre_entry_ret_from_open: Option<f64>,
    pre_entry_volume_from_open: Option<f64>,
    pre_entry_dollar_volume_from_open: Option<f64>,
    pre_entry_vwap_from_open: Option<f64>,
    pre_entry_high_return_so_far: Option<f64>,
    pre_entry_low_return_so_far: Option<f64>,
    pre_entry_minutes_since_high: Option<i32>,
    pre_entry_minutes_since_low: Option<i32>,
    pre_entry_ret_from_high: Option<f64>,
    pre_entry_ret_from_low: Option<f64>,
    // entry-quality
    entry_price_location_in_1m_bar: Option<f64>,
    entry_open_to_close_1m_return: Option<f64>,
    entry_bar_upper_wick_pct: Option<f64>,
    entry_bar_lower_wick_pct: Option<f64>,
    entry_slippage_proxy_bps: Option<f64>,
    entry_participation_capacity_1pct_adv: Option<f64>,
    entry_participation_capacity_5pct_1m_volume: Option<f64>,
    entry_1m_volume: Option<f64>,
    entry_1m_range: Option<f64>,
    entry_1m_dollar_volume: Option<f64>,
    entry_range_vs_atr_14d: Option<f64>,
    entry_range_vs_yz_vol_14d: Option<f64>,
    entry_dollar_volume_vs_addv_20d: Option<f64>,
    // per-horizon (indexed by SHORT_HORIZONS order)
    horizons: Vec<HorizonStat>,
    // excess per horizon × [spy,qqq,iwm]
    excess: Vec<[Option<f64>; 3]>,
    // threshold crossings + target-before-stop labels (None for blank rows)
    cross: Option<CrossLabels>,
    // day-0 segments/shape, time-underwater, next-day, gap-vs-RTH
    aux: Option<Aux>,
    // cumulative pre-entry volume (04:00 ET → entry bar inclusive)
    cum_volume_to_entry: Option<f64>,
    cum_dollar_volume_to_entry: Option<f64>,
    // (B5) multi-day horizons (10d–252d) + ret_total + dividend flags
    multiday: Option<MultiDay>,
    // (B5) bar_gap_minutes_max per the 13 horizons (None for blank rows)
    bar_gap: Vec<Option<i32>>,
    // (B5b) terminal events (sid-level info + per-offset return)
    terminal: TerminalInfo,
    terminal_return: Option<f64>,
}

/// Parse an offset label like "0935" or "1530" into ET (hour, minute).
pub(crate) fn offset_hm(label: &str) -> (u32, u32) {
    let h: u32 = label[..2].parse().expect("offset hh");
    let m: u32 = label[2..].parse().expect("offset mm");
    (h, m)
}

/// Find the entry bar: first RTH bar at/after the entry minute on day D.
/// Returns (index into rth, halted) where halted = the exact minute was
/// absent. None if no RTH bar at/after the entry minute exists.
pub(crate) fn find_entry(rth: &[Bar], entry_t: DateTime<Utc>) -> Option<(usize, bool)> {
    let idx = rth.iter().position(|b| b.t >= entry_t)?;
    Some((idx, rth[idx].t > entry_t))
}

/// B3 third increment — day-0 segments/shape, time-underwater, next-day,
/// gap-vs-RTH. All positional, in schema column order (see `build`).
///
/// Adjudicated conventions (encoded in the L2 validator):
/// - `ret_to_<seg>` = D close at/ before the segment clock time ÷ entry − 1;
///   null when the segment ends before the entry FILL bar.
/// - day-0 session segments (post-entry): morning `[09:30,11:30)`, midday
///   `[11:30,14:00)`, afternoon `[14:00, close]`, power hour `[15:00, close]`;
///   high/low extremes taken over bars STRICTLY AFTER the entry bar within
///   the segment, as a return from entry; null if the segment has no such bar.
/// - time-underwater is CLOSE-based over the tape `[entry, horizon-end]`
///   (entry bar = index 0); a bar exactly at entry is neither.
///   `time_to_recover` = bars from first underwater bar to first subsequent
///   recovery; 0 if never underwater or never recovered.
/// - next-day fields are on D+1: intraday returns are vs D+1 open; the gap is
///   D+1 open ÷ D close − 1; `open_return` is cumulative from entry.
/// - gap-vs-RTH day k: gap = D+k open ÷ D+(k−1) close − 1; rth/open_to_close =
///   D+k close ÷ D+k open − 1; close_to_close = D+k close ÷ D+(k−1) close − 1.
struct Aux {
    ret_to: [Option<f64>; 9],
    shape: [Option<f64>; 9],
    uw_f: [[Option<f64>; 2]; 5],
    uw_u: [[Option<u32>; 3]; 5],
    next_day: [Option<f64>; 11],
    gap_rth: [[Option<f64>; 4]; 5],
}

fn day_oc(rth: &[Bar]) -> Option<(f64, f64)> {
    (!rth.is_empty()).then(|| (rth[0].open, rth[rth.len() - 1].close))
}

fn compute_aux(
    day: NaiveDate,
    inp: &ForwardInput<'_>,
    eidx: usize,
    entry: f64,
    tape: &[Bar],
    ends: &[Option<usize>],
) -> Aux {
    let d0 = &inp.days[0];
    let rth = &d0.rth_bars;
    let entry_t = rth[eidx].t;
    let post = &rth[(eidx + 1).min(rth.len())..]; // bars strictly after the fill
    let d_close = rth[rth.len() - 1].close;

    // ---- ret_to_<seg> ---- (segment clock times on day D)
    let seg_t = [
        et(day, 10, 30), et(day, 11, 0), et(day, 11, 30), et(day, 12, 0),
        et(day, 13, 0), et(day, 14, 0), et(day, 15, 0), et(day, 15, 30), d0.session_close,
    ];
    let mut ret_to = [None; 9];
    for (i, &t) in seg_t.iter().enumerate() {
        if t < entry_t {
            continue; // segment ends before the fill
        }
        if let Some(b) = rth.iter().rev().find(|b| b.t <= t) {
            if b.t >= entry_t {
                ret_to[i] = Some(b.close / entry - 1.0);
            }
        }
    }

    // ---- day-0 session-shape (post-entry) ----
    let seg_hilo = |lo: DateTime<Utc>, hi: Option<DateTime<Utc>>| -> (Option<f64>, Option<f64>) {
        let it = post.iter().filter(|b| b.t >= lo && hi.is_none_or(|h| b.t < h));
        let mut mh = f64::NEG_INFINITY;
        let mut ml = f64::INFINITY;
        for b in it {
            mh = mh.max(b.high);
            ml = ml.min(b.low);
        }
        if mh == f64::NEG_INFINITY {
            (None, None)
        } else {
            (Some(mh / entry - 1.0), Some(ml / entry - 1.0))
        }
    };
    let (mh, ml) = seg_hilo(et(day, 9, 30), Some(et(day, 11, 30)));
    let (dh, dl) = seg_hilo(et(day, 11, 30), Some(et(day, 14, 0)));
    let (ah, al) = seg_hilo(et(day, 14, 0), None);
    let power_open = post.iter().find(|b| b.t >= et(day, 15, 0)).map(|b| b.open);
    let power = power_open.map(|o| d_close / o - 1.0);
    let (post_hi, post_lo) = {
        let mut h = f64::NEG_INFINITY;
        let mut l = f64::INFINITY;
        for b in post {
            h = h.max(b.high);
            l = l.min(b.low);
        }
        if h == f64::NEG_INFINITY { (None, None) } else { (Some(h), Some(l)) }
    };
    let shape = [
        mh, ml, dh, dl, ah, al, power,
        post_hi.map(|h| d_close / h - 1.0),
        post_lo.map(|l| d_close / l - 1.0),
    ];

    // ---- time-underwater (close-based, per short horizon EOD,1d,2d,3d,5d) ----
    let mut uw_f = [[None; 2]; 5];
    let mut uw_u = [[None; 3]; 5];
    for (slot, hi) in [3usize, 4, 5, 6, 7].into_iter().enumerate() {
        let Some(end) = ends[hi] else { continue };
        let n = end + 1;
        let (mut prof, mut under) = (0u32, 0u32);
        let (mut cur_p, mut cur_u, mut max_p, mut max_u) = (0u32, 0u32, 0u32, 0u32);
        let (mut first_under, mut recover) = (None, None);
        for (i, b) in tape.iter().take(n).enumerate() {
            if b.close > entry {
                prof += 1;
                cur_p += 1;
                cur_u = 0;
                max_p = max_p.max(cur_p);
                if first_under.is_some() && recover.is_none() {
                    recover = Some(i);
                }
            } else if b.close < entry {
                under += 1;
                cur_u += 1;
                cur_p = 0;
                max_u = max_u.max(cur_u);
                if first_under.is_none() {
                    first_under = Some(i);
                }
            } else {
                cur_p = 0;
                cur_u = 0;
                if first_under.is_some() && recover.is_none() {
                    recover = Some(i); // back to flat counts as recovered
                }
            }
        }
        uw_f[slot] = [Some(prof as f64 / n as f64), Some(under as f64 / n as f64)];
        let ttr = match (first_under, recover) {
            (Some(f), Some(r)) => (r - f) as u32,
            _ => 0,
        };
        uw_u[slot] = [Some(max_p), Some(max_u), Some(ttr)];
    }

    // ---- next-day (D+1) ----
    let mut next_day = [None; 11];
    if let Some(nd) = inp.days.get(1).filter(|d| !d.rth_bars.is_empty()) {
        let b = &nd.rth_bars;
        let (o, c) = (b[0].open, b[b.len() - 1].close);
        let hi = b.iter().map(|x| x.high).fold(f64::NEG_INFINITY, f64::max);
        let lo = b.iter().map(|x| x.low).fold(f64::INFINITY, f64::min);
        let at_plus = |mins: i64| {
            let t = b[0].t + chrono::Duration::minutes(mins);
            b.iter().rev().find(|x| x.t <= t).map(|x| x.close / o - 1.0)
        };
        next_day = [
            Some(o / entry - 1.0),          // open_return (cumulative from entry)
            Some(o / d_close - 1.0),        // gap_return
            at_plus(5),                     // first_5m_return
            at_plus(15),                    // first_15m_return
            at_plus(30),                    // first_30m_return
            Some(hi / o - 1.0),             // high_return
            Some(lo / o - 1.0),             // low_return
            Some(c / o - 1.0),              // close_return
            (hi > lo).then(|| (c - lo) / (hi - lo)), // close_location_in_range
            Some(c / hi - 1.0),             // fade_from_open (close vs high)
            Some(c / o - 1.0),              // continuation_from_open
        ];
    }

    // ---- gap-vs-RTH days 1..5 ----
    let mut gap_rth = [[None; 4]; 5];
    let mut prev_close = Some(d_close); // D+0 close = entry day's close
    for k in 1..=5 {
        let cur = inp.days.get(k).and_then(|d| day_oc(&d.rth_bars));
        if let (Some((o, c)), Some(pc)) = (cur, prev_close) {
            gap_rth[k - 1] = [
                Some(o / pc - 1.0),  // gap
                Some(c / o - 1.0),   // rth
                Some(c / pc - 1.0),  // close_to_close
                Some(c / o - 1.0),   // open_to_close (≡ rth by definition)
            ];
        }
        prev_close = cur.map(|(_, c)| c);
    }

    Aux { ret_to, shape, uw_f, uw_u, next_day, gap_rth }
}

/// Resolve all horizons for one entry on a flattened forward tape.
/// `tape` is the concatenation of D's RTH-from-entry-bar plus D+1…D+5 RTH,
/// in time order. `ends[hi]` is the tape index of the last bar in
/// `SHORT_HORIZONS[hi]`'s window, or None if that horizon has no forward
/// data (truncated at the corpus tail / missing day).
fn resolve_horizons(tape: &[Bar], entry_price: f64, ends: &[Option<usize>]) -> Vec<HorizonStat> {
    // Running extremes scanned once; horizon stats read prefix maxima.
    let n = tape.len();
    let mut run_max_high = f64::NEG_INFINITY;
    let mut run_max_high_at = 0usize;
    let mut run_min_low = f64::INFINITY;
    let mut run_min_low_at = 0usize;
    let mut run_max_close = f64::NEG_INFINITY;
    let mut run_max_close_at = 0usize;
    let mut run_min_close = f64::INFINITY;
    let mut run_min_close_at = 0usize;
    // Prefix arrays of the running extremes (value + bar index) at each i.
    let mut pref: Vec<(f64, usize, f64, usize, f64, usize, f64, usize)> = Vec::with_capacity(n);
    for (i, b) in tape.iter().enumerate() {
        if b.high > run_max_high {
            run_max_high = b.high;
            run_max_high_at = i;
        }
        if b.low < run_min_low {
            run_min_low = b.low;
            run_min_low_at = i;
        }
        if b.close > run_max_close {
            run_max_close = b.close;
            run_max_close_at = i;
        }
        if b.close < run_min_close {
            run_min_close = b.close;
            run_min_close_at = i;
        }
        pref.push((
            run_max_high, run_max_high_at, run_min_low, run_min_low_at,
            run_max_close, run_max_close_at, run_min_close, run_min_close_at,
        ));
    }

    let stat_at = |end: Option<usize>| -> HorizonStat {
        let Some(end) = end else { return HorizonStat::default() };
        if end >= n {
            return HorizonStat::default();
        }
        let p = pref[end];
        HorizonStat {
            ret: Some(tape[end].close / entry_price - 1.0),
            max_runup: Some(p.0 / entry_price - 1.0),
            bars_to_max_runup: Some(p.1 as u32),
            max_drawdown: Some(p.2 / entry_price - 1.0),
            bars_to_max_drawdown: Some(p.3 as u32),
            close_max_ret: Some(p.4 / entry_price - 1.0),
            bars_to_close_max: Some(p.5 as u32),
            close_min_ret: Some(p.6 / entry_price - 1.0),
            bars_to_close_min: Some(p.7 as u32),
        }
    };

    (0..SHORT_HORIZONS.len()).map(|hi| stat_at(ends[hi])).collect()
}

/// First-cross / target-before-stop resolution for one entry (B3 second
/// increment). All crossings are reported as **1-based** bar indices
/// (entry bar = 1) with `0` = "horizon reached, threshold never crossed"
/// and `null` = "horizon has no forward data". Up-crossings test the bar
/// HIGH (first touch of +threshold), down-crossings the bar LOW.
struct CrossLabels {
    /// per (short-horizon, pct-threshold): [up, down], schema order.
    pct: Vec<Option<u32>>,
    /// per (short-horizon, atr-threshold): [up, down]; null when atr_14d is
    /// unavailable (can't form the price threshold).
    atr: Vec<Option<u32>>,
    /// per frozen pair (TARGET_STOP_PAIRS_V6 order, 9 entries; the 21d pair
    /// is B5 → None here).
    first_event: Vec<Option<&'static str>>,
    hit: Vec<Option<bool>>,
}

fn pct_frac(label: &str) -> f64 {
    label.replace('_', ".").parse::<f64>().expect("pct label") / 100.0
}
fn atr_mult(label: &str) -> f64 {
    label.replace('_', ".").parse::<f64>().expect("atr label")
}

/// 1-based report: None if horizon absent; Some(0) if reached but never
/// crossed; Some(idx+1) if first crossed at tape index `idx` within `end`.
fn report_cross(first: Option<usize>, end: Option<usize>) -> Option<u32> {
    match end {
        None => None,
        Some(e) => match first {
            Some(idx) if idx <= e => Some(idx as u32 + 1),
            _ => Some(0),
        },
    }
}

fn compute_cross_labels(
    tape: &[Bar],
    entry_price: f64,
    atr_14d: Option<f64>,
    ends: &[Option<usize>],
) -> CrossLabels {
    let pct_fracs: Vec<f64> = PCT_THRESHOLDS_V2.iter().map(|s| pct_frac(s)).collect();
    let atr_mults: Vec<f64> = ATR_THRESHOLDS_V2.iter().map(|s| atr_mult(s)).collect();
    let a14 = atr_14d.filter(|a| *a > 0.0);

    // Single pass: first global tape index where each threshold is touched.
    let mut up_pct = vec![None; pct_fracs.len()];
    let mut dn_pct = vec![None; pct_fracs.len()];
    let mut up_atr = vec![None; atr_mults.len()];
    let mut dn_atr = vec![None; atr_mults.len()];
    for (i, b) in tape.iter().enumerate() {
        let hi_ret = b.high / entry_price - 1.0;
        let lo_ret = b.low / entry_price - 1.0;
        for (t, &v) in pct_fracs.iter().enumerate() {
            if up_pct[t].is_none() && hi_ret >= v {
                up_pct[t] = Some(i);
            }
            if dn_pct[t].is_none() && lo_ret <= -v {
                dn_pct[t] = Some(i);
            }
        }
        if let Some(a) = a14 {
            for (t, &m) in atr_mults.iter().enumerate() {
                if up_atr[t].is_none() && b.high >= entry_price + m * a {
                    up_atr[t] = Some(i);
                }
                if dn_atr[t].is_none() && b.low <= entry_price - m * a {
                    dn_atr[t] = Some(i);
                }
            }
        }
    }

    let mut pct = Vec::with_capacity(SHORT_HORIZONS.len() * pct_fracs.len() * 2);
    let mut atr = Vec::with_capacity(SHORT_HORIZONS.len() * atr_mults.len() * 2);
    for hi in 0..SHORT_HORIZONS.len() {
        for t in 0..pct_fracs.len() {
            pct.push(report_cross(up_pct[t], ends[hi]));
            pct.push(report_cross(dn_pct[t], ends[hi]));
        }
        for t in 0..atr_mults.len() {
            if a14.is_none() {
                atr.push(None);
                atr.push(None);
            } else {
                atr.push(report_cross(up_atr[t], ends[hi]));
                atr.push(report_cross(dn_atr[t], ends[hi]));
            }
        }
    }

    // Target-before-stop labels for the 9 frozen pairs (TARGET_STOP_PAIRS_V6
    // order). B3 fills the 8 pairs whose horizon ≤ 5d; the 21d pair is B5.
    // Thresholds here are pair-specific (e.g. the 1.5% stop is NOT in the
    // fixed-% column set), so each pair is resolved directly from the tape.
    let mut first_event = vec![None; TARGET_STOP_PAIRS_V6.len()];
    let mut hit = vec![None; TARGET_STOP_PAIRS_V6.len()];
    let hidx = |label: &str| SHORT_HORIZONS.iter().position(|h| horizon_label(h) == label);
    // (pair index, up threshold, down threshold, horizon label)
    let pairs: &[(usize, Thr, Thr, &str)] = &[
        (0, Thr::Pct(0.005), Thr::Pct(0.005), "30min"),
        (1, Thr::Pct(0.01), Thr::Pct(0.01), "EOD"),
        (2, Thr::Pct(0.02), Thr::Pct(0.01), "EOD"),
        (3, Thr::Pct(0.03), Thr::Pct(0.015), "EOD"),
        (4, Thr::Pct(0.02), Thr::Pct(0.02), "1d"),
        (5, Thr::Pct(0.03), Thr::Pct(0.03), "5d"),
        (6, Thr::Atr(1.0), Thr::Atr(0.5), "EOD"),
        (7, Thr::Atr(2.0), Thr::Atr(1.0), "5d"),
    ];
    for &(pi, up, dn, hlabel) in pairs {
        let end = hidx(hlabel).and_then(|hi| ends[hi]);
        let (ev, h) = resolve_pair(tape, entry_price, a14, up, dn, end);
        first_event[pi] = Some(ev);
        hit[pi] = h;
    }

    CrossLabels { pct, atr, first_event, hit }
}

#[derive(Clone, Copy)]
enum Thr {
    Pct(f64),
    Atr(f64),
}

/// Resolve one target/stop pair's first event within `[0, end]`. Up uses
/// the bar HIGH, down the bar LOW; a same-bar double touch is adjudicated
/// **stop_first** (pessimistic — intrabar order is unknowable at 1m).
fn resolve_pair(
    tape: &[Bar],
    entry_price: f64,
    atr_14d: Option<f64>,
    up: Thr,
    dn: Thr,
    end: Option<usize>,
) -> (&'static str, Option<bool>) {
    let Some(end) = end else { return ("no_data", None) };
    // ATR pairs need atr_14d; without it the pair is unevaluable.
    let up_thr = match up {
        Thr::Pct(v) => Some(entry_price * (1.0 + v)),
        Thr::Atr(m) => atr_14d.map(|a| entry_price + m * a),
    };
    let dn_thr = match dn {
        Thr::Pct(v) => Some(entry_price * (1.0 - v)),
        Thr::Atr(m) => atr_14d.map(|a| entry_price - m * a),
    };
    let (Some(up_thr), Some(dn_thr)) = (up_thr, dn_thr) else { return ("no_data", None) };
    let mut up_at = None;
    let mut dn_at = None;
    for (i, b) in tape.iter().enumerate().take(end + 1) {
        if up_at.is_none() && b.high >= up_thr {
            up_at = Some(i);
        }
        if dn_at.is_none() && b.low <= dn_thr {
            dn_at = Some(i);
        }
        if up_at.is_some() && dn_at.is_some() {
            break;
        }
    }
    let ev = match (up_at, dn_at) {
        (None, None) => "neither",
        (Some(_), None) => "target_first",
        (None, Some(_)) => "stop_first",
        (Some(u), Some(d)) => {
            if u < d {
                "target_first"
            } else {
                "stop_first" // d < u, or same bar (pessimistic tie-break)
            }
        }
    };
    (ev, Some(ev == "target_first"))
}

pub fn build(day: NaiveDate, inputs: &[ForwardInput<'_>]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let schema = forward_outcomes_schema();
    let n_offsets = ENTRY_OFFSETS_V1.len();
    let cap = inputs.len() * n_offsets;

    // Precompute index horizon returns per (offset, horizon, idx) once.
    // index_ret[offset_i][idx] = Vec over SHORT_HORIZONS of Option<ret>.
    let mut index_ret: Vec<[Vec<Option<f64>>; 3]> = Vec::with_capacity(n_offsets);
    let index_lookup: Vec<Option<&ForwardInput>> = INDEX_SIDS
        .iter()
        .map(|s| inputs.iter().find(|i| i.display_symbol == *s))
        .collect();
    for off in ENTRY_OFFSETS_V1 {
        let mut per_idx: [Vec<Option<f64>>; 3] = Default::default();
        for (j, idx_in) in index_lookup.iter().enumerate() {
            per_idx[j] = match idx_in {
                Some(inp) => entry_horizon_rets(day, inp, off),
                None => vec![None; SHORT_HORIZONS.len()],
            };
        }
        index_ret.push(per_idx);
    }

    // Accumulators for the core columns.
    let mut sid_col: Vec<String> = Vec::with_capacity(cap);
    let mut off_col: Vec<&str> = Vec::with_capacity(cap);
    let mut rows: Vec<EntryRow> = Vec::with_capacity(cap);

    for inp in inputs {
        for (oi, off) in ENTRY_OFFSETS_V1.iter().enumerate() {
            let row = resolve_entry(day, inp, off, &index_ret[oi]);
            sid_col.push(inp.security_id.to_string());
            off_col.push(off);
            rows.push(row);
        }
    }

    // ---- assemble arrays in schema field order ----
    let total = sid_col.len();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let mut fill: std::collections::HashMap<&str, ArrayRef> = std::collections::HashMap::new();

    macro_rules! f64col {
        ($name:expr, $get:expr) => {
            fill.insert($name, Arc::new(rows.iter().map($get).collect::<Float64Array>()) as ArrayRef);
        };
    }
    macro_rules! i32col {
        ($name:expr, $get:expr) => {
            fill.insert($name, Arc::new(rows.iter().map($get).collect::<Int32Array>()) as ArrayRef);
        };
    }
    // identity
    fill.insert("day", Arc::new(Date32Array::from(vec![date32(day); total])) as ArrayRef);
    fill.insert("security_id", Arc::new(StringArray::from(sid_col.clone())) as ArrayRef);
    let off_dict: DictionaryArray<Int32Type> = off_col.iter().copied().map(Some).collect();
    fill.insert("entry_offset", Arc::new(off_dict) as ArrayRef);
    f64col!("entry_price", |r| r.entry_price);
    f64col!("entry_unadjusted_price", |r| r.entry_unadjusted_price);

    // pre-entry
    f64col!("pre_entry_ret_from_open", |r| r.pre_entry_ret_from_open);
    f64col!("pre_entry_volume_from_open", |r| r.pre_entry_volume_from_open);
    f64col!("pre_entry_dollar_volume_from_open", |r| r.pre_entry_dollar_volume_from_open);
    f64col!("pre_entry_vwap_from_open", |r| r.pre_entry_vwap_from_open);
    f64col!("pre_entry_high_return_so_far", |r| r.pre_entry_high_return_so_far);
    f64col!("pre_entry_low_return_so_far", |r| r.pre_entry_low_return_so_far);
    i32col!("pre_entry_minutes_since_high", |r| r.pre_entry_minutes_since_high);
    i32col!("pre_entry_minutes_since_low", |r| r.pre_entry_minutes_since_low);
    f64col!("pre_entry_ret_from_high", |r| r.pre_entry_ret_from_high);
    f64col!("pre_entry_ret_from_low", |r| r.pre_entry_ret_from_low);

    // entry-quality
    f64col!("entry_price_location_in_1m_bar", |r| r.entry_price_location_in_1m_bar);
    f64col!("entry_open_to_close_1m_return", |r| r.entry_open_to_close_1m_return);
    f64col!("entry_bar_upper_wick_pct", |r| r.entry_bar_upper_wick_pct);
    f64col!("entry_bar_lower_wick_pct", |r| r.entry_bar_lower_wick_pct);
    f64col!("entry_slippage_proxy_bps", |r| r.entry_slippage_proxy_bps);
    f64col!("entry_participation_capacity_1pct_adv", |r| r.entry_participation_capacity_1pct_adv);
    f64col!("entry_participation_capacity_5pct_1m_volume", |r| r.entry_participation_capacity_5pct_1m_volume);
    f64col!("entry_1m_volume", |r| r.entry_1m_volume);
    f64col!("entry_1m_range", |r| r.entry_1m_range);
    f64col!("entry_1m_dollar_volume", |r| r.entry_1m_dollar_volume);
    f64col!("entry_range_vs_atr_14d", |r| r.entry_range_vs_atr_14d);
    f64col!("entry_range_vs_yz_vol_14d", |r| r.entry_range_vs_yz_vol_14d);
    f64col!("entry_dollar_volume_vs_addv_20d", |r| r.entry_dollar_volume_vs_addv_20d);
    fill.insert(
        "is_halted_at_entry",
        Arc::new(rows.iter().map(|r| r.is_halted_at_entry).collect::<BooleanArray>()) as ArrayRef,
    );

    // per-horizon core + excess
    for (hi, h) in SHORT_HORIZONS.iter().enumerate() {
        let label = horizon_label(h);
        fill.insert(leak(format!("ret_{label}")), Arc::new(rows.iter().map(|r| r.horizons[hi].ret).collect::<Float64Array>()) as ArrayRef);
        fill.insert(leak(format!("max_drawdown_{label}")), Arc::new(rows.iter().map(|r| r.horizons[hi].max_drawdown).collect::<Float64Array>()) as ArrayRef);
        fill.insert(leak(format!("max_runup_{label}")), Arc::new(rows.iter().map(|r| r.horizons[hi].max_runup).collect::<Float64Array>()) as ArrayRef);
        fill.insert(leak(format!("close_max_ret_{label}")), Arc::new(rows.iter().map(|r| r.horizons[hi].close_max_ret).collect::<Float64Array>()) as ArrayRef);
        fill.insert(leak(format!("close_min_ret_{label}")), Arc::new(rows.iter().map(|r| r.horizons[hi].close_min_ret).collect::<Float64Array>()) as ArrayRef);
        fill.insert(leak(format!("bars_to_max_drawdown_{label}")), Arc::new(rows.iter().map(|r| r.horizons[hi].bars_to_max_drawdown).collect::<UInt32Array>()) as ArrayRef);
        fill.insert(leak(format!("bars_to_max_runup_{label}")), Arc::new(rows.iter().map(|r| r.horizons[hi].bars_to_max_runup).collect::<UInt32Array>()) as ArrayRef);
        fill.insert(leak(format!("bars_to_close_min_{label}")), Arc::new(rows.iter().map(|r| r.horizons[hi].bars_to_close_min).collect::<UInt32Array>()) as ArrayRef);
        fill.insert(leak(format!("bars_to_close_max_{label}")), Arc::new(rows.iter().map(|r| r.horizons[hi].bars_to_close_max).collect::<UInt32Array>()) as ArrayRef);
        for (j, idx) in ["spy", "qqq", "iwm"].iter().enumerate() {
            fill.insert(leak(format!("ret_{label}_excess_{idx}")), Arc::new(rows.iter().map(|r| r.excess[hi][j]).collect::<Float64Array>()) as ArrayRef);
        }
    }

    // Threshold crossings (1-based bar index; 0 = never within horizon;
    // null = horizon truncated). Only the 8 short horizons are filled here;
    // the 5 multi-day horizons stay null (B5). CrossLabels.pct/atr are laid
    // out [up, down] per (short-horizon, threshold), matching this order.
    let np = PCT_THRESHOLDS_V2.len();
    let na = ATR_THRESHOLDS_V2.len();
    for (hi, h) in SHORT_HORIZONS.iter().enumerate() {
        let label = horizon_label(h);
        for (ti, t) in PCT_THRESHOLDS_V2.iter().enumerate() {
            let p = hi * np * 2 + ti * 2;
            fill.insert(leak(format!("first_cross_up_{t}pct_{label}")), Arc::new(rows.iter().map(|r| r.cross.as_ref().and_then(|c| c.pct[p])).collect::<UInt32Array>()) as ArrayRef);
            fill.insert(leak(format!("first_cross_down_{t}pct_{label}")), Arc::new(rows.iter().map(|r| r.cross.as_ref().and_then(|c| c.pct[p + 1])).collect::<UInt32Array>()) as ArrayRef);
        }
        for (ti, t) in ATR_THRESHOLDS_V2.iter().enumerate() {
            let p = hi * na * 2 + ti * 2;
            fill.insert(leak(format!("first_cross_up_{t}atr_{label}")), Arc::new(rows.iter().map(|r| r.cross.as_ref().and_then(|c| c.atr[p])).collect::<UInt32Array>()) as ArrayRef);
            fill.insert(leak(format!("first_cross_down_{t}atr_{label}")), Arc::new(rows.iter().map(|r| r.cross.as_ref().and_then(|c| c.atr[p + 1])).collect::<UInt32Array>()) as ArrayRef);
        }
    }

    // Target-before-stop: hit_<pair> (bool) + first_event_<pair> (dict).
    // Pairs 0–7 (≤5d) from the 1m tape (B3); pair 8 (the 21d pair) from the
    // daily multiday resolution (B5b).
    let last_pair = TARGET_STOP_PAIRS_V6.len() - 1;
    for (pi, pair) in TARGET_STOP_PAIRS_V6.iter().enumerate() {
        let (hit, ev): (Vec<Option<bool>>, Vec<Option<&str>>) = if pi == last_pair {
            (
                rows.iter().map(|r| r.multiday.as_ref().and_then(|m| m.label21_hit)).collect(),
                rows.iter().map(|r| r.multiday.as_ref().and_then(|m| m.label21_event)).collect(),
            )
        } else {
            (
                rows.iter().map(|r| r.cross.as_ref().and_then(|c| c.hit[pi])).collect(),
                rows.iter().map(|r| r.cross.as_ref().and_then(|c| c.first_event[pi])).collect(),
            )
        };
        fill.insert(leak(format!("hit_{pair}")), Arc::new(BooleanArray::from(hit)) as ArrayRef);
        let evd: DictionaryArray<Int32Type> = ev.into_iter().collect();
        fill.insert(leak(format!("first_event_{pair}")), Arc::new(evd) as ArrayRef);
    }

    // Day-0 segments / shape, time-underwater, next-day, gap-vs-RTH (Aux).
    for (i, seg) in ["1030", "1100", "1130", "1200", "1300", "1400", "1500", "1530", "close"].iter().enumerate() {
        fill.insert(leak(format!("ret_to_{seg}")), Arc::new(rows.iter().map(|r| r.aux.as_ref().and_then(|a| a.ret_to[i])).collect::<Float64Array>()) as ArrayRef);
    }
    for (i, name) in [
        "post_entry_morning_high_return", "post_entry_morning_low_return",
        "post_entry_midday_high_return", "post_entry_midday_low_return",
        "post_entry_afternoon_high_return", "post_entry_afternoon_low_return",
        "post_entry_power_hour_return", "post_entry_close_vs_high_return",
        "post_entry_close_vs_low_return",
    ].iter().enumerate() {
        fill.insert(name, Arc::new(rows.iter().map(|r| r.aux.as_ref().and_then(|a| a.shape[i])).collect::<Float64Array>()) as ArrayRef);
    }
    for (slot, h) in ["EOD", "1d", "2d", "3d", "5d"].iter().enumerate() {
        fill.insert(leak(format!("pct_bars_profitable_{h}")), Arc::new(rows.iter().map(|r| r.aux.as_ref().and_then(|a| a.uw_f[slot][0])).collect::<Float64Array>()) as ArrayRef);
        fill.insert(leak(format!("pct_bars_underwater_{h}")), Arc::new(rows.iter().map(|r| r.aux.as_ref().and_then(|a| a.uw_f[slot][1])).collect::<Float64Array>()) as ArrayRef);
        fill.insert(leak(format!("max_consecutive_bars_profitable_{h}")), Arc::new(rows.iter().map(|r| r.aux.as_ref().and_then(|a| a.uw_u[slot][0])).collect::<UInt32Array>()) as ArrayRef);
        fill.insert(leak(format!("max_consecutive_bars_underwater_{h}")), Arc::new(rows.iter().map(|r| r.aux.as_ref().and_then(|a| a.uw_u[slot][1])).collect::<UInt32Array>()) as ArrayRef);
        fill.insert(leak(format!("time_to_recover_after_first_drawdown_{h}")), Arc::new(rows.iter().map(|r| r.aux.as_ref().and_then(|a| a.uw_u[slot][2])).collect::<UInt32Array>()) as ArrayRef);
    }
    for (i, name) in [
        "next_day_open_return", "next_day_gap_return", "next_day_first_5m_return",
        "next_day_first_15m_return", "next_day_first_30m_return", "next_day_high_return",
        "next_day_low_return", "next_day_close_return", "next_day_close_location_in_range",
        "next_day_fade_from_open", "next_day_continuation_from_open",
    ].iter().enumerate() {
        fill.insert(name, Arc::new(rows.iter().map(|r| r.aux.as_ref().and_then(|a| a.next_day[i])).collect::<Float64Array>()) as ArrayRef);
    }
    for k in 1..=5 {
        for (j, fam) in ["gap_return", "rth_return", "close_to_close_return", "open_to_close_return"].iter().enumerate() {
            fill.insert(leak(format!("{fam}_day_{k}")), Arc::new(rows.iter().map(|r| r.aux.as_ref().and_then(|a| a.gap_rth[k - 1][j])).collect::<Float64Array>()) as ArrayRef);
        }
    }

    // Cumulative pre-entry volume (04:00 ET → entry bar inclusive).
    fill.insert("cumulative_volume_to_entry", Arc::new(rows.iter().map(|r| r.cum_volume_to_entry).collect::<Float64Array>()) as ArrayRef);
    fill.insert("cumulative_dollar_volume_to_entry", Arc::new(rows.iter().map(|r| r.cum_dollar_volume_to_entry).collect::<Float64Array>()) as ArrayRef);

    // B5: multi-day horizon stats (10d–252d, daily resolution).
    let md = |get: &dyn Fn(&HorizonStat) -> Option<f64>, hi: usize| -> ArrayRef {
        Arc::new(rows.iter().map(|r| r.multiday.as_ref().and_then(|m| get(&m.stats[hi]))).collect::<Float64Array>()) as ArrayRef
    };
    let mdu = |get: &dyn Fn(&HorizonStat) -> Option<u32>, hi: usize| -> ArrayRef {
        Arc::new(rows.iter().map(|r| r.multiday.as_ref().and_then(|m| get(&m.stats[hi]))).collect::<UInt32Array>()) as ArrayRef
    };
    let idx_pos: Vec<Option<usize>> = INDEX_SIDS.iter().map(|s| inputs.iter().position(|i| i.display_symbol == *s)).collect();
    for (hi, (label, _)) in LONG_HORIZONS.iter().enumerate() {
        fill.insert(leak(format!("ret_{label}")), md(&|s| s.ret, hi));
        fill.insert(leak(format!("max_drawdown_{label}")), md(&|s| s.max_drawdown, hi));
        fill.insert(leak(format!("max_runup_{label}")), md(&|s| s.max_runup, hi));
        fill.insert(leak(format!("close_max_ret_{label}")), md(&|s| s.close_max_ret, hi));
        fill.insert(leak(format!("close_min_ret_{label}")), md(&|s| s.close_min_ret, hi));
        fill.insert(leak(format!("bars_to_max_drawdown_{label}")), mdu(&|s| s.bars_to_max_drawdown, hi));
        fill.insert(leak(format!("bars_to_max_runup_{label}")), mdu(&|s| s.bars_to_max_runup, hi));
        fill.insert(leak(format!("bars_to_close_min_{label}")), mdu(&|s| s.bars_to_close_min, hi));
        fill.insert(leak(format!("bars_to_close_max_{label}")), mdu(&|s| s.bars_to_close_max, hi));
        // excess vs each index at the same offset
        for (j, idx) in ["spy", "qqq", "iwm"].iter().enumerate() {
            let vals: Vec<Option<f64>> = (0..total)
                .map(|ri| {
                    let oi = ri % n_offsets;
                    let sec = rows[ri].multiday.as_ref().and_then(|m| m.stats[hi].ret)?;
                    let ip = idx_pos[j]?;
                    let iret = rows[ip * n_offsets + oi].multiday.as_ref().and_then(|m| m.stats[hi].ret)?;
                    Some(sec - iret)
                })
                .collect();
            fill.insert(leak(format!("ret_{label}_excess_{idx}")), Arc::new(Float64Array::from(vals)) as ArrayRef);
        }
    }
    // ret_<H>_total + dividend_ex_date_within_<H> (9 multi-day horizons)
    for (hi, (label, _)) in MULTIDAY_DAYS.iter().enumerate() {
        fill.insert(leak(format!("ret_{label}_total")), Arc::new(rows.iter().map(|r| r.multiday.as_ref().and_then(|m| m.ret_total[hi])).collect::<Float64Array>()) as ArrayRef);
        fill.insert(leak(format!("dividend_ex_date_within_{label}")), Arc::new(rows.iter().map(|r| r.multiday.as_ref().and_then(|m| m.div_ex[hi])).collect::<BooleanArray>()) as ArrayRef);
    }
    // (B5b) DAILY threshold crossings for the 5 long horizons.
    for (hi, (label, _)) in LONG_HORIZONS.iter().enumerate() {
        for (ti, t) in PCT_THRESHOLDS_V2.iter().enumerate() {
            let p = hi * np * 2 + ti * 2;
            fill.insert(leak(format!("first_cross_up_{t}pct_{label}")), Arc::new(rows.iter().map(|r| r.multiday.as_ref().and_then(|m| m.long_pct[p])).collect::<UInt32Array>()) as ArrayRef);
            fill.insert(leak(format!("first_cross_down_{t}pct_{label}")), Arc::new(rows.iter().map(|r| r.multiday.as_ref().and_then(|m| m.long_pct[p + 1])).collect::<UInt32Array>()) as ArrayRef);
        }
        for (ti, t) in ATR_THRESHOLDS_V2.iter().enumerate() {
            let p = hi * na * 2 + ti * 2;
            fill.insert(leak(format!("first_cross_up_{t}atr_{label}")), Arc::new(rows.iter().map(|r| r.multiday.as_ref().and_then(|m| m.long_atr[p])).collect::<UInt32Array>()) as ArrayRef);
            fill.insert(leak(format!("first_cross_down_{t}atr_{label}")), Arc::new(rows.iter().map(|r| r.multiday.as_ref().and_then(|m| m.long_atr[p + 1])).collect::<UInt32Array>()) as ArrayRef);
        }
    }
    // bar_gap_minutes_max_<H> for all 13 horizons (short from 1m tape; long null)
    for (hi, label) in FORWARD_HORIZONS_V2.iter().enumerate() {
        fill.insert(leak(format!("bar_gap_minutes_max_{label}")), Arc::new(rows.iter().map(|r| r.bar_gap.get(hi).copied().flatten()).collect::<Int32Array>()) as ArrayRef);
    }

    // Cross-sectional pre-entry ranks — ranked across the universe at each
    // entry_offset (rank 1 = largest, percentile = (m-1-r0)/m; ranks_desc,
    // matching daily_observation). Rows are inp-major: index = sec*offsets +
    // offset_index, so column `p` of offset `oi` is row sec*n_offsets+oi.
    let mut pe_ret_rank: Vec<Option<i32>> = vec![None; total];
    let mut pe_ret_pct: Vec<Option<f64>> = vec![None; total];
    let mut pe_dv_rank: Vec<Option<i32>> = vec![None; total];
    let n_sec = inputs.len();
    for oi in 0..n_offsets {
        let rets: Vec<Option<f64>> = (0..n_sec).map(|s| rows[s * n_offsets + oi].pre_entry_ret_from_open).collect();
        let dvs: Vec<Option<f64>> = (0..n_sec).map(|s| rows[s * n_offsets + oi].pre_entry_dollar_volume_from_open).collect();
        let (rr, rp) = ranks_desc(&rets);
        let (dr, _) = ranks_desc(&dvs);
        for s in 0..n_sec {
            let ri = s * n_offsets + oi;
            pe_ret_rank[ri] = rr[s];
            pe_ret_pct[ri] = rp[s];
            pe_dv_rank[ri] = dr[s];
        }
    }
    fill.insert("pre_entry_ret_rank_today", Arc::new(Int32Array::from(pe_ret_rank)) as ArrayRef);
    fill.insert("pre_entry_ret_percentile_today", Arc::new(Float64Array::from(pe_ret_pct)) as ArrayRef);
    fill.insert("pre_entry_dollar_volume_rank_today", Arc::new(Int32Array::from(pe_dv_rank)) as ArrayRef);

    // (B5b) terminal events.
    let tt: DictionaryArray<Int32Type> = rows.iter().map(|r| Some(r.terminal.event_type)).collect();
    fill.insert("terminal_event_type", Arc::new(tt) as ArrayRef);
    let tc: DictionaryArray<Int32Type> = rows.iter().map(|r| r.terminal.confidence).collect();
    fill.insert("terminal_event_confidence", Arc::new(tc) as ArrayRef);
    fill.insert("terminal_event_date", Arc::new(rows.iter().map(|r| r.terminal.date.map(date32)).collect::<Date32Array>()) as ArrayRef);
    fill.insert("last_valid_trade_date", Arc::new(rows.iter().map(|r| r.terminal.last_valid_trade_date.map(date32)).collect::<Date32Array>()) as ArrayRef);
    fill.insert("terminal_event_return", Arc::new(rows.iter().map(|r| r.terminal_return).collect::<Float64Array>()) as ArrayRef);
    fill.insert("days_with_missing_forward_bars", Arc::new(rows.iter().map(|r| r.terminal.days_with_missing_forward_bars).collect::<Int32Array>()) as ArrayRef);

    // Build the batch in schema order; anything not in `fill` is a typed null.
    for field in schema.fields() {
        let a = fill.remove(field.name().as_str()).unwrap_or_else(|| new_null_array(field.data_type(), total));
        arrays.push(a);
    }
    RecordBatch::try_new(schema, arrays)
}

// Leak short-lived format!() strings to &'static str for the HashMap key.
// Bounded: called O(columns) times per build, not per row.
fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn horizon_label(h: &Horizon) -> &'static str {
    match h {
        Horizon::Intraday(l, _) => l,
        Horizon::Eod => "EOD",
        Horizon::Day(l, _) => l,
    }
}

fn date32(day: NaiveDate) -> i32 {
    (day - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
}

/// Just the per-horizon ret vector for an index (used for excess).
fn entry_horizon_rets(day: NaiveDate, inp: &ForwardInput<'_>, off: &str) -> Vec<Option<f64>> {
    let row = resolve_entry(day, inp, off, &Default::default());
    row.horizons.iter().map(|h| h.ret).collect()
}

/// Resolve one (security, entry_offset) into a full EntryRow.
fn resolve_entry(
    day: NaiveDate,
    inp: &ForwardInput<'_>,
    off: &str,
    index_ret: &[Vec<Option<f64>>; 3],
) -> EntryRow {
    let blank = || EntryRow {
        entry_price: None,
        entry_unadjusted_price: None,
        is_halted_at_entry: None,
        pre_entry_ret_from_open: None,
        pre_entry_volume_from_open: None,
        pre_entry_dollar_volume_from_open: None,
        pre_entry_vwap_from_open: None,
        pre_entry_high_return_so_far: None,
        pre_entry_low_return_so_far: None,
        pre_entry_minutes_since_high: None,
        pre_entry_minutes_since_low: None,
        pre_entry_ret_from_high: None,
        pre_entry_ret_from_low: None,
        entry_price_location_in_1m_bar: None,
        entry_open_to_close_1m_return: None,
        entry_bar_upper_wick_pct: None,
        entry_bar_lower_wick_pct: None,
        entry_slippage_proxy_bps: None,
        entry_participation_capacity_1pct_adv: None,
        entry_participation_capacity_5pct_1m_volume: None,
        entry_1m_volume: None,
        entry_1m_range: None,
        entry_1m_dollar_volume: None,
        entry_range_vs_atr_14d: None,
        entry_range_vs_yz_vol_14d: None,
        entry_dollar_volume_vs_addv_20d: None,
        horizons: (0..SHORT_HORIZONS.len()).map(|_| HorizonStat::default()).collect(),
        excess: vec![[None; 3]; SHORT_HORIZONS.len()],
        cross: None,
        aux: None,
        cum_volume_to_entry: None,
        cum_dollar_volume_to_entry: None,
        multiday: None,
        bar_gap: vec![None; FORWARD_HORIZONS_V2.len()],
        terminal: TerminalInfo::default(),
        terminal_return: None,
    };
    if inp.ambiguous {
        return blank(); // collided sid — honestly null for both listings
    }
    let d0 = match inp.days.first() {
        Some(d) if !d.rth_bars.is_empty() => d,
        _ => return blank(),
    };
    let (h, m) = offset_hm(off);
    let entry_t = et(day, h, m);
    let (eidx, halted) = match find_entry(&d0.rth_bars, entry_t) {
        Some(x) => x,
        None => return blank(),
    };
    let entry_bar = d0.rth_bars[eidx];
    let entry_price = entry_bar.open;
    if !(entry_price > 0.0) {
        return blank();
    }
    let rth_open = d0.rth_bars[0].open;

    // ---- pre-entry (bars [09:30, entry) on D) ----
    let pre = &d0.rth_bars[..eidx];
    let (pre_vol, pre_dollar) = pre
        .iter()
        .fold((0.0, 0.0), |(v, d), b| (v + b.volume, d + b.close * b.volume));
    let pre_vwap = (pre_vol > 0.0).then(|| pre_dollar / pre_vol);
    let pre_high = pre.iter().map(|b| b.high).fold(f64::NEG_INFINITY, f64::max);
    let pre_low = pre.iter().map(|b| b.low).fold(f64::INFINITY, f64::min);
    let has_pre = !pre.is_empty();
    let high_at = pre.iter().enumerate().max_by(|a, b| a.1.high.total_cmp(&b.1.high)).map(|(i, _)| i);
    let low_at = pre.iter().enumerate().min_by(|a, b| a.1.low.total_cmp(&b.1.low)).map(|(i, _)| i);

    // ---- entry-quality proxies (the entry 1m bar) ----
    let rng = entry_bar.high - entry_bar.low;
    let loc = (rng > 0.0).then(|| (entry_bar.close - entry_bar.low) / rng);
    let dollar_1m = entry_bar.close * entry_bar.volume;
    let ctx = inp.entry_ctx;

    // ---- cumulative pre-entry volume (04:00 ET → entry bar inclusive) ----
    // premarket (from daily_observation[D]) + RTH from open through the entry
    // bar. `pre` is [open, entry) so the entry bar is added explicitly.
    let cum_volume_to_entry =
        Some(ctx.premarket_volume.unwrap_or(0.0) + pre_vol + entry_bar.volume);
    let cum_dollar_volume_to_entry =
        Some(ctx.premarket_dollar_volume.unwrap_or(0.0) + pre_dollar + dollar_1m);

    // ---- horizons + crossings + labels + day-0/next-day/gap families ----
    let (tape, bounds) = build_tape_with_bounds(inp, eidx);
    let entry_t = inp.days[0].rth_bars[eidx].t;
    let ends = ends_from_bounds(&tape, &bounds, entry_t);
    let horizons = resolve_horizons(&tape, entry_price, &ends);
    let cross = compute_cross_labels(&tape, entry_price, ctx.atr_14d, &ends);
    let aux = compute_aux(day, inp, eidx, entry_price, &tape, &ends);
    // ---- B5: multi-day horizons + ret_total + dividend flags + bar_gap ----
    let (d0_hi, d0_lo, d0_c) = day0_extremes(&tape, bounds[0]);
    let multiday = compute_multiday(entry_price, ctx.atr_14d, d0_hi, d0_lo, d0_c, inp.forward_daily, inp.dividends_fwd);
    let bar_gap = compute_bar_gap(&tape, &ends, &bounds);
    let excess: Vec<[Option<f64>; 3]> = horizons
        .iter()
        .enumerate()
        .map(|(hi, hs)| {
            let mut e = [None; 3];
            for j in 0..3 {
                if let (Some(r), Some(ir)) = (hs.ret, index_ret[j].get(hi).copied().flatten()) {
                    e[j] = Some(r - ir);
                }
            }
            e
        })
        .collect();

    EntryRow {
        entry_price: Some(entry_price),
        // d0 is on D's basis (entry-day scale = 1), so the raw open is the
        // adjusted open divided by factor_at(D).
        entry_unadjusted_price: (inp.entry_day_factor > 0.0)
            .then(|| entry_price / inp.entry_day_factor),
        is_halted_at_entry: Some(halted),
        pre_entry_ret_from_open: Some(entry_price / rth_open - 1.0),
        pre_entry_volume_from_open: Some(pre_vol),
        pre_entry_dollar_volume_from_open: Some(pre_dollar),
        pre_entry_vwap_from_open: pre_vwap,
        pre_entry_high_return_so_far: has_pre.then(|| pre_high / rth_open - 1.0),
        pre_entry_low_return_so_far: has_pre.then(|| pre_low / rth_open - 1.0),
        pre_entry_minutes_since_high: high_at.map(|i| (eidx - i) as i32),
        pre_entry_minutes_since_low: low_at.map(|i| (eidx - i) as i32),
        pre_entry_ret_from_high: has_pre.then(|| entry_price / pre_high - 1.0),
        pre_entry_ret_from_low: has_pre.then(|| entry_price / pre_low - 1.0),
        entry_price_location_in_1m_bar: loc,
        entry_open_to_close_1m_return: (entry_bar.open > 0.0).then(|| entry_bar.close / entry_bar.open - 1.0),
        entry_bar_upper_wick_pct: (entry_bar.open > 0.0)
            .then(|| (entry_bar.high - entry_bar.open.max(entry_bar.close)) / entry_bar.open),
        entry_bar_lower_wick_pct: (entry_bar.open > 0.0)
            .then(|| (entry_bar.open.min(entry_bar.close) - entry_bar.low) / entry_bar.open),
        entry_slippage_proxy_bps: (entry_price > 0.0).then(|| (rng / 2.0) / entry_price * 1e4),
        entry_participation_capacity_1pct_adv: ctx.adv_20d.map(|adv| 0.01 * adv * entry_price),
        entry_participation_capacity_5pct_1m_volume: Some(0.05 * dollar_1m),
        entry_1m_volume: Some(entry_bar.volume),
        entry_1m_range: Some(rng),
        entry_1m_dollar_volume: Some(dollar_1m),
        entry_range_vs_atr_14d: ctx.atr_14d.filter(|a| *a > 0.0).map(|a| rng / a),
        entry_range_vs_yz_vol_14d: ctx.yang_zhang_vol_14d.filter(|v| *v > 0.0).map(|v| rng / v),
        entry_dollar_volume_vs_addv_20d: ctx.addv_20d.filter(|a| *a > 0.0).map(|a| dollar_1m / a),
        horizons,
        excess,
        cross: Some(cross),
        aux: Some(aux),
        cum_volume_to_entry,
        cum_dollar_volume_to_entry,
        multiday: Some(multiday),
        bar_gap,
        terminal: inp.terminal,
        terminal_return: inp.terminal.last_valid_close.map(|c| c / entry_price - 1.0),
    }
}

/// Build the flattened forward tape and resolve all short horizons.
/// Build the flattened forward tape (D's RTH from the entry bar, then
/// D+1…D+5 full RTH) and the per-`SHORT_HORIZONS` end index into it.
/// Intraday horizons end at the last bar ≤ entry_bar.t + N minutes within
/// day D; `EOD` / `1d…5d` at the close of D / D+k. None = no forward data.
/// Build the flattened forward tape and the per-day `(start, close)` tape
/// index bounds. `bounds[0]` is day D (from the entry bar); `bounds[k]` is
/// D+k; None when that day has no buffered bars. Shared by the wide
/// (forward_outcomes) and long (forward_path_short) builders.
pub(crate) fn build_tape_with_bounds(
    inp: &ForwardInput<'_>,
    eidx: usize,
) -> (Vec<Bar>, [Option<(usize, usize)>; 6]) {
    let d0 = &inp.days[0];
    let mut tape: Vec<Bar> = Vec::new();
    tape.extend_from_slice(&d0.rth_bars[eidx..]);
    let mut bounds: [Option<(usize, usize)>; 6] = [None; 6];
    if !tape.is_empty() {
        bounds[0] = Some((0, tape.len() - 1));
    }
    for k in 1..=5 {
        if let Some(fd) = inp.days.get(k) {
            if !fd.rth_bars.is_empty() {
                let start = tape.len();
                tape.extend_from_slice(&fd.rth_bars);
                bounds[k] = Some((start, tape.len() - 1));
            }
        }
    }
    (tape, bounds)
}

/// Last tape index within day D (≤ `day0_close`) whose bar time is
/// ≤ `entry_t + minutes`. None if day D has no bars.
pub(crate) fn intraday_end(
    tape: &[Bar],
    day0_close: Option<usize>,
    entry_t: DateTime<Utc>,
    minutes: i64,
) -> Option<usize> {
    let upper = day0_close?;
    let cutoff = entry_t + chrono::Duration::minutes(minutes);
    let mut last = None;
    for (i, b) in tape.iter().enumerate().take(upper + 1) {
        if b.t <= cutoff {
            last = Some(i);
        } else {
            break;
        }
    }
    last
}

/// `SHORT_HORIZONS` end indices into the tape, from precomputed bounds.
fn ends_from_bounds(tape: &[Bar], bounds: &[Option<(usize, usize)>; 6], entry_t: DateTime<Utc>) -> Vec<Option<usize>> {
    let day0_close = bounds[0].map(|(_, c)| c);
    SHORT_HORIZONS
        .iter()
        .map(|h| match h {
            Horizon::Intraday(_, mins) => intraday_end(tape, day0_close, entry_t, *mins),
            Horizon::Eod => day0_close,
            Horizon::Day(_, k) => bounds[*k].map(|(_, c)| c),
        })
        .collect()
}

/// (B5) Day-D intraday extremes from the entry bar (tape[0..=day0_close]):
/// (high, low, close). The day-0 contribution to multi-day path stats.
fn day0_extremes(tape: &[Bar], day0: Option<(usize, usize)>) -> (f64, f64, f64) {
    let (s, c) = day0.expect("day 0 present for a valid entry");
    let mut hi = f64::NEG_INFINITY;
    let mut lo = f64::INFINITY;
    for b in &tape[s..=c] {
        hi = hi.max(b.high);
        lo = lo.min(b.low);
    }
    (hi, lo, tape[c].close)
}

/// Trading-day counts for the 9 `MULTIDAY_HORIZONS_V2` (ret_total + ex-date).
const MULTIDAY_DAYS: &[(&str, usize)] = &[
    ("1d", 1), ("2d", 2), ("3d", 3), ("5d", 5),
    ("10d", 10), ("21d", 21), ("42d", 42), ("63d", 63), ("252d", 252),
];

/// (B5) Multi-day horizon stats (10d–252d, daily resolution) + `ret_<H>_total`
/// + `dividend_ex_date_within_<H>` (9 horizons). `bars_to_*` here are in
/// TRADING-DAY units (0 = entry day D's intraday extreme; k = D+k).
struct MultiDay {
    stats: Vec<HorizonStat>,      // per LONG_HORIZONS (5); excess computed in build()
    ret_total: Vec<Option<f64>>,  // per MULTIDAY_DAYS (9)
    div_ex: Vec<Option<bool>>,    // per MULTIDAY_DAYS (9)
    // (B5b) DAILY-resolution threshold crossings for the 5 long horizons.
    // 1-based DAY index (day 0 = entry-day intraday extreme → 1; D+k → k+1);
    // 0 = horizon reached, never crossed; null = horizon truncated. ATR null
    // without atr_14d. Laid out [up, down] per (long-horizon, threshold).
    long_pct: Vec<Option<u32>>,   // 5 × 7 × 2
    long_atr: Vec<Option<u32>>,   // 5 × 6 × 2
    // (B5b) the 21d target-before-stop pair (3atr before −1.5atr, 21d, daily).
    label21_event: Option<&'static str>,
    label21_hit: Option<bool>,
}

/// First DAY index (0-based: 0 = entry-day intraday, k = D+k) at which the
/// daily series first satisfies `pred`, over `[day0, forward_daily]`.
fn first_daily_cross(
    day0: f64,
    series: &[DailyBar],
    field: impl Fn(&DailyBar) -> f64,
    day0_val: f64,
    pred: impl Fn(f64) -> bool,
) -> Option<usize> {
    let _ = day0;
    if pred(day0_val) {
        return Some(0);
    }
    series.iter().position(|db| pred(field(db))).map(|i| i + 1)
}

fn report_daily(first: Option<usize>, h: usize, have: bool) -> Option<u32> {
    if !have {
        return None; // horizon truncated (forward_daily < H)
    }
    match first {
        Some(idx) if idx <= h => Some(idx as u32 + 1), // 1-based; day0 → 1
        _ => Some(0),
    }
}

fn compute_multiday(
    entry: f64,
    atr_14d: Option<f64>,
    day0_high: f64,
    day0_low: f64,
    day0_close: f64,
    forward_daily: &[DailyBar],
    dividends_fwd: &[(NaiveDate, f64)],
) -> MultiDay {
    let stats = LONG_HORIZONS
        .iter()
        .map(|&(_, h)| {
            if forward_daily.len() < h {
                return HorizonStat::default();
            }
            let win = &forward_daily[..h];
            // running extremes: index 0 = day D intraday, k = D+k (1-based)
            let (mut mh, mut mh_at) = (day0_high, 0u32);
            let (mut ml, mut ml_at) = (day0_low, 0u32);
            let (mut mc, mut mc_at) = (day0_close, 0u32);
            let (mut nc, mut nc_at) = (day0_close, 0u32);
            for (i, db) in win.iter().enumerate() {
                let k = i as u32 + 1;
                if db.high > mh { mh = db.high; mh_at = k; }
                if db.low < ml { ml = db.low; ml_at = k; }
                if db.close > mc { mc = db.close; mc_at = k; }
                if db.close < nc { nc = db.close; nc_at = k; }
            }
            HorizonStat {
                ret: Some(win[h - 1].close / entry - 1.0),
                max_runup: Some(mh / entry - 1.0),
                bars_to_max_runup: Some(mh_at),
                max_drawdown: Some(ml / entry - 1.0),
                bars_to_max_drawdown: Some(ml_at),
                close_max_ret: Some(mc / entry - 1.0),
                bars_to_close_max: Some(mc_at),
                close_min_ret: Some(nc / entry - 1.0),
                bars_to_close_min: Some(nc_at),
            }
        })
        .collect();

    let mut ret_total = Vec::with_capacity(MULTIDAY_DAYS.len());
    let mut div_ex = Vec::with_capacity(MULTIDAY_DAYS.len());
    for &(_, h) in MULTIDAY_DAYS {
        if forward_daily.len() < h {
            ret_total.push(None);
            div_ex.push(None);
            continue;
        }
        let day_h = forward_daily[h - 1].day;
        let close_h = forward_daily[h - 1].close;
        let div_sum: f64 = dividends_fwd.iter().filter(|(ex, _)| *ex <= day_h).map(|(_, a)| *a).sum();
        let any = dividends_fwd.iter().any(|(ex, _)| *ex <= day_h);
        ret_total.push(Some((close_h + div_sum) / entry - 1.0));
        div_ex.push(Some(any));
    }

    // ---- (B5b) daily threshold crossings for the 5 long horizons ----
    let a14 = atr_14d.filter(|a| *a > 0.0);
    let cross_day = |up_thr: f64, dn_thr: f64| -> (Option<usize>, Option<usize>) {
        let up = first_daily_cross(day0_high, forward_daily, |db| db.high, day0_high, |x| x >= up_thr);
        let dn = first_daily_cross(day0_low, forward_daily, |db| db.low, day0_low, |x| x <= dn_thr);
        (up, dn)
    };
    let mut long_pct = Vec::with_capacity(LONG_HORIZONS.len() * PCT_THRESHOLDS_V2.len() * 2);
    let mut long_atr = Vec::with_capacity(LONG_HORIZONS.len() * ATR_THRESHOLDS_V2.len() * 2);
    // precompute first-cross day index per threshold (global over forward_daily)
    let pct_cross: Vec<(Option<usize>, Option<usize>)> = PCT_THRESHOLDS_V2
        .iter()
        .map(|s| {
            let v = pct_frac(s);
            cross_day(entry * (1.0 + v), entry * (1.0 - v))
        })
        .collect();
    let atr_cross: Vec<(Option<usize>, Option<usize>)> = ATR_THRESHOLDS_V2
        .iter()
        .map(|s| {
            let m = atr_mult(s);
            match a14 {
                Some(a) => cross_day(entry + m * a, entry - m * a),
                None => (None, None),
            }
        })
        .collect();
    for &(_, h) in LONG_HORIZONS {
        let have = forward_daily.len() >= h;
        for &(up, dn) in &pct_cross {
            long_pct.push(report_daily(up, h, have));
            long_pct.push(report_daily(dn, h, have));
        }
        for (ti, &(up, dn)) in atr_cross.iter().enumerate() {
            let _ = ti;
            if a14.is_none() {
                long_atr.push(None);
                long_atr.push(None);
            } else {
                long_atr.push(report_daily(up, h, have));
                long_atr.push(report_daily(dn, h, have));
            }
        }
    }

    // ---- (B5b) the 21d target-before-stop pair: 3atr before −1.5atr ----
    let (label21_event, label21_hit) = if forward_daily.len() < 21 || a14.is_none() {
        ("no_data", None)
    } else {
        let a = a14.unwrap();
        let (up, dn) = cross_day(entry + 3.0 * a, entry - 1.5 * a);
        let up = up.filter(|&i| i <= 21);
        let dn = dn.filter(|&i| i <= 21);
        let ev = match (up, dn) {
            (None, None) => "neither",
            (Some(_), None) => "target_first",
            (None, Some(_)) => "stop_first",
            (Some(u), Some(d)) => {
                if u < d { "target_first" } else { "stop_first" }
            }
        };
        (ev, Some(ev == "target_first"))
    };

    MultiDay {
        stats,
        ret_total,
        div_ex,
        long_pct,
        long_atr,
        label21_event: Some(label21_event),
        label21_hit,
    }
}

/// (B5) `bar_gap_minutes_max_<H>` for the 13 horizons: largest intra-RTH
/// (same-day) gap between consecutive bars in `[entry, end]`. From the 1m
/// tape for the 8 short horizons; null for the 5 long horizons (no 1m).
fn compute_bar_gap(tape: &[Bar], ends: &[Option<usize>], bounds: &[Option<(usize, usize)>; 6]) -> Vec<Option<i32>> {
    let day_start: std::collections::HashSet<usize> =
        bounds.iter().skip(1).filter_map(|b| b.map(|(s, _)| s)).collect();
    // running max same-day gap up to each tape index
    let mut prefix = vec![0i32; tape.len()];
    let mut run = 0i32;
    for i in 1..tape.len() {
        if !day_start.contains(&i) {
            let g = (tape[i].t - tape[i - 1].t).num_minutes() as i32;
            run = run.max(g);
        }
        prefix[i] = run;
    }
    // 13 horizons: first 8 are SHORT (ends), last 5 LONG (null)
    let mut out: Vec<Option<i32>> = ends.iter().map(|e| e.map(|e| prefix[e])).collect();
    out.extend(std::iter::repeat_n(None, LONG_HORIZONS.len()));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slices::et;

    fn b(day: NaiveDate, h: u32, m: u32, o: f64, hi: f64, lo: f64, c: f64, v: f64) -> Bar {
        Bar { t: et(day, h, m), open: o, high: hi, low: lo, close: c, volume: v }
    }
    fn fday(day: NaiveDate, bars: Vec<Bar>) -> ForwardDay {
        ForwardDay { day, rth_bars: bars, session_close: et(day, 16, 0) }
    }
    fn hidx(label: &str) -> usize {
        SHORT_HORIZONS.iter().position(|h| horizon_label(h) == label).unwrap()
    }
    fn d(n: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2020, 1, n).unwrap()
    }
    fn row(days: &[ForwardDay], off: &str) -> EntryRow {
        let inp = ForwardInput {
            security_id: "X",
            display_symbol: "X",
            days,
            entry_ctx: EntryCtx::default(),
            entry_day_factor: 1.0,
            forward_daily: &[],
            dividends_fwd: &[],
            terminal: Default::default(),
            ambiguous: false,
        };
        resolve_entry(days[0].day, &inp, off, &Default::default())
    }

    #[test]
    fn eod_extremes_hand_computed() {
        let day = d(2);
        let bars = vec![
            b(day, 9, 35, 100.0, 101.0, 99.0, 100.5, 10.0),
            b(day, 9, 36, 100.5, 102.0, 100.0, 101.0, 20.0),
            b(day, 9, 37, 101.0, 101.5, 98.0, 99.0, 30.0), // min low 98 @idx2
            b(day, 9, 38, 99.0, 103.0, 99.0, 102.0, 40.0),  // max high 103 & max close 102 @idx3
            b(day, 9, 39, 102.0, 102.5, 101.5, 102.0, 50.0), // EOD close 102
        ];
        let r = row(&[fday(day, bars)], "0935");
        assert_eq!(r.entry_price, Some(100.0));
        let h = &r.horizons[hidx("EOD")];
        assert!((h.ret.unwrap() - 0.02).abs() < 1e-12);
        assert!((h.max_runup.unwrap() - 0.03).abs() < 1e-12);
        assert_eq!(h.bars_to_max_runup, Some(3));
        assert!((h.max_drawdown.unwrap() + 0.02).abs() < 1e-12);
        assert_eq!(h.bars_to_max_drawdown, Some(2));
        assert_eq!(h.bars_to_close_max, Some(3)); // first occurrence of 102
        assert_eq!(h.bars_to_close_min, Some(2));
        // entry is the first RTH bar -> no pre-entry history
        assert_eq!(r.pre_entry_volume_from_open, Some(0.0));
        assert_eq!(r.pre_entry_high_return_so_far, None);
    }

    #[test]
    fn intraday_cap_measured_from_entry_bar() {
        let day = d(2);
        let bars = vec![
            b(day, 9, 35, 100.0, 100.0, 100.0, 100.0, 1.0),
            b(day, 9, 40, 101.0, 101.0, 101.0, 101.0, 1.0),
            b(day, 9, 45, 102.0, 102.0, 102.0, 102.0, 1.0), // exactly +10min
            b(day, 9, 50, 110.0, 110.0, 110.0, 110.0, 1.0), // past the 10min window
        ];
        let r = row(&[fday(day, bars)], "0935");
        assert!((r.horizons[hidx("10min")].ret.unwrap() - 0.02).abs() < 1e-12);
        assert!((r.horizons[hidx("EOD")].ret.unwrap() - 0.10).abs() < 1e-12);
    }

    #[test]
    fn multiday_uses_dk_close_and_nulls_absent_days() {
        let day0 = fday(d(2), vec![b(d(2), 9, 35, 100.0, 100.0, 100.0, 100.0, 1.0)]);
        let day1 = fday(d(3), vec![
            b(d(3), 9, 35, 101.0, 101.0, 101.0, 101.0, 1.0),
            b(d(3), 15, 59, 105.0, 105.0, 105.0, 105.0, 1.0),
        ]);
        let r = row(&[day0, day1], "0935");
        assert!((r.horizons[hidx("EOD")].ret.unwrap()).abs() < 1e-12);
        assert!((r.horizons[hidx("1d")].ret.unwrap() - 0.05).abs() < 1e-12);
        assert_eq!(r.horizons[hidx("2d")].ret, None); // D+2 not buffered
    }

    #[test]
    fn halted_entry_fills_at_next_bar_and_caps_from_it() {
        let day = d(2);
        // 09:35 then a gap to the 16:00 closing-auction print (half-day shape)
        let bars = vec![
            b(day, 9, 35, 100.0, 100.0, 100.0, 100.0, 1.0),
            b(day, 16, 0, 200.0, 201.0, 199.0, 200.5, 1.0),
        ];
        let r = row(&[fday(day, bars)], "1530");
        assert_eq!(r.is_halted_at_entry, Some(true));
        assert_eq!(r.entry_price, Some(200.0)); // open of the 16:00 fill bar
        let ten = &r.horizons[hidx("10min")];
        assert!((ten.ret.unwrap() - (200.5 / 200.0 - 1.0)).abs() < 1e-12);
    }

    fn cross_pct(r: &EntryRow, h: &str, t: &str, down: bool) -> Option<u32> {
        let c = r.cross.as_ref().unwrap();
        let hi = SHORT_HORIZONS.iter().position(|x| horizon_label(x) == h).unwrap();
        let ti = PCT_THRESHOLDS_V2.iter().position(|x| *x == t).unwrap();
        c.pct[hi * PCT_THRESHOLDS_V2.len() * 2 + ti * 2 + down as usize]
    }
    fn cross_atr(r: &EntryRow, h: &str, t: &str, down: bool) -> Option<u32> {
        let c = r.cross.as_ref().unwrap();
        let hi = SHORT_HORIZONS.iter().position(|x| horizon_label(x) == h).unwrap();
        let ti = ATR_THRESHOLDS_V2.iter().position(|x| *x == t).unwrap();
        c.atr[hi * ATR_THRESHOLDS_V2.len() * 2 + ti * 2 + down as usize]
    }
    fn evt(r: &EntryRow, pair: &str) -> Option<&'static str> {
        let c = r.cross.as_ref().unwrap();
        c.first_event[TARGET_STOP_PAIRS_V6.iter().position(|p| *p == pair).unwrap()]
    }

    // Rising-then-dipping path (thresholds cleared by a margin to avoid
    // f64 boundary effects): +0.6% @idx1, +1.2% @idx2, -1.1% @idx3.
    fn cross_bars(day: NaiveDate) -> Vec<Bar> {
        vec![
            b(day, 9, 35, 100.0, 100.0, 100.0, 100.0, 1.0),
            b(day, 9, 36, 100.0, 100.6, 100.0, 100.3, 1.0),
            b(day, 9, 37, 100.3, 101.2, 100.0, 101.0, 1.0),
            b(day, 9, 38, 101.0, 101.0, 98.9, 99.0, 1.0),
            b(day, 9, 39, 99.0, 99.0, 99.0, 99.0, 1.0),
        ]
    }

    #[test]
    fn crossings_are_1based_with_zero_for_never_and_null_when_absent() {
        let r = row(&[fday(d(2), cross_bars(d(2)))], "0935");
        // 1-based bar indices (entry bar = 1):
        assert_eq!(cross_pct(&r, "EOD", "0_5", false), Some(2)); // +0.5% high @idx1
        assert_eq!(cross_pct(&r, "EOD", "1", false), Some(3)); // +1% high @idx2
        assert_eq!(cross_pct(&r, "EOD", "2", false), Some(0)); // +2% never (reached EOD)
        assert_eq!(cross_pct(&r, "EOD", "1", true), Some(4)); // -1% low @idx3
        // 1d horizon has no forward data -> null, not 0
        assert_eq!(cross_pct(&r, "1d", "1", false), None);
    }

    #[test]
    fn atr_crossings_use_price_thresholds_and_null_without_atr() {
        let day = d(2);
        let bars = cross_bars(day);
        // with atr_14d = 1.0: +1 ATR = 101 -> first high>=101 at idx2 -> 3
        let inp = ForwardInput {
            security_id: "X",
            display_symbol: "X",
            days: &[fday(day, bars.clone())],
            entry_ctx: EntryCtx { atr_14d: Some(1.0), ..Default::default() },
            entry_day_factor: 1.0,
            forward_daily: &[],
            dividends_fwd: &[],
            terminal: Default::default(),
            ambiguous: false,
        };
        let r = resolve_entry(day, &inp, "0935", &Default::default());
        assert_eq!(cross_atr(&r, "EOD", "1", false), Some(3));
        assert_eq!(cross_atr(&r, "EOD", "3", false), Some(0)); // +3 ATR never
        // without atr_14d, atr crossings are null
        let r2 = row(&[fday(day, bars)], "0935");
        assert_eq!(cross_atr(&r2, "EOD", "1", false), None);
    }

    #[test]
    fn target_before_stop_labels() {
        // target_first: +1% (@idx2) before -1% (@idx3) within EOD
        let r = row(&[fday(d(2), cross_bars(d(2)))], "0935");
        assert_eq!(evt(&r, "1pct_before_minus_1pct_EOD"), Some("target_first"));
        let c = r.cross.as_ref().unwrap();
        let pi = TARGET_STOP_PAIRS_V6.iter().position(|p| *p == "1pct_before_minus_1pct_EOD").unwrap();
        assert_eq!(c.hit[pi], Some(true));
        // 21d pair is B5 -> null
        let p21 = TARGET_STOP_PAIRS_V6.iter().position(|p| *p == "3atr_before_minus_1_5atr_21d").unwrap();
        assert_eq!(c.first_event[p21], None);
    }

    #[test]
    fn stop_first_and_neither_labels() {
        let day = d(2);
        // drops to -1.1% (@idx1 low 98.9) before ever reaching +1%
        let down_first = vec![
            b(day, 9, 35, 100.0, 100.0, 100.0, 100.0, 1.0),
            b(day, 9, 36, 100.0, 100.2, 98.9, 99.5, 1.0),
            b(day, 9, 37, 99.5, 100.4, 99.5, 100.3, 1.0),
        ];
        let r = row(&[fday(day, down_first)], "0935");
        assert_eq!(evt(&r, "1pct_before_minus_1pct_EOD"), Some("stop_first"));
        // flat path: neither +1% nor -1%
        let flat = vec![
            b(day, 9, 35, 100.0, 100.1, 99.9, 100.0, 1.0),
            b(day, 9, 36, 100.0, 100.2, 99.8, 100.0, 1.0),
        ];
        let r2 = row(&[fday(day, flat)], "0935");
        assert_eq!(evt(&r2, "1pct_before_minus_1pct_EOD"), Some("neither"));
    }

    #[test]
    fn time_underwater_is_close_based_with_runs_and_recovery() {
        let day = d(2);
        let bars = vec![
            b(day, 9, 35, 100.0, 101.0, 99.0, 100.0, 1.0), // idx0 close=entry -> neither
            b(day, 9, 36, 100.0, 101.5, 100.0, 101.0, 1.0), // idx1 profitable
            b(day, 9, 37, 101.0, 101.0, 98.0, 99.0, 1.0),  // idx2 underwater (first)
            b(day, 9, 38, 99.0, 100.0, 98.5, 99.5, 1.0),   // idx3 underwater
            b(day, 9, 39, 99.5, 101.0, 99.5, 100.5, 1.0),  // idx4 profitable (recover)
        ];
        let r = row(&[fday(day, bars)], "0935");
        let a = r.aux.as_ref().unwrap();
        assert!((a.uw_f[0][0].unwrap() - 0.4).abs() < 1e-12); // pct profitable 2/5
        assert!((a.uw_f[0][1].unwrap() - 0.4).abs() < 1e-12); // pct underwater 2/5
        assert_eq!(a.uw_u[0][0], Some(1)); // max consecutive profitable
        assert_eq!(a.uw_u[0][1], Some(2)); // max consecutive underwater
        assert_eq!(a.uw_u[0][2], Some(2)); // time-to-recover: idx2 -> idx4
    }

    #[test]
    fn ret_to_segment_null_before_the_entry_fill() {
        let day = d(2);
        let bars = vec![
            b(day, 11, 0, 100.0, 100.0, 100.0, 100.0, 1.0),
            b(day, 15, 59, 102.0, 102.0, 102.0, 102.0, 1.0),
        ];
        let r = row(&[fday(day, bars)], "1100");
        let a = r.aux.as_ref().unwrap();
        assert_eq!(a.ret_to[0], None); // 10:30 segment ends before the 11:00 fill
        assert!((a.ret_to[8].unwrap() - 0.02).abs() < 1e-12); // ret_to_close
    }

    #[test]
    fn gap_vs_rth_and_next_day_decomposition() {
        let day0 = fday(d(2), vec![
            b(d(2), 9, 35, 100.0, 100.0, 100.0, 100.0, 1.0),
            b(d(2), 15, 59, 110.0, 110.0, 110.0, 110.0, 1.0),
        ]);
        let day1 = fday(d(3), vec![
            b(d(3), 9, 35, 121.0, 121.0, 121.0, 121.0, 1.0),
            b(d(3), 15, 59, 133.1, 133.1, 133.1, 133.1, 1.0),
        ]);
        let r = row(&[day0, day1], "0935");
        let a = r.aux.as_ref().unwrap();
        let g = a.gap_rth[0]; // day 1; prev close = D close 110
        assert!((g[0].unwrap() - (121.0 / 110.0 - 1.0)).abs() < 1e-12); // gap
        assert!((g[1].unwrap() - (133.1 / 121.0 - 1.0)).abs() < 1e-12); // rth
        assert!((g[2].unwrap() - (133.1 / 110.0 - 1.0)).abs() < 1e-12); // close-to-close
        assert!((g[3].unwrap() - g[1].unwrap()).abs() < 1e-12); // open_to_close ≡ rth
        assert!((a.next_day[1].unwrap() - (121.0 / 110.0 - 1.0)).abs() < 1e-12); // next_day_gap
    }

    #[test]
    fn cumulative_volume_is_premarket_plus_rth_through_entry() {
        let day = d(2);
        let bars = vec![
            b(day, 9, 35, 100.0, 100.0, 100.0, 100.0, 10.0),
            b(day, 9, 36, 100.0, 100.0, 100.0, 100.0, 20.0),
        ];
        let inp = ForwardInput {
            security_id: "X",
            display_symbol: "X",
            days: &[fday(day, bars)],
            entry_ctx: EntryCtx {
                premarket_volume: Some(5.0),
                premarket_dollar_volume: Some(500.0),
                ..Default::default()
            },
            entry_day_factor: 1.0,
            forward_daily: &[],
            dividends_fwd: &[],
            terminal: Default::default(),
            ambiguous: false,
        };
        // entry at 09:35 (idx0): RTH-through-entry = the entry bar only.
        let r = resolve_entry(day, &inp, "0935", &Default::default());
        assert_eq!(r.cum_volume_to_entry, Some(5.0 + 10.0));
        assert_eq!(r.cum_dollar_volume_to_entry, Some(500.0 + 100.0 * 10.0));
    }
}
