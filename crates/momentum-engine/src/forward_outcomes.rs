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

use crate::slices::et;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, DictionaryArray, Float64Array, Int32Array,
    RecordBatch, StringArray, UInt32Array, new_null_array,
};
use arrow::datatypes::Int32Type;
use chrono::{DateTime, NaiveDate, Utc};
use momentum_core::bar::Bar;
use momentum_core::phase0_outputs::{ENTRY_OFFSETS_V1, forward_outcomes_schema};
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
}

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
}

/// Parse an offset label like "0935" or "1530" into ET (hour, minute).
fn offset_hm(label: &str) -> (u32, u32) {
    let h: u32 = label[..2].parse().expect("offset hh");
    let m: u32 = label[2..].parse().expect("offset mm");
    (h, m)
}

/// Find the entry bar: first RTH bar at/after the entry minute on day D.
/// Returns (index into rth, halted) where halted = the exact minute was
/// absent. None if no RTH bar at/after the entry minute exists.
fn find_entry(rth: &[Bar], entry_t: DateTime<Utc>) -> Option<(usize, bool)> {
    let idx = rth.iter().position(|b| b.t >= entry_t)?;
    Some((idx, rth[idx].t > entry_t))
}

/// Resolve all horizons for one entry on a flattened forward tape.
/// `tape` is the concatenation of D's RTH-from-entry-bar plus D+1…D+5 RTH,
/// in time order; `day_close_idx[k]` is the index in `tape` of the last
/// bar on D+k (k=0 is D's EOD), or None if that day isn't buffered.
fn resolve_horizons(
    tape: &[Bar],
    entry_price: f64,
    intraday_caps: &[(usize, i64)], // (tape index of last bar ≤ entry+min, minutes) per intraday horizon
    day_close_idx: &[Option<usize>; 6],
) -> Vec<HorizonStat> {
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

    let mut out = Vec::with_capacity(SHORT_HORIZONS.len());
    let mut intraday_i = 0;
    for h in SHORT_HORIZONS {
        let end = match h {
            Horizon::Intraday(_, _) => {
                let (idx, _) = intraday_caps[intraday_i];
                intraday_i += 1;
                if idx == usize::MAX { None } else { Some(idx) }
            }
            Horizon::Eod => day_close_idx[0],
            Horizon::Day(_, k) => day_close_idx[*k],
        };
        out.push(stat_at(end));
    }
    out
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
    };
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

    // ---- horizons ----
    let horizons = compute_horizons(day, inp, eidx, entry_price);
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
    }
}

/// Build the flattened forward tape and resolve all short horizons.
fn compute_horizons(day: NaiveDate, inp: &ForwardInput<'_>, eidx: usize, entry_price: f64) -> Vec<HorizonStat> {
    let d0 = &inp.days[0];
    // Tape = D's RTH from entry bar onward, then D+1…D+5 full RTH.
    let mut tape: Vec<Bar> = Vec::new();
    tape.extend_from_slice(&d0.rth_bars[eidx..]);
    let mut day_close_idx: [Option<usize>; 6] = [None; 6];
    day_close_idx[0] = (!tape.is_empty()).then(|| tape.len() - 1);
    for k in 1..=5 {
        if let Some(fd) = inp.days.get(k) {
            if !fd.rth_bars.is_empty() {
                tape.extend_from_slice(&fd.rth_bars);
                day_close_idx[k] = Some(tape.len() - 1);
            }
        }
    }
    // Intraday caps: last tape index with t ≤ entry_t + N minutes, on day D.
    let entry_t = d0.rth_bars[eidx].t;
    let intraday_caps: Vec<(usize, i64)> = SHORT_HORIZONS
        .iter()
        .filter_map(|hz| match hz {
            Horizon::Intraday(_, mins) => Some(*mins),
            _ => None,
        })
        .map(|mins| {
            let cutoff = entry_t + chrono::Duration::minutes(mins);
            // search within D's portion of the tape (indices 0..=day_close_idx[0])
            let upper = day_close_idx[0].unwrap_or(0);
            let mut last = usize::MAX;
            for (i, b) in tape.iter().enumerate().take(upper + 1) {
                if b.t <= cutoff {
                    last = i;
                } else {
                    break;
                }
            }
            (last, mins)
        })
        .collect();
    let _ = day;
    resolve_horizons(&tape, entry_price, &intraday_caps, &day_close_idx)
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
}
