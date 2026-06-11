//! `daily_observation` builder — B1 coverage.
//!
//! Fills, per (day, security):
//! - identity + the two `[SRC]` bar-list columns (RFC §7);
//! - materialized signal snapshots (`intraday_ret_0930_to_*`, first-30m
//!   volume/dollar/vwap);
//! - `eod_*` aggregates (RES-only) + unadjusted variants + adjustment
//!   factor;
//! - premarket aggregates and overnight context (prior close, gap,
//!   prior-day last-30m shape);
//! - trailing windows from [`crate::rolling`]: ATR, realized vol,
//!   ADV/ADDV, 52w range, days-since-move, consecutive-up,
//!   days_since_first_bar, premarket-volume-vs-median (+ spike flag),
//!   gap-filled flag;
//! - bar-count data-quality + calendar fields.
//!
//! Plus (B1 increment 2): Yang-Zhang vols, index betas (date-aligned
//! via [`DayContext::index_cumlog`]), cross-sectional ranks +
//! percentiles, first-30m/first-hour shape descriptors, signal
//! freshness + concentration (trailing share percentile + HHI), the
//! remaining data-quality flags, and `prior_day_unadjusted_eod_close`.
//!
//! Still null after B1 (B2 fills via the earnings-calendar join, files
//! stamped `B1-partial`): `days_to_next_known_earnings`,
//! `days_since_last_earnings`, `is_earnings_day`,
//! `earnings_report_timing`.
//!
//! Leakage contract (RFC §6.2): every trailing value is read from
//! `RollingState` BEFORE the sweep pushes today's aggregates — bare-name
//! columns only ever see `[D-N, D-1]`.

use crate::aggregates::DailyAgg;
use crate::rolling::RollingState;
use crate::slices::{aggregate, et, slice};
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, ListArray,
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

/// One security's per-day inputs, precomputed by the sweep so the
/// aggregates are shared between this builder and the rolling update.
pub struct RowInput<'a> {
    pub session: &'a DaySession,
    /// None when the session has no RTH bars.
    pub agg: Option<DailyAgg>,
    /// Pin-basis adjustment factor for this (sid, day).
    pub adjustment_factor: f64,
    /// Reference-data event flags, resolved by the sweep (None = the
    /// reference source wasn't loaded).
    pub dividend_event_today: Option<bool>,
    pub ticker_event_today: Option<bool>,
    pub split_event_nearby: Option<bool>,
    /// Earnings proximity from the derived calendar (B2). Calendar days;
    /// 0 = announced today (see `earnings_report_timing` for whether
    /// that's knowable before the entry decision).
    /// `days_to_next_known_earnings` has NO input here by design — the
    /// SEC-filings source records announcements, not schedules, so that
    /// column stays null until a scheduled-earnings feed is ingested.
    pub days_since_last_earnings: Option<i32>,
    pub is_earnings_day: Option<bool>,
    pub earnings_report_timing: Option<String>,
}

/// Cross-day sweep state the builder reads (never writes). Contents
/// cover `[start, D-1]` only — the sweep updates them AFTER each build.
pub struct DayContext<'a> {
    /// Cumulative log close-to-close return per index trading day,
    /// `[spy, qqq, iwm]` — date-aligned beta inputs.
    pub index_cumlog: [&'a HashMap<NaiveDate, f64>; 3],
    /// Prior days' universe signal-share values (share of valid names
    /// with `intraday_ret_0930_to_1000 > 0`), for the trailing
    /// point-in-time concentration percentile.
    pub signal_share_history: &'a [f64],
}

pub fn build(
    day: NaiveDate,
    rows: &[RowInput<'_>],
    session_close: DateTime<Utc>,
    rolling: &RollingState,
    ctx: &DayContext<'_>,
) -> Result<RecordBatch, ArrowError> {
    let schema = daily_observation_schema();
    let n = rows.len();

    let premarket_start = et(day, 4, 0);
    let rth_open_t = et(day, 9, 30);
    let first_30m_end = et(day, 10, 0);
    let first_hour_end = et(day, 10, 30);
    let rth_end = session_close + chrono::Duration::minutes(1);
    let snapshot_ends: [(u32, u32, &str); 5] = [
        (9, 40, "0940"),
        (9, 50, "0950"),
        (10, 0, "1000"),
        (10, 10, "1010"),
        (10, 30, "1030"),
    ];

    let close_et = session_close.with_timezone(&New_York);
    let is_half_day = close_et.hour() < 14;

    // Column accumulators -------------------------------------------------
    let mut first_hour_slices: Vec<&[Bar]> = Vec::with_capacity(n);
    let mut rest_10m: Vec<Vec<Bar>> = Vec::with_capacity(n);
    let mut snap_ret: [Vec<Option<f64>>; 5] = std::array::from_fn(|_| Vec::with_capacity(n));
    let mut snap_vol = Vec::with_capacity(n);
    let mut snap_dvol = Vec::with_capacity(n);
    let mut snap_vwap = Vec::with_capacity(n);
    let mut eod: [Vec<Option<f64>>; 7] = std::array::from_fn(|_| Vec::with_capacity(n));
    let mut eod_unadj: [Vec<Option<f64>>; 4] = std::array::from_fn(|_| Vec::with_capacity(n));
    let mut adj_factor = Vec::with_capacity(n);
    let mut pm: [Vec<Option<f64>>; 5] = std::array::from_fn(|_| Vec::with_capacity(n));
    let mut prior_close = Vec::with_capacity(n);
    let mut overnight_gap = Vec::with_capacity(n);
    let mut prior_last30_ret = Vec::with_capacity(n);
    let mut prior_last30_share = Vec::with_capacity(n);
    let mut atr5 = Vec::with_capacity(n);
    let mut atr14 = Vec::with_capacity(n);
    let mut atr42 = Vec::with_capacity(n);
    let mut rvol21 = Vec::with_capacity(n);
    let mut adv: [Vec<Option<f64>>; 3] = std::array::from_fn(|_| Vec::with_capacity(n));
    let mut addv: [Vec<Option<f64>>; 3] = std::array::from_fn(|_| Vec::with_capacity(n));
    let mut high52 = Vec::with_capacity(n);
    let mut low52 = Vec::with_capacity(n);
    let mut pm_vs_med = Vec::with_capacity(n);
    let mut pm_spike = Vec::with_capacity(n);
    let mut since_move: [Vec<Option<i32>>; 3] = std::array::from_fn(|_| Vec::with_capacity(n));
    let mut consec_up = Vec::with_capacity(n);
    let mut gap_filled = Vec::with_capacity(n);
    let mut days_since_first = Vec::with_capacity(n);
    let mut bar_count_rth = Vec::with_capacity(n);
    let mut bar_count_first_hour = Vec::with_capacity(n);
    let mut bar_count_first_30m = Vec::with_capacity(n);
    let mut bar_count_premarket = Vec::with_capacity(n);
    let mut first_rth_bar_time: Vec<Option<String>> = Vec::with_capacity(n);
    let mut minutes_after_open: Vec<Option<i32>> = Vec::with_capacity(n);
    // First-30m / first-hour shape descriptors.
    let mut f30_high_ret: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut f30_low_ret: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut f30_time_high: Vec<Option<String>> = Vec::with_capacity(n);
    let mut f30_time_low: Vec<Option<String>> = Vec::with_capacity(n);
    let mut f30_ret_from_high_1000: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut f30_min_since_high_1000: Vec<Option<i32>> = Vec::with_capacity(n);
    let mut f15_share_of_30: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut fh_high_ret: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut fh_low_ret: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut fh_time_high: Vec<Option<String>> = Vec::with_capacity(n);
    let mut fh_ret_from_high_1030: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut f30_share_of_hour: Vec<Option<f64>> = Vec::with_capacity(n);
    // Vol estimators + betas.
    let mut yz: [Vec<Option<f64>>; 4] = std::array::from_fn(|_| Vec::with_capacity(n));
    let mut beta: [Vec<Option<f64>>; 3] = std::array::from_fn(|_| Vec::with_capacity(n));
    // Signal freshness.
    let mut sig_first: [Vec<Option<bool>>; 3] = std::array::from_fn(|_| Vec::with_capacity(n));
    // Remaining data quality + events.
    let mut missing_30m: Vec<i32> = Vec::with_capacity(n);
    let mut missing_hour: Vec<i32> = Vec::with_capacity(n);
    let mut zero_vol_30m: Vec<i32> = Vec::with_capacity(n);
    let mut bad_ohlc: Vec<bool> = Vec::with_capacity(n);
    let mut prior_unadj_close: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut div_event: Vec<Option<bool>> = Vec::with_capacity(n);
    let mut ticker_event: Vec<Option<bool>> = Vec::with_capacity(n);
    let mut split_nearby: Vec<Option<bool>> = Vec::with_capacity(n);
    let mut since_earnings: Vec<Option<i32>> = Vec::with_capacity(n);
    let mut is_earnings: Vec<Option<bool>> = Vec::with_capacity(n);
    let mut earnings_timing: Vec<Option<String>> = Vec::with_capacity(n);

    let fmt_hhmm = |t: DateTime<Utc>| {
        let e = t.with_timezone(&New_York);
        format!("{:02}:{:02}", e.hour(), e.minute())
    };

    for r in rows {
        let bars = &r.session.session.bars;
        let first_hour = slice(bars, rth_open_t, first_hour_end);
        let rth = slice(bars, rth_open_t, rth_end);
        let first_30m = slice(bars, rth_open_t, first_30m_end);

        // Data quality + identity-adjacent
        bar_count_rth.push(rth.len() as i32);
        bar_count_first_hour.push(first_hour.len() as i32);
        bar_count_first_30m.push(first_30m.len() as i32);
        bar_count_premarket.push(slice(bars, premarket_start, rth_open_t).len() as i32);
        match rth.first() {
            Some(b) => {
                let t_et = b.t.with_timezone(&New_York);
                first_rth_bar_time.push(Some(format!("{:02}:{:02}", t_et.hour(), t_et.minute())));
                minutes_after_open.push(Some((b.t - rth_open_t).num_minutes() as i32));
            }
            None => {
                first_rth_bar_time.push(None);
                minutes_after_open.push(None);
            }
        }

        // Signal snapshots: last close strictly before each cutoff ÷ RTH open.
        let rth_open_px = rth.first().map(|b| b.open);
        for (i, (h, m, _)) in snapshot_ends.iter().enumerate() {
            let w = slice(bars, rth_open_t, et(day, *h, *m));
            snap_ret[i].push(match (rth_open_px, w.last()) {
                (Some(o), Some(l)) if o > 0.0 => Some(l.close / o - 1.0),
                _ => None,
            });
        }
        let v30: f64 = first_30m.iter().map(|b| b.volume).sum();
        let dv30: f64 = first_30m.iter().map(|b| b.close * b.volume).sum();
        snap_vol.push((!first_30m.is_empty()).then_some(v30));
        snap_dvol.push((!first_30m.is_empty()).then_some(dv30));
        snap_vwap.push((v30 > 0.0).then(|| dv30 / v30));

        // Window shape: (high, t_high, low, t_low) over a bar slice.
        let extremes = |w: &[Bar]| {
            w.iter().fold(None, |acc: Option<(f64, DateTime<Utc>, f64, DateTime<Utc>)>, b| {
                Some(match acc {
                    None => (b.high, b.t, b.low, b.t),
                    Some((hi, hi_t, lo, lo_t)) => (
                        if b.high > hi { b.high } else { hi },
                        if b.high > hi { b.t } else { hi_t },
                        if b.low < lo { b.low } else { lo },
                        if b.low < lo { b.t } else { lo_t },
                    ),
                })
            })
        };

        // First-30m shape descriptors.
        match (rth_open_px, extremes(first_30m)) {
            (Some(o), Some((hi, hi_t, lo, lo_t))) if o > 0.0 => {
                f30_high_ret.push(Some(hi / o - 1.0));
                f30_low_ret.push(Some(lo / o - 1.0));
                f30_time_high.push(Some(fmt_hhmm(hi_t)));
                f30_time_low.push(Some(fmt_hhmm(lo_t)));
                let close_1000 = first_30m.last().map(|b| b.close);
                f30_ret_from_high_1000.push(close_1000.filter(|_| hi > 0.0).map(|c| c / hi - 1.0));
                f30_min_since_high_1000
                    .push(Some((first_30m_end - hi_t).num_minutes() as i32));
                let v15: f64 = slice(bars, rth_open_t, et(day, 9, 45))
                    .iter()
                    .map(|b| b.volume)
                    .sum();
                f15_share_of_30.push((v30 > 0.0).then(|| v15 / v30));
            }
            _ => {
                f30_high_ret.push(None);
                f30_low_ret.push(None);
                f30_time_high.push(None);
                f30_time_low.push(None);
                f30_ret_from_high_1000.push(None);
                f30_min_since_high_1000.push(None);
                f15_share_of_30.push(None);
            }
        }

        // First-hour shape descriptors.
        match (rth_open_px, extremes(first_hour)) {
            (Some(o), Some((hi, hi_t, lo, _))) if o > 0.0 => {
                fh_high_ret.push(Some(hi / o - 1.0));
                fh_low_ret.push(Some(lo / o - 1.0));
                fh_time_high.push(Some(fmt_hhmm(hi_t)));
                let close_1030 = first_hour.last().map(|b| b.close);
                fh_ret_from_high_1030.push(close_1030.filter(|_| hi > 0.0).map(|c| c / hi - 1.0));
                let vh: f64 = first_hour.iter().map(|b| b.volume).sum();
                f30_share_of_hour.push((vh > 0.0).then(|| v30 / vh));
            }
            _ => {
                fh_high_ret.push(None);
                fh_low_ret.push(None);
                fh_time_high.push(None);
                fh_ret_from_high_1030.push(None);
                f30_share_of_hour.push(None);
            }
        }

        // EOD / premarket aggregates (today's — RES-only columns).
        let f = r.adjustment_factor;
        adj_factor.push(Some(f));
        match &r.agg {
            Some(a) => {
                for (vec, v) in eod.iter_mut().zip([
                    a.rth_open, a.rth_high, a.rth_low, a.rth_close,
                    a.rth_volume, a.rth_dollar_volume, a.rth_vwap,
                ]) {
                    vec.push(Some(v));
                }
                for (vec, v) in eod_unadj.iter_mut().zip([a.rth_open, a.rth_high, a.rth_low, a.rth_close]) {
                    vec.push((f != 0.0).then(|| v / f));
                }
                for (vec, v) in pm.iter_mut().zip([
                    Some(a.premarket_volume),
                    a.premarket_high,
                    a.premarket_low,
                    a.premarket_vwap,
                    Some(a.premarket_dollar_volume),
                ]) {
                    vec.push(v);
                }
            }
            None => {
                for vec in eod.iter_mut() { vec.push(None); }
                for vec in eod_unadj.iter_mut() { vec.push(None); }
                for vec in pm.iter_mut() { vec.push(None); }
            }
        }

        // Trailing state — strictly [D-N, D-1] (rolling not yet updated).
        let hist = rolling.get(r.session.security_id.as_str());
        let prior = hist.and_then(|h| h.prior());
        let pc = prior.map(|p| p.rth_close);
        prior_close.push(pc);
        overnight_gap.push(match (pc, &r.agg) {
            (Some(pc), Some(a)) if pc > 0.0 => Some(a.rth_open / pc - 1.0),
            _ => None,
        });
        prior_last30_ret.push(prior.and_then(|p| p.last_30m_return));
        prior_last30_share.push(prior.and_then(|p| p.last_30m_volume_share));
        atr5.push(hist.and_then(|h| h.atr(5)));
        atr14.push(hist.and_then(|h| h.atr(14)));
        atr42.push(hist.and_then(|h| h.atr(42)));
        rvol21.push(hist.and_then(|h| h.realized_vol(21)));
        for (vec, w) in adv.iter_mut().zip([5, 20, 60]) {
            vec.push(hist.and_then(|h| h.adv(w)));
        }
        for (vec, w) in addv.iter_mut().zip([5, 20, 60]) {
            vec.push(hist.and_then(|h| h.addv(w)));
        }
        high52.push(hist.and_then(|h| h.high_52w()));
        low52.push(hist.and_then(|h| h.low_52w()));
        let ratio = match (hist.and_then(|h| h.premarket_volume_median(20)), &r.agg) {
            (Some(med), Some(a)) if med > 0.0 => Some(a.premarket_volume / med),
            _ => None,
        };
        pm_vs_med.push(ratio);
        pm_spike.push(ratio.map(|x| x > 5.0));
        for (i, vec) in since_move.iter_mut().enumerate() {
            vec.push(hist.and_then(|h| h.days_since_move(i)));
        }
        consec_up.push(hist.map(|h| h.consecutive_up_days()));
        gap_filled.push(match (pc, &r.agg) {
            (Some(pc), Some(a)) => Some(a.rth_low <= pc && pc <= a.rth_high),
            _ => None,
        });
        days_since_first.push(hist.map(|h| h.days_since_first_bar()).unwrap_or(0));

        // Vol estimators + betas (trailing, [D-N, D-1]).
        for (vec, w) in yz.iter_mut().zip([5usize, 14, 21, 42]) {
            vec.push(hist.and_then(|h| h.yang_zhang_vol(w)));
        }
        for (i, vec) in beta.iter_mut().enumerate() {
            vec.push(hist.and_then(|h| h.beta(ctx.index_cumlog[i], 60)));
        }

        // Signal freshness: fired today AND on none of the prior N days.
        // Unknown today → null; not fired today → false; fired but
        // insufficient history to verify the lookback → null.
        let fired_today = snap_ret[2].last().expect("pushed above");
        for (vec, w) in sig_first.iter_mut().zip([5usize, 10, 20]) {
            vec.push(match fired_today {
                None => None,
                Some(r) if *r <= 0.0 => Some(false),
                Some(_) => hist
                    .and_then(|h| h.signal_fired_in_last(w))
                    .map(|prior_fired| !prior_fired),
            });
        }

        prior_unadj_close.push(prior.map(|p| p.rth_close_unadjusted));

        // Remaining data quality. The 30/60 expectations hold on half
        // days too — the first hour is never truncated.
        missing_30m.push((30 - first_30m.len() as i32).max(0));
        missing_hour.push((60 - first_hour.len() as i32).max(0));
        zero_vol_30m.push(first_30m.iter().filter(|b| b.volume == 0.0).count() as i32);
        bad_ohlc.push(bars.iter().any(|b| {
            b.high < b.low
                || b.high < b.open
                || b.high < b.close
                || b.low > b.open
                || b.low > b.close
                || b.low <= 0.0
        }));
        div_event.push(r.dividend_event_today);
        ticker_event.push(r.ticker_event_today);
        split_nearby.push(r.split_event_nearby);
        since_earnings.push(r.days_since_last_earnings);
        is_earnings.push(r.is_earnings_day);
        earnings_timing.push(r.earnings_report_timing.clone());

        first_hour_slices.push(first_hour);
        rest_10m.push(aggregate(bars, first_hour_end, rth_end, 10));
    }

    // Cross-sectional ranks (within-day; rank 1 = largest; percentile =
    // rank-based share of valid cross-section strictly below).
    let (ret1000_rank, ret1000_pct) = ranks_desc(&snap_ret[2]);
    let (dvol1000_rank, _) = ranks_desc(&snap_dvol);
    let (pm_vol_rank, _) = ranks_desc(&pm[0]);
    let (pm_dvol_rank, _) = ranks_desc(&pm[4]);
    let (gap_rank, _) = ranks_desc(&overnight_gap);
    let (addv20_rank, _) = ranks_desc(&addv[1]);
    let (rvol21_rank, _) = ranks_desc(&rvol21);

    // Day-level signal concentration (same value on every row).
    let valid_signals = snap_ret[2].iter().flatten().count();
    let fired_signals = snap_ret[2].iter().flatten().filter(|r| **r > 0.0).count();
    let share_today = (valid_signals > 0).then(|| fired_signals as f64 / valid_signals as f64);
    let concentration_pct = match share_today {
        Some(s) if !ctx.signal_share_history.is_empty() => Some(
            ctx.signal_share_history.iter().filter(|&&x| x < s).count() as f64
                / ctx.signal_share_history.len() as f64,
        ),
        _ => None,
    };
    let hhi = {
        let strengths: Vec<f64> =
            snap_ret[2].iter().flatten().map(|r| r.max(0.0)).collect();
        let tot: f64 = strengths.iter().sum();
        (tot > 0.0).then(|| strengths.iter().map(|s| (s / tot).powi(2)).sum::<f64>())
    };

    // Assemble -------------------------------------------------------------
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let day32 = (day - epoch).num_days() as i32;

    let mut filled: HashMap<String, ArrayRef> = HashMap::new();
    let mut put = |name: &str, arr: ArrayRef| {
        filled.insert(name.to_string(), arr);
    };

    put("day", Arc::new(Date32Array::from(vec![day32; n])));
    put(
        "security_id",
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.session.security_id.as_str()).collect::<Vec<_>>(),
        )),
    );
    put(
        "display_symbol_on_day",
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.session.display_symbol.as_str()).collect::<Vec<_>>(),
        )),
    );
    put(
        "intraday_1m_first_hour",
        bars_list_array(
            schema.field_with_name("intraday_1m_first_hour").expect("field").data_type(),
            &first_hour_slices,
        ),
    );
    let rest_slices: Vec<&[Bar]> = rest_10m.iter().map(Vec::as_slice).collect();
    put(
        "intraday_10m_rest",
        bars_list_array(
            schema.field_with_name("intraday_10m_rest").expect("field").data_type(),
            &rest_slices,
        ),
    );
    for (i, (_, _, label)) in snapshot_ends.iter().enumerate() {
        put(
            &format!("intraday_ret_0930_to_{label}"),
            Arc::new(Float64Array::from(std::mem::take(&mut snap_ret[i]))),
        );
    }
    put("intraday_volume_0930_to_1000", Arc::new(Float64Array::from(snap_vol)));
    put("intraday_dollar_volume_0930_to_1000", Arc::new(Float64Array::from(snap_dvol)));
    put("intraday_vwap_0930_to_1000", Arc::new(Float64Array::from(snap_vwap)));
    for (i, name) in ["open", "high", "low", "close", "volume", "dollar_volume", "vwap"]
        .iter()
        .enumerate()
    {
        put(&format!("eod_day_{name}"), Arc::new(Float64Array::from(std::mem::take(&mut eod[i]))));
    }
    for (i, name) in ["open", "high", "low", "close"].iter().enumerate() {
        put(
            &format!("eod_unadjusted_day_{name}"),
            Arc::new(Float64Array::from(std::mem::take(&mut eod_unadj[i]))),
        );
    }
    put("adjustment_factor_on_day", Arc::new(Float64Array::from(adj_factor)));
    for (i, name) in ["volume", "high", "low", "vwap", "dollar_volume"].iter().enumerate() {
        put(&format!("premarket_{name}"), Arc::new(Float64Array::from(std::mem::take(&mut pm[i]))));
    }
    put("prior_day_eod_close", Arc::new(Float64Array::from(prior_close)));
    put("overnight_gap", Arc::new(Float64Array::from(overnight_gap)));
    put("prior_day_last_30m_return", Arc::new(Float64Array::from(prior_last30_ret)));
    put("prior_day_last_30m_volume_share", Arc::new(Float64Array::from(prior_last30_share)));
    put("atr_5d", Arc::new(Float64Array::from(atr5)));
    put("atr_14d", Arc::new(Float64Array::from(atr14)));
    put("atr_42d", Arc::new(Float64Array::from(atr42)));
    put("realized_vol_21d", Arc::new(Float64Array::from(rvol21)));
    for (i, w) in ["5d", "20d", "60d"].iter().enumerate() {
        put(&format!("adv_{w}"), Arc::new(Float64Array::from(std::mem::take(&mut adv[i]))));
        put(&format!("addv_{w}"), Arc::new(Float64Array::from(std::mem::take(&mut addv[i]))));
    }
    put("high_52w", Arc::new(Float64Array::from(high52)));
    put("low_52w", Arc::new(Float64Array::from(low52)));
    put("premarket_volume_vs_20d_median", Arc::new(Float64Array::from(pm_vs_med)));
    put("pre_market_volume_spike_flag", Arc::new(BooleanArray::from(pm_spike)));
    for (i, x) in ["5", "10", "20"].iter().enumerate() {
        put(
            &format!("days_since_last_{x}pct_move"),
            Arc::new(Int32Array::from(std::mem::take(&mut since_move[i]))),
        );
    }
    put("consecutive_up_days_close_to_close", Arc::new(Int32Array::from(consec_up)));
    put("gap_filled_today_flag", Arc::new(BooleanArray::from(gap_filled)));
    put("days_since_first_bar", Arc::new(Int32Array::from(days_since_first)));
    put("bar_count_rth", Arc::new(Int32Array::from(bar_count_rth)));
    put("bar_count_first_hour", Arc::new(Int32Array::from(bar_count_first_hour)));
    put("bar_count_first_30m", Arc::new(Int32Array::from(bar_count_first_30m)));
    put("bar_count_premarket", Arc::new(Int32Array::from(bar_count_premarket)));
    put("first_rth_bar_time", Arc::new(StringArray::from(first_rth_bar_time)));
    put("minutes_after_open_first_bar", Arc::new(Int32Array::from(minutes_after_open)));
    put(
        "day_of_week",
        Arc::new(Int32Array::from(vec![
            chrono::Datelike::weekday(&day).num_days_from_monday() as i32;
            n
        ])),
    );
    put("is_half_day", Arc::new(BooleanArray::from(vec![is_half_day; n])));

    // Shape descriptors.
    put("intraday_first_30m_high_return", Arc::new(Float64Array::from(f30_high_ret)));
    put("intraday_first_30m_low_return", Arc::new(Float64Array::from(f30_low_ret)));
    put("intraday_time_of_first_30m_high", Arc::new(StringArray::from(f30_time_high)));
    put("intraday_time_of_first_30m_low", Arc::new(StringArray::from(f30_time_low)));
    put("intraday_ret_from_first_30m_high_to_1000", Arc::new(Float64Array::from(f30_ret_from_high_1000)));
    put("intraday_minutes_since_first_30m_high_at_1000", Arc::new(Int32Array::from(f30_min_since_high_1000)));
    put("intraday_first_15m_volume_share_of_first_30m", Arc::new(Float64Array::from(f15_share_of_30)));
    put("intraday_first_hour_high_return", Arc::new(Float64Array::from(fh_high_ret)));
    put("intraday_first_hour_low_return", Arc::new(Float64Array::from(fh_low_ret)));
    put("intraday_time_of_first_hour_high", Arc::new(StringArray::from(fh_time_high)));
    put("intraday_ret_from_first_hour_high_to_1030", Arc::new(Float64Array::from(fh_ret_from_high_1030)));
    put("intraday_first_30m_volume_share_of_first_hour", Arc::new(Float64Array::from(f30_share_of_hour)));

    // Cross-sectional ranks.
    put("intraday_ret_0930_to_1000_rank_today", Arc::new(Int32Array::from(ret1000_rank)));
    put("intraday_ret_0930_to_1000_percentile_today", Arc::new(Float64Array::from(ret1000_pct)));
    put("intraday_dollar_volume_0930_to_1000_rank_today", Arc::new(Int32Array::from(dvol1000_rank)));
    put("premarket_volume_rank_today", Arc::new(Int32Array::from(pm_vol_rank)));
    put("premarket_dollar_volume_rank_today", Arc::new(Int32Array::from(pm_dvol_rank)));
    put("overnight_gap_rank_today", Arc::new(Int32Array::from(gap_rank)));
    put("addv_20d_rank_today", Arc::new(Int32Array::from(addv20_rank)));
    put("realized_vol_21d_rank_today", Arc::new(Int32Array::from(rvol21_rank)));

    // Vol estimators + betas.
    for (i, w) in ["5d", "14d", "21d", "42d"].iter().enumerate() {
        put(&format!("yang_zhang_vol_{w}"), Arc::new(Float64Array::from(std::mem::take(&mut yz[i]))));
    }
    for (i, idx) in ["spy", "qqq", "iwm"].iter().enumerate() {
        put(&format!("beta_{idx}_60d"), Arc::new(Float64Array::from(std::mem::take(&mut beta[i]))));
    }

    // Signal freshness / concentration (v2, §3.7).
    for (i, w) in ["5d", "10d", "20d"].iter().enumerate() {
        put(&format!("signal_first_in_{w}"), Arc::new(BooleanArray::from(std::mem::take(&mut sig_first[i]))));
    }
    put("signal_concentration_percentile_today", Arc::new(Float64Array::from(vec![concentration_pct; n])));
    put("signal_concentration_hhi_today", Arc::new(Float64Array::from(vec![hhi; n])));

    // Remaining data quality + events + prior unadjusted.
    put("prior_day_unadjusted_eod_close", Arc::new(Float64Array::from(prior_unadj_close)));
    put("missing_1m_bars_first_30m", Arc::new(Int32Array::from(missing_30m)));
    put("missing_1m_bars_first_hour", Arc::new(Int32Array::from(missing_hour)));
    put("zero_volume_1m_bars_first_30m", Arc::new(Int32Array::from(zero_vol_30m)));
    put("has_bad_ohlc", Arc::new(BooleanArray::from(bad_ohlc)));
    put("split_event_nearby", Arc::new(BooleanArray::from(split_nearby)));
    put("dividend_event_today", Arc::new(BooleanArray::from(div_event)));
    put("ticker_event_today", Arc::new(BooleanArray::from(ticker_event)));

    // Earnings proximity (B2 calendar join). days_to_next_known_earnings
    // intentionally NOT put — null by source (SEC filings record
    // announcements, not schedules; see RowInput docs).
    put("days_since_last_earnings", Arc::new(Int32Array::from(since_earnings)));
    put("is_earnings_day", Arc::new(BooleanArray::from(is_earnings)));
    {
        use arrow::array::StringDictionaryBuilder;
        use arrow::datatypes::Int32Type;
        let mut b = StringDictionaryBuilder::<Int32Type>::new();
        for v in &earnings_timing {
            b.append_option(v.as_deref());
        }
        put("earnings_report_timing", Arc::new(b.finish()));
    }

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

/// Descending ranks over an optional-valued cross-section: rank 1 = the
/// largest finite value; percentile = rank-based share of the valid
/// cross-section strictly below (ties broken by order, documented
/// approximation). Nulls stay null.
fn ranks_desc(vals: &[Option<f64>]) -> (Vec<Option<i32>>, Vec<Option<f64>>) {
    let mut idx: Vec<usize> = (0..vals.len())
        .filter(|&i| vals[i].is_some_and(|v| v.is_finite()))
        .collect();
    idx.sort_by(|&a, &b| vals[b].partial_cmp(&vals[a]).expect("finite"));
    let m = idx.len();
    let mut rank = vec![None; vals.len()];
    let mut pct = vec![None; vals.len()];
    for (r0, &i) in idx.iter().enumerate() {
        rank[i] = Some((r0 + 1) as i32);
        pct[i] = Some((m - 1 - r0) as f64 / m as f64);
    }
    (rank, pct)
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
    use crate::aggregates;
    use arrow::array::{Array, AsArray};
    use momentum_core::ids::SecurityId;
    use momentum_core::store::Session;

    fn mk_session(
        day: NaiveDate,
        sid: &str,
        symbol: &str,
        bars_spec: &[(u32, u32, f64)], // (hour_et, minute_et, price)
    ) -> DaySession {
        let bars = bars_spec
            .iter()
            .map(|&(h, m, px)| Bar {
                t: et(day, h, m),
                open: px,
                high: px + 1.0,
                low: px - 1.0,
                close: px + 0.5,
                volume: 100.0,
            })
            .collect();
        DaySession {
            security_id: SecurityId::new(sid),
            display_symbol: symbol.to_string(),
            session: Session::new(day, bars),
        }
    }

    fn inputs<'a>(
        sessions: &'a [DaySession],
        day: NaiveDate,
        close: DateTime<Utc>,
    ) -> Vec<RowInput<'a>> {
        sessions
            .iter()
            .map(|s| RowInput {
                session: s,
                agg: aggregates::compute(&s.session, day, close, 1.0),
                adjustment_factor: 1.0,
                dividend_event_today: None,
                ticker_event_today: None,
                split_event_nearby: None,
                days_since_last_earnings: None,
                is_earnings_day: None,
                earnings_report_timing: None,
            })
            .collect()
    }

    fn with_empty_ctx<T>(f: impl FnOnce(&DayContext<'_>) -> T) -> T {
        let empty: HashMap<NaiveDate, f64> = HashMap::new();
        let ctx = DayContext {
            index_cumlog: [&empty, &empty, &empty],
            signal_share_history: &[],
        };
        f(&ctx)
    }

    #[test]
    fn b1_batch_fills_snapshots_eod_and_trailing_state() {
        let day1 = NaiveDate::from_ymd_opt(2021, 3, 15).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2021, 3, 16).unwrap();
        let close1 = et(day1, 15, 59);
        let close2 = et(day2, 15, 59);
        let mut rolling = RollingState::new();

        // Day 1: establishes prior state. Close = 100.5.
        let s1 = vec![mk_session(day1, "BBG1", "AAA", &[(9, 30, 100.0), (15, 59, 100.0)])];
        for r in inputs(&s1, day1, close1) {
            rolling.update("BBG1", day1, r.agg.unwrap());
        }

        // Day 2: opens at 103 (gap up vs 100.5 close), 9:45 print at 104,
        // intraday low 102 (does NOT touch 100.5 → gap unfilled).
        let s2 = vec![mk_session(day2, "BBG1", "AAA", &[(9, 30, 103.0), (9, 45, 104.0), (12, 0, 103.5)])];
        let rows = inputs(&s2, day2, close2);
        let batch = with_empty_ctx(|ctx| build(day2, &rows, close2, &rolling, ctx)).unwrap();

        let col = |name: &str| batch.column_by_name(name).unwrap().clone();
        let f64v = |name: &str| col(name).as_primitive::<arrow::datatypes::Float64Type>().value(0);

        // Snapshot at 10:00: last bar before 10:00 is the 9:45 bar
        // (close 104.5), RTH open 103 → ret = 104.5/103 − 1.
        assert!((f64v("intraday_ret_0930_to_1000") - (104.5 / 103.0 - 1.0)).abs() < 1e-12);
        // 09:40 snapshot only sees the 9:30 bar (close 103.5).
        assert!((f64v("intraday_ret_0930_to_0940") - (103.5 / 103.0 - 1.0)).abs() < 1e-12);

        // EOD aggregates for day 2.
        assert_eq!(f64v("eod_day_open"), 103.0);
        assert_eq!(f64v("eod_day_close"), 104.0); // 103.5 + 0.5
        assert_eq!(f64v("eod_day_volume"), 300.0);
        assert_eq!(f64v("eod_unadjusted_day_open"), 103.0); // factor 1.0

        // Overnight context from day-1 state.
        assert_eq!(f64v("prior_day_eod_close"), 100.5);
        assert!((f64v("overnight_gap") - (103.0 / 100.5 - 1.0)).abs() < 1e-12);

        // Day-2 low is 102 (103−1); prior close 100.5 untouched.
        let gf = col("gap_filled_today_flag");
        assert!(!gf.as_boolean().value(0));

        // One prior day on record.
        let dsf = col("days_since_first_bar");
        assert_eq!(dsf.as_primitive::<arrow::datatypes::Int32Type>().value(0), 1);

        // Not enough history for ATR/ADV windows → null.
        assert!(col("atr_14d").null_count() == 1);
        assert!(col("adv_20d").null_count() == 1);

        // Schema integrity.
        assert_eq!(batch.schema(), daily_observation_schema());
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn first_day_security_gets_zero_days_since_first_bar_and_null_trailing() {
        let day = NaiveDate::from_ymd_opt(2021, 3, 15).unwrap();
        let close = et(day, 15, 59);
        let sessions = vec![mk_session(day, "BBGNEW", "NEW", &[(9, 30, 10.0)])];
        let rows = inputs(&sessions, day, close);
        let batch = with_empty_ctx(|ctx| build(day, &rows, close, &RollingState::new(), ctx)).unwrap();

        let dsf = batch.column_by_name("days_since_first_bar").unwrap();
        assert_eq!(dsf.as_primitive::<arrow::datatypes::Int32Type>().value(0), 0);
        assert_eq!(batch.column_by_name("prior_day_eod_close").unwrap().null_count(), 1);
        assert_eq!(batch.column_by_name("overnight_gap").unwrap().null_count(), 1);
    }

    #[test]
    fn split_factor_recovers_unadjusted_prices() {
        let day = NaiveDate::from_ymd_opt(2020, 8, 28).unwrap();
        let close = et(day, 15, 59);
        // Adjusted price 100 with factor 0.25 → raw 400.
        let sessions = vec![mk_session(day, "BBGAAPL", "AAPL", &[(9, 30, 100.0)])];
        let mut rows = inputs(&sessions, day, close);
        rows[0].adjustment_factor = 0.25;
        let batch = with_empty_ctx(|ctx| build(day, &rows, close, &RollingState::new(), ctx)).unwrap();
        let v = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(0)
        };
        assert_eq!(v("eod_day_open"), 100.0);
        assert_eq!(v("eod_unadjusted_day_open"), 400.0);
        assert_eq!(v("adjustment_factor_on_day"), 0.25);
    }
}
