//! `forward_path_short` builder — B4 (long-format path checkpoints).
//!
//! Grain: one row per `(day, security_id, entry_offset, path_checkpoint)` —
//! RFC §9.5. Produced by the same forward pass as `forward_outcomes`
//! (`bin/write-forward-outcomes`): the per-entry tape (D's RTH from the entry
//! bar + D+1…D+5 RTH, pin basis) is shared via
//! [`crate::forward_outcomes::build_tape_with_bounds`].
//!
//! For each (sid, offset) with a valid entry, emits the 23 checkpoints of
//! `PATH_CHECKPOINTS_V1` with position-state-so-far measured over the tape
//! window `[entry, checkpoint-end]` (entry bar = index 0). (sid, offset)
//! pairs with no fill produce no path rows.
//!
//! Conventions (mirror forward_outcomes + the L2 validator):
//! - intraday checkpoints `Nm` end at the last day-D bar ≤ entry+N min
//!   (capped at D's close); `EOD` = D close; `Kd_open`/`Kd_close` = first/last
//!   RTH bar of D+K; `1d_30m` = last D+1 bar ≤ D+1 open + 30 min.
//! - `bars_elapsed` = tape index of the checkpoint end (entry bar = 0).
//! - `volatility_within_trade` = sample stddev of per-bar simple returns over
//!   the window (null if < 2 returns).
//! - `rate_of_change` = (ret − ret_prev_checkpoint) / (bars − bars_prev);
//!   null when the bar delta is 0 (a later checkpoint resolved onto the same
//!   bar). The first valid checkpoint's prev is entry (ret 0, bars 0).
//! - `current_ret_over_atr_14d` = (price − entry) / atr_14d (null without atr).
//! - `halt_gap_crossed` = any intra-RTH (same-day) gap ≥ 5 min between
//!   consecutive bars up to the checkpoint (overnight gaps don't count).

use crate::forward_outcomes::{ForwardInput, build_tape_with_bounds, find_entry, offset_hm};
use crate::slices::et;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, DictionaryArray, Float64Array, Int32Array, RecordBatch,
    StringArray,
};
use arrow::datatypes::Int32Type;
use chrono::{DateTime, NaiveDate, Utc};
use momentum_core::bar::Bar;
use momentum_core::phase0_outputs::{ENTRY_OFFSETS_V1, PATH_CHECKPOINTS_V1, forward_path_short_schema};
use std::sync::Arc;

/// One emitted checkpoint row's metric values.
struct CpRow {
    ret: Option<f64>,
    high_ret: Option<f64>,
    low_ret: Option<f64>,
    close_max_ret: Option<f64>,
    close_min_ret: Option<f64>,
    volume: Option<f64>,
    dollar_volume: Option<f64>,
    vwap: Option<f64>,
    bars_elapsed: Option<i32>,
    pct_prof: Option<f64>,
    pct_under: Option<f64>,
    vol_within: Option<f64>,
    roc: Option<f64>,
    ret_over_atr: Option<f64>,
    halt_gap: Option<bool>,
}

/// Map each `PATH_CHECKPOINTS_V1` label to its tape end index for one entry.
fn checkpoint_ends(
    tape: &[Bar],
    bounds: &[Option<(usize, usize)>; 6],
    entry_t: DateTime<Utc>,
    day: NaiveDate,
) -> Vec<Option<usize>> {
    let day0_close = bounds[0].map(|(_, c)| c);
    let intraday = |mins: i64| crate::forward_outcomes::intraday_end(tape, day0_close, entry_t, mins);
    // last bar within day K's segment whose time ≤ that day's open + mins
    let day_plus = |k: usize, mins: i64| -> Option<usize> {
        let (start, close) = bounds[k]?;
        let cutoff = tape[start].t + chrono::Duration::minutes(mins);
        let mut last = None;
        for (i, b) in tape.iter().enumerate().take(close + 1).skip(start) {
            if b.t <= cutoff {
                last = Some(i);
            } else {
                break;
            }
        }
        last
    };
    let _ = day;
    PATH_CHECKPOINTS_V1
        .iter()
        .map(|cp| match *cp {
            "EOD" => day0_close,
            "1d_open" => bounds[1].map(|(s, _)| s),
            "1d_30m" => day_plus(1, 30),
            "1d_close" => bounds[1].map(|(_, c)| c),
            "2d_open" => bounds[2].map(|(s, _)| s),
            "2d_close" => bounds[2].map(|(_, c)| c),
            "3d_open" => bounds[3].map(|(s, _)| s),
            "3d_close" => bounds[3].map(|(_, c)| c),
            "5d_open" => bounds[5].map(|(s, _)| s),
            "5d_close" => bounds[5].map(|(_, c)| c),
            other => {
                // intraday "<N>m"
                let mins: i64 = other.trim_end_matches('m').parse().expect("checkpoint minutes");
                intraday(mins)
            }
        })
        .collect()
}

/// Resolve all 23 checkpoint rows for one (sid, offset) entry.
fn resolve_path(
    tape: &[Bar],
    bounds: &[Option<(usize, usize)>; 6],
    entry: f64,
    entry_t: DateTime<Utc>,
    atr_14d: Option<f64>,
    day: NaiveDate,
) -> Vec<CpRow> {
    let n = tape.len();
    // Prefix scans over the tape (window [0, e], entry bar = 0).
    let mut p_high = vec![0.0; n];
    let mut p_low = vec![0.0; n];
    let mut p_cmax = vec![0.0; n];
    let mut p_cmin = vec![0.0; n];
    let mut p_vol = vec![0.0; n];
    let mut p_dollar = vec![0.0; n];
    let mut p_prof = vec![0u32; n];
    let mut p_under = vec![0u32; n];
    let mut p_sr = vec![0.0; n]; // Σ r_j, j=1..=i
    let mut p_sr2 = vec![0.0; n]; // Σ r_j²
    let mut p_halt = vec![false; n];
    // day-start tape indices (overnight boundaries — not intra-RTH gaps)
    let day_start: std::collections::HashSet<usize> =
        bounds.iter().skip(1).filter_map(|b| b.map(|(s, _)| s)).collect();

    let (mut mh, mut ml, mut mc, mut nc) = (f64::NEG_INFINITY, f64::INFINITY, f64::NEG_INFINITY, f64::INFINITY);
    let (mut sv, mut sd, mut cp, mut cu, mut sr, mut sr2) = (0.0, 0.0, 0u32, 0u32, 0.0, 0.0);
    let mut halt = false;
    for i in 0..n {
        let b = tape[i];
        mh = mh.max(b.high);
        ml = ml.min(b.low);
        mc = mc.max(b.close);
        nc = nc.min(b.close);
        sv += b.volume;
        sd += b.close * b.volume;
        if b.close > entry {
            cp += 1;
        } else if b.close < entry {
            cu += 1;
        }
        if i >= 1 {
            let r = tape[i].close / tape[i - 1].close - 1.0;
            sr += r;
            sr2 += r * r;
            let gap_min = (tape[i].t - tape[i - 1].t).num_minutes();
            if !day_start.contains(&i) && gap_min >= 5 {
                halt = true;
            }
        }
        p_high[i] = mh;
        p_low[i] = ml;
        p_cmax[i] = mc;
        p_cmin[i] = nc;
        p_vol[i] = sv;
        p_dollar[i] = sd;
        p_prof[i] = cp;
        p_under[i] = cu;
        p_sr[i] = sr;
        p_sr2[i] = sr2;
        p_halt[i] = halt;
    }

    let atr = atr_14d.filter(|a| *a > 0.0);
    let ends = checkpoint_ends(tape, bounds, entry_t, day);
    let mut out = Vec::with_capacity(ends.len());
    let mut prev: Option<(f64, usize)> = Some((0.0, 0)); // (ret, bars) at entry
    for end in ends {
        let Some(e) = end.filter(|&e| e < n) else {
            out.push(CpRow {
                ret: None, high_ret: None, low_ret: None, close_max_ret: None,
                close_min_ret: None, volume: None, dollar_volume: None, vwap: None,
                bars_elapsed: None, pct_prof: None, pct_under: None, vol_within: None,
                roc: None, ret_over_atr: None, halt_gap: None,
            });
            continue;
        };
        let nbar = (e + 1) as f64;
        let ret = tape[e].close / entry - 1.0;
        let vol_within = if e >= 2 {
            let nr = e as f64; // returns r_1..r_e
            let var = (p_sr2[e] - p_sr[e] * p_sr[e] / nr) / (nr - 1.0);
            Some(var.max(0.0).sqrt())
        } else {
            None
        };
        let roc = prev.and_then(|(pr, pb)| {
            (e > pb).then(|| (ret - pr) / (e - pb) as f64)
        });
        prev = Some((ret, e));
        out.push(CpRow {
            ret: Some(ret),
            high_ret: Some(p_high[e] / entry - 1.0),
            low_ret: Some(p_low[e] / entry - 1.0),
            close_max_ret: Some(p_cmax[e] / entry - 1.0),
            close_min_ret: Some(p_cmin[e] / entry - 1.0),
            volume: Some(p_vol[e]),
            dollar_volume: Some(p_dollar[e]),
            vwap: (p_vol[e] > 0.0).then(|| p_dollar[e] / p_vol[e]),
            bars_elapsed: Some(e as i32),
            pct_prof: Some(p_prof[e] as f64 / nbar),
            pct_under: Some(p_under[e] as f64 / nbar),
            vol_within,
            roc,
            ret_over_atr: atr.map(|a| (tape[e].close - entry) / a),
            halt_gap: Some(p_halt[e]),
        });
    }
    out
}

pub fn build(day: NaiveDate, inputs: &[ForwardInput<'_>]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let schema = forward_path_short_schema();
    // Long format: up to inputs × 17 offsets × 23 checkpoints rows.
    let mut sid: Vec<String> = Vec::new();
    let mut off: Vec<&str> = Vec::new();
    let mut cp: Vec<&str> = Vec::new();
    let mut rows: Vec<CpRow> = Vec::new();

    for inp in inputs {
        if inp.ambiguous {
            continue; // collided sid — no path rows (honest ambiguity)
        }
        let d0 = match inp.days.first() {
            Some(d) if !d.rth_bars.is_empty() => d,
            _ => continue,
        };
        for offset in ENTRY_OFFSETS_V1 {
            let (h, m) = offset_hm(offset);
            let entry_t = et(day, h, m);
            let Some((eidx, _)) = find_entry(&d0.rth_bars, entry_t) else { continue };
            let entry = d0.rth_bars[eidx].open;
            if !(entry > 0.0) {
                continue;
            }
            let (tape, bounds) = build_tape_with_bounds(inp, eidx);
            let etr = tape[0].t; // actual fill bar time (entry bar = tape[0])
            let path = resolve_path(&tape, &bounds, entry, etr, inp.entry_ctx.atr_14d, day);
            for (ci, r) in path.into_iter().enumerate() {
                sid.push(inp.security_id.to_string());
                off.push(offset);
                cp.push(PATH_CHECKPOINTS_V1[ci]);
                rows.push(r);
            }
        }
    }

    let total = sid.len();
    let day32 = (day - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
    let off_dict: DictionaryArray<Int32Type> = off.iter().copied().map(Some).collect();
    let cp_dict: DictionaryArray<Int32Type> = cp.iter().copied().map(Some).collect();

    let cols: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(vec![day32; total])),
        Arc::new(StringArray::from(sid)),
        Arc::new(off_dict),
        Arc::new(cp_dict),
        Arc::new(rows.iter().map(|r| r.ret).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.high_ret).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.low_ret).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.close_max_ret).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.close_min_ret).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.volume).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.dollar_volume).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.vwap).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.bars_elapsed).collect::<Int32Array>()),
        Arc::new(rows.iter().map(|r| r.pct_prof).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.pct_under).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.vol_within).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.roc).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.ret_over_atr).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.halt_gap).collect::<BooleanArray>()),
    ];
    RecordBatch::try_new(schema, cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(day: NaiveDate, h: u32, m: u32, o: f64, hi: f64, lo: f64, c: f64, v: f64) -> Bar {
        Bar { t: et(day, h, m), open: o, high: hi, low: lo, close: c, volume: v }
    }
    fn cp<'a>(path: &'a [CpRow], label: &str) -> &'a CpRow {
        &path[PATH_CHECKPOINTS_V1.iter().position(|x| *x == label).unwrap()]
    }

    #[test]
    fn checkpoints_eod_and_next_day_and_halt() {
        let d0 = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2020, 1, 3).unwrap();
        // day-0: entry bar then a 7-minute gap (>=5 -> halt) to the next bar
        let mut tape = vec![
            b(d0, 9, 35, 100.0, 100.0, 100.0, 100.0, 10.0),
            b(d0, 9, 42, 101.0, 101.5, 100.5, 101.0, 20.0),
        ];
        let mut bounds: [Option<(usize, usize)>; 6] = [None; 6];
        bounds[0] = Some((0, 1));
        let s = tape.len();
        tape.push(b(d1, 9, 35, 102.0, 102.0, 102.0, 102.0, 5.0));
        tape.push(b(d1, 15, 59, 105.0, 105.0, 105.0, 105.0, 5.0));
        bounds[1] = Some((s, tape.len() - 1));
        let entry_t = tape[0].t;

        let path = resolve_path(&tape, &bounds, 100.0, entry_t, Some(2.0), d0);
        let eod = cp(&path, "EOD");
        assert!((eod.ret.unwrap() - 0.01).abs() < 1e-12); // 101/100-1
        assert_eq!(eod.bars_elapsed, Some(1));
        assert_eq!(eod.halt_gap, Some(true)); // 7-min intraday gap crossed
        assert!((eod.high_ret.unwrap() - 0.015).abs() < 1e-12); // max high 101.5
        assert!((eod.ret_over_atr.unwrap() - (101.0 - 100.0) / 2.0).abs() < 1e-12);

        let d1c = cp(&path, "1d_close");
        assert!((d1c.ret.unwrap() - 0.05).abs() < 1e-12); // 105/100-1
        // the overnight gap (day-0 close -> day-1 open) is NOT an intra-RTH gap
        // but halt stays latched from the day-0 gap:
        assert_eq!(d1c.halt_gap, Some(true));

        let d1o = cp(&path, "1d_open");
        assert!((d1o.ret.unwrap() - 0.02).abs() < 1e-12); // 102/100-1
    }
}
