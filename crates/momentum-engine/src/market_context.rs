//! `market_context_daily` builder — one row per trading day (RFC §8 +
//! the §3.7 v2 dispersion/liquidity columns).
//!
//! Fills, per day:
//! - the three `[SRC]` index paths `{spy,qqq,iwm}_intraday_10m`;
//! - index EOD aggregates, overnight gap, 09:30→10:00 return, and
//!   21d realized vol (all precomputed by the sweep, passed in via
//!   [`IndexDay`]);
//! - `vix_close`;
//! - the nine breadth columns over the day's universe snapshots;
//! - the six v2 cross-sectional dispersion / liquidity columns.
//!
//! ## Definition choices
//!
//! - **`{idx}_intraday_10m` covers 04:00–20:00 ET**, the full extended
//!   session, not just RTH. The raw tape covers 04:00–20:00 and this is
//!   the table's only `[SRC]` column — per the no-arbitrary-cutoffs rule
//!   we record everything and let downstream slice RTH (or any other
//!   window) at query time. A missing index day writes an **empty list**
//!   (the column is non-nullable `List`), distinguishable from a present
//!   index with sparse bars by `breadth_total_universe_with_bars` and the
//!   sibling `{idx}_eod_*` nulls.
//! - **`vix_open` is ALWAYS null.** The on-disk VIX source is FRED's
//!   VIXCLS series, which is close-only; there is no open print to
//!   record. The column stays in the schema so a future intraday VIX
//!   source can fill it without a schema bump.
//! - **Breadth null rule:** each breadth column is computed over the
//!   universe entries where *that column's* input is `Some`; if zero
//!   entries are valid the column is null (a 0 would be a lie — it
//!   means "no data", not "nothing moved"). `breadth_total_universe_with_bars`
//!   is always `universe.len()` (0 is meaningful there).
//! - **Advance/decline ratios** count `> 0` as advance and `< 0` as
//!   decline (exact zeros are neither); null when there are zero
//!   decliners (the ratio is undefined, and `+inf` poisons Parquet
//!   stats), which also covers the zero-valid case.
//! - **Dispersion** is the *population* standard deviation (÷ n, not
//!   n−1): the universe on a day is the whole population, not a sample.
//!   Defined for n ≥ 1 (a single name has dispersion 0).
//! - **IQR / median** use linear-interpolation quantiles (numpy
//!   `interpolation='linear'` default): position `q·(n−1)`, interpolate
//!   between the two straddling order statistics.
//!
//! Leakage contract: every input here is precomputed by the sweep from
//! same-day or trailing state; the `[ML if entry >= T]` annotations in
//! RFC §8 govern downstream use, not construction.

use crate::slices::{aggregate, et};
use arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int32Array, ListArray, RecordBatch, StructArray,
    TimestampNanosecondArray, new_null_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use chrono::{DateTime, NaiveDate, Utc};
use momentum_core::bar::Bar;
use momentum_core::phase0_outputs::market_context_daily_schema;
use std::collections::HashMap;
use std::sync::Arc;

/// Per-security same-day snapshot, precomputed by the sweep.
pub struct UniverseSnapshot {
    pub ret_0930_to_1000: Option<f64>,
    pub ret_0930_to_1030: Option<f64>,
    /// rth_close / rth_open − 1 (intraday day return).
    pub eod_intraday_return: Option<f64>,
    /// close@10:00 > premarket vwap; None when either side missing.
    pub above_premarket_vwap_at_1000: Option<bool>,
    /// |close@10:00 − rth_open| / atr_14d; None without ATR history.
    pub move_vs_atr14_at_1000: Option<f64>,
    pub rth_dollar_volume: Option<f64>,
    pub addv_20d: Option<f64>,
}

/// One index's (SPY/QQQ/IWM) day inputs.
pub struct IndexDay<'a> {
    /// Full session bars (premarket+RTH+AH), sorted by t.
    pub bars: &'a [Bar],
    pub eod_open: Option<f64>,
    pub eod_high: Option<f64>,
    pub eod_low: Option<f64>,
    pub eod_close: Option<f64>,
    pub eod_volume: Option<f64>,
    pub overnight_gap: Option<f64>,
    pub ret_0930_to_1000: Option<f64>,
    pub realized_vol_21d: Option<f64>,
}

pub fn build(
    day: NaiveDate,
    session_close: DateTime<Utc>,
    indices: [Option<IndexDay<'_>>; 3], // [spy, qqq, iwm]; None = index missing that day
    universe: &[UniverseSnapshot],
    vix_close: Option<f64>,
) -> Result<RecordBatch, ArrowError> {
    let schema = market_context_daily_schema();

    // The [SRC] paths cover the full extended session regardless of the
    // RTH close (half-days simply have no bars after 13:00 ET — the
    // aggregation window is a superset of the tape, never a cut).
    let ext_start = et(day, 4, 0);
    let ext_end = et(day, 20, 0);
    debug_assert!(
        session_close <= ext_end,
        "RTH close must precede the 20:00 ET extended-session end"
    );

    let mut filled: HashMap<String, ArrayRef> = HashMap::new();
    let mut put = |name: &str, arr: ArrayRef| {
        filled.insert(name.to_string(), arr);
    };

    // Identity --------------------------------------------------------------
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    put("day", Arc::new(Date32Array::from(vec![(day - epoch).num_days() as i32])));

    // Index columns ----------------------------------------------------------
    for (i, name) in ["spy", "qqq", "iwm"].iter().enumerate() {
        let d = indices[i].as_ref();
        let bars_10m: Vec<Bar> = d
            .map(|x| aggregate(x.bars, ext_start, ext_end, 10))
            .unwrap_or_default();
        let col = format!("{name}_intraday_10m");
        put(
            &col,
            bars_list_array(
                schema.field_with_name(&col).expect("field").data_type(),
                &[bars_10m.as_slice()],
            ),
        );
        put(&format!("{name}_eod_open"), f64_col(d.and_then(|x| x.eod_open)));
        put(&format!("{name}_eod_high"), f64_col(d.and_then(|x| x.eod_high)));
        put(&format!("{name}_eod_low"), f64_col(d.and_then(|x| x.eod_low)));
        put(&format!("{name}_eod_close"), f64_col(d.and_then(|x| x.eod_close)));
        put(&format!("{name}_eod_volume"), f64_col(d.and_then(|x| x.eod_volume)));
        put(&format!("{name}_overnight_gap"), f64_col(d.and_then(|x| x.overnight_gap)));
        put(
            &format!("{name}_ret_0930_to_1000"),
            f64_col(d.and_then(|x| x.ret_0930_to_1000)),
        );
        put(
            &format!("{name}_realized_vol_21d"),
            f64_col(d.and_then(|x| x.realized_vol_21d)),
        );
    }

    // VIX --------------------------------------------------------------------
    // vix_open is intentionally NOT filled (always null): the on-disk VIX
    // source (FRED VIXCLS) is close-only. See module doc.
    put("vix_close", f64_col(vix_close));

    // Breadth ----------------------------------------------------------------
    let ret_1000: Vec<f64> = universe.iter().filter_map(|u| u.ret_0930_to_1000).collect();
    let ret_1030: Vec<f64> = universe.iter().filter_map(|u| u.ret_0930_to_1030).collect();
    let ret_eod: Vec<f64> = universe.iter().filter_map(|u| u.eod_intraday_return).collect();
    let above_vwap: Vec<bool> = universe
        .iter()
        .filter_map(|u| u.above_premarket_vwap_at_1000)
        .collect();
    let move_atr: Vec<f64> = universe.iter().filter_map(|u| u.move_vs_atr14_at_1000).collect();

    put("breadth_pct_universe_green_at_1000", f64_col(share(&ret_1000, |r| r > 0.0)));
    put("breadth_pct_universe_green_at_1030", f64_col(share(&ret_1030, |r| r > 0.0)));
    put(
        "breadth_pct_universe_above_premarket_vwap_at_1000",
        f64_col(share(&above_vwap, |b| b)),
    );
    put(
        "breadth_advance_decline_ratio_at_1000",
        f64_col(advance_decline_ratio(&ret_1000)),
    );
    put(
        "breadth_count_movers_above_5pct_at_1000",
        i32_col(count(&ret_1000, |r| r.abs() >= 0.05)),
    );
    put(
        "breadth_count_movers_above_1atr_at_1000",
        i32_col(count(&move_atr, |m| m >= 1.0)),
    );
    put(
        "breadth_count_movers_above_5pct_eod",
        i32_col(count(&ret_eod, |r| r.abs() >= 0.05)),
    );
    put(
        "breadth_advance_decline_ratio_eod",
        f64_col(advance_decline_ratio(&ret_eod)),
    );
    put(
        "breadth_total_universe_with_bars",
        i32_col(Some(universe.len() as i32)),
    );

    // v2 dispersion / liquidity (§3.7) ----------------------------------------
    put(
        "cross_sectional_ret_dispersion_at_1000",
        f64_col(population_stddev(&ret_1000)),
    );
    put(
        "cross_sectional_ret_dispersion_eod",
        f64_col(population_stddev(&ret_eod)),
    );
    put("cross_sectional_ret_iqr_at_1000", f64_col(iqr(&ret_1000)));
    put("cross_sectional_ret_iqr_eod", f64_col(iqr(&ret_eod)));
    let addv: Vec<f64> = universe.iter().filter_map(|u| u.addv_20d).collect();
    let dvol: Vec<f64> = universe.iter().filter_map(|u| u.rth_dollar_volume).collect();
    put("universe_median_addv_20d", f64_col(median(&addv)));
    put(
        "universe_total_dollar_volume",
        f64_col((!dvol.is_empty()).then(|| dvol.iter().sum())),
    );

    // Assemble: anything unfilled (only vix_open) becomes a null array.
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| {
            filled
                .remove(f.name().as_str())
                .unwrap_or_else(|| new_null_array(f.data_type(), 1))
        })
        .collect();

    RecordBatch::try_new(schema, columns)
}

// -----------------------------------------------------------------------------
// Single-row column helpers
// -----------------------------------------------------------------------------

fn f64_col(v: Option<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(vec![v]))
}

fn i32_col(v: Option<i32>) -> ArrayRef {
    Arc::new(Int32Array::from(vec![v]))
}

// -----------------------------------------------------------------------------
// Breadth / dispersion math
// -----------------------------------------------------------------------------

/// Share of `vals` satisfying `pred`; None when zero valid entries.
fn share<T: Copy>(vals: &[T], pred: impl Fn(T) -> bool) -> Option<f64> {
    (!vals.is_empty())
        .then(|| vals.iter().filter(|&&v| pred(v)).count() as f64 / vals.len() as f64)
}

/// Count of `vals` satisfying `pred`; None when zero valid entries.
fn count(vals: &[f64], pred: impl Fn(f64) -> bool) -> Option<i32> {
    (!vals.is_empty()).then(|| vals.iter().filter(|&&v| pred(v)).count() as i32)
}

/// advancers (> 0) / decliners (< 0); None when zero decliners — which
/// also covers the zero-valid case. Exact zeros count as neither.
fn advance_decline_ratio(rets: &[f64]) -> Option<f64> {
    let adv = rets.iter().filter(|&&r| r > 0.0).count();
    let dec = rets.iter().filter(|&&r| r < 0.0).count();
    (dec > 0).then(|| adv as f64 / dec as f64)
}

/// Population standard deviation (÷ n); None when empty, 0 for n = 1.
fn population_stddev(xs: &[f64]) -> Option<f64> {
    if xs.is_empty() {
        return None;
    }
    let n = xs.len() as f64;
    let mean = xs.iter().sum::<f64>() / n;
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    Some(var.sqrt())
}

/// Linear-interpolation quantile over a SORTED slice (numpy default):
/// position `q·(n−1)`, interpolated between straddling order statistics.
fn quantile_linear(sorted: &[f64], q: f64) -> f64 {
    debug_assert!(!sorted.is_empty());
    let pos = q * (sorted.len() - 1) as f64;
    let lo = pos.floor() as usize;
    let frac = pos - lo as f64;
    if frac == 0.0 || lo + 1 >= sorted.len() {
        sorted[lo]
    } else {
        sorted[lo] + frac * (sorted[lo + 1] - sorted[lo])
    }
}

/// Q3 − Q1 (linear-interpolation quantiles); None when empty.
fn iqr(xs: &[f64]) -> Option<f64> {
    if xs.is_empty() {
        return None;
    }
    let mut sorted = xs.to_vec();
    sorted.sort_by(f64::total_cmp);
    Some(quantile_linear(&sorted, 0.75) - quantile_linear(&sorted, 0.25))
}

/// Median via the 0.5 linear-interpolation quantile (mean of the middle
/// two for even n); None when empty.
fn median(xs: &[f64]) -> Option<f64> {
    if xs.is_empty() {
        return None;
    }
    let mut sorted = xs.to_vec();
    sorted.sort_by(f64::total_cmp);
    Some(quantile_linear(&sorted, 0.5))
}

// -----------------------------------------------------------------------------
// List<Struct> assembly
// -----------------------------------------------------------------------------

/// Build a `List<Struct<t,o,h,l,c,v>>` array (one list per row) matching
/// the schema's exact field layout, from per-row bar slices.
///
/// Re-implemented from `daily_observation` (the helper there is private);
/// keep the two in sync.
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
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Float64Type, Int32Type};

    fn mk_bar(t: DateTime<Utc>, px: f64) -> Bar {
        Bar {
            t,
            open: px,
            high: px + 1.0,
            low: px - 1.0,
            close: px + 0.5,
            volume: 100.0,
        }
    }

    fn snap(
        ret_1000: Option<f64>,
        ret_1030: Option<f64>,
        eod: Option<f64>,
        above_vwap: Option<bool>,
        move_atr: Option<f64>,
        dvol: Option<f64>,
        addv: Option<f64>,
    ) -> UniverseSnapshot {
        UniverseSnapshot {
            ret_0930_to_1000: ret_1000,
            ret_0930_to_1030: ret_1030,
            eod_intraday_return: eod,
            above_premarket_vwap_at_1000: above_vwap,
            move_vs_atr14_at_1000: move_atr,
            rth_dollar_volume: dvol,
            addv_20d: addv,
        }
    }

    fn f64v(batch: &RecordBatch, name: &str) -> f64 {
        batch
            .column_by_name(name)
            .unwrap()
            .as_primitive::<Float64Type>()
            .value(0)
    }

    fn i32v(batch: &RecordBatch, name: &str) -> i32 {
        batch
            .column_by_name(name)
            .unwrap()
            .as_primitive::<Int32Type>()
            .value(0)
    }

    fn is_null(batch: &RecordBatch, name: &str) -> bool {
        batch.column_by_name(name).unwrap().null_count() == 1
    }

    #[test]
    fn schema_matches_and_index_paths_aggregate_full_extended_session() {
        let day = NaiveDate::from_ymd_opt(2021, 3, 16).unwrap();
        let close = et(day, 15, 59);
        // SPY: one premarket bar (04:00) + two bars in the 09:30 bucket
        // → two 10-minute aggregated bars over the 04:00–20:00 window.
        let spy_bars = vec![
            mk_bar(et(day, 4, 0), 400.0),
            mk_bar(et(day, 9, 31), 401.0),
            mk_bar(et(day, 9, 38), 402.0),
        ];
        let spy = IndexDay {
            bars: &spy_bars,
            eod_open: Some(401.0),
            eod_high: Some(403.0),
            eod_low: Some(399.0),
            eod_close: Some(402.5),
            eod_volume: Some(300.0),
            overnight_gap: Some(0.001),
            ret_0930_to_1000: Some(0.002),
            realized_vol_21d: Some(0.15),
        };
        let batch = build(day, close, [Some(spy), None, None], &[], Some(19.4)).unwrap();

        assert_eq!(batch.schema(), market_context_daily_schema());
        assert_eq!(batch.num_rows(), 1);

        // day = days since epoch.
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let d32 = batch
            .column_by_name("day")
            .unwrap()
            .as_primitive::<arrow::datatypes::Date32Type>()
            .value(0);
        assert_eq!(d32 as i64, (day - epoch).num_days());

        // SPY path: two 10m buckets (04:00 and 09:30), bucket starts as t.
        let spy_list = batch.column_by_name("spy_intraday_10m").unwrap();
        let spy_bars_10m = spy_list.as_list::<i32>().value(0);
        let s = spy_bars_10m.as_struct();
        assert_eq!(s.len(), 2);
        let ts = s.column(0).as_primitive::<arrow::datatypes::TimestampNanosecondType>();
        assert_eq!(ts.value(0), et(day, 4, 0).timestamp_nanos_opt().unwrap());
        assert_eq!(ts.value(1), et(day, 9, 30).timestamp_nanos_opt().unwrap());
        // 09:30 bucket merges the 09:31 + 09:38 bars.
        let vol = s.column(5).as_primitive::<Float64Type>();
        assert_eq!(vol.value(1), 200.0);

        // SPY scalar fields.
        assert_eq!(f64v(&batch, "spy_eod_close"), 402.5);
        assert_eq!(f64v(&batch, "spy_overnight_gap"), 0.001);
        assert_eq!(f64v(&batch, "spy_realized_vol_21d"), 0.15);

        // VIX: close from the param, open ALWAYS null (FRED VIXCLS).
        assert_eq!(f64v(&batch, "vix_close"), 19.4);
        assert!(is_null(&batch, "vix_open"));
    }

    #[test]
    fn breadth_and_dispersion_math_on_hand_computed_universe() {
        let day = NaiveDate::from_ymd_opt(2021, 3, 16).unwrap();
        let close = et(day, 15, 59);
        let universe = vec![
            // A: big green mover, above vwap, > 1 ATR.
            snap(Some(0.10), Some(0.02), Some(0.06), Some(true), Some(1.5), Some(1000.0), Some(100.0)),
            // B: red, below vwap, sub-ATR.
            snap(Some(-0.02), Some(-0.01), Some(-0.06), Some(false), Some(0.5), Some(2000.0), Some(300.0)),
            // C: small green at 10:00, flat EOD, sparse fields.
            snap(Some(0.01), None, Some(0.0), None, None, None, Some(200.0)),
            // D: bars but no usable snapshots (counts toward total only).
            snap(None, None, None, None, None, None, None),
        ];
        let batch = build(day, close, [None, None, None], &universe, None).unwrap();

        let eps = 1e-12;
        // Green shares: at 1000 valid {A,B,C}, green {A,C}; at 1030 valid {A,B}.
        assert!((f64v(&batch, "breadth_pct_universe_green_at_1000") - 2.0 / 3.0).abs() < eps);
        assert!((f64v(&batch, "breadth_pct_universe_green_at_1030") - 0.5).abs() < eps);
        // Above-vwap: Some(true) {A} over Some(_) {A,B}.
        assert!(
            (f64v(&batch, "breadth_pct_universe_above_premarket_vwap_at_1000") - 0.5).abs() < eps
        );
        // A/D at 1000: advancers {A,C} / decliners {B} = 2.
        assert_eq!(f64v(&batch, "breadth_advance_decline_ratio_at_1000"), 2.0);
        // A/D EOD: {A} / {B} = 1 (C's exact 0 is neither).
        assert_eq!(f64v(&batch, "breadth_advance_decline_ratio_eod"), 1.0);
        // Movers: |0.10| ≥ 5% at 1000; A's 1.5 ≥ 1 ATR; |±0.06| ≥ 5% EOD.
        assert_eq!(i32v(&batch, "breadth_count_movers_above_5pct_at_1000"), 1);
        assert_eq!(i32v(&batch, "breadth_count_movers_above_1atr_at_1000"), 1);
        assert_eq!(i32v(&batch, "breadth_count_movers_above_5pct_eod"), 2);
        assert_eq!(i32v(&batch, "breadth_total_universe_with_bars"), 4);

        // Dispersion at 1000 over {0.10, −0.02, 0.01}: mean 0.03,
        // population var (0.07² + 0.05² + 0.02²)/3 = 0.0026.
        assert!(
            (f64v(&batch, "cross_sectional_ret_dispersion_at_1000") - 0.0026f64.sqrt()).abs() < eps
        );
        // Dispersion EOD over {0.06, −0.06, 0}: var 0.0024.
        assert!(
            (f64v(&batch, "cross_sectional_ret_dispersion_eod") - 0.0024f64.sqrt()).abs() < eps
        );
        // IQR at 1000: sorted {−0.02, 0.01, 0.10} → Q1 −0.005, Q3 0.055.
        assert!((f64v(&batch, "cross_sectional_ret_iqr_at_1000") - 0.06).abs() < eps);
        // IQR EOD: sorted {−0.06, 0, 0.06} → Q1 −0.03, Q3 0.03.
        assert!((f64v(&batch, "cross_sectional_ret_iqr_eod") - 0.06).abs() < eps);
        // Median ADDV over {100, 200, 300} = 200; total $vol = 3000.
        assert_eq!(f64v(&batch, "universe_median_addv_20d"), 200.0);
        assert_eq!(f64v(&batch, "universe_total_dollar_volume"), 3000.0);
    }

    #[test]
    fn missing_indices_and_empty_universe_produce_empty_lists_and_nulls() {
        let day = NaiveDate::from_ymd_opt(2021, 3, 16).unwrap();
        let close = et(day, 15, 59);
        let batch = build(day, close, [None, None, None], &[], None).unwrap();

        assert_eq!(batch.schema(), market_context_daily_schema());
        assert_eq!(batch.num_rows(), 1);

        for idx in ["spy", "qqq", "iwm"] {
            // [SRC] list column is non-nullable: missing index = empty list.
            let list = batch.column_by_name(&format!("{idx}_intraday_10m")).unwrap();
            assert_eq!(list.null_count(), 0);
            assert_eq!(list.as_list::<i32>().value(0).len(), 0);
            // Scalar index fields are null.
            for name in ["eod_open", "eod_close", "eod_volume", "overnight_gap",
                         "ret_0930_to_1000", "realized_vol_21d"] {
                assert!(is_null(&batch, &format!("{idx}_{name}")), "{idx}_{name}");
            }
        }
        assert!(is_null(&batch, "vix_open"));
        assert!(is_null(&batch, "vix_close"));

        // Zero-valid breadth → null everywhere except the total (a true 0).
        for name in [
            "breadth_pct_universe_green_at_1000",
            "breadth_pct_universe_green_at_1030",
            "breadth_pct_universe_above_premarket_vwap_at_1000",
            "breadth_advance_decline_ratio_at_1000",
            "breadth_count_movers_above_5pct_at_1000",
            "breadth_count_movers_above_1atr_at_1000",
            "breadth_count_movers_above_5pct_eod",
            "breadth_advance_decline_ratio_eod",
            "cross_sectional_ret_dispersion_at_1000",
            "cross_sectional_ret_dispersion_eod",
            "cross_sectional_ret_iqr_at_1000",
            "cross_sectional_ret_iqr_eod",
            "universe_median_addv_20d",
            "universe_total_dollar_volume",
        ] {
            assert!(is_null(&batch, name), "{name} should be null on empty universe");
        }
        assert_eq!(i32v(&batch, "breadth_total_universe_with_bars"), 0);
    }
}
