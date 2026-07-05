#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 1 — BARRIER THRESHOLD SWEEP: is the AUC-0.63 "will it move" signal a genuine
          stock-NORMALIZED volatility signal, or an artifact of the fixed ±2% choice?

Critique (correct): ±2% means different things per stock (normal for NVDA, huge for a
utility), so the fixed-% AUC may partly just be "which stocks are always volatile"
(a static, trivial fact — ATR already tells you that). ATR-NORMALIZED barriers ask the
harder question: "will this stock move more than ITS OWN typical range today?" =
volatility EXPANSION. If the AUC survives ATR-normalization, the signal is genuine and
stock-normalized; if it collapses vs fixed-%, the fixed number was static vol ranking.

Holds everything constant (same cohort/features/folds, horizon = 1d) and varies ONLY the
barrier, built from first_cross_up/down_{X}_1d TIMING (minute of first cross; 0 = never):
  fixed-pct: ±1% ±2% ±3% ±5%   ·   ATR:  ±0.5 ±1 ±2 ±3 ATR
Two AUCs each:
  AUC_move  = predict "resolves to EITHER barrier vs neither"  (pure magnitude, direction-free)
  AUC_L2    = predict "target_first (+X before -X)"            (the original 0.63 metric)
Validates the constructed ±2% against the materialized first_event column.
Read-only. Cohort: top-quintile momentum, deployable/liquid, enter 10:00.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
TRAIN_TEST = [([2016, 2017], 2018), ([2016, 2017, 2018], 2019), ([2016, 2017, 2018, 2019], 2020)]
EMBARGO_DAYS = 2
SEED = 20260705
# (label, up_col, down_col) — symmetric barriers at horizon 1d
BARRIERS = [
    ("pct ±1%",  "first_cross_up_1pct_1d",  "first_cross_down_1pct_1d"),
    ("pct ±2%",  "first_cross_up_2pct_1d",  "first_cross_down_2pct_1d"),
    ("pct ±3%",  "first_cross_up_3pct_1d",  "first_cross_down_3pct_1d"),
    ("pct ±5%",  "first_cross_up_5pct_1d",  "first_cross_down_5pct_1d"),
    ("atr ±0.5", "first_cross_up_0_5atr_1d", "first_cross_down_0_5atr_1d"),
    ("atr ±1",   "first_cross_up_1atr_1d",  "first_cross_down_1atr_1d"),
    ("atr ±2",   "first_cross_up_2atr_1d",  "first_cross_down_2atr_1d"),
    ("atr ±3",   "first_cross_up_3atr_1d",  "first_cross_down_3atr_1d"),
]

# original metalabel feature set (94) — so constructed ±2% reproduces the 0.63 anchor
DO_FEATS = ["intraday_ret_0930_to_0940","intraday_ret_0930_to_0950","intraday_ret_0930_to_1000",
    "intraday_volume_0930_to_1000","intraday_dollar_volume_0930_to_1000","intraday_first_30m_high_return",
    "intraday_first_30m_low_return","intraday_ret_from_first_30m_high_to_1000",
    "intraday_minutes_since_first_30m_high_at_1000","intraday_first_15m_volume_share_of_first_30m",
    "intraday_ret_0930_to_1000_rank_today","intraday_ret_0930_to_1000_percentile_today",
    "intraday_dollar_volume_0930_to_1000_rank_today","premarket_volume_rank_today",
    "premarket_dollar_volume_rank_today","overnight_gap_rank_today","addv_20d_rank_today",
    "realized_vol_21d_rank_today","premarket_volume","premarket_dollar_volume","overnight_gap",
    "prior_day_last_30m_return","prior_day_last_30m_volume_share","atr_5d","atr_14d","atr_42d",
    "realized_vol_21d","yang_zhang_vol_5d","yang_zhang_vol_14d","yang_zhang_vol_21d","yang_zhang_vol_42d",
    "adv_5d","adv_20d","adv_60d","addv_5d","addv_20d","addv_60d","beta_spy_60d","beta_qqq_60d",
    "beta_iwm_60d","days_to_next_known_earnings","days_since_last_earnings",
    "signal_concentration_percentile_today","signal_concentration_hhi_today",
    "premarket_volume_vs_20d_median","days_since_last_5pct_move","days_since_last_10pct_move",
    "days_since_last_20pct_move","consecutive_up_days_close_to_close","days_since_first_bar",
    "signal_first_in_5d","signal_first_in_10d","signal_first_in_20d","bar_count_premarket","bar_count_first_30m"]
FO_FEATS = ["pre_entry_ret_from_open","pre_entry_vwap_from_open","pre_entry_ret_from_high",
    "pre_entry_ret_from_low","pre_entry_minutes_since_high","pre_entry_minutes_since_low",
    "pre_entry_ret_rank_today","pre_entry_ret_percentile_today","pre_entry_dollar_volume_rank_today",
    "entry_bar_upper_wick_pct","entry_bar_lower_wick_pct","entry_slippage_proxy_bps","entry_1m_range",
    "entry_range_vs_atr_14d","entry_dollar_volume_vs_addv_20d","cumulative_volume_to_entry",
    "cumulative_dollar_volume_to_entry"]
CL_FEATS = ["market_cap","market_cap_rank_today","market_cap_percentile_today",
    "volatility_percentile_today","days_since_ipo_or_first_bar"]
MC_FEATS = ["vix_open","spy_overnight_gap","qqq_overnight_gap","iwm_overnight_gap","spy_ret_0930_to_1000",
    "qqq_ret_0930_to_1000","iwm_ret_0930_to_1000","spy_realized_vol_21d","qqq_realized_vol_21d",
    "iwm_realized_vol_21d","breadth_pct_universe_green_at_1000","breadth_pct_universe_above_premarket_vwap_at_1000",
    "breadth_advance_decline_ratio_at_1000","breadth_count_movers_above_5pct_at_1000",
    "breadth_count_movers_above_1atr_at_1000","cross_sectional_ret_dispersion_at_1000",
    "cross_sectional_ret_iqr_at_1000","universe_median_addv_20d","universe_total_dollar_volume"]


def wf(t):
    out = []
    for f in sorted(glob.glob(f"data/outputs/{t}/*.parquet")):
        try:
            d = dt.date.fromisoformat(Path(f).stem)
        except ValueError:
            continue
        if EXP_START <= d <= EXP_END:
            out.append(f)
    return out


def keep(c, h):
    return [x for x in c if x in h]


def gbm():
    return HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
        min_samples_leaf=200, l2_regularization=1.0, early_stopping=True,
        validation_fraction=0.1, n_iter_no_change=20, random_state=SEED)


def walk_auc(X, y, valid, yr, day):
    pr = np.full(len(y), np.nan)
    for tr_y, te_y in TRAIN_TEST:
        te = (yr == te_y) & valid; trn = np.isin(yr, tr_y) & valid
        if trn.any():
            trn = trn & (day <= day[trn].max() - np.timedelta64(EMBARGO_DAYS, "D"))
        if not trn.any() or not te.any() or len(np.unique(y[trn])) < 2:
            continue
        pr[te] = gbm().fit(X[trn], y[trn]).predict_proba(X[te])[:, 1]
    m = valid & ~np.isnan(pr)
    return roc_auc_score(y[m], pr[m]) if len(np.unique(y[m])) > 1 else np.nan


def main():
    t0 = dt.datetime.now()
    print("=" * 92)
    print("Phase 1 — BARRIER THRESHOLD SWEEP (is 0.63 a genuine stock-normalized vol signal?)")
    print("=" * 92)
    do_h = set(pl.scan_parquet(wf("daily_observation")).collect_schema().names())
    fo_h = set(pl.scan_parquet(wf("forward_outcomes")).collect_schema().names())
    cl_h = set(pl.scan_parquet(wf("security_classification_daily")).collect_schema().names())
    mc_h = set(pl.scan_parquet(wf("market_context_daily")).collect_schema().names())
    feats = keep(DO_FEATS, do_h) + keep(FO_FEATS, fo_h) + keep(CL_FEATS, cl_h) + keep(MC_FEATS, mc_h)
    cross_cols = sorted({c for _, u, d in BARRIERS for c in (u, d)} & fo_h)

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select(["day", "security_id"] + keep(CL_FEATS, cl_h)))
    do = pl.scan_parquet(wf("daily_observation")).select(["day", "security_id"] + keep(DO_FEATS, do_h))
    mc = pl.scan_parquet(wf("market_context_daily")).select(["day"] + keep(MC_FEATS, mc_h))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + keep(FO_FEATS, fo_h) + cross_cols
                  + ["first_event_2pct_before_minus_2pct_1d"]))
    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner").join(mc, on="day", how="left")
          .filter(pl.col("intraday_ret_0930_to_1000_percentile_today") >= 0.8)
          .with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in feats])
          .with_columns(pl.col("day").dt.year().alias("yr")).sort("day").collect())
    feats = [f for f in feats if df[f].drop_nulls().n_unique() >= 2]
    X = df.select(feats).to_numpy()
    yr = df["yr"].to_numpy(); day = df["day"].to_numpy().astype("datetime64[D]")
    print(f"cohort {df.height:,} · {len(feats)} feats · horizon 1d · fixed cohort/features/folds\n")

    # sanity: constructed ±2% vs materialized first_event
    up = df["first_cross_up_2pct_1d"].to_numpy(); dn = df["first_cross_down_2pct_1d"].to_numpy()
    tf = (up > 0) & ((dn == 0) | (up < dn)); sf = (dn > 0) & ((up == 0) | (dn < up))
    ev = df["first_event_2pct_before_minus_2pct_1d"].to_numpy().astype(object)
    agree = np.mean(tf[ev != None] == (ev[ev != None] == "target_first"))
    print(f"[sanity] constructed ±2% target_first vs materialized first_event: {100*agree:.1f}% agree "
          f"(constructed tgt {100*tf.mean():.1f}% / stop {100*sf.mean():.1f}%)\n")

    print(f"  {'barrier':<10}{'base move%':>11}{'base tgt%':>11}{'AUC_move':>11}{'AUC_L2':>10}")
    print(f"  {'':<10}{'(resolves)':>11}{'(+X 1st)':>11}{'(magnitude)':>11}{'(orig)':>10}")
    results = []
    for name, uc, dc in BARRIERS:
        if uc not in df.columns or dc not in df.columns:
            continue
        up = df[uc].to_numpy(); dn = df[dc].to_numpy()
        valid = ~np.isnan(up) & ~np.isnan(dn)
        uh = (up > 0); dh = (dn > 0)
        moved = (uh | dh).astype(np.int64)          # resolves to a barrier at all
        target_first = (uh & ((~dh) | (up < dn))).astype(np.int64)
        auc_move = walk_auc(X, moved, valid, yr, day)
        auc_l2 = walk_auc(X, target_first, valid, yr, day)
        bm = moved[valid].mean() * 100; bt = target_first[valid].mean() * 100
        results.append((name, bm, bt, auc_move, auc_l2))
        print(f"  {name:<10}{bm:>10.1f}%{bt:>10.1f}%{auc_move:>11.4f}{auc_l2:>10.4f}")

    print("\n--- reading ---")
    print("AUC_move ~0.63 for FIXED-% but COLLAPSING for ATR -> the 0.63 was largely static")
    print("  'which stocks are always volatile' (ATR barrier removes that, model can't cheat).")
    print("AUC_move staying HIGH for ATR too -> genuine stock-normalized vol-EXPANSION signal")
    print("  (predicts an unusually large move vs the stock's own baseline) — the stronger result.")
    print("Either way AUC_L2 ~ AUC_move (direction adds ~nothing) — consistent with the coin-flip")
    print("direction finding; this sweep only sharpens what the MAGNITUDE signal is.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
