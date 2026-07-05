#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 1 — DIRECT META-LABELING TEST (accept/reject, not return-ranking).

The joint probe tested RETURN RANKING (rank-IC on continuous excess return). Meta-
labeling is a different objective: a conditional BINARY classifier that, given the
primary signal fired, decides whether to TAKE the trade. It can add value even when
ranking is weak — it only needs to identify a reliable SUBSET to accept/reject.
This tests that objective directly, with trade-quality labels.

PRIMARY signal (fires candidate trades): top-quintile morning momentum
  (intraday_ret_0930_to_1000_percentile_today >= 0.8), enter 10:00, deployable/liquid.
META-LABELS (binary trade quality), tested separately:
  L1  beat SPY 1d      : ret_1d_excess_spy > 0
  L2  +2% before -2%   : first_event_2pct_before_minus_2pct_1d == 'target_first'   (embeds an exit)
  L3  beat SPY 5d      : ret_5d_excess_spy > 0
MODEL: HistGradientBoostingClassifier on the 95 pre-entry features (native NaN),
  expanding walk-forward by year, 2-day embargo.
THE TESTS THAT SETTLE IT:
  - OOS ROC-AUC vs a LABEL-permutation null (shuffle label within train days, refit).
  - ECONOMIC LIFT: bin OOS trades by predicted P(good) into deciles; report realized
    win-rate + mean excess (bps) per decile. A working meta-label => top decile >> base,
    MONOTONIC lift, and STABLE across years. Also the "take top-K%" gated expectancy.
Read-only; writes data/phase1_analysis/metalabel.parquet.
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
TRAIN_TEST = [([2016, 2017], 2018), ([2016, 2017, 2018], 2019),
              ([2016, 2017, 2018, 2019], 2020)]
EMBARGO_DAYS = 2
N_PERM = 6
SEED = 20260705
OUT = Path("data/phase1_analysis")

# --- strict pre-10:00 feature set (same universe as the joint probe) ---
DO_FEATS = [
    "intraday_ret_0930_to_0940", "intraday_ret_0930_to_0950", "intraday_ret_0930_to_1000",
    "intraday_volume_0930_to_1000", "intraday_dollar_volume_0930_to_1000",
    "intraday_first_30m_high_return", "intraday_first_30m_low_return",
    "intraday_ret_from_first_30m_high_to_1000", "intraday_minutes_since_first_30m_high_at_1000",
    "intraday_first_15m_volume_share_of_first_30m",
    "intraday_ret_0930_to_1000_rank_today", "intraday_ret_0930_to_1000_percentile_today",
    "intraday_dollar_volume_0930_to_1000_rank_today", "premarket_volume_rank_today",
    "premarket_dollar_volume_rank_today", "overnight_gap_rank_today", "addv_20d_rank_today",
    "realized_vol_21d_rank_today", "premarket_volume", "premarket_dollar_volume",
    "overnight_gap", "prior_day_last_30m_return", "prior_day_last_30m_volume_share",
    "atr_5d", "atr_14d", "atr_42d", "realized_vol_21d",
    "yang_zhang_vol_5d", "yang_zhang_vol_14d", "yang_zhang_vol_21d", "yang_zhang_vol_42d",
    "adv_5d", "adv_20d", "adv_60d", "addv_5d", "addv_20d", "addv_60d",
    "beta_spy_60d", "beta_qqq_60d", "beta_iwm_60d",
    "days_to_next_known_earnings", "days_since_last_earnings",
    "signal_concentration_percentile_today", "signal_concentration_hhi_today",
    "premarket_volume_vs_20d_median", "days_since_last_5pct_move",
    "days_since_last_10pct_move", "days_since_last_20pct_move",
    "consecutive_up_days_close_to_close", "days_since_first_bar",
    "signal_first_in_5d", "signal_first_in_10d", "signal_first_in_20d",
    "bar_count_premarket", "bar_count_first_30m",
]
FO_FEATS = [
    "pre_entry_ret_from_open", "pre_entry_vwap_from_open", "pre_entry_ret_from_high",
    "pre_entry_ret_from_low", "pre_entry_minutes_since_high", "pre_entry_minutes_since_low",
    "pre_entry_ret_rank_today", "pre_entry_ret_percentile_today",
    "pre_entry_dollar_volume_rank_today", "entry_bar_upper_wick_pct",
    "entry_bar_lower_wick_pct", "entry_slippage_proxy_bps", "entry_1m_range",
    "entry_range_vs_atr_14d", "entry_dollar_volume_vs_addv_20d",
    "cumulative_volume_to_entry", "cumulative_dollar_volume_to_entry",
]
CL_FEATS = ["market_cap", "market_cap_rank_today", "market_cap_percentile_today",
            "volatility_percentile_today", "days_since_ipo_or_first_bar"]
MC_FEATS = ["vix_open", "spy_overnight_gap", "qqq_overnight_gap", "iwm_overnight_gap",
            "spy_ret_0930_to_1000", "qqq_ret_0930_to_1000", "iwm_ret_0930_to_1000",
            "spy_realized_vol_21d", "qqq_realized_vol_21d", "iwm_realized_vol_21d",
            "breadth_pct_universe_green_at_1000", "breadth_pct_universe_above_premarket_vwap_at_1000",
            "breadth_advance_decline_ratio_at_1000", "breadth_count_movers_above_5pct_at_1000",
            "breadth_count_movers_above_1atr_at_1000", "cross_sectional_ret_dispersion_at_1000",
            "cross_sectional_ret_iqr_at_1000", "universe_median_addv_20d",
            "universe_total_dollar_volume"]

LABELS = [
    ("L1 beat-SPY 1d", "ret_1d_excess_spy", "gt0"),
    ("L2 +2%bef-2% 1d", "first_event_2pct_before_minus_2pct_1d", "target"),
    ("L3 beat-SPY 5d", "ret_5d_excess_spy", "gt0"),
]
# excess-return column used to price each label's economic lift (bps)
LIFT_RET = {"L1 beat-SPY 1d": "ret_1d_excess_spy", "L2 +2%bef-2% 1d": "ret_1d_excess_spy",
            "L3 beat-SPY 5d": "ret_5d_excess_spy"}


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


def walk_forward_proba(X, ylab, day_arr, yr_arr, valid, perm_rng=None, seed=SEED):
    tds, prob, lab, tey = [], [], [], []
    for train_yrs, test_yr in TRAIN_TEST:
        te = (yr_arr == test_yr) & valid
        tr = np.isin(yr_arr, train_yrs) & valid
        if tr.any():
            emb = day_arr[tr].max() - np.timedelta64(EMBARGO_DAYS, "D")
            tr = tr & (day_arr <= emb)
        if not tr.any() or not te.any() or len(np.unique(ylab[tr])) < 2:
            continue
        ytr = ylab[tr].copy()
        if perm_rng is not None:
            dtr = day_arr[tr]; o = np.argsort(dtr, kind="stable")
            u, idx = np.unique(dtr[o], return_index=True); ends = np.append(idx[1:], len(o))
            ysh = ytr[o].copy()
            for i in range(len(u)):
                ysh[idx[i]:ends[i]] = perm_rng.permutation(ysh[idx[i]:ends[i]])
            ytr = np.empty_like(ytr); ytr[o] = ysh
        m = HistGradientBoostingClassifier(
            max_iter=300, learning_rate=0.05, max_leaf_nodes=31, min_samples_leaf=200,
            l2_regularization=1.0, early_stopping=True, validation_fraction=0.1,
            n_iter_no_change=20, random_state=seed)
        m.fit(X[tr], ytr)
        p = m.predict_proba(X[te])[:, 1]
        tds.append(day_arr[te]); prob.append(p); lab.append(ylab[te])
        tey.append(te)
    if not tds:
        return None
    return np.concatenate(tds), np.concatenate(prob), np.concatenate(lab), np.concatenate([np.where(t)[0] for t in tey])


def main():
    t0 = dt.datetime.now(); OUT.mkdir(parents=True, exist_ok=True)
    print("=" * 92)
    print("Phase 1 — DIRECT META-LABELING TEST (accept/reject) · primary: top-quintile momentum")
    print("=" * 92)
    do_h = set(pl.scan_parquet(wf("daily_observation")).collect_schema().names())
    fo_h = set(pl.scan_parquet(wf("forward_outcomes")).collect_schema().names())
    cl_h = set(pl.scan_parquet(wf("security_classification_daily")).collect_schema().names())
    mc_h = set(pl.scan_parquet(wf("market_context_daily")).collect_schema().names())
    feats = keep(DO_FEATS, do_h) + keep(FO_FEATS, fo_h) + keep(CL_FEATS, cl_h) + keep(MC_FEATS, mc_h)

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select(["day", "security_id"] + keep(CL_FEATS, cl_h)))
    do = pl.scan_parquet(wf("daily_observation")).select(["day", "security_id"] + keep(DO_FEATS, do_h))
    mc = pl.scan_parquet(wf("market_context_daily")).select(["day"] + keep(MC_FEATS, mc_h))
    lab_cols = list({c for _, c, _ in LABELS} | set(LIFT_RET.values()))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + keep(FO_FEATS, fo_h) + lab_cols))
    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner").join(mc, on="day", how="left")
          # PRIMARY signal: top-quintile morning momentum
          .filter(pl.col("intraday_ret_0930_to_1000_percentile_today") >= 0.8)
          .with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in feats])
          .with_columns(pl.col("day").dt.year().alias("yr")).sort("day").collect())
    feats = [f for f in feats if df[f].drop_nulls().n_unique() >= 2]
    X = df.select(feats).to_numpy()
    day_arr = df["day"].to_numpy().astype("datetime64[D]"); yr_arr = df["yr"].to_numpy()
    print(f"primary cohort (top-quintile momentum): {df.height:,} trades · {len(feats)} features")
    print(f"walk-forward {[f'{a[-1]+1 if False else b}' for a,b in TRAIN_TEST]} test yrs 2018-2020 · embargo {EMBARGO_DAYS}d\n")

    recs = []
    for name, col, kind in LABELS:
        if col not in df.columns:
            continue
        if kind == "gt0":
            y = (df[col].to_numpy() > 0).astype(np.int64); valid = ~np.isnan(df[col].to_numpy())
        else:  # target_first vs everything resolved (target/stop/neither)
            s = df[col].to_numpy().astype(object)
            valid = np.isin(s, ["target_first", "stop_first", "neither"])
            y = (s == "target_first").astype(np.int64)
        retc = df[LIFT_RET[name]].to_numpy()
        valid = valid & ~np.isnan(retc)

        res = walk_forward_proba(X, y, day_arr, yr_arr, valid, seed=SEED)
        if res is None:
            print(f"### {name}: no folds\n"); continue
        td, prob, lab, ridx = res
        ret_oos = retc[ridx]; yr_oos = yr_arr[ridx]
        auc = roc_auc_score(lab, prob)
        base = lab.mean()
        # permutation null on AUC
        nulls = []
        for k in range(N_PERM):
            rr = walk_forward_proba(X, y, day_arr, yr_arr, valid,
                                    perm_rng=np.random.default_rng(SEED + 500 + k), seed=SEED)
            if rr is not None:
                nulls.append(roc_auc_score(rr[2], rr[1]))
        null95 = float(np.quantile(nulls, 0.95)) if nulls else np.nan
        beats = auc > null95

        # decile lift
        order = np.argsort(prob)
        dec = np.floor(np.argsort(order).argsort() * 10 / len(prob)).astype(int)  # 0..9 by prob
        dec = np.clip((np.searchsorted(np.sort(prob), prob, side="right") - 1) * 10 // len(prob), 0, 9)
        print(f"### {name}  (base rate {100*base:.1f}%) ###")
        print(f"  OOS AUC {auc:.4f}  | perm-null 95pct {null95:.4f}  -> {'BEATS ✓' if beats else 'inside null ✗'}")
        print(f"  {'score decile':<14}{'n':>8}{'win%':>8}{'mean excess bps':>18}")
        wins = []
        for d in range(10):
            mm = dec == d
            if mm.sum() == 0:
                continue
            w = 100 * lab[mm].mean(); mb = ret_oos[mm].mean() * 1e4
            wins.append(w)
            print(f"  {'D'+str(d)+(' (top)' if d==9 else ' (bot)' if d==0 else ''):<14}"
                  f"{mm.sum():>8,}{w:>7.1f}%{mb:>17.1f}")
        top = dec == 9; bot = dec == 0
        lift = 100*lab[top].mean() - 100*base
        mono = all(wins[i] <= wins[i+1] + 3 for i in range(len(wins)-1))  # ~monotone within 3pp noise
        print(f"  top-decile lift vs base: {lift:+.1f} pp | top-dec mean excess "
              f"{ret_oos[top].mean()*1e4:+.1f} bps vs base {ret_oos.mean()*1e4:+.1f} bps"
              f" | monotone(±3pp): {mono}")
        # by-year top-decile stability
        print(f"  by-year top-decile win% vs base:")
        for yv in [2018, 2019, 2020]:
            ym = (yr_oos == yv)
            if ym.sum() == 0:
                continue
            tw = 100*lab[ym & top].mean() if (ym & top).sum() else np.nan
            bw = 100*lab[ym].mean()
            print(f"     {yv}: top {tw:>5.1f}%  base {bw:>5.1f}%  lift {tw-bw:+.1f}pp")
        print()
        recs.append(dict(label=name, base_rate=float(base), auc=float(auc), null95=null95,
                         beats_null=bool(beats), top_decile_lift_pp=float(lift),
                         top_decile_excess_bps=float(ret_oos[top].mean()*1e4),
                         base_excess_bps=float(ret_oos.mean()*1e4), monotone=bool(mono)))

    if recs:
        pl.DataFrame(recs).write_parquet(OUT / "metalabel.parquet")
    print("--- verdict ---")
    print("A working meta-label => AUC beats null AND top-decile win% >> base AND lift is")
    print("MONOTONIC across deciles AND STABLE across 2018-2020 AND top-decile excess clears")
    print("cost. Faint/unstable/non-monotone lift that doesn't clear cost => the accept/reject")
    print("objective is also null on this feature set (tested directly, not inferred).")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
