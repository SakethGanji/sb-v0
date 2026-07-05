#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 1 — META-LABEL L2 CONTROLS: (1) linear-model cross-check, (2) what the model uses.

Two closure checks on the L2 finding ("+2% before -2% 1d" is AUC-0.63 predictable but
it's volatility, not money):
  (1) LINEAR CONTROL — is the AUC-0.63 structure GBM-specific, or does a plain
      (imputed+scaled) L2 logistic regression see it too? If linear matches GBM, the
      structure is simple/monotone (a volatility gradient), not exotic interactions —
      and the GBM null on DIRECTION is not a model-capacity artifact. Also check linear's
      top-decile BARRIER P&L: it should fail to monetize just like the GBM.
  (2) MECHANISM — GBM permutation importance (OOS 2020) + linear |standardized coef|.
      Confirms the model leans on ATR / realized-vol / beta / range (volatility), i.e.
      it predicts barrier RESOLUTION, not up-vs-down.
Same cohort/features/folds/barrier as phase1_metalabel_economic.py. Read-only.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
TRAIN_TEST = [([2016, 2017], 2018), ([2016, 2017, 2018], 2019), ([2016, 2017, 2018, 2019], 2020)]
EMBARGO_DAYS = 2
SEED = 20260705
TARGET_PCT, STOP_PCT = 0.02, -0.02
COSTS_BPS = [0.0, 10.0, 20.0]
LABEL_COL = "first_event_2pct_before_minus_2pct_1d"
IMP_SUBSAMPLE = 25000

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


def barrier_pnl(event, ret1d):
    return np.where(event == "target_first", TARGET_PCT,
           np.where(event == "stop_first", STOP_PCT, ret1d))


def gbm():
    return HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
        min_samples_leaf=200, l2_regularization=1.0, early_stopping=True,
        validation_fraction=0.1, n_iter_no_change=20, random_state=SEED)


def linear():
    return Pipeline([("imp", SimpleImputer(strategy="median")),
                     ("sc", StandardScaler()),
                     ("lr", LogisticRegression(C=1.0, max_iter=1000))])


def main():
    t0 = dt.datetime.now()
    print("=" * 92)
    print("Phase 1 — META-LABEL L2 CONTROLS: linear cross-check + feature-importance mechanism")
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
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + keep(FO_FEATS, fo_h) + [LABEL_COL, "ret_1d"]))
    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner").join(mc, on="day", how="left")
          .filter(pl.col("intraday_ret_0930_to_1000_percentile_today") >= 0.8)
          .with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in feats])
          .with_columns(pl.col("day").dt.year().alias("yr")).sort("day").collect())
    feats = [f for f in feats if df[f].drop_nulls().n_unique() >= 2]

    X = df.select(feats).to_numpy()
    event_all = df[LABEL_COL].to_numpy().astype(object); ret1d_all = df["ret_1d"].to_numpy()
    yr = df["yr"].to_numpy(); day = df["day"].to_numpy().astype("datetime64[D]")
    y = (event_all == "target_first").astype(np.int64)
    valid = np.isin(event_all, ["target_first", "stop_first", "neither"]) & ~np.isnan(ret1d_all)
    print(f"cohort {df.height:,} · {len(feats)} feats\n")

    pg = np.full(df.height, np.nan); pln = np.full(df.height, np.nan); got = np.zeros(df.height, bool)
    last_gbm = last_lin = last_te = None
    for train_yrs, test_yr in TRAIN_TEST:
        te = (yr == test_yr) & valid; tr = np.isin(yr, train_yrs) & valid
        if tr.any():
            tr = tr & (day <= day[tr].max() - np.timedelta64(EMBARGO_DAYS, "D"))
        if not tr.any() or not te.any():
            continue
        g = gbm().fit(X[tr], y[tr]); pg[te] = g.predict_proba(X[te])[:, 1]
        L = linear().fit(X[tr], y[tr]); pln[te] = L.predict_proba(X[te])[:, 1]
        got[te] = True
        last_gbm, last_lin, last_te = g, L, te

    oos = got & valid
    auc_g = roc_auc_score(y[oos], pg[oos]); auc_l = roc_auc_score(y[oos], pln[oos])
    print("── (1) LINEAR CONTROL: OOS AUC on L2 (target-first) ──")
    print(f"   GBM (HistGradientBoosting) : {auc_g:.4f}")
    print(f"   Linear (L2 logistic)       : {auc_l:.4f}")
    print(f"   -> {'linear MATCHES GBM: structure is a simple volatility gradient, not GBM-only interactions' if auc_l > 0.58 else 'linear below GBM: some nonlinearity'}\n")

    # linear top-decile barrier P&L (does the linear signal monetize either?)
    pnl = barrier_pnl(event_all[oos], ret1d_all[oos])
    for nm, sc in [("GBM", pg[oos]), ("Linear", pln[oos])]:
        dec = np.clip((np.searchsorted(np.sort(sc), sc, side="right") - 1) * 10 // len(sc), 0, 9)
        g0 = pnl[dec == 9].mean() * 1e4
        print(f"   {nm} top-decile barrier P&L: gross {g0:+.1f} bps · " +
              " · ".join(f"net@{int(c)}bp {g0-c:+.1f}" for c in COSTS_BPS))
    print()

    # ── (2) MECHANISM: GBM permutation importance on 2020 OOS + linear |coef| ──
    print("── (2) MECHANISM: what the model uses ──")
    te = last_te
    idx = np.where(te)[0]
    rng = np.random.default_rng(SEED)
    if len(idx) > IMP_SUBSAMPLE:
        idx = rng.choice(idx, IMP_SUBSAMPLE, replace=False)
    pi = permutation_importance(last_gbm, X[idx], y[idx], scoring="roc_auc",
                                n_repeats=4, random_state=SEED, n_jobs=-1)
    order = np.argsort(pi.importances_mean)[::-1][:15]
    print(f"  GBM permutation importance (Δ AUC when shuffled, OOS 2020, top 15):")
    for i in order:
        print(f"     {feats[i]:<44}{pi.importances_mean[i]:+.4f}")

    coef = last_lin.named_steps["lr"].coef_[0]  # standardized (scaler in pipeline)
    lo = np.argsort(np.abs(coef))[::-1][:15]
    print(f"\n  Linear |standardized coef| (top 15, sign = toward target-first):")
    for i in lo:
        print(f"     {feats[i]:<44}{coef[i]:+.3f}")

    print("\n--- verdict ---")
    print("If linear ≈ GBM AUC and BOTH top-deciles fail to monetize, and the important")
    print("features are ATR/realized-vol/beta/range -> confirmed: the signal is a VOLATILITY")
    print("gradient (predicts barrier resolution), model-agnostic, not directional edge.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
