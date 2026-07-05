#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 1 — VOL-FORECAST NON-TRIVIALITY TEST: does our volatility signal beat the naive
          vol forecast that IMPLIED VOL roughly tracks?

The one real signal we found is stock-normalized volatility MAGNITUDE. It is only
economically interesting (for an options/straddle cycle-2) if it forecasts realized vol
BETTER than the naive baselines the option market already prices — because to make money
on a straddle you must beat implied vol, and IV ≈ (static vol + today's known info).
We have no IV data, so we test the PRECONDITION: does a full ML model beat an "informed
naive" vol forecast built only from what's knowable/priced at 10:00?

Target: realized 1d RANGE = max_runup_1d - max_drawdown_1d (path movement, straddle-relevant).
Models (expanding walk-forward, OOS, per-day rank-IC = Spearman within day, day-clustered):
  ATR-only   : atr_14d as a direct predictor (the naive-est forecast)
  STATIC-VOL : historical vol only (atr/RV/yang-zhang) — what IV is mostly priced on
  INFORMED   : STATIC-VOL + today's early observables (gap, opening range, earnings prox,
               morning return) — ~ what an informed vol trader / IV knows at 10:00
  FULL-ML    : all 94 features
DECISIVE READ: FULL-ML >> INFORMED (large, stable lift) -> genuine edge over IV-priced info
  -> path 2 (options) has a real basis. FULL-ML ~ INFORMED -> the signal is just vol
  persistence already priced into every option -> no options edge; data is exhausted.

Universe: deployable/liquid, enter 10:00, 2016-2020. Read-only.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
TRAIN_TEST = [([2016, 2017], 2018), ([2016, 2017, 2018], 2019), ([2016, 2017, 2018, 2019], 2020)]
EMBARGO_DAYS = 2
N_BOOT = 2000
SEED = 20260705

STATIC_VOL = ["atr_5d", "atr_14d", "atr_42d", "realized_vol_21d",
              "yang_zhang_vol_5d", "yang_zhang_vol_14d", "yang_zhang_vol_21d", "yang_zhang_vol_42d"]
EARLY_EXTRA = ["overnight_gap", "intraday_first_30m_high_return", "intraday_first_30m_low_return",
               "intraday_ret_0930_to_1000", "entry_1m_range", "entry_range_vs_atr_14d",
               "premarket_volume_vs_20d_median", "days_to_next_known_earnings",
               "days_since_last_earnings", "realized_vol_21d_rank_today", "vix_open"]
# FULL = STATIC_VOL + EARLY_EXTRA + the rest of the standard feature set
DO_MORE = ["intraday_ret_0930_to_0940", "intraday_ret_0930_to_0950", "intraday_volume_0930_to_1000",
    "intraday_dollar_volume_0930_to_1000", "intraday_ret_from_first_30m_high_to_1000",
    "intraday_first_15m_volume_share_of_first_30m", "intraday_ret_0930_to_1000_rank_today",
    "intraday_ret_0930_to_1000_percentile_today", "overnight_gap_rank_today", "addv_20d_rank_today",
    "premarket_volume", "premarket_dollar_volume", "prior_day_last_30m_return",
    "prior_day_last_30m_volume_share", "adv_20d", "addv_20d", "addv_60d",
    "beta_spy_60d", "beta_qqq_60d", "beta_iwm_60d", "signal_concentration_percentile_today",
    "signal_concentration_hhi_today", "days_since_last_5pct_move", "days_since_last_10pct_move",
    "consecutive_up_days_close_to_close", "bar_count_premarket", "bar_count_first_30m"]
FO_MORE = ["pre_entry_ret_from_open", "pre_entry_ret_from_high", "pre_entry_ret_from_low",
    "pre_entry_ret_rank_today", "entry_bar_upper_wick_pct", "entry_bar_lower_wick_pct",
    "entry_slippage_proxy_bps", "entry_dollar_volume_vs_addv_20d", "cumulative_dollar_volume_to_entry"]
CL_MORE = ["market_cap", "market_cap_rank_today", "volatility_percentile_today", "days_since_ipo_or_first_bar"]
MC_MORE = ["spy_overnight_gap", "spy_ret_0930_to_1000", "spy_realized_vol_21d",
    "breadth_pct_universe_green_at_1000", "cross_sectional_ret_dispersion_at_1000",
    "cross_sectional_ret_iqr_at_1000", "breadth_count_movers_above_1atr_at_1000",
    "universe_total_dollar_volume"]


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


def daily_rank_ic(day, pred, act):
    order = np.argsort(day, kind="stable")
    d, p, a = day[order], pred[order], act[order]
    u, idx = np.unique(d, return_index=True); ends = np.append(idx[1:], len(d))
    ics = []
    for i in range(len(u)):
        s, e = idx[i], ends[i]
        if e - s < 10:
            continue
        pr = np.argsort(np.argsort(p[s:e])).astype(float)
        ar = np.argsort(np.argsort(a[s:e])).astype(float)
        pr -= pr.mean(); ar -= ar.mean()
        den = np.sqrt((pr*pr).sum() * (ar*ar).sum())
        if den > 0:
            ics.append(float((pr*ar).sum()/den))
    return np.array(ics)


def boot_ci(x, rng):
    if len(x) == 0:
        return (np.nan, np.nan)
    bm = x[rng.integers(0, len(x), size=(N_BOOT, len(x)))].mean(axis=1)
    return float(np.quantile(bm, 0.025)), float(np.quantile(bm, 0.975))


def walk_pred(X, y, day, yr, valid):
    pr = np.full(len(y), np.nan)
    for tr_y, te_y in TRAIN_TEST:
        te = (yr == te_y) & valid; trn = np.isin(yr, tr_y) & valid
        if trn.any():
            trn = trn & (day <= day[trn].max() - np.timedelta64(EMBARGO_DAYS, "D"))
        if not trn.any() or not te.any():
            continue
        m = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
            min_samples_leaf=200, l2_regularization=1.0, early_stopping=True,
            validation_fraction=0.1, n_iter_no_change=20, random_state=SEED)
        m.fit(X[trn], y[trn]); pr[te] = m.predict(X[te])
    return pr


def main():
    t0 = dt.datetime.now()
    print("=" * 92)
    print("Phase 1 — VOL-FORECAST NON-TRIVIALITY (does our vol signal beat the naive/IV forecast?)")
    print("=" * 92)
    do_h = set(pl.scan_parquet(wf("daily_observation")).collect_schema().names())
    fo_h = set(pl.scan_parquet(wf("forward_outcomes")).collect_schema().names())
    cl_h = set(pl.scan_parquet(wf("security_classification_daily")).collect_schema().names())
    mc_h = set(pl.scan_parquet(wf("market_context_daily")).collect_schema().names())

    do_feats = keep(STATIC_VOL + [f for f in EARLY_EXTRA if f not in ("days_to_next_known_earnings",)] + DO_MORE
                    + ["days_to_next_known_earnings"], do_h)
    fo_feats = keep(["entry_1m_range", "entry_range_vs_atr_14d"] + FO_MORE, fo_h)
    cl_feats = keep(CL_MORE, cl_h)
    mc_feats = keep(["vix_open"] + MC_MORE, mc_h)

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select(["day", "security_id"] + cl_feats))
    do = pl.scan_parquet(wf("daily_observation")).select(["day", "security_id"] + do_feats)
    mc = pl.scan_parquet(wf("market_context_daily")).select(["day"] + mc_feats)
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id", "max_runup_1d", "max_drawdown_1d"] + fo_feats))
    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner").join(mc, on="day", how="left")
          .with_columns((pl.col("max_runup_1d") - pl.col("max_drawdown_1d")).alias("realized_range"))
          .with_columns(pl.col("day").dt.year().alias("yr")).sort("day").collect())

    allfeats = list(dict.fromkeys(do_feats + fo_feats + cl_feats + mc_feats))
    for c in allfeats:
        if df[c].dtype not in (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int8):
            df = df.with_columns(pl.col(c).cast(pl.Float64, strict=False))
    allfeats = [f for f in allfeats if df[f].drop_nulls().n_unique() >= 2]

    y = df["realized_range"].to_numpy()
    valid = ~np.isnan(y)
    day = df["day"].to_numpy().astype("datetime64[D]"); yr = df["yr"].to_numpy()
    rng = np.random.default_rng(SEED)
    print(f"deployable rows @{OFFSET}: {df.height:,} · target = realized 1d range "
          f"(median {np.nanmedian(y)*100:.2f}%) · {len(allfeats)} full feats\n")

    sv = keep(STATIC_VOL, set(allfeats))
    early = list(dict.fromkeys(sv + keep(EARLY_EXTRA + ["entry_1m_range", "entry_range_vs_atr_14d"], set(allfeats))))
    models = {
        "ATR-only (atr_14d)": ["atr_14d"],
        "STATIC-VOL (hist vol)": sv,
        "INFORMED (+early/earnings/vix)": early,
        "FULL-ML (all feats)": allfeats,
    }
    print(f"  {'model':<34}{'n feats':>8}{'OOS rank-IC':>13}{'95% CI':>20}")
    res = {}
    for name, fs in models.items():
        X = df.select(fs).to_numpy()
        pr = walk_pred(X, y, day, yr, valid)
        m = valid & ~np.isnan(pr)
        ics = daily_rank_ic(day[m], pr[m], y[m])
        lo, hi = boot_ci(ics, rng)
        res[name] = ics
        print(f"  {name:<34}{len(fs):>8}{ics.mean():>12.4f} [{lo:>7.4f},{hi:>7.4f}]")

    # incremental lift + per-year
    inf_ic = res["INFORMED (+early/earnings/vix)"].mean()
    full_ic = res["FULL-ML (all feats)"].mean()
    atr_ic = res["ATR-only (atr_14d)"].mean()
    print(f"\n  LIFT full-ML over INFORMED baseline : {full_ic - inf_ic:+.4f} "
          f"({100*(full_ic-inf_ic)/max(inf_ic,1e-9):+.0f}% relative)")
    print(f"  LIFT full-ML over ATR-only          : {full_ic - atr_ic:+.4f}")

    print(f"\n  per-year OOS rank-IC (stability):")
    print(f"    {'model':<34}" + "".join(f"{y:>9}" for y in [2018, 2019, 2020]))
    for name, fs in models.items():
        X = df.select(fs).to_numpy(); pr = walk_pred(X, y, day, yr, valid)
        row = f"    {name:<34}"
        for yv in [2018, 2019, 2020]:
            m = valid & ~np.isnan(pr) & (yr == yv)
            ics = daily_rank_ic(day[m], pr[m], y[m])
            row += f"{ics.mean():>9.4f}"
        print(row)

    print("\n--- verdict ---")
    print("Large, stable LIFT of FULL-ML over INFORMED -> our vol signal knows something beyond")
    print("static vol + today's obvious info (~what IV prices) -> options/straddle cycle-2 has a")
    print("real, testable basis (still must confirm vs actual IV). Small/unstable lift -> the vol")
    print("signal is just persistence already in every option premium -> path 2 likely dead too;")
    print("this dataset is exhausted and ALL paths need genuinely new (directional) data.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
