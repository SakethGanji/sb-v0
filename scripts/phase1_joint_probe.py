#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 1 — JOINT MULTIVARIATE PREDICTABILITY PROBE (the multivariate ceiling test).

Every prior Phase-1 script is UNIVARIATE (one feature / one signal at a time). The
predictability ceiling (`phase1_ceiling.py`) that underwrites "no ML headroom" is
also univariate: it caps the MI of any SINGLE feature. Two features each ~0 on their
own can be jointly predictive (interactions). This script closes that gap directly.

QUESTION: on the DEPLOYABLE universe, can a joint model over the FULL pre-entry
feature set — including market-state (VIX / breadth / dispersion), which no prior
script ever used — extract out-of-sample predictability the univariate scans missed?

METHOD (leakage-hardened):
  - Universe: CS × cap∈{mega,large,mid} × liq∈{highly_liquid,liquid,normal}, offset 1000.
  - Features: ALL pre-entry columns knowable by 10:00 ET (strict no-look-ahead list;
    per-name from daily_observation + forward_outcomes pre-entry/entry-microstructure
    + classification, PLUS day-level market_context @1000). Trees see them jointly.
  - Target: ret_{1d,5d}_excess_spy (beta-neutral outcome).
  - Model: HistGradientBoostingRegressor (native NaN, monotone-free, captures interactions).
  - Validation: EXPANDING WALK-FORWARD by year (train ≤Y-1 → test Y), 2-day embargo.
    Pooled OOS predictions → per-DAY rank-IC (Spearman) → day-clustered bootstrap CI.
    Primary verdict = is mean OOS daily rank-IC > 0 with CI lower bound > 0.
  - Tradeable translation: OOS top-decile−bottom-decile realized excess (bps/day) vs
    the cost floor.
  - Leakage guard: PERMUTATION NULL — shuffle y within train days, refit, score on REAL
    test y; real IC must beat the null 95th pct (catches overf-to-noise / subtle leak).

Read-only on data/outputs/; writes data/phase1_analysis/joint_probe.parquet.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
TARGETS = ["ret_1d_excess_spy", "ret_5d_excess_spy"]
TRAIN_TEST = [([2016, 2017], 2018), ([2016, 2017, 2018], 2019),
              ([2016, 2017, 2018, 2019], 2020)]
EMBARGO_DAYS = 2
N_PERM = 8
N_BOOT = 2000
SEED = 20260705
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
OUT = Path("data/phase1_analysis")

# ---- STRICT no-look-ahead feature lists (everything knowable by 10:00 ET) ----
# daily_observation: exclude first_hour/1010/1030 (post-10:00), all eod_*, next_day_*.
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
    "consecutive_up_days_close_to_close", "day_of_week", "days_since_first_bar",
    "signal_first_in_5d", "signal_first_in_10d", "signal_first_in_20d",
    "bar_count_premarket", "bar_count_first_30m",
]
# forward_outcomes: pre-entry + entry-microstructure (all AT entry=10:00).
FO_FEATS = [
    "pre_entry_ret_from_open", "pre_entry_vwap_from_open", "pre_entry_ret_from_high",
    "pre_entry_ret_from_low", "pre_entry_minutes_since_high", "pre_entry_minutes_since_low",
    "pre_entry_ret_rank_today", "pre_entry_ret_percentile_today",
    "pre_entry_dollar_volume_rank_today", "entry_bar_upper_wick_pct",
    "entry_bar_lower_wick_pct", "entry_slippage_proxy_bps", "entry_1m_range",
    "entry_range_vs_atr_14d", "entry_dollar_volume_vs_addv_20d",
    "cumulative_volume_to_entry", "cumulative_dollar_volume_to_entry",
]
# classification: numeric conditioners (static-ish; size/vol/beta/age).
CL_FEATS = ["market_cap", "market_cap_rank_today", "market_cap_percentile_today",
            "volatility_percentile_today", "days_since_ipo_or_first_bar"]
# market_context: day-level state knowable by 10:00 (NO _1030 / _eod / vix_close).
MC_FEATS = ["vix_open", "spy_overnight_gap", "qqq_overnight_gap", "iwm_overnight_gap",
            "spy_ret_0930_to_1000", "qqq_ret_0930_to_1000", "iwm_ret_0930_to_1000",
            "spy_realized_vol_21d", "qqq_realized_vol_21d", "iwm_realized_vol_21d",
            "breadth_pct_universe_green_at_1000", "breadth_pct_universe_above_premarket_vwap_at_1000",
            "breadth_advance_decline_ratio_at_1000", "breadth_count_movers_above_5pct_at_1000",
            "breadth_count_movers_above_1atr_at_1000", "cross_sectional_ret_dispersion_at_1000",
            "cross_sectional_ret_iqr_at_1000", "universe_median_addv_20d",
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


def keep(cands, have):
    return [c for c in cands if c in have]


def daily_rank_ic(day, pred, actual):
    """Per-day Spearman(pred, actual); return array of daily ICs (days with >=10 names)."""
    order = np.argsort(day, kind="stable")
    d, p, a = day[order], pred[order], actual[order]
    u, idx = np.unique(d, return_index=True)
    ends = np.append(idx[1:], len(d))
    ics = []
    for i in range(len(u)):
        s, e = idx[i], ends[i]
        if e - s < 10:
            continue
        pr = np.argsort(np.argsort(p[s:e])).astype(float)
        ar = np.argsort(np.argsort(a[s:e])).astype(float)
        pr -= pr.mean(); ar -= ar.mean()
        den = np.sqrt((pr * pr).sum() * (ar * ar).sum())
        if den > 0:
            ics.append(float((pr * ar).sum() / den))
    return np.array(ics)


def daily_decile_spread(day, pred, actual):
    """Per-day mean(actual | pred top decile) - mean(actual | pred bottom decile), in return units."""
    order = np.argsort(day, kind="stable")
    d, p, a = day[order], pred[order], actual[order]
    u, idx = np.unique(d, return_index=True)
    ends = np.append(idx[1:], len(d))
    sp = []
    for i in range(len(u)):
        s, e = idx[i], ends[i]
        if e - s < 20:
            continue
        pp, aa = p[s:e], a[s:e]
        hi = pp >= np.quantile(pp, 0.9); lo = pp <= np.quantile(pp, 0.1)
        if hi.sum() and lo.sum():
            sp.append(aa[hi].mean() - aa[lo].mean())
    return np.array(sp)


def boot_ci(x, rng, B=N_BOOT):
    if len(x) == 0:
        return (float("nan"), float("nan"))
    bm = x[rng.integers(0, len(x), size=(B, len(x)))].mean(axis=1)
    return float(np.quantile(bm, 0.025)), float(np.quantile(bm, 0.975))


def fit_predict(Xtr, ytr, Xte, seed):
    m = HistGradientBoostingRegressor(
        max_iter=300, learning_rate=0.05, max_leaf_nodes=31, min_samples_leaf=200,
        l2_regularization=1.0, early_stopping=True, validation_fraction=0.1,
        n_iter_no_change=20, random_state=seed)
    m.fit(Xtr, ytr)
    return m.predict(Xte)


def walk_forward(df, feats, target, day_arr, yr_arr, perm_rng=None, seed=SEED):
    """Expanding walk-forward; returns pooled (test_day, pred, actual). If perm_rng given,
    shuffle ytr WITHIN train days (leakage/overfit null)."""
    X = df.select(feats).to_numpy()
    y = df[target].to_numpy()
    ok = ~np.isnan(y)
    tds, preds, acts = [], [], []
    for train_yrs, test_yr in TRAIN_TEST:
        te = (yr_arr == test_yr) & ok
        tr = np.isin(yr_arr, train_yrs) & ok
        # embargo: drop last EMBARGO_DAYS train days before the test year boundary
        if tr.any():
            tmax = day_arr[tr].max()
            emb = tmax - np.timedelta64(EMBARGO_DAYS, "D")
            tr = tr & (day_arr <= emb)
        if not tr.any() or not te.any():
            continue
        ytr = y[tr].copy()
        if perm_rng is not None:
            # shuffle within each train day (preserve marginals, destroy feature->y link)
            dtr = day_arr[tr]
            o = np.argsort(dtr, kind="stable")
            u, idx = np.unique(dtr[o], return_index=True)
            ends = np.append(idx[1:], len(o))
            yshuf = ytr[o].copy()
            for i in range(len(u)):
                seg = slice(idx[i], ends[i])
                yshuf[seg] = perm_rng.permutation(yshuf[seg])
            ytr = np.empty_like(ytr); ytr[o] = yshuf
        pr = fit_predict(X[tr], ytr, X[te], seed)
        tds.append(day_arr[te]); preds.append(pr); acts.append(y[te])
    if not tds:
        return None
    return np.concatenate(tds), np.concatenate(preds), np.concatenate(acts)


def main():
    t0 = dt.datetime.now(); OUT.mkdir(parents=True, exist_ok=True)
    print("=" * 92)
    print("Phase 1 — JOINT MULTIVARIATE PREDICTABILITY PROBE (deployable universe)")
    print("=" * 92)

    do_have = set(pl.scan_parquet(wf("daily_observation")).collect_schema().names())
    fo_have = set(pl.scan_parquet(wf("forward_outcomes")).collect_schema().names())
    cl_have = set(pl.scan_parquet(wf("security_classification_daily")).collect_schema().names())
    mc_have = set(pl.scan_parquet(wf("market_context_daily")).collect_schema().names())
    do_f, fo_f = keep(DO_FEATS, do_have), keep(FO_FEATS, fo_have)
    cl_f, mc_f = keep(CL_FEATS, cl_have), keep(MC_FEATS, mc_have)
    feats = do_f + fo_f + cl_f + mc_f

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS")
                   & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select(["day", "security_id"] + cl_f))
    do = pl.scan_parquet(wf("daily_observation")).select(["day", "security_id"] + do_f)
    mc = pl.scan_parquet(wf("market_context_daily")).select(["day"] + mc_f)
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + fo_f + TARGETS))
    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner")
          .join(mc, on="day", how="left")
          .with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in feats])
          .with_columns(pl.col("day").dt.year().alias("yr"))
          .sort("day").collect())
    # prune degenerate columns (all-null / constant) that break the GBM binner
    feats = [f for f in feats if df[f].drop_nulls().n_unique() >= 2]
    print(f"deployable rows @{OFFSET}: {df.height:,} · {len(feats)} joint features")
    print(f"  daily_obs {len(do_f)} · fwd_out {len(fo_f)} · class {len(cl_f)} · MARKET-STATE {len(mc_f)}")
    print(f"  walk-forward: {[f'{a}->{b}' for a,b in TRAIN_TEST]} · embargo {EMBARGO_DAYS}d · perms {N_PERM}\n")

    day_arr = df["day"].to_numpy().astype("datetime64[D]")
    yr_arr = df["yr"].to_numpy()
    rng = np.random.default_rng(SEED)
    recs, ic_series_store = [], {}

    for target in TARGETS:
        print(f"### target: {target} ###")
        res = walk_forward(df, feats, target, day_arr, yr_arr, seed=SEED)
        if res is None:
            print("  (no folds)\n"); continue
        td, pr, ac = res
        ics = daily_rank_ic(td, pr, ac)
        spr = daily_decile_spread(td, pr, ac)
        ic_lo, ic_hi = boot_ci(ics, rng)
        sp_lo, sp_hi = boot_ci(spr, rng)
        ic_series_store[target] = ics

        # permutation null on OOS IC
        null_ic = np.empty(N_PERM)
        for k in range(N_PERM):
            pr_rng = np.random.default_rng(SEED + 1000 + k)
            rr = walk_forward(df, feats, target, day_arr, yr_arr, perm_rng=pr_rng, seed=SEED)
            null_ic[k] = daily_rank_ic(*rr).mean() if rr is not None else np.nan
        null95 = np.nanquantile(null_ic, 0.95)
        beats = ics.mean() > null95

        print(f"  OOS daily rank-IC : mean {ics.mean():+.4f}  CI[{ic_lo:+.4f},{ic_hi:+.4f}]  "
              f"(n_days={len(ics)})")
        print(f"  permutation null  : mean {np.nanmean(null_ic):+.4f}  95pct {null95:+.4f}  "
              f"-> real {'BEATS null ✓' if beats else 'inside null ✗'}")
        print(f"  OOS decile spread : {spr.mean()*1e4:+.1f} bps/day  CI[{sp_lo*1e4:+.1f},{sp_hi*1e4:+.1f}]"
              f"  (gross; cost floor ~tens of bps)")
        recs.append(dict(target=target, n_days=len(ics), mean_ic=float(ics.mean()),
                         ic_ci_lo=ic_lo, ic_ci_hi=ic_hi, null_mean=float(np.nanmean(null_ic)),
                         null_95=float(null95), beats_null=bool(beats),
                         decile_spread_bps=float(spr.mean()*1e4),
                         spread_ci_lo_bps=sp_lo*1e4, spread_ci_hi_bps=sp_hi*1e4))
        # feature importance (permutation on a held-out slice of the last fold)
        print()

    if recs:
        pl.DataFrame(recs).write_parquet(OUT / "joint_probe.parquet")

    print("--- verdict ---")
    print("If OOS rank-IC CI lower bound > 0 AND it BEATS the permutation null AND the")
    print("decile spread clears the cost floor -> a JOINT edge the univariate scans missed")
    print("(cycle-2 target). If IC ~ 0 / inside the null -> the multivariate ceiling is")
    print("CLOSED too, and 'limit of the data' is earned, not assumed.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
