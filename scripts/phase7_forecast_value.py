#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 7 — TEST 7.1: MARGINAL FORECAST VALUE FOR RISK OVERLAYS.
Pre-registered in phase7-preregistration.md (frozen 2026-07-06, before first run).

Cell: Channel G / hatch H1 (phase7-magnitude-program.md). This test CANNOT produce an
alpha verdict by construction; its only PASS is "risk-tool improved."

The one question, three arms:
  Does the full-ML magnitude forecast improve a volatility-managed portfolio over the
  SAME portfolio built on naive trailing vol?
    Arm A: vol-targeted SPY  — ML time-series sigma vs RV21 / BLEND (Phase 5 rule, frozen)
    Arm B: cross-sectional inverse-vol portfolio — ML predicted range vs atr_14d
    Arm C: volatility-harvest basket — harvest(ML top decile) vs harvest(ATR14 top decile)

Registered expectation: NULL on all arms (forecast lift over persistence is +0.0076 IC).
PASS per arm = ECONOMIC UTILITY AT MATCHED RISK, never return: 95% joint-block-bootstrap
CI of ΔSharpe (scale-invariant) or Δlog-growth AT COMMON REALIZED VOL (both series
rescaled to 10% ann.) excludes 0 in ML's favor against EVERY naive baseline in the arm.
A PASS is a risk tool, not a trading edge. OOS 2018-2020 only.

Run: scripts/phase7_forecast_value.py [--arm A|B|C]   (default: all)
"""
from __future__ import annotations
import glob, sys, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OOS_START = dt.date(2018, 1, 1)
OFFSET = "1000"
TRAIN_TEST = [([2016, 2017], 2018), ([2016, 2017, 2018], 2019), ([2016, 2017, 2018, 2019], 2020)]
EMBARGO_XS_DAYS = 2      # cross-sectional 1d-range target (phase1_vol_forecast convention)
EMBARGO_TS_DAYS = 5      # Arm A 5d-vol target
SIGMA_STAR = 0.15
COST_SPY = 2e-4          # per unit turnover (Phase 5 convention)
COST_RT_STOCK = 15e-4    # round-trip, frozen cost model liquid-cell upper bound
REBAL_EVERY = 21
BLOCK, N_BOOT, SEED, ANN = 21, 4000, 20260706, 252

# --- feature lists: verbatim from phase1_vol_forecast.py (the validated forecast) ---
STATIC_VOL = ["atr_5d", "atr_14d", "atr_42d", "realized_vol_21d",
              "yang_zhang_vol_5d", "yang_zhang_vol_14d", "yang_zhang_vol_21d", "yang_zhang_vol_42d"]
EARLY_EXTRA = ["overnight_gap", "intraday_first_30m_high_return", "intraday_first_30m_low_return",
               "intraday_ret_0930_to_1000", "entry_1m_range", "entry_range_vs_atr_14d",
               "premarket_volume_vs_20d_median", "days_to_next_known_earnings",
               "days_since_last_earnings", "realized_vol_21d_rank_today", "vix_open"]
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
TRAIN_CAPS, TRAIN_LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
PORT_CAPS, PORT_LIQS = ["mega", "large"], ["highly_liquid", "liquid"]   # deployable corner
RET_1D_CANDIDATES = ["ret_1d", "ret_1d_raw", "fwd_ret_1d", "return_1d"]


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


def resolve(schema, candidates, what):
    for c in candidates:
        if c in schema:
            print(f"  [contract] {what} -> column '{c}'")
            return c
    sys.exit(f"FATAL: none of {candidates} present for {what}. Available: {sorted(schema)}")


def gbm():
    return HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
        min_samples_leaf=200, l2_regularization=1.0, early_stopping=True,
        validation_fraction=0.1, n_iter_no_change=20, random_state=SEED)


def walk_pred(X, y, day, yr, valid, embargo_days):
    pr = np.full(len(y), np.nan)
    for tr_y, te_y in TRAIN_TEST:
        te = (yr == te_y) & valid
        trn = np.isin(yr, tr_y) & valid
        if trn.any():
            trn = trn & (day <= day[trn].max() - np.timedelta64(embargo_days, "D"))
        if not trn.any() or not te.any():
            continue
        m = gbm(); m.fit(X[trn], y[trn]); pr[te] = m.predict(X[te])
    return pr


def stats(r):
    r = np.asarray(r, dtype=float)
    mu, sd = r.mean() * ANN, r.std(ddof=1) * np.sqrt(ANN)
    sh = mu / sd if sd > 0 else np.nan
    g = np.log1p(r).mean() * ANN
    eq = np.cumprod(1 + r)
    dd = (eq / np.maximum.accumulate(eq) - 1).min()
    return sh, g, dd


def diff_ci(ra, rb, rng, metric):
    """joint block bootstrap CI of metric(ra) - metric(rb); same blocks for both."""
    n = len(ra)
    nb = int(np.ceil(n / BLOCK))
    idx = (rng.integers(0, n, size=(N_BOOT, nb))[:, :, None] + np.arange(BLOCK)[None, None, :]) % n
    ii = idx.reshape(N_BOOT, -1)[:, :n]
    a, b = ra[ii], rb[ii]
    if metric == "sharpe":
        d = (a.mean(1) / a.std(1, ddof=1) - b.mean(1) / b.std(1, ddof=1)) * np.sqrt(ANN)
    elif metric == "growth":
        d = (np.log1p(a).mean(1) - np.log1p(b).mean(1)) * ANN
    else:  # "mean": inputs are already log-return (or generic) series; annualize the mean diff
        d = (a.mean(1) - b.mean(1)) * ANN
    return float(np.quantile(d, 0.025)), float(np.quantile(d, 0.975))


def vol_match(r, target=0.10):
    """rescale a net return series to a common realized vol so growth comparisons are
    at equal risk (a biased-low forecast can't win growth by just running hotter).
    Full-sample scalar rescale — noted approximation for the bootstrap."""
    sd = r.std(ddof=1) * np.sqrt(ANN)
    return r * (target / sd) if sd > 0 else r


def verdict_line(name, r_ml, r_nv, rng):
    sh_m, g_m, dd_m = stats(r_ml); sh_n, g_n, dd_n = stats(r_nv)
    slo, shi = diff_ci(r_ml, r_nv, rng, "sharpe")
    glo, ghi = diff_ci(vol_match(r_ml), vol_match(r_nv), rng, "growth")  # matched-risk growth
    ok = slo > 0 or glo > 0
    print(f"  {name:<34} Sharpe {sh_m:5.2f} vs {sh_n:5.2f}  ΔSh CI[{slo:+.2f},{shi:+.2f}]"
          f"  Δg@10%vol CI[{glo*100:+.2f}%,{ghi*100:+.2f}%]/yr  maxDD {dd_m*100:5.1f}/{dd_n*100:5.1f}%"
          f"  -> {'ml-better (risk tool)' if ok else 'null'}")
    return ok


# ---------------------------------------------------------------- ARM A: SPY time-series
def arm_a(rng):
    print("\n" + "=" * 96)
    print("ARM A — vol-targeted SPY: ML σ̂ vs RV21 / BLEND (INV-VOL rule frozen from Phase 5)")
    print("=" * 96)
    mc_h = set(pl.scan_parquet(wf("market_context_daily")).collect_schema().names())
    feats = keep(["spy_realized_vol_21d", "vix_close", "vix_open"] + MC_MORE, mc_h)
    d = (pl.scan_parquet(wf("market_context_daily"))
         .select(["day", "spy_eod_close"] + feats).sort("day").collect()
         .with_columns((pl.col("spy_eod_close") / pl.col("spy_eod_close").shift(1) - 1).alias("ret"))
         .drop_nulls(["ret"]))
    day = d["day"].to_numpy().astype("datetime64[D]")
    ret = d["ret"].to_numpy()
    rv21 = d["spy_realized_vol_21d"].to_numpy()
    vix = d["vix_close"].to_numpy() / 100.0 if "vix_close" in d.columns else np.full(len(ret), np.nan)

    lam = 0.5 ** (1 / 10)
    ew = np.empty(len(ret)); v = ret[:20].var()
    for i, r in enumerate(ret):
        v = lam * v + (1 - lam) * r * r
        ew[i] = np.sqrt(v * ANN)

    # target: forward 5d realized vol (annualized), t+1..t+5
    y = np.full(len(ret), np.nan)
    for i in range(len(ret) - 5):
        y[i] = ret[i + 1:i + 6].std(ddof=1) * np.sqrt(ANN)
    Xcols = [d[c].to_numpy().astype(float) for c in feats] + [ew, np.abs(ret), ret]
    X = np.column_stack(Xcols)
    yr = day.astype("datetime64[Y]").astype(int) + 1970
    valid = ~np.isnan(y) & ~np.isnan(X).any(axis=1)
    pred = walk_pred(X, y, day, yr, valid, EMBARGO_TS_DAYS)

    oos = day >= np.datetime64(OOS_START)
    fc_rmse = {}
    for nm, s in [("ML", pred), ("RV21", rv21), ("BLEND", np.sqrt(np.clip(rv21, 1e-8, None) * np.clip(vix, 1e-8, None)))]:
        m = oos & ~np.isnan(s) & ~np.isnan(y)
        fc_rmse[nm] = float(np.sqrt(np.mean((s[m] - y[m]) ** 2)))
    print(f"  forecast RMSE vs fwd-5d realized (OOS): " +
          "  ".join(f"{k} {v:.4f}" for k, v in fc_rmse.items()))

    def overlay(sig):
        sig_lag = np.concatenate([[np.nan], sig[:-1]])
        w = np.nan_to_num(np.minimum(1.0, SIGMA_STAR / sig_lag), nan=1.0)
        turn = np.abs(np.diff(np.concatenate([[w[0]], w])))
        return (w * ret - turn * COST_SPY)[oos], float(w[oos].mean())

    r_ml, w_ml = overlay(pred)
    passes = []
    for nm, s in [("RV21", rv21), ("BLEND", np.sqrt(np.clip(rv21, 1e-8, None) * np.clip(vix, 1e-8, None)))]:
        r_nv, w_nv = overlay(s)
        print(f"  avg exposure ⟨w⟩: ML {w_ml:.2f} vs {nm} {w_nv:.2f}"
              + ("  [exposure gap >0.05 — disclose with verdict]" if abs(w_ml - w_nv) > 0.05 else ""))
        passes.append(verdict_line(f"ML vs {nm}", r_ml, r_nv, rng))
    ok = all(passes)
    print(f"  ARM A verdict: {'PASS (risk-tool)' if ok else 'NULL'} (must beat every baseline)")
    return ok


# ------------------------------------------------- shared: cross-sectional forecast (B, C)
def build_xs_forecast():
    print("\n[shared] building the Phase-1 FULL-ML cross-sectional range forecast (walk-forward OOS)…")
    do_h = set(pl.scan_parquet(wf("daily_observation")).collect_schema().names())
    fo_h = set(pl.scan_parquet(wf("forward_outcomes")).collect_schema().names())
    cl_h = set(pl.scan_parquet(wf("security_classification_daily")).collect_schema().names())
    mc_h = set(pl.scan_parquet(wf("market_context_daily")).collect_schema().names())
    ret_col = resolve(fo_h, RET_1D_CANDIDATES, "next-day return (portfolio compounding)")

    do_feats = keep(STATIC_VOL + [f for f in EARLY_EXTRA if f != "days_to_next_known_earnings"] + DO_MORE
                    + ["days_to_next_known_earnings"], do_h)
    fo_feats = keep(["entry_1m_range", "entry_range_vs_atr_14d"] + FO_MORE, fo_h)
    cl_feats = keep(CL_MORE, cl_h)
    mc_feats = keep(["vix_open"] + MC_MORE, mc_h)

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(TRAIN_CAPS)
                   & pl.col("liquidity_bucket").is_in(TRAIN_LIQS))
           .select(["day", "security_id", "market_cap_bucket", "liquidity_bucket"] + cl_feats))
    do = pl.scan_parquet(wf("daily_observation")).select(["day", "security_id"] + do_feats)
    mc = pl.scan_parquet(wf("market_context_daily")).select(["day"] + mc_feats)
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id", "max_runup_1d", "max_drawdown_1d", ret_col] + fo_feats))
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
    day = df["day"].to_numpy().astype("datetime64[D]")
    yr = df["yr"].to_numpy()
    valid = ~np.isnan(y)
    pred = walk_pred(df.select(allfeats).to_numpy(), y, day, yr, valid, EMBARGO_XS_DAYS)
    df = df.with_columns(pl.Series("pred_ml", pred))
    port = df.filter(pl.col("market_cap_bucket").is_in(PORT_CAPS)
                     & pl.col("liquidity_bucket").is_in(PORT_LIQS)
                     & (pl.col("day") >= OOS_START)
                     & pl.col("pred_ml").is_not_null())
    print(f"  training rows {df.height:,} · portfolio-universe OOS rows {port.height:,}")
    return port.select("day", "security_id", ret_col, "pred_ml", "atr_14d").rename({ret_col: "ret1d"})


def to_matrices(port):
    """pivot to (days × sids) numpy matrices for ret / pred / atr."""
    days = np.sort(port["day"].unique().to_numpy())
    sids = port["security_id"].unique().to_list()
    sid_ix = {s: i for i, s in enumerate(sids)}
    day_ix = {d: i for i, d in enumerate(days.tolist())}
    R = np.full((len(days), len(sids)), np.nan)
    P = np.full_like(R, np.nan); A = np.full_like(R, np.nan)
    for d_, s_, r_, p_, a_ in port.iter_rows():
        i, j = day_ix[d_], sid_ix[s_]
        R[i, j], P[i, j], A[i, j] = r_, p_, a_
    return days, R, P, A


def run_blocks(R, sig, weight_fn, daily_eq=False):
    """21d-block portfolio with intra-block drift. weight_fn(sig_row, ret_row_valid) -> weights.
    daily_eq: re-equalize held names every day (Arm C rebalanced leg), costing daily turnover.
    Returns daily net return array (skips warm-up day 0 of each block alignment)."""
    n_days = R.shape[0]
    out = np.zeros(n_days); held_prev = None
    for b0 in range(0, n_days - 1, REBAL_EVERY):
        srow = sig[b0]
        ok = ~np.isnan(srow) & ~np.isnan(R[b0]) & (srow > 0)
        w = np.zeros(R.shape[1])
        if ok.sum() >= 20:
            w[ok] = weight_fn(srow[ok])
            w /= w.sum()
        # turnover vs drifted previous holdings
        prev = held_prev if held_prev is not None else np.zeros_like(w)
        cost0 = COST_RT_STOCK * 0.5 * np.abs(w - prev).sum()
        h = w.copy()
        for t in range(b0, min(b0 + REBAL_EVERY, n_days)):
            r = np.nan_to_num(R[t], nan=0.0)          # delisted mid-block -> 0 further return
            gross = (h * r).sum() / h.sum() if h.sum() > 0 else 0.0
            h = h * (1 + r)
            if h.sum() > 0:
                h /= h.sum()
            c = cost0 if t == b0 else 0.0
            if daily_eq and t > b0:
                tgt = np.where(h > 0, 1.0, 0.0); tgt = tgt / tgt.sum() if tgt.sum() > 0 else tgt
                c += COST_RT_STOCK * 0.5 * np.abs(tgt - h).sum()
                h = tgt
            out[t] = gross - c
        held_prev = h
    return out


def arm_b(port, rng):
    print("\n" + "=" * 96)
    print("ARM B — cross-sectional inverse-vol portfolio: ML σ̂ vs atr_14d (21d rebalance, 15bp RT)")
    print("=" * 96)
    days, R, P, A = to_matrices(port)
    r_ml = run_blocks(R, P, lambda s: 1.0 / s)
    r_atr = run_blocks(R, A, lambda s: 1.0 / s)
    r_ew = run_blocks(R, np.where(np.isnan(P), np.nan, 1.0), lambda s: np.ones_like(s))
    ok = verdict_line("inv-ML vs inv-ATR14 (registered)", r_ml, r_atr, rng)
    sh, g, dd = stats(r_ew)
    print(f"  context only: equal-weight Sharpe {sh:.2f}, growth {g*100:+.2f}%/yr, maxDD {dd*100:.1f}%")
    print(f"  ARM B verdict: {'PASS (risk-tool)' if ok else 'NULL'}")
    return ok


def arm_c(port, rng):
    print("\n" + "=" * 96)
    print("ARM C — volatility-harvest basket: harvest(ML top-decile) vs harvest(ATR14 top-decile)")
    print("=" * 96)
    days, R, P, A = to_matrices(port)

    def top_decile(sig):
        out = np.full_like(sig, np.nan)
        for i in range(sig.shape[0]):
            row = sig[i]; ok = ~np.isnan(row)
            if ok.sum() >= 50:
                thr = np.nanquantile(row, 0.9)
                out[i] = np.where(row >= thr, 1.0, np.nan)
        return out

    res = {}
    for nm, sig in [("ML", top_decile(P)), ("ATR14", top_decile(A))]:
        rb = run_blocks(R, sig, lambda s: np.ones_like(s), daily_eq=True)   # daily re-equalized
        bh = run_blocks(R, sig, lambda s: np.ones_like(s), daily_eq=False)  # drift within block
        glo, ghi = diff_ci(rb, bh, rng, "growth")
        harvest = (np.log1p(rb).mean() - np.log1p(bh).mean()) * ANN
        res[nm] = (rb, bh, harvest)
        print(f"  {nm:<6} net harvest (rebal − hold): {harvest*100:+.2f}%/yr  CI[{glo*100:+.2f},{ghi*100:+.2f}]")
    d_ml = np.log1p(res["ML"][0]) - np.log1p(res["ML"][1])
    d_at = np.log1p(res["ATR14"][0]) - np.log1p(res["ATR14"][1])
    lo, hi = diff_ci(d_ml, d_at, rng, "mean")   # difference-of-differences, already log space
    print(f"  registered estimand Δharvest (ML − ATR14): {(d_ml.mean()-d_at.mean())*ANN*100:+.2f}%/yr  CI[{lo*100:+.2f},{hi*100:+.2f}]")
    ok = lo > 0
    print(f"  ARM C verdict: {'PASS (risk-tool)' if ok else 'NULL'}")
    return ok


def main():
    t0 = dt.datetime.now()
    which = sys.argv[sys.argv.index("--arm") + 1].upper() if "--arm" in sys.argv else "ABC"
    rng = np.random.default_rng(SEED)
    print("=" * 96)
    print("Phase 7 · Test 7.1 — marginal value of the ML magnitude forecast for risk overlays")
    print("Pre-registered NULL. PASS is risk-tool-only; alpha verdicts are unavailable by design.")
    print("=" * 96)
    verdicts = {}
    if "A" in which:
        verdicts["A"] = arm_a(rng)
    if "B" in which or "C" in which:
        port = build_xs_forecast()
        if "B" in which:
            verdicts["B"] = arm_b(port, rng)
        if "C" in which:
            verdicts["C"] = arm_c(port, rng)
    print("\n--- pre-registered verdict ---")
    for k, v in verdicts.items():
        print(f"  Arm {k}: {'PASS (risk-tool)' if v else 'NULL'}")
    print("Interpretation is fixed by phase7-preregistration.md: any PASS is a better RISK INPUT,")
    print("never alpha; a PASS also requires the 2021-22 confirmation protocol before belief.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
