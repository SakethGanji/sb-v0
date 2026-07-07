#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 7 — TEST 7.1: MARGINAL FORECAST VALUE FOR RISK OVERLAYS.
Pre-registered in phase7-preregistration.md (frozen 2026-07-06, before first run;
amendment v2 of 2026-07-06 — also pre-run — fixes the data-contract and inference
defects found in review; every deviation from the v1 text is listed there).

Cell: Channel G / hatch H1 (phase7-magnitude-program.md). This test CANNOT produce an
alpha verdict by construction; its only PASS is "risk-tool improved."

The one question, three arms:
  Does the full-ML magnitude forecast improve a volatility-managed portfolio over the
  SAME portfolio built on naive trailing vol?
    Arm A: vol-targeted SPY  — ML time-series sigma vs RV21 / BLEND (Phase 5 rule, frozen)
    Arm B: cross-sectional inverse-vol portfolio — ML predicted range vs atr_14d/price
    Arm C: volatility-harvest basket — harvest(ML top decile) vs harvest(ATR top decile)

Registered expectation: NULL on all arms. (Vol-forecast IC ladder, _vol_forecast.log:
atr-only 0.040 / persistence 0.576 / informed 0.604 / full-ML 0.612 — the lift over
trailing-vol persistence is +0.036 rank-IC; the oft-quoted +0.0076 is over INFORMED.)
PASS per arm = ECONOMIC UTILITY AT MATCHED RISK, never return: 95% joint-block-bootstrap
CI of ΔSharpe (scale-invariant) or Δlog-growth AT COMMON REALIZED VOL (both series
rescaled to 10% ann., per bootstrap replicate) excludes 0 in ML's favor against EVERY
naive baseline in the arm. A PASS is a risk tool, not a trading edge. OOS 2018-2020 only.

Data contract (verified 2026-07-06): forward_outcomes.ret_1d spans entry(10:00 D) to
close(D+1) — consecutive values OVERLAP and cannot be compounded. Portfolio returns are
therefore built as entry_price(D+1)/entry_price(D) − 1 (disjoint 10:00->10:00 clock,
adjusted basis, price-only — dividends excluded for both strategies alike, disclosed).
daily_observation.atr_14d is in DOLLARS; the naive baseline is atr_14d/entry_price.

Run: scripts/phase7_forecast_value.py [--arm A|B|C] [--smoke]   (default: all arms)
  --smoke: engineering test on 2017 pseudo-OOS (train 2016), N_BOOT=200. NOT the
  registered run; carries no verdict and does not unblind 2018-2020.
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
EMBARGO_XS_DAYS = 2      # TRADING days (amendment v2; phase1 used calendar days on a 1d target)
EMBARGO_TS_DAYS = 5      # TRADING days — must cover the 5-trading-day Arm A target span
SIGMA_STAR = 0.15
COST_SPY = 2e-4          # per unit turnover (Phase 5 convention)
COST_RT_STOCK = 15e-4    # round-trip, frozen cost model liquid-cell upper bound
REBAL_EVERY = 21
BLOCK, N_BOOT, SEED, ANN = 21, 4000, 20260706, 252
GBM_SEED_XS = 20260705   # phase1_vol_forecast.py's seed — keeps the XS model bit-identical
TARGET_VOL = 0.10        # common realized vol for matched-risk growth comparisons

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


def uniq(c):
    return list(dict.fromkeys(c))


def keep(c, h, what):
    c = uniq(c)
    dropped = [x for x in c if x not in h]
    if dropped:
        print(f"  [contract] {what}: expected columns ABSENT and dropped: {dropped}")
    return [x for x in c if x in h]


def gbm_xs():
    return HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
        min_samples_leaf=200, l2_regularization=1.0, early_stopping=True,
        validation_fraction=0.1, n_iter_no_change=20, random_state=GBM_SEED_XS)


def gbm_ts():
    # Arm A single-asset series: ~250-1000 rows/fold. The XS config (min_samples_leaf=200)
    # cannot split at all on fold 1 -> constant sigma-hat. Small-sample config, frozen in
    # amendment v2 before first run.
    return HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, max_leaf_nodes=7,
        min_samples_leaf=20, l2_regularization=1.0, early_stopping=True,
        validation_fraction=0.15, n_iter_no_change=20, random_state=SEED)


def walk_pred(X, y, day, yr, valid, embargo_days, model_fn):
    """Walk-forward by year. Embargo drops the last `embargo_days` TRADING days of each
    training span so no training label overlaps the test year (amendment v2)."""
    pr = np.full(len(y), np.nan)
    for tr_y, te_y in TRAIN_TEST:
        te = (yr == te_y) & valid
        trn = np.isin(yr, tr_y) & valid
        if trn.any():
            ud = np.unique(day[trn])
            if len(ud) > embargo_days:
                trn = trn & (day <= ud[-(embargo_days + 1)])
            else:
                trn = np.zeros_like(trn)
        if not trn.any() or not te.any():
            continue
        # a feature that is constant/all-NaN in this fold's training slice crashes
        # HistGB's binning (sliding_window_view over <2 distinct values) — drop it
        # for this fold (same n_unique>=2 rule build_xs_forecast applies globally)
        Xtr = X[trn]
        cols = [j for j in range(X.shape[1])
                if np.unique(Xtr[:, j][~np.isnan(Xtr[:, j])]).size >= 2]
        m = model_fn(); m.fit(Xtr[:, cols], y[trn]); pr[te] = m.predict(X[te][:, cols])
    return pr


def stats(r):
    r = np.asarray(r, dtype=float)
    mu, sd = r.mean() * ANN, r.std(ddof=1) * np.sqrt(ANN)
    sh = mu / sd if sd > 0 else np.nan
    g = np.log1p(r).mean() * ANN
    eq = np.cumprod(1 + r)
    dd = (eq / np.maximum.accumulate(eq) - 1).min()
    return sh, g, dd


def _log1p_safe(x):
    return np.log1p(np.maximum(x, -0.9999))


def diff_ci(ra, rb, rng, metric):
    """Joint block bootstrap CI of metric(ra) - metric(rb); the SAME resampled blocks
    price both series so the CI is on the difference with correlation preserved.
    metric 'growth' vol-matches each series to TARGET_VOL PER REPLICATE (amendment v2 —
    a full-sample scalar rescale would let vol differences leak back into the CI)."""
    n = len(ra)
    nb = int(np.ceil(n / BLOCK))
    idx = (rng.integers(0, n, size=(N_BOOT, nb))[:, :, None] + np.arange(BLOCK)[None, None, :]) % n
    ii = idx.reshape(N_BOOT, -1)[:, :n]
    a, b = ra[ii], rb[ii]
    if metric == "sharpe":
        d = (a.mean(1) / a.std(1, ddof=1) - b.mean(1) / b.std(1, ddof=1)) * np.sqrt(ANN)
    elif metric == "growth":
        ka = (TARGET_VOL / np.sqrt(ANN)) / a.std(1, ddof=1)
        kb = (TARGET_VOL / np.sqrt(ANN)) / b.std(1, ddof=1)
        d = (_log1p_safe(a * ka[:, None]).mean(1) - _log1p_safe(b * kb[:, None]).mean(1)) * ANN
    else:  # "mean": inputs are already per-day log-diff (or generic) series
        d = (a.mean(1) - b.mean(1)) * ANN
    return float(np.quantile(d, 0.025)), float(np.quantile(d, 0.975))


def vol_match(r, target=TARGET_VOL):
    """Full-sample scalar rescale for POINT ESTIMATES (the CI does its own per-replicate
    matching inside diff_ci)."""
    sd = r.std(ddof=1) * np.sqrt(ANN)
    return r * (target / sd) if sd > 0 else r


def verdict_line(name, r_ml, r_nv, rng):
    sh_m, _, dd_m = stats(r_ml); sh_n, _, dd_n = stats(r_nv)
    g_m = _log1p_safe(vol_match(r_ml)).mean() * ANN
    g_n = _log1p_safe(vol_match(r_nv)).mean() * ANN
    slo, shi = diff_ci(r_ml, r_nv, rng, "sharpe")
    glo, ghi = diff_ci(r_ml, r_nv, rng, "growth")   # matched-risk growth, per replicate
    ok = slo > 0 or glo > 0
    print(f"  {name:<34} Sharpe {sh_m:5.2f} vs {sh_n:5.2f}  ΔSh CI[{slo:+.2f},{shi:+.2f}]"
          f"  g@10%vol {g_m*100:+.2f}/{g_n*100:+.2f}%  Δg CI[{glo*100:+.2f}%,{ghi*100:+.2f}%]/yr"
          f"  maxDD {dd_m*100:5.1f}/{dd_n*100:5.1f}%  -> {'ml-better (risk tool)' if ok else 'null'}")
    return ok


# ---------------------------------------------------------------- ARM A: SPY time-series
def arm_a(rng):
    print("\n" + "=" * 96)
    print("ARM A — vol-targeted SPY: ML σ̂ vs RV21 / BLEND (INV-VOL rule frozen from Phase 5)")
    print("=" * 96)
    mc_h = set(pl.scan_parquet(wf("market_context_daily")).collect_schema().names())
    feats = keep(["spy_realized_vol_21d", "vix_close", "vix_open"] + MC_MORE, mc_h, "market_context_daily")
    if "spy_realized_vol_21d" not in feats:
        sys.exit("FATAL: market_context_daily lacks spy_realized_vol_21d — Arm A contract broken.")
    if "vix_close" not in feats:
        sys.exit("FATAL: market_context_daily lacks vix_close — BLEND baseline cannot be built. "
                 "Running Arm A without its registered baseline would be a silent deviation.")
    d = (pl.scan_parquet(wf("market_context_daily"))
         .select(["day", "spy_eod_close"] + feats).sort("day").collect()
         .with_columns((pl.col("spy_eod_close") / pl.col("spy_eod_close").shift(1) - 1).alias("ret"))
         .drop_nulls(["ret"]))
    day = d["day"].to_numpy().astype("datetime64[D]")
    ret = d["ret"].to_numpy()
    rv21 = d["spy_realized_vol_21d"].to_numpy()
    vix = d["vix_close"].to_numpy() / 100.0

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
    valid = ~np.isnan(y)                      # HistGB handles NaN features natively
    pred = walk_pred(X, y, day, yr, valid, EMBARGO_TS_DAYS, gbm_ts)

    blend = np.sqrt(np.clip(rv21, 1e-8, None) * np.clip(vix, 1e-8, None))
    baselines = [("RV21", rv21), ("BLEND", blend)]
    oos = day >= np.datetime64(OOS_START)

    m_rmse = oos & ~np.isnan(y) & ~np.isnan(pred)
    rmse_txt = []
    for nm, s in [("ML", pred)] + baselines:
        m = m_rmse & ~np.isnan(s)
        rmse_txt.append(f"{nm} {np.sqrt(np.mean((s[m] - y[m]) ** 2)):.4f}")
    print(f"  forecast RMSE vs fwd-5d realized (OOS): " + "  ".join(rmse_txt))
    print(f"  ML σ̂ OOS dispersion (std of forecast): {np.nanstd(pred[oos]):.4f}"
          f"  (a near-zero value means the model degenerated to a constant)")

    def lag(sig):
        return np.concatenate([[np.nan], sig[:-1]])

    sig_ml, sig_rv, sig_bl = lag(pred), lag(rv21), lag(blend)
    # Paired comparison: only days where EVERY strategy has a finite lagged sigma-hat
    # (amendment v2 — a NaN sigma-hat used to become silent 100% exposure).
    m = oos & np.isfinite(sig_ml) & np.isfinite(sig_rv) & np.isfinite(sig_bl)
    print(f"  eval days: {int(m.sum())} of {int(oos.sum())} OOS days"
          f"  ({int(oos.sum() - m.sum())} dropped for missing σ̂ in any strategy — paired-sample rule)")

    def overlay(sig):
        w = np.minimum(1.0, SIGMA_STAR / sig[m])
        turn = np.abs(np.diff(np.concatenate([[w[0]], w])))
        return w * ret[m] - turn * COST_SPY, float(w.mean())

    r_ml, w_ml = overlay(sig_ml)
    passes = []
    for nm, s in baselines:
        r_nv, w_nv = overlay(lag(s))
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
    for c in ("entry_price", "max_runup_1d", "max_drawdown_1d"):
        if c not in fo_h:
            sys.exit(f"FATAL: forward_outcomes lacks '{c}' — data contract broken.")

    do_feats = keep(STATIC_VOL + [f for f in EARLY_EXTRA if f != "days_to_next_known_earnings"] + DO_MORE
                    + ["days_to_next_known_earnings"], do_h, "daily_observation")
    fo_feats = keep(["entry_1m_range", "entry_range_vs_atr_14d"] + FO_MORE, fo_h, "forward_outcomes")
    cl_feats = keep(CL_MORE, cl_h, "security_classification_daily")
    mc_feats = keep(["vix_open"] + MC_MORE, mc_h, "market_context_daily")

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(TRAIN_CAPS)
                   & pl.col("liquidity_bucket").is_in(TRAIN_LIQS))
           .select(["day", "security_id", "market_cap_bucket", "liquidity_bucket"] + cl_feats))
    do = pl.scan_parquet(wf("daily_observation")).select(["day", "security_id"] + do_feats)
    mc = pl.scan_parquet(wf("market_context_daily")).select(["day"] + mc_feats)
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id", "max_runup_1d", "max_drawdown_1d", "entry_price"] + fo_feats))
    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner").join(mc, on="day", how="left")
          .with_columns((pl.col("max_runup_1d") - pl.col("max_drawdown_1d")).alias("realized_range"))
          .with_columns(pl.col("day").dt.year().alias("yr")).sort("day").collect())

    allfeats = uniq(do_feats + fo_feats + cl_feats + mc_feats)
    for c in allfeats:
        if df[c].dtype not in (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int8):
            df = df.with_columns(pl.col(c).cast(pl.Float64, strict=False))
    allfeats = [f for f in allfeats if df[f].drop_nulls().n_unique() >= 2]

    y = df["realized_range"].to_numpy()
    day = df["day"].to_numpy().astype("datetime64[D]")
    yr = df["yr"].to_numpy()
    valid = ~np.isnan(y)
    pred = walk_pred(df.select(allfeats).to_numpy(), y, day, yr, valid, EMBARGO_XS_DAYS, gbm_xs)
    df = df.with_columns(pl.Series("pred_ml", pred))
    port = (df.filter(pl.col("market_cap_bucket").is_in(PORT_CAPS)
                      & pl.col("liquidity_bucket").is_in(PORT_LIQS)
                      & (pl.col("day") >= OOS_START)
                      & pl.col("pred_ml").is_not_null() & pl.col("pred_ml").is_not_nan()
                      & pl.col("entry_price").is_not_null() & (pl.col("entry_price") > 0))
              # naive baseline in the SAME fractional units as pred_ml (atr_14d is dollars)
              .with_columns((pl.col("atr_14d") / pl.col("entry_price")).alias("atr_norm"))
              .select("day", "security_id", "pred_ml", "atr_norm"))
    print(f"  training rows {df.height:,} · portfolio-universe OOS rows {port.height:,}")

    # Disjoint 10:00->10:00 next-day returns from entry_price (adjusted basis).
    # ret_1d is NOT usable here: it spans entry(D)->close(D+1), overlapping across days.
    sids = port["security_id"].unique()
    prices = (pl.scan_parquet(wf("forward_outcomes"))
              .filter((pl.col("entry_offset") == OFFSET) & (pl.col("day") >= OOS_START)
                      & pl.col("security_id").is_in(sids.to_list()))
              .select("day", "security_id", "entry_price").collect())
    return port, prices


def build_matrices(port, prices):
    """Pivot to (days × sids) numpy matrices. R[t] = entry_price(t+1)/entry_price(t) - 1
    (10:00-clock daily return accruing from day t's row); P/A are the signals known at
    10:00 of day t. Last day is dropped (no forward return). Vectorized pivot."""
    days = np.sort(prices["day"].unique().to_numpy().astype("datetime64[D]"))
    sids = np.sort(prices["security_id"].unique().to_numpy())

    def pivot(frame, col):
        M = np.full((len(days), len(sids)), np.nan)
        di = np.searchsorted(days, frame["day"].to_numpy().astype("datetime64[D]"))
        si = np.searchsorted(sids, frame["security_id"].to_numpy())
        M[di, si] = frame[col].to_numpy()
        return M

    E = pivot(prices, "entry_price")
    with np.errstate(invalid="ignore", divide="ignore"):
        R = E[1:] / E[:-1] - 1.0
    P = pivot(port, "pred_ml")[:-1]
    A = pivot(port, "atr_norm")[:-1]
    return R, P, A


def run_blocks(R, sig, weight_fn, daily_eq=False):
    """21d-block portfolio with intra-block drift. weight_fn(sig_row_valid) -> weights.
    daily_eq: re-equalize surviving names daily (Arm C rebalanced leg), costing turnover.
    Missing returns (delist/coverage gap) contribute 0 further return, are counted, and
    are frozen OUT of the daily_eq re-equalization target (amendment v2).
    Returns (daily net return array, diagnostics dict)."""
    n_days = R.shape[0]
    out = np.zeros(n_days)
    held_prev = np.zeros(R.shape[1])
    zero_fill = 0; uninvested = 0
    for b0 in range(0, n_days, REBAL_EVERY):
        srow = sig[b0]
        ok = ~np.isnan(srow) & (srow > 0)
        w = np.zeros(R.shape[1])
        if ok.sum() >= 20:
            w[ok] = weight_fn(srow[ok])
            w /= w.sum()
        cost0 = COST_RT_STOCK * 0.5 * np.abs(w - held_prev).sum()
        h = w.copy()
        dead = np.zeros(R.shape[1], dtype=bool)
        for t in range(b0, min(b0 + REBAL_EVERY, n_days)):
            raw = R[t]
            gap = np.isnan(raw) & (h > 0)
            zero_fill += int(gap.sum()); dead |= gap
            r = np.nan_to_num(raw, nan=0.0)
            if h.sum() > 0:
                gross = (h * r).sum() / h.sum()
            else:
                gross = 0.0; uninvested += 1
            h = h * (1 + r)
            if h.sum() > 0:
                h /= h.sum()
            c = cost0 if t == b0 else 0.0
            if daily_eq and t > b0:
                alive = (h > 0) & ~dead
                if alive.any():
                    tgt = np.where(alive, 1.0, 0.0); tgt /= tgt.sum()
                    # turnover charged over tradeable names only: a delisted position
                    # converts to cash for free, it is not "sold" at 15bp
                    c += COST_RT_STOCK * 0.5 * np.abs(tgt[alive] - h[alive]).sum()
                    h = tgt
            out[t] = gross - c
        held_prev = h
    return out, {"zero_fill_days": zero_fill, "uninvested_days": uninvested}


def arm_b(port, prices, rng):
    print("\n" + "=" * 96)
    print("ARM B — cross-sectional inverse-vol portfolio: ML σ̂ vs atr_14d/price (21d rebalance, 15bp RT)")
    print("=" * 96)
    R, P, A = build_matrices(port, prices)
    r_ml, d_ml = run_blocks(R, P, lambda s: 1.0 / s)
    r_atr, d_at = run_blocks(R, A, lambda s: 1.0 / s)
    r_ew, _ = run_blocks(R, np.where(np.isnan(P), np.nan, 1.0), lambda s: np.ones_like(s))
    for nm, dg in [("ML", d_ml), ("ATR", d_at)]:
        print(f"  {nm}: zero-filled (delist/gap) name-days {dg['zero_fill_days']:,}"
              f" · uninvested days {dg['uninvested_days']}")
    ok = verdict_line("inv-ML vs inv-ATR/price (registered)", r_ml, r_atr, rng)
    sh, g, dd = stats(r_ew)
    print(f"  context only: equal-weight Sharpe {sh:.2f}, growth {g*100:+.2f}%/yr, maxDD {dd*100:.1f}%")
    print(f"  ARM B verdict: {'PASS (risk-tool)' if ok else 'NULL'}")
    return ok


def arm_c(port, prices, rng):
    print("\n" + "=" * 96)
    print("ARM C — volatility-harvest basket: harvest(ML top-decile) vs harvest(ATR top-decile)")
    print("=" * 96)
    R, P, A = build_matrices(port, prices)

    def top_decile(sig):
        out = np.full_like(sig, np.nan)
        for i in range(sig.shape[0]):
            row = sig[i]; ok = ~np.isnan(row)
            if ok.sum() >= 50:
                thr = np.nanquantile(row, 0.9)
                out[i] = np.where(row >= thr, 1.0, np.nan)
        return out

    res = {}
    for nm, sig in [("ML", top_decile(P)), ("ATR", top_decile(A))]:
        rb, drb = run_blocks(R, sig, lambda s: np.ones_like(s), daily_eq=True)
        bh, dbh = run_blocks(R, sig, lambda s: np.ones_like(s), daily_eq=False)
        # Matched risk (amendment v2): lever BOTH legs of a basket by the same scalar
        # k = TARGET_VOL / realized vol of the buy-hold leg, so a hotter basket cannot
        # win the harvest comparison on sigma^2 alone.
        bh_vol = bh.std(ddof=1) * np.sqrt(ANN)
        k = TARGET_VOL / bh_vol if bh_vol > 0 else 1.0
        d_raw = _log1p_safe(rb) - _log1p_safe(bh)
        d_mat = _log1p_safe(k * rb) - _log1p_safe(k * bh)
        res[nm] = d_mat
        print(f"  {nm:<4} net harvest raw {d_raw.mean()*ANN*100:+.2f}%/yr"
              f" · matched@10%vol {d_mat.mean()*ANN*100:+.2f}%/yr"
              f" · basket bh vol {bh_vol*100:.1f}% (k={k:.2f})"
              f" · zero-filled {drb['zero_fill_days']:,}")
    lo, hi = diff_ci(res["ML"], res["ATR"], rng, "mean")   # matched log-diff streams
    print(f"  registered estimand Δharvest@10%vol (ML − ATR): "
          f"{(res['ML'].mean()-res['ATR'].mean())*ANN*100:+.2f}%/yr  CI[{lo*100:+.2f},{hi*100:+.2f}]")
    ok = lo > 0
    print(f"  ARM C verdict: {'PASS (risk-tool)' if ok else 'NULL'}")
    return ok


def main():
    global TRAIN_TEST, OOS_START, EXP_END, N_BOOT
    t0 = dt.datetime.now()
    which = sys.argv[sys.argv.index("--arm") + 1].upper() if "--arm" in sys.argv else "ABC"
    smoke = "--smoke" in sys.argv
    if smoke:
        TRAIN_TEST = [([2016], 2017)]
        OOS_START, EXP_END = dt.date(2017, 1, 1), dt.date(2017, 12, 31)
        N_BOOT = 200
    print("=" * 96)
    print("Phase 7 · Test 7.1 — marginal value of the ML magnitude forecast for risk overlays")
    print("Pre-registered NULL. PASS is risk-tool-only; alpha verdicts are unavailable by design.")
    print("Amendment v2 (pre-run) applied: entry-to-entry returns, atr/price baseline,")
    print("trading-day embargo, paired NaN policy, matched-risk CIs, per-arm seeds.")
    if smoke:
        print("*** SMOKE MODE: 2017 pseudo-OOS engineering test — NOT the registered run. ***")
        print("*** Verdict lines below carry NO pre-registered meaning. 2018-2020 stays blind. ***")
    print("=" * 96)
    verdicts = {}
    if "A" in which:
        verdicts["A"] = arm_a(np.random.default_rng([SEED, 1]))
    if "B" in which or "C" in which:
        port, prices = build_xs_forecast()
        if "B" in which:
            verdicts["B"] = arm_b(port, prices, np.random.default_rng([SEED, 2]))
        if "C" in which:
            verdicts["C"] = arm_c(port, prices, np.random.default_rng([SEED, 3]))
    print("\n--- pre-registered verdict ---" if not smoke else "\n--- smoke summary (no verdict) ---")
    for k, v in verdicts.items():
        print(f"  Arm {k}: {'PASS (risk-tool)' if v else 'NULL'}")
    print("Interpretation is fixed by phase7-preregistration.md: any PASS is a better RISK INPUT,")
    print("never alpha; a PASS also requires the 2021-22 confirmation protocol before belief.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
