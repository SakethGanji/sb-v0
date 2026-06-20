#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — SYSTEMATIC CROSS-SECTIONAL FACTOR SCAN (hundreds of cheap runs).

Not hand-picked signals — EVERY pre-entry feature × EVERY horizon, the way the
aggregate dataset was built to be used. For each (feature, horizon): rank all
securities into cross-sectional deciles WITHIN each day, measure the top-decile
minus bottom-decile mean forward excess-over-SPY return (a beta-neutral, within-day,
high-power long-short spread). Then test:
  - significance: day-clustered bootstrap CI on the daily spread series;
  - CONSISTENCY: sign of the spread per era (2016-2020) — stable = real structure;
  - multiplicity: Benjamini-Yekutieli over the full feature×horizon grid.

Signal-agnostic, full relaxed universe (CS + ETFs + ADRs). This is the broad
"find ANY consistent structure" scan. Read-only; writes sweep output parquet.
"""
from __future__ import annotations
import glob, datetime as dt
from itertools import combinations
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
YEARS = [2016, 2017, 2018, 2019, 2020]
HORIZONS = ["EOD", "1d", "2d", "5d", "10d", "21d"]
ALPHA = 0.10
N_BOOT = 2000
SEED = 20260620
MIN_PER_DECILE = 5
OUT = Path("data/phase1_analysis")

# pre-entry features knowable by 10:00 ET (no look-ahead). levels are fine — the
# cross-sectional decile rank makes them scale-free (size/liquidity/vol factors).
DO_FEATS = [
    "intraday_ret_0930_to_0940", "intraday_ret_0930_to_0950", "intraday_ret_0930_to_1000",
    "intraday_volume_0930_to_1000", "intraday_dollar_volume_0930_to_1000",
    "intraday_first_30m_high_return", "intraday_first_30m_low_return",
    "intraday_ret_from_first_30m_high_to_1000", "intraday_first_15m_volume_share_of_first_30m",
    "overnight_gap", "premarket_volume", "premarket_dollar_volume", "premarket_volume_vs_20d_median",
    "atr_5d", "atr_14d", "atr_42d", "addv_5d", "addv_20d", "addv_60d",
    "beta_spy_60d", "beta_qqq_60d", "beta_iwm_60d", "realized_vol_21d_rank_today",
    "signal_concentration_percentile_today", "signal_concentration_hhi_today",
    "days_since_last_5pct_move", "days_since_last_10pct_move", "days_since_last_20pct_move",
    "consecutive_up_days_close_to_close", "days_since_last_earnings", "days_since_first_bar",
    "bar_count_premarket", "bar_count_first_30m",
]
FO_FEATS = [
    "pre_entry_ret_from_open", "pre_entry_vwap_from_open", "pre_entry_ret_from_high",
    "pre_entry_ret_from_low", "entry_bar_upper_wick_pct", "entry_bar_lower_wick_pct",
    "entry_slippage_proxy_bps", "entry_range_vs_atr_14d", "entry_dollar_volume_vs_addv_20d",
    "cumulative_volume_to_entry",
]


def window_files(t):
    out = []
    for f in sorted(glob.glob(f"data/outputs/{t}/*.parquet")):
        try:
            d = dt.date.fromisoformat(Path(f).stem)
        except ValueError:
            continue
        if EXP_START <= d <= EXP_END:
            out.append(f)
    return out


def harmonic(m):
    return float(np.sum(1.0 / np.arange(1, m + 1))) if m > 0 else 1.0


def by_mask(p, alpha=ALPHA):
    m = len(p); o = np.argsort(p); sp = np.asarray(p)[o]
    thr = (np.arange(1, m + 1) * alpha) / (m * harmonic(m))
    below = sp <= thr
    k = int(np.max(np.where(below)[0]) + 1) if below.any() else 0
    rs = np.zeros(m, bool); rs[:k] = True
    out = np.zeros(m, bool); out[o] = rs
    return out


def main():
    t0 = dt.datetime.now(); OUT.mkdir(parents=True, exist_ok=True)
    print("=" * 92)
    print("Phase 1 — SYSTEMATIC CROSS-SECTIONAL FACTOR SCAN (feature × horizon)")
    print("=" * 92)
    do_have = set(pl.scan_parquet(window_files("daily_observation")).collect_schema().names())
    fo_have = set(pl.scan_parquet(window_files("forward_outcomes")).collect_schema().names())
    do_feats = [f for f in DO_FEATS if f in do_have]
    fo_feats = [f for f in FO_FEATS if f in fo_have]
    feats = do_feats + fo_feats
    hz_cols = [f"ret_{h}_excess_spy" for h in HORIZONS if f"ret_{h}_excess_spy" in fo_have]
    hzs = [h for h in HORIZONS if f"ret_{h}_excess_spy" in fo_have]

    df = (pl.scan_parquet(window_files("forward_outcomes"))
          .filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + fo_feats + hz_cols)
          .join(pl.scan_parquet(window_files("daily_observation")).select(["day", "security_id"] + do_feats),
                on=["day", "security_id"], how="inner")
          # cross-sectional percentile rank per day for each feature
          .with_columns([(pl.col(f).rank("average").over("day") /
                          pl.col(f).count().over("day")).alias(f"__q_{f}") for f in feats])
          .with_columns(pl.col("day").dt.year().alias("yr"))
          .collect())
    print(f"universe @1000: {df.height:,} trades · {len(feats)} features × {len(hzs)} horizons "
          f"= {len(feats)*len(hzs)} hypotheses\n")

    rng = np.random.default_rng(SEED)
    yr = df["yr"].to_numpy(); day = df["day"].to_numpy()
    recs = []
    for f in feats:
        q = df[f"__q_{f}"].to_numpy()
        top = q >= 0.9; bot = q <= 0.1
        for h in hzs:
            o = df[f"ret_{h}_excess_spy"].to_numpy()
            ok = ~np.isnan(o)
            tm = top & ok; bm = bot & ok
            # daily top-minus-bottom spread
            def daily_means(mask):
                d = day[mask]; x = o[mask]
                order = np.argsort(d); ds, xs = d[order], x[order]
                u, idx = np.unique(ds, return_index=True)
                ends = np.append(idx[1:], len(xs))
                cnt = ends - idx
                mu = np.array([xs[idx[i]:ends[i]].mean() for i in range(len(u))])
                return u, mu, cnt
            ut, mt, ct = daily_means(tm); ub, mb, cb = daily_means(bm)
            common = np.intersect1d(ut[ct >= MIN_PER_DECILE], ub[cb >= MIN_PER_DECILE])
            if len(common) < 60:
                continue
            spr = mt[np.isin(ut, common)] - mb[np.isin(ub, common)]
            n = len(spr); mean = spr.mean()
            boot = spr[rng.integers(0, n, size=(N_BOOT, n))].mean(axis=1)
            p_two = 2 * min(np.mean(boot <= 0), np.mean(boot >= 0))
            lb, ub_ = np.quantile(boot, 0.025), np.quantile(boot, 0.975)
            # era sign consistency
            signs = []
            for y in YEARS:
                ym = yr == y
                ut2, mt2, ct2 = daily_means(tm & ym); ub2, mb2, cb2 = daily_means(bm & ym)
                cc = np.intersect1d(ut2[ct2 >= MIN_PER_DECILE], ub2[cb2 >= MIN_PER_DECILE])
                if len(cc) >= 20:
                    s = (mt2[np.isin(ut2, cc)] - mb2[np.isin(ub2, cc)]).mean()
                    signs.append(np.sign(s))
            era_consistent = len(signs) == len(YEARS) and (all(s > 0 for s in signs) or all(s < 0 for s in signs))
            recs.append(dict(feature=f, horizon=h, spread_bps=mean * 1e4,
                             ci_lo_bps=lb * 1e4, ci_hi_bps=ub_ * 1e4, p_two=p_two,
                             n_days=n, era_consistent=bool(era_consistent),
                             n_eras_same_sign=int(max(sum(s > 0 for s in signs), sum(s < 0 for s in signs)))))
    res = pl.DataFrame(recs)
    rej = by_mask(res["p_two"].to_numpy())
    res = res.with_columns(pl.Series("by_survivor", rej))
    res.write_parquet(OUT / "factor_scan.parquet")

    nby = int(res["by_survivor"].sum())
    nstable = res.filter(pl.col("by_survivor") & pl.col("era_consistent")).height
    print(f"BY survivors (α={ALPHA}): {nby}/{res.height} · "
          f"AND era-sign-consistent (all 5 years): {nstable}")
    print("\n--- top 20 by |spread|, with significance + era stability ---")
    top = res.with_columns(pl.col("spread_bps").abs().alias("a")).sort("a", descending=True).head(20)
    print(f"  {'feature':<42}{'hz':>4}{'spread':>9}{'95% CI':>20}{'BY':>4}{'eras':>6}")
    for r in top.iter_rows(named=True):
        by = "✓" if r["by_survivor"] else ""
        st = f"{r['n_eras_same_sign']}/5" + ("*" if r["era_consistent"] else "")
        print(f"  {r['feature']:<42}{r['horizon']:>4}{r['spread_bps']:>8.1f}b"
              f"  [{r['ci_lo_bps']:>6.1f},{r['ci_hi_bps']:>6.1f}]{by:>4}{st:>6}")

    print("\n--- the consistency shortlist: BY-significant AND all-5-eras-same-sign ---")
    sl = res.filter(pl.col("by_survivor") & pl.col("era_consistent")).sort(
        pl.col("spread_bps").abs(), descending=True)
    if sl.height == 0:
        print("  (none — no feature ranks forward excess return both significantly AND")
        print("   sign-consistently across all five eras)")
    else:
        for r in sl.iter_rows(named=True):
            print(f"  {r['feature']:<42}{r['horizon']:>4}  spread={r['spread_bps']:+.1f}b  "
                  f"5/5 same sign  (CI [{r['ci_lo_bps']:+.1f},{r['ci_hi_bps']:+.1f}])")

    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
