#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — §7.9 SYNTHETIC KNOWN-EDGE INJECTION (detection-power validation).

The placebo (§7.8 step 0.5) proved the pipeline doesn't INVENT findings
(false-positive control). This proves the complement: that it can DETECT an edge
that genuinely exists — and is correctly BLIND below the MDE floor. Together they
earn "our nulls are real nulls, not pipeline blindness."

Method: take a real cell's daily GROSS-excess series (real volatility / fat tails
/ N), inject a known-annualized-Sharpe drift, and push it through the IDENTICAL
machinery used in discovery — day-clustered bootstrap → one-sided positive test →
Benjamini-Yekutieli over the real m=35 family (34 real ≈null cells + the injected
one). Detection = the injected cell survives BY on the positive side. Repeat K
times per Sharpe → an empirical power curve.

Expected (cross-check vs §1.5.1): realized t ≈ Sharpe·√(N/252). For the family
MDE (strict ≈1.4), power should be ~0 at Sharpe 0.5, cross ~50% near Sharpe 1.5,
and saturate by 2–3 — and the smaller-N cell should need a bigger edge.

Read-only. Reuses the blacklist family/series exactly.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CELL = ["market_cap_bucket", "liquidity_bucket", "price_bucket"]
MIN_NAMES, MIN_DAYS = 5, 60
ALPHA = 0.10
TRADING_DAYS_YR = 252
N_BOOT = 5000
K_TRIALS = 200
SEED = 20260620
SHARPES = [0.0, 0.5, 1.0, 1.5, 2.0, 3.0]
INJECT_CELLS = [("large", "liquid", "above_100"), ("mega", "liquid", "20_to_100")]
OUT = Path("data/phase1_analysis")


def window_files(tbl):
    out = []
    for f in sorted(glob.glob(f"data/outputs/{tbl}/*.parquet")):
        try:
            d = dt.date.fromisoformat(Path(f).stem)
        except ValueError:
            continue
        if EXP_START <= d <= EXP_END:
            out.append(f)
    return out


def harmonic(m):
    return float(np.sum(1.0 / np.arange(1, m + 1))) if m > 0 else 1.0


def by_reject_mask(pvals, alpha=ALPHA):
    m = len(pvals)
    Hm = harmonic(m)
    order = np.argsort(pvals)
    sp = np.asarray(pvals)[order]
    thr = (np.arange(1, m + 1) * alpha) / (m * Hm)
    below = sp <= thr
    k = int(np.max(np.where(below)[0]) + 1) if below.any() else 0
    rej_sorted = np.zeros(m, dtype=bool); rej_sorted[:k] = True
    out = np.zeros(m, dtype=bool); out[order] = rej_sorted
    return out


def pos_pvalue(series, rng, B=N_BOOT):
    """One-sided bootstrap p for mean>0 = frac of day-resampled means <= 0."""
    n = len(series)
    bm = series[rng.integers(0, n, size=(B, n))].mean(axis=1)
    return float(np.mean(bm <= 0.0))


def main():
    t0 = dt.datetime.now()
    print("=" * 84)
    print("Phase 1 — SYNTHETIC KNOWN-EDGE INJECTION  (§7.9 detection power)")
    print("=" * 84)

    # rebuild per-cell daily GROSS-excess series for the m=35 family
    cls = (pl.scan_parquet(window_files("security_classification_daily"))
           .filter(pl.col("ticker_type") == "CS").select(["day", "security_id"] + CELL))
    sig = (pl.scan_parquet(window_files("daily_observation"))
           .filter(pl.col("intraday_ret_0930_to_1000") > 0.0).select("day", "security_id"))
    daily = (pl.scan_parquet(window_files("forward_outcomes"))
             .filter((pl.col("entry_offset") == OFFSET) & pl.col("ret_1d_excess_spy").is_not_null())
             .select("day", "security_id", pl.col("ret_1d_excess_spy").alias("g"))
             .join(cls, on=["day", "security_id"], how="inner").drop_nulls(CELL)
             .join(sig, on=["day", "security_id"], how="inner")
             .group_by(CELL + ["day"]).agg(pl.col("g").mean().alias("g"), pl.len().alias("n"))
             .filter(pl.col("n") >= MIN_NAMES).collect())
    cells = (daily.group_by(CELL).agg(pl.col("day").n_unique().alias("nd"))
             .filter(pl.col("nd") >= MIN_DAYS).sort("nd", descending=True))
    keys = [tuple(r) for r in cells.select(CELL).iter_rows()]
    m = len(keys)
    series = {}
    for key in keys:
        cond = pl.lit(True)
        for c, v in zip(CELL, key):
            cond = cond & (pl.col(c) == v)
        series[key] = daily.filter(cond)["g"].to_numpy()
    print(f"family m={m} (H_m={harmonic(m):.2f}), BY α={ALPHA}, "
          f"rank-1 threshold p≤{ALPHA/(m*harmonic(m)):.2e}")

    rng = np.random.default_rng(SEED)
    # fixed null p-values for all real cells (positive side) — the family backdrop
    null_p = {k: pos_pvalue(series[k], rng) for k in keys}

    print(f"\nK={K_TRIALS} trials/Sharpe · B={N_BOOT} · power = P(injected cell survives BY+)")
    for inj in INJECT_CELLS:
        x = series[inj]
        N = len(x)
        sigma = x.std(ddof=1)
        noise = x - x.mean()
        idx_inj = keys.index(inj)
        others = [null_p[k] for k in keys if k != inj]
        print(f"\ninjected cell {inj}  (N={N}, σ_daily={sigma*1e4:.0f}bps)")
        print(f"  {'Sharpe':>7}{'real t≈':>9}{'power':>9}")
        for S in SHARPES:
            mu = S * sigma / np.sqrt(TRADING_DAYS_YR)   # daily drift for target ann Sharpe
            det = 0
            for _ in range(K_TRIALS):
                synth = noise[rng.integers(0, N, size=N)] + mu
                p_inj = pos_pvalue(synth, rng)
                fam = np.array([p_inj] + others)
                if by_reject_mask(fam)[0]:
                    det += 1
            t_real = S * np.sqrt(N / TRADING_DAYS_YR)
            print(f"  {S:>7.1f}{t_real:>9.2f}{det/K_TRIALS:>9.0%}")

    print("\n--- verdict ---")
    print("If power ≈ 0 at Sharpe 0.5 and rises to ≈1 by Sharpe 2–3 (crossing ~50%")
    print("near the family MDE), the pipeline DETECTS detectable edges and is correctly")
    print("BLIND below the floor — so the all-null Phase-1 results are real nulls, not")
    print("pipeline blindness. The smaller-N cell needing a bigger edge confirms §1.5.1.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
