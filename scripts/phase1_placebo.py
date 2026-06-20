#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — §7.8 step 0.5 PLACEBO VALIDATION of the blacklist Part-B FDR machinery.

The blacklist's empirical layer rejected 23/35 cells (gross-negative test gated 5
as ROBUST). Before trusting that, validate the day-clustered bootstrap + BY
pipeline isn't over-rejecting, and separate SIGNAL-driven negativity from
UNCONDITIONAL cell effects (size underperformance, cost).

Method (the doc's "signals shifted by a random number of weeks"): load the full
CS base table once, then re-pair each day's OUTCOME with the signal taken from a
day L trading-days away (12 lags, ±21…±168). A large lag decorrelates the signal
from the day's 1d outcome while preserving the signal's marginal firing structure.
Re-run the identical gross-negative BY test per placebo; compare the REAL-signal
survivor count to the placebo distribution.

Read:  real ≈ placebo  → negativity is unconditional cell structure, NOT signal-
       driven, and the FDR is calibrated (no spurious signal discoveries).
       real ≫ placebo  → the machinery over-rejects / signal manufactures findings.

Read-only. Reuses the blacklist's family (cap×liq×price, offset 1000, exploration).
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
N_BOOT = 2000
SEED = 20260620
LAGS = [21, 42, 63, 84, 105, 126, 147, 168, -21, -42, -63, -84]
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


def by_count(pvals, alpha=ALPHA):
    m = len(pvals)
    if m == 0:
        return 0
    Hm = harmonic(m)
    sp = np.sort(pvals)
    thresh = (np.arange(1, m + 1) * alpha) / (m * Hm)
    below = sp <= thresh
    return int(np.max(np.where(below)[0]) + 1) if below.any() else 0


def gross_negative_pvals(trades, rng):
    """Run the blacklist's per-cell gross-negative bootstrap on a signal-filtered
    trade frame; return (p-values array, cell keys) for testable cells."""
    daily = (trades.group_by(CELL + ["day"])
             .agg(pl.col("gross").mean().alias("g"), pl.len().alias("n"))
             .filter(pl.col("n") >= MIN_NAMES))
    cells = (daily.group_by(CELL).agg(pl.col("day").n_unique().alias("nd"))
             .filter(pl.col("nd") >= MIN_DAYS))
    keys = [tuple(r) for r in cells.select(CELL).iter_rows()]
    pv = []
    for key in keys:
        cond = pl.lit(True)
        for c, v in zip(CELL, key):
            cond = cond & (pl.col(c) == v)
        g = daily.filter(cond)["g"].to_numpy()
        nd = len(g)
        bm = g[rng.integers(0, nd, size=(N_BOOT, nd))].mean(axis=1)
        pv.append(float(np.mean(bm >= 0.0)))   # one-sided p for mean<0
    return np.array(pv), keys


def main():
    t0 = dt.datetime.now()
    print("=" * 84)
    print("Phase 1 — PLACEBO VALIDATION  (§7.8 step 0.5; blacklist Part-B FDR)")
    print("=" * 84)

    # base: all CS trades @ offset 1000 with cell + gross excess (NO signal filter)
    cls = (pl.scan_parquet(window_files("security_classification_daily"))
           .filter(pl.col("ticker_type") == "CS").select(["day", "security_id"] + CELL))
    base = (pl.scan_parquet(window_files("forward_outcomes"))
            .filter((pl.col("entry_offset") == OFFSET) & pl.col("ret_1d_excess_spy").is_not_null())
            .select("day", "security_id", pl.col("ret_1d_excess_spy").alias("gross"))
            .join(cls, on=["day", "security_id"], how="inner").drop_nulls(CELL)
            .collect())
    sig = (pl.scan_parquet(window_files("daily_observation"))
           .filter(pl.col("intraday_ret_0930_to_1000") > 0.0)
           .select("day", "security_id").with_columns(pl.lit(True).alias("fires"))
           .collect())
    print(f"base CS trades @1000: {base.height:,} · signal-firing rows: {sig.height:,}")

    # global trading-day calendar for lag remapping
    days = sorted(base["day"].unique().to_list())
    idx = {d: i for i, d in enumerate(days)}

    def shifted_sig(L):
        # relabel each signal day d -> day L positions later, so outcome day D
        # pairs with the signal from day D-L (decorrelated by |L| trading days)
        remap = {days[i]: days[i + L] for i in range(len(days))
                 if 0 <= i + L < len(days)}
        return (sig.with_columns(pl.col("day").replace_strict(remap, default=None).alias("day"))
                .drop_nulls("day"))

    rng = np.random.default_rng(SEED)

    # REAL signal
    real_tr = base.join(sig, on=["day", "security_id"], how="inner")
    pv_real, keys_real = gross_negative_pvals(real_tr, rng)
    real_surv = by_count(pv_real)
    m = len(keys_real)
    print(f"\nfamily m={m} (H_m={harmonic(m):.2f}) · BY α={ALPHA}")
    print(f"REAL signal     : {real_surv}/{m} cells gross-negative (BY)")

    # PLACEBO shifts
    placebo_counts = []
    for L in LAGS:
        tr = base.join(shifted_sig(L), on=["day", "security_id"], how="inner")
        pv, _ = gross_negative_pvals(tr, rng)
        c = by_count(pv)
        placebo_counts.append(c)
        print(f"  placebo lag {L:+4d}d : {c}/{len(pv)} gross-negative (BY)")

    pc = np.array(placebo_counts)
    print("\n--- verdict ---")
    print(f"REAL survivors      : {real_surv}")
    print(f"placebo survivors   : mean={pc.mean():.1f}  median={np.median(pc):.0f}  "
          f"min={pc.min()}  max={pc.max()}")
    delta = real_surv - pc.mean()
    print(f"signal marginal Δ   : {delta:+.1f} cells (real − placebo mean)")
    # is real within the placebo distribution?
    pctile = float(np.mean(pc <= real_surv)) * 100
    print(f"real is at the {pctile:.0f}th percentile of the placebo distribution")
    print()
    if abs(delta) <= max(2.0, pc.std() * 1.5):
        print("CALIBRATED: real ≈ placebo → gross-negativity is UNCONDITIONAL cell")
        print("structure (size/cost), not signal-driven. The FDR is not manufacturing")
        print("signal-specific discoveries; the blacklist is a cell-level map, as §2.2")
        print("predicts. ROBUST cells are real cell effects (not signal artifacts).")
    else:
        print("WARNING: real diverges from placebo — investigate before trusting Part B.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
