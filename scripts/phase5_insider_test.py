#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 5 — INSIDER CLUSTER-BUYING test (the last free directional information source).

PRE-REGISTERED (frozen before the gate ran; see session log):
  * Signal at (name, day): officer/director open-market purchases with FILING date in the
    trailing 30 calendar days (day-30 .. day-1 -> entry at 10:00 is always info-safe).
      ANY     = >=1 O/D purchase-filing in window
      CLUSTER = >=2 distinct O/D buyers in window   (primary; literature's strong form)
  * Portfolio: calendar-time — each day hold all deployable names with the signal;
    outcome ret_21d_excess_spy @1000; daily series, block bootstrap block=21.
  * GATE (default mode, run FIRST): nd / names-per-day / MDE95 only — no signal-outcome
    means computed. Plausible post-2015 effect: ~20-60bp/21d gross for cluster buys.
    Gate rule: PASS MDE95 <= ~40bp · MARGIN 40-80 · FAIL > 80 (don't run the test).
  * TEST (--test, only if gate not FAIL): gross + net@20, CI, per-era signs, placebo
    (within-day shuffle of the signal), ANY vs CLUSTER, dollar-size split.
    PASS = CLUSTER net@20 > 0, CI excl 0, >=4/5 eras pos, placebo ~0.
  * Train 2016-06..2020-12 only. Validation 2021-22 on PASS. Holdout sealed.

Run: scripts/phase5_insider_test.py [--test]
"""
from __future__ import annotations
import glob, sys, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

TEST = "--test" in sys.argv
EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
YEARS = [2016, 2017, 2018, 2019, 2020]
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
WINDOW = 30
SEED = 20260706
N_BOOT = 4000
BLOCK = 21
MINP = 3
COSTS = [20]


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


def block_ci_se(x, rng, block=BLOCK, B=N_BOOT):
    n = len(x)
    if n < block + 5:
        return (np.nan, np.nan, np.nan)
    nb = int(np.ceil(n / block))
    idx = (rng.integers(0, n, size=(B, nb))[:, :, None] + np.arange(block)[None, None, :]) % n
    bm = x[idx.reshape(B, -1)[:, :n]].mean(axis=1)
    return float(np.quantile(bm, 0.025)), float(np.quantile(bm, 0.975)), float(bm.std(ddof=1))


def daily_series(df: pl.DataFrame, mask_expr) -> pl.DataFrame:
    return (df.filter(mask_expr)
            .group_by("day").agg(pl.col("ret_21d_excess_spy").mean().alias("port"),
                                 pl.len().alias("n"))
            .filter(pl.col("n") >= MINP).sort("day"))


def main():
    t0 = dt.datetime.now()
    mode = "TEST (pre-registered)" if TEST else "POWER GATE (variance only)"
    print("=" * 96)
    print(f"Phase 5 — INSIDER CLUSTER-BUYING · {mode} · train 2016-06..2020-12")
    print("=" * 96)

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select("day", "security_id"))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "ret_21d_excess_spy"))
    panel = (fo.join(cls, on=["day", "security_id"], how="inner")
             .filter(pl.col("ret_21d_excess_spy").is_not_null()).collect())

    sidmap = pl.read_parquet("data/phase1_analysis/edgar_sid_cik_map.parquet")
    buys = (pl.read_parquet("data/phase1_analysis/form4_purchases.parquet")
            .filter(pl.col("is_od"))
            .group_by("issuer_cik", "filing_date")
            .agg(pl.col("owner_cik").n_unique().alias("n_owners"),
                 pl.col("dollars").sum().alias("dollars")))
    panel = panel.join(sidmap, on="security_id", how="inner")
    print(f"panel rows: {panel.height:,} · purchase filing-days: {buys.height:,} "
          f"(O/D only)")

    sig = (panel.select("security_id", "cik", "day").join(
               buys.rename({"issuer_cik": "cik"}), on="cik", how="inner")
           .filter(((pl.col("day") - pl.col("filing_date")).dt.total_days()).is_between(1, WINDOW))
           .group_by("security_id", "day")
           .agg(pl.col("n_owners").sum().alias("buyers_30d"),
                pl.col("dollars").sum().alias("dollars_30d")))
    df = (panel.join(sig, on=["security_id", "day"], how="left")
          .with_columns(pl.col("buyers_30d").fill_null(0), pl.col("dollars_30d").fill_null(0.0))
          .with_columns(pl.col("day").dt.year().alias("yr")))
    n_any = df.filter(pl.col("buyers_30d") >= 1).height
    n_clu = df.filter(pl.col("buyers_30d") >= 2).height
    print(f"signal rows: ANY {n_any:,} ({100*n_any/df.height:.2f}%) · "
          f"CLUSTER {n_clu:,} ({100*n_clu/df.height:.2f}%)\n")
    rng = np.random.default_rng(SEED)

    tiers = [("ANY  (>=1 buyer)", pl.col("buyers_30d") >= 1),
             ("CLUSTER (>=2)", pl.col("buyers_30d") >= 2),
             ("CLUSTER+$100k", (pl.col("buyers_30d") >= 2) & (pl.col("dollars_30d") >= 1e5))]

    if not TEST:
        print(f"{'tier':<18} {'nd':>5} {'names/day':>10} {'MDE95':>9}")
        for nm, expr in tiers:
            ds = daily_series(df, expr)
            port = ds["port"].to_numpy()
            _, _, se = block_ci_se(port, rng)
            print(f"{nm:<18} {ds.height:>5} {ds['n'].mean() if ds.height else float('nan'):>10.1f} "
                  f"{1.96*se*1e4:>7.1f}bp")
        print("\n--- gate rule (frozen): PASS <=40bp · MARGIN 40-80 · FAIL >80 (don't run) ---")
        print("plausible post-2015 cluster-buy effect: ~20-60bp/21d gross")
        print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")
        return

    # ---- TEST mode ----
    for nm, expr in tiers:
        ds = daily_series(df, expr)
        port = ds["port"].to_numpy()
        d = ds["day"].to_numpy().astype("datetime64[D]")
        if len(port) < 40:
            print(f"  {nm}: insufficient days ({len(port)})"); continue
        lo, hi, _ = block_ci_se(port, rng)
        yrs = d.astype("datetime64[Y]").astype(int) + 1970
        signs = [np.sign(port[yrs == y].mean()) for y in YEARS if (yrs == y).sum() >= 10]
        pos = sum(1 for s in signs if s > 0)
        g = port.mean() * 1e4
        c = COSTS[0]
        print(f"  {nm:<18} gross {g:+7.1f}  CI[{lo*1e4:+6.1f},{hi*1e4:+6.1f}]  "
              f"net@{c} {g-c:+7.1f} CI[{lo*1e4-c:+6.1f},{hi*1e4-c:+6.1f}]  "
              f"nd={len(port)}  eras {pos}/{len(signs)} pos")

    # placebo: shuffle the signal across names within day (CLUSTER tier)
    print("\n── placebo: CLUSTER signal shuffled within day ──")
    dfp = df.select("day", "security_id", "buyers_30d", "ret_21d_excess_spy")
    sh = (dfp.with_columns(pl.col("buyers_30d").shuffle(seed=SEED).over("day").alias("b_sh")))
    ds = (sh.filter(pl.col("b_sh") >= 2)
          .group_by("day").agg(pl.col("ret_21d_excess_spy").mean().alias("port"), pl.len().alias("n"))
          .filter(pl.col("n") >= MINP).sort("day"))
    port = ds["port"].to_numpy()
    lo, hi, _ = block_ci_se(port, rng)
    print(f"  placebo CLUSTER: {port.mean()*1e4:+6.1f}  CI[{lo*1e4:+6.1f},{hi*1e4:+6.1f}]  nd={len(port)}")

    print("\n--- pre-registered verdict rule ---")
    print("PASS = CLUSTER net@20 > 0, CI excludes 0, >=4/5 eras pos, placebo ~0; else NULL")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
