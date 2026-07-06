#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 9b — selection-adjusted graduation test for the "quiet vol expansion" cell
(phase9b-preregistration.md, frozen). Candidate: (large, volT_up, volmT_dn), 21d
primary. Null: 1000 within-(day,cap) permutations of the (vt,mt) pair, family-max |t|
over 27 cells x 2 horizons. Registered prior: dies.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
N_PERM, SEED = 1000, 20260708


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


def build():
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select("day", "security_id", "market_cap_bucket"))
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "atr_5d", "atr_42d", "adv_5d", "adv_60d"))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "ret_5d_excess_spy", "ret_21d_excess_spy"))
    df = (cls.join(do, on=["day", "security_id"], how="inner")
             .join(fo, on=["day", "security_id"], how="left")
             .with_columns((pl.col("atr_5d") / pl.col("atr_42d")).alias("vol_trend"),
                           (pl.col("adv_5d") / pl.col("adv_60d")).alias("volm_trend"))
             .collect()
             .sort(["security_id", "day"])
             # atlas clock rule: outcomes from the NEXT day's 10:00 entry
             .with_columns(pl.col("ret_5d_excess_spy").shift(-1).over("security_id"),
                           pl.col("ret_21d_excess_spy").shift(-1).over("security_id"))
             .with_columns([(pl.col(v).rank("average").over("day") / pl.col(v).count().over("day"))
                            .alias(f"rk_{v}") for v in ("vol_trend", "volm_trend")])
             .drop_nulls(["rk_vol_trend", "rk_volm_trend", "ret_21d_excess_spy", "ret_5d_excess_spy"]))
    ter = lambda c: pl.when(pl.col(c) < 1/3).then(0).when(pl.col(c) > 2/3).then(2).otherwise(1)
    df = df.with_columns(ter("rk_vol_trend").alias("vt"), ter("rk_volm_trend").alias("mt"))
    return df


def cell_ts(day_ix, cap, cell9, x, n_days):
    """day-clustered t per (cap,cell9): mean over days of per-day cell means / se."""
    slot = (day_ix * 27 + cap * 9 + cell9)
    sums = np.bincount(slot, weights=x, minlength=n_days * 27)
    cnts = np.bincount(slot, minlength=n_days * 27)
    M = (sums / np.where(cnts == 0, np.nan, cnts)).reshape(n_days, 27)
    mu = np.nanmean(M, axis=0)
    D = np.sum(~np.isnan(M), axis=0)
    sd = np.nanstd(M, axis=0, ddof=1)
    se = sd / np.sqrt(np.maximum(D, 1))
    return mu, se, mu / se


def main():
    t0 = dt.datetime.now()
    print("=" * 96)
    print("Phase 9b · quiet-vol-expansion graduation — family-max permutation null (prior: dies)")
    print("=" * 96)
    df = build()
    days = {d: i for i, d in enumerate(sorted(df["day"].unique().to_list()))}
    n_days = len(days)
    day_ix = df["day"].replace_strict(days, return_dtype=pl.Int32).to_numpy()
    cap_ix = df["market_cap_bucket"].replace_strict(
        {"mega": 0, "large": 1, "mid": 2}, return_dtype=pl.Int8).to_numpy().astype(np.int64)
    cell9 = (df["vt"].to_numpy().astype(np.int64) * 3 + df["mt"].to_numpy()).astype(np.int64)
    x5 = df["ret_5d_excess_spy"].to_numpy()
    x21 = df["ret_21d_excess_spy"].to_numpy()
    print(f"panel rows {len(x5):,} · days {n_days}")

    TARGET = 1 * 9 + 2 * 3 + 0   # cap=large(1), vt=up(2), mt=dn(0)
    mu5, se5, t5 = cell_ts(day_ix, cap_ix, cell9, x5, n_days)
    mu21, se21, t21 = cell_ts(day_ix, cap_ix, cell9, x21, n_days)
    print(f"observed cell (large, volT_up, volmT_dn): "
          f"5d {mu5[TARGET]*1e4:+.1f}bp (t={t5[TARGET]:+.2f}) · "
          f"21d {mu21[TARGET]*1e4:+.1f}bp (t={t21[TARGET]:+.2f})")

    # era stability (calendar years) of the 21d cell mean
    yr = df["day"].dt.year().to_numpy()
    in_cell = (cap_ix == 1) & (cell9 == 2 * 3 + 0)
    eras = {}
    for y in range(2016, 2021):
        m = in_cell & (yr == y)
        eras[y] = float(x21[m].mean()) if m.sum() else float("nan")
    era_ok = len({np.sign(v) for v in eras.values() if not np.isnan(v)}) == 1
    print("era means 21d (bp): " + "  ".join(f"{y}:{v*1e4:+.1f}" for y, v in eras.items())
          + f"  -> sign-stable 5/5: {era_ok}")

    # permutation: shuffle (vt,mt) pair within (day, cap)
    rng = np.random.default_rng(SEED)
    seg = day_ix * 3 + cap_ix                     # segment id per row
    order = np.argsort(seg, kind="stable")        # rows grouped by segment
    seg_sorted = seg[order]
    maxt = np.empty(N_PERM)
    for p in range(N_PERM):
        keys = rng.random(len(seg))
        perm_within = order[np.lexsort((keys, seg_sorted))]  # random order within segment
        c9p = np.empty_like(cell9)
        c9p[order] = cell9[perm_within]           # permuted labels, same segments
        _, _, tt5 = cell_ts(day_ix, cap_ix, c9p, x5, n_days)
        _, _, tt21 = cell_ts(day_ix, cap_ix, c9p, x21, n_days)
        maxt[p] = max(np.nanmax(np.abs(tt5)), np.nanmax(np.abs(tt21)))
        if (p + 1) % 200 == 0:
            print(f"  perm {p+1}/{N_PERM}", flush=True)
    p95 = float(np.quantile(maxt, 0.95))
    obs = abs(t21[TARGET])
    frac = float((maxt >= obs).mean())
    print(f"\nnull family-max |t|: median {np.median(maxt):.2f} · 95pct {p95:.2f}")
    print(f"observed 21d |t| = {obs:.2f} -> selection-adjusted p = {frac:.3f}"
          f"  ({'clears' if obs > p95 else 'does NOT clear'} the 95pct family bar)")
    net = mu21[TARGET] - 15e-4
    print(f"net economics context: {mu21[TARGET]*1e4:+.1f}bp/21d gross − 15bp RT = {net*1e4:+.1f}bp/21d")
    verdict = "PASS" if (obs > p95 and era_ok and net > 0) else "FAIL — atlas-noise, recorded, closed"
    print(f"\nREGISTERED VERDICT: {verdict}")
    print(f"WALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
