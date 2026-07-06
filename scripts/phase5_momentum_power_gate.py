#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 5 — POWER GATE for the long-horizon momentum test (run BEFORE the test).

The gap: every momentum test so far used lookbacks <= 21d and holds <= 21d. Classic
cross-sectional momentum is 6-12 MONTH lookback, 1-3 month hold. ret_42d/ret_63d exist in
forward_outcomes and were never used.

QUESTION (gate only — no signal-outcome means are computed or printed, so the test can
still be pre-registered cleanly): given the deployable universe and the available window,
what is the minimum detectable long-leg effect (bp per hold, 95% CI excluding 0) for
  "rank by trailing 12-1 (or 6-1) return, hold top quintile for 21/42/63 trading days,
   excess-over-SPY, daily overlapping series, block bootstrap (block = horizon)"?

Constraint the gate must surface: a 252d lookback consumes the first year of the window —
12-1 entries exist only from ~2017-06 (about 3.5yr); 6-1 from ~2017-01.

MDE95 here = 1.96 * block-bootstrap SE of the daily top-quintile portfolio series' mean,
computed on the REAL series but WITHOUT reporting its mean (variance-only peek, same
convention as phase4_pead_power_gate).

Calibration context (printed): post-2010 large/mid-cap 12-1 long-leg gross premia in the
literature are roughly 10-30bp/month above market -> ~30-90bp per 63d hold; the amortized
cost is ~20bp per round trip.

Run: scripts/phase5_momentum_power_gate.py
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
SEED = 20260706
N_BOOT = 4000
MIN_NAMES_DAY = 100     # need a real cross-section to rank
QUANT = 0.8             # top quintile
LOOKBACKS = {"12-1": (252, 21), "6-1": (126, 21)}
HORIZONS = {"21d": 21, "42d": 42, "63d": 63}


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


def block_se(x, rng, block, B=N_BOOT):
    n = len(x)
    if n < block + 5:
        return np.nan
    nb = int(np.ceil(n / block))
    idx = (rng.integers(0, n, size=(B, nb))[:, :, None] + np.arange(block)[None, None, :]) % n
    bm = x[idx.reshape(B, -1)[:, :n]].mean(axis=1)
    return float(bm.std(ddof=1))


def main():
    t0 = dt.datetime.now()
    print("=" * 96)
    print("Phase 5 — POWER GATE: long-horizon momentum (12-1 / 6-1 x 21/42/63d holds)")
    print("=" * 96)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select("day", "security_id"))
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "eod_day_close")
          .join(cls, on=["day", "security_id"], how="inner")
          .sort(["security_id", "day"]))
    for nm, (lb, skip) in LOOKBACKS.items():
        do = do.with_columns(
            (pl.col("eod_day_close").shift(skip) / pl.col("eod_day_close").shift(lb) - 1.0)
            .over("security_id").alias(f"mom_{nm}"))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + [f"ret_{h}_excess_spy" for h in HORIZONS]))
    df = do.join(fo, on=["day", "security_id"], how="inner").collect()
    day = df["day"].to_numpy().astype("datetime64[D]")
    rng = np.random.default_rng(SEED)
    print(f"panel rows: {df.height:,}\n")

    print(f"{'signal':>6} {'horizon':>8} {'nd':>6} {'first day':>12} {'names/day':>10} "
          f"{'MDE95 long-leg':>15} {'MDE95 spread':>13}")
    for nm in LOOKBACKS:
        sig_all = df[f"mom_{nm}"].to_numpy()
        for hz, hdays in HORIZONS.items():
            out = df[f"ret_{hz}_excess_spy"].to_numpy()
            m = ~np.isnan(sig_all) & ~np.isnan(out)
            d, s, o = day[m], sig_all[m], out[m]
            order = np.argsort(d, kind="stable")
            d, s, o = d[order], s[order], o[order]
            u, idx = np.unique(d, return_index=True)
            ends = np.append(idx[1:], len(d))
            top, spr, sizes = [], [], []
            first = None
            for i in range(len(u)):
                a, b = idx[i], ends[i]
                if b - a < MIN_NAMES_DAY:
                    continue
                if first is None:
                    first = u[i]
                ss, oo = s[a:b], o[a:b]
                hi = ss >= np.quantile(ss, QUANT)
                lo = ss <= np.quantile(ss, 1 - QUANT)
                top.append(oo[hi].mean())
                spr.append(oo[hi].mean() - oo[lo].mean())
                sizes.append(int(hi.sum()))
            top, spr = np.array(top), np.array(spr)
            se_t = block_se(top, rng, hdays)
            se_s = block_se(spr, rng, hdays)
            print(f"{nm:>6} {hz:>8} {len(top):>6} {str(first):>12} {np.mean(sizes):>10.0f} "
                  f"{1.96*se_t*1e4:>13.1f}bp {1.96*se_s*1e4:>11.1f}bp")

    print("\n── context (pre-registered before any signal-outcome mean is looked at) ──")
    print("  plausible post-2010 12-1 long-leg effect: ~30-90bp per 63d hold gross;")
    print("  amortized cost ~20bp per round trip -> plausible NET effect ~10-70bp/63d.")
    print("\n--- gate rule ---")
    print("  PASS  : MDE95(long leg, 63d) <= ~40bp -> the window can detect a mid-range effect")
    print("  MARGIN: 40-80bp -> only the top of the plausible range is detectable")
    print("  FAIL  : > ~80bp -> this window cannot adjudicate long-horizon momentum; don't run")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
