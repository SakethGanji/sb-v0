#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 8 — POWER GATE for the short-interest test (phase8-preregistration.md, frozen).
OUTCOMES-ONLY: real forward returns are paired only with SYNTHETIC persistent signals
(AR(1) rho=0.9 across settlements — conservative: as sticky as SI itself), never with
real short-interest values. MDE95 at 80% power = 2.80 x sd of the null mean-IC /
mean-decile-spread across 200 synthetic-signal draws.
Bars (frozen): |IC| >= 0.03; spread >= 20bp/21d, >= 10bp/5d gross.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

SET_START, SET_END = dt.date(2017, 12, 29), dt.date(2020, 11, 15)
LAG_CAL_DAYS = 11
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
N_SIM, RHO, SEED = 200, 0.9, 20260707
BARS = {"ic": 0.03, "spread_5d": 10e-4, "spread_21d": 20e-4}


def trading_days():
    out = []
    for f in sorted(glob.glob("data/outputs/forward_outcomes/*.parquet")):
        try:
            out.append(dt.date.fromisoformat(Path(f).stem))
        except ValueError:
            pass
    return out


def main():
    t0 = dt.datetime.now()
    print("=" * 96)
    print("Phase 8 · POWER GATE — can this window adjudicate a short-interest effect?")
    print("Outcomes paired with SYNTHETIC persistent signals only. Bars: IC 0.03, 10bp/5d, 20bp/21d.")
    print("=" * 96)
    days = trading_days()
    sets_ = (pl.scan_parquet("data/reference/short_interest.parquet")
             .filter(pl.col("security_id").is_not_null())
             .select(pl.col("settlement_date").unique()).collect()
             ["settlement_date"].cast(pl.Date).sort().to_list())
    sets_ = [s for s in sets_ if SET_START <= s <= SET_END]
    t_days = []
    for s in sets_:
        avail = s + dt.timedelta(days=LAG_CAL_DAYS)
        t = next((d for d in days if d >= avail), None)
        if t:
            t_days.append(t)
    t_days = sorted(set(t_days))
    print(f"settlements {len(sets_)} -> signal days {len(t_days)}  ({t_days[0]} .. {t_days[-1]})")

    # per signal day: universe outcomes (5d/21d excess), stored with global name index
    name_ix, panels = {}, []
    for t in t_days:
        cls = (pl.scan_parquet(f"data/outputs/security_classification_daily/{t}.parquet")
               .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                       & pl.col("liquidity_bucket").is_in(LIQS)).select("security_id"))
        fo = (pl.scan_parquet(f"data/outputs/forward_outcomes/{t}.parquet")
              .filter(pl.col("entry_offset") == OFFSET)
              .select("security_id", "ret_5d_excess_spy", "ret_21d_excess_spy"))
        d = cls.join(fo, on="security_id", how="inner").collect().drop_nulls()
        ids = np.array([name_ix.setdefault(s, len(name_ix)) for s in d["security_id"]])
        panels.append((ids, d["ret_5d_excess_spy"].to_numpy(), d["ret_21d_excess_spy"].to_numpy()))
    ns = [len(p[0]) for p in panels]
    print(f"universe per day: median {int(np.median(ns))}, min {min(ns)} · distinct names {len(name_ix):,}")

    rng = np.random.default_rng(SEED)
    n_names = len(name_ix)
    null_ic5, null_ic21, null_sp5, null_sp21 = [], [], [], []
    for _ in range(N_SIM):
        z = rng.standard_normal(n_names)
        ic5s, ic21s, sp5s, sp21s = [], [], [], []
        for ids, r5, r21 in panels:
            z = RHO * z + np.sqrt(1 - RHO**2) * rng.standard_normal(n_names)
            zs = z[ids]
            rz = np.argsort(np.argsort(zs)).astype(float)
            for r, ics, sps in ((r5, ic5s, sp5s), (r21, ic21s, sp21s)):
                rr = np.argsort(np.argsort(r)).astype(float)
                ics.append(np.corrcoef(rz, rr)[0, 1])
                k = max(1, len(zs) // 10)
                top, bot = np.argpartition(zs, -k)[-k:], np.argpartition(zs, k)[:k]
                sps.append(r[top].mean() - r[bot].mean())
        null_ic5.append(np.mean(ic5s)); null_ic21.append(np.mean(ic21s))
        null_sp5.append(np.mean(sp5s)); null_sp21.append(np.mean(sp21s))

    print(f"\n{'estimand':<22} {'null sd':>9} {'MDE95@80%':>10} {'bar':>9}  verdict")
    verdicts = {}
    for nm, arr, bar, fmt in [("mean IC (5d)", null_ic5, BARS["ic"], 1),
                              ("mean IC (21d)", null_ic21, BARS["ic"], 1),
                              ("decile spread 5d", null_sp5, BARS["spread_5d"], 1e4),
                              ("decile spread 21d", null_sp21, BARS["spread_21d"], 1e4)]:
        sd = float(np.std(arr, ddof=1)); mde = 2.80 * sd
        ok = mde <= bar
        verdicts[nm] = ok
        u = "" if fmt == 1 else "bp"
        print(f"{nm:<22} {sd*fmt:>9.4f} {mde*fmt:>10.4f} {bar*fmt:>8.3f}{u}  {'ANSWERABLE' if ok else 'UNANSWERABLE'}")
    print("\nGATE:", "PASS — registered test may run on every answerable estimand"
          if any(verdicts.values()) else "FAIL — UNANSWERABLE at this window; stop per prereg")
    print(f"WALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
