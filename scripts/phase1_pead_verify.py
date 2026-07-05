#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — PEAD VERIFICATION: is the clean +58.9bp/5d post-earnings drift real, and is
                             ANY of it long-only tradeable?

The first pass found a clean, 5/5-era post-earnings drift (5d decile spread +58.9bp) but
(a) its control was look-ahead-biased (trailing return used TODAY's close while entering at
10:00 today), and (b) the spread was almost all SHORT leg (neg-surprise −53bp; pos-surprise
+5.8bp). This verifies:
  1. PLACEBO — shuffle the surprise across names within day -> must collapse to ~0.
  2. LOOK-AHEAD-SAFE control — trailing 2d return through PRIOR day's close (knowable at 10:00).
  3. LONG-leg net of cost, by cap bucket + name concentration of the short-leg drift.
  4. entry-window sensitivity (days_since 2-6 vs 3-8) — a real multi-day drift persists.
Deployable/liquid, enter 10:00, excess-over-SPY, block bootstrap (block=5). Read-only.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
YEARS = [2016, 2017, 2018, 2019, 2020]
SEED = 20260705
N_BOOT = 2000
MINDEC = 10
BLOCK = 5


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


def block_ci(spr, rng, block=BLOCK, B=N_BOOT):
    n = len(spr)
    if n < block + 5:
        return (np.nan, np.nan)
    nb = int(np.ceil(n / block))
    idx = (rng.integers(0, n, size=(B, nb))[:, :, None] + np.arange(block)[None, None, :]) % n
    bm = spr[idx.reshape(B, -1)[:, :n]].mean(axis=1)
    return float(np.quantile(bm, 0.025)), float(np.quantile(bm, 0.975))


def daily_decile(day, sig, out):
    m = ~np.isnan(sig) & ~np.isnan(out)
    d, s, o = day[m], sig[m], out[m]
    order = np.argsort(d, kind="stable"); d, s, o = d[order], s[order], o[order]
    u, idx = np.unique(d, return_index=True); ends = np.append(idx[1:], len(d))
    spr, top, bot = [], [], []
    for i in range(len(u)):
        a, b = idx[i], ends[i]
        if b - a < MINDEC * 3:
            continue
        ss, oo = s[a:b], o[a:b]
        hi = ss >= np.quantile(ss, 0.9); lo = ss <= np.quantile(ss, 0.1)
        if hi.sum() >= MINDEC and lo.sum() >= MINDEC:
            spr.append(oo[hi].mean() - oo[lo].mean()); top.append(oo[hi].mean()); bot.append(oo[lo].mean())
    return np.array(spr), np.array(top), np.array(bot)


def line(nm, day, sig, out, rng):
    spr, top, bot = daily_decile(day, sig, out)
    if len(spr) < 20:
        print(f"  {nm:<30} (insufficient)"); return
    lo, hi = block_ci(spr, rng)
    star = "" if (lo <= 0 <= hi) else "✓"
    print(f"  {nm:<30}{spr.mean()*1e4:>8.1f}b CI[{lo*1e4:>6.1f},{hi*1e4:>6.1f}]{star:>2}"
          f"  long {top.mean()*1e4:>+6.1f}b short {bot.mean()*1e4:>+6.1f}b nd={len(spr)}")


def main():
    t0 = dt.datetime.now()
    print("=" * 92)
    print("Phase 1 — PEAD VERIFICATION (clean? long-only tradeable?)")
    print("=" * 92)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id", "market_cap_bucket"))
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "days_since_last_earnings", "eod_day_close", "prior_day_eod_close")
          .join(cls, on=["day", "security_id"], how="inner")
          .sort(["security_id", "day"])
          .with_columns((pl.col("eod_day_close") / pl.col("eod_day_close").shift(2).over("security_id") - 1.0).alias("r2"))
          .with_columns(pl.when(pl.col("days_since_last_earnings") == 1).then(pl.col("r2")).otherwise(None)
                        .alias("_react"))
          .with_columns(pl.col("_react").forward_fill().over("security_id").alias("reaction_ret"))
          # look-ahead-safe trailing 2d return: through PRIOR day's close (shift 1 within security)
          .with_columns(pl.col("r2").shift(1).over("security_id").alias("r2_safe")))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "ret_5d_excess_spy"))
    df = (do.join(fo, on=["day", "security_id"], how="inner")
          .with_columns(pl.col("day").dt.year().alias("yr")).collect())

    day = df["day"].to_numpy().astype("datetime64[D]"); yr = df["yr"].to_numpy()
    dsle = df["days_since_last_earnings"].to_numpy(); react = df["reaction_ret"].to_numpy()
    r2safe = df["r2_safe"].to_numpy(); out = df["ret_5d_excess_spy"].to_numpy()
    cap = df["market_cap_bucket"].to_numpy(); sid = df["security_id"].to_numpy()
    rng = np.random.default_rng(SEED)
    post = (dsle >= 2) & (dsle <= 6)
    ctrl = (dsle >= 30) & (dsle <= 250)

    print("\n── 5d decile spread (top-minus-bottom by surprise), excess-over-SPY ──")
    line("PEAD post-earn (clean)", day[post], react[post], out[post], rng)
    # placebo: shuffle surprise within day
    pr = react[post].copy(); dd = day[post]
    for u in np.unique(dd):
        mm = dd == u; idxs = np.where(mm)[0]; pr[idxs] = rng.permutation(pr[idxs])
    line("PEAD placebo (shuffled)", day[post], pr, out[post], rng)
    line("CONTROL momentum (LA-safe)", day[ctrl], r2safe[ctrl], out[ctrl], rng)

    print("\n── entry-window sensitivity (does the drift persist further from the event?) ──")
    for lo_, hi_ in [(2, 6), (3, 8), (4, 10)]:
        mk = (dsle >= lo_) & (dsle <= hi_)
        line(f"PEAD days_since {lo_}-{hi_}", day[mk], react[mk], out[mk], rng)

    print("\n── PEAD 5d by cap bucket (is it a mid-cap effect?) ──")
    for cb in ["mega", "large", "mid"]:
        mk = post & (cap == cb)
        line(f"PEAD {cb}", day[mk], react[mk], out[mk], rng)

    # long-leg net of cost + short-leg name concentration
    print("\n── economics ──")
    _, topser, botser = daily_decile(day[post], react[post], out[post])
    print(f"  LONG leg (buy positive surprise): {topser.mean()*1e4:+.1f} bps / 5d  "
          f"-> net of ~15bp round-trip: {topser.mean()*1e4 - 15:+.1f} bps  (long-only tradeable?)")
    print(f"  SHORT leg (negative surprise):    {botser.mean()*1e4:+.1f} bps / 5d  (needs shorting = GATED)")
    # name concentration of the bottom (short) decile
    m = post & ~np.isnan(react) & ~np.isnan(out)
    dd, ss, oo, si = day[m], react[m], out[m], sid[m]
    order = np.argsort(dd, kind="stable"); dd, ss, oo, si = dd[order], ss[order], oo[order], si[order]
    u, idx = np.unique(dd, return_index=True); ends = np.append(idx[1:], len(dd))
    botids = []
    for i in range(len(u)):
        a, b = idx[i], ends[i]
        if b - a < MINDEC * 3:
            continue
        q = ss[a:b] <= np.quantile(ss[a:b], 0.1)
        botids.extend(si[a:b][q].tolist())
    if botids:
        vals, cnts = np.unique(botids, return_counts=True)
        top5 = np.sort(cnts)[::-1][:5].sum()
        print(f"  short-decile name concentration: {len(vals)} unique names, "
              f"top-5 names = {100*top5/len(botids):.1f}% of short-leg slots")

    print("\n--- verdict ---")
    print("Clean if: PEAD ✓ survives, PLACEBO ~0, drift persists across entry windows. Tradeable")
    print("ONLY if the LONG leg clears cost. If long-leg ~0/negative net and the signal is all")
    print("short-leg -> real DIRECTIONAL drift but long-only-null + short-gated ('behind glass'),")
    print("and the value is (a) an avoid-filter, (b) justification for REAL EPS-surprise data (SUE),")
    print("which would likely restore a tradeable long leg the price-proxy attenuates.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
