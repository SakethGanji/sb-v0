#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 2 (descriptive) — GREEN RATES & MAGNITUDE by timeframe (buy @10:00).

Two questions:
  1. If I buy at 10:00, what's the chance the stock is GREEN — at the end of a window, and
     the chance it STAYS green (fraction of time above entry)?
  2. The "magnitude" signal — how much does a stock move, in what timeframe?

Answers, per horizon {30m, 60m, EOD, 1d, 5d}, for (a) all deployable/liquid names and
(b) the morning-momentum cohort (top-quintile 0930-1000):
  GREEN:  P(ret>0 at end) raw & excess-over-SPY; median % of time spent green (pct_bars);
          P(stayed green ~whole time, i.e. never closed >0.5% below entry).
  MAGNITUDE: median realized range (max_runup - max_drawdown); P(moved >=1/2/3% either way).
Purely descriptive base rates. Read-only.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
HZ = ["30min", "60min", "EOD", "1d", "5d"]
CKPT = {"30min": "30m", "60min": "60m", "EOD": "EOD", "1d": "1d_close", "5d": "5d_close"}


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


def main():
    t0 = dt.datetime.now()
    print("=" * 96)
    print("GREEN RATES & MAGNITUDE by timeframe — buy @10:00, deployable/liquid US equities 2016-2020")
    print("=" * 96)

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id"))
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "intraday_ret_0930_to_1000_percentile_today"))

    runup = [f"max_runup_{h}" for h in HZ]; ddown = [f"max_drawdown_{h}" for h in HZ]
    pbars = [f"pct_bars_profitable_{h}" for h in HZ if h in ("EOD", "1d", "5d")]
    xcols = [f"first_cross_{d}_{p}pct_{h}" for h in HZ for p in (1, 2, 3) for d in ("up", "down")]
    exc = [f"ret_{h}_excess_spy" for h in HZ if h in ("EOD", "1d", "5d")]
    fo_have = set(pl.scan_parquet(wf("forward_outcomes")).collect_schema().names())
    sel = [c for c in (runup + ddown + pbars + xcols + exc) if c in fo_have]
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + sel))

    # endpoint returns at each checkpoint from the path table
    path = pl.scan_parquet(wf("forward_path_short")).filter(pl.col("entry_offset") == OFFSET)
    endret = None
    for h, ck in CKPT.items():
        p = path.filter(pl.col("path_checkpoint") == ck).select(
            "day", "security_id", pl.col("ret").alias(f"end_{h}"))
        endret = p if endret is None else endret.join(p, on=["day", "security_id"], how="full", coalesce=True)

    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner")
          .join(endret, on=["day", "security_id"], how="left").collect())
    mom = df["intraday_ret_0930_to_1000_percentile_today"].to_numpy()

    def block(name, mask):
        print(f"\n### {name}  (n={int(mask.sum()):,} entries) ###")
        print(f"  {'horizon':<8}{'P(green end)':>13}{'P(green exc)':>13}{'%time green':>12}"
              f"{'stayed green':>13}{'med range':>11}{'P>=1%':>7}{'P>=2%':>7}{'P>=3%':>7}")
        for h in HZ:
            er = df[f"end_{h}"].to_numpy() if f"end_{h}" in df.columns else np.full(df.height, np.nan)
            ru = df[f"max_runup_{h}"].to_numpy() if f"max_runup_{h}" in df.columns else np.full(df.height, np.nan)
            dd = df[f"max_drawdown_{h}"].to_numpy() if f"max_drawdown_{h}" in df.columns else np.full(df.height, np.nan)
            m = mask & ~np.isnan(er)
            pg = 100*np.nanmean(er[m] > 0) if m.sum() else np.nan
            # excess green
            ec = f"ret_{h}_excess_spy"
            if ec in df.columns:
                ex = df[ec].to_numpy(); me = mask & ~np.isnan(ex)
                pge = 100*np.nanmean(ex[me] > 0) if me.sum() else np.nan
            else:
                pge = np.nan
            # % time green + stayed green
            pb = f"pct_bars_profitable_{h}"
            if pb in df.columns:
                pbv = df[pb].to_numpy(); mb = mask & ~np.isnan(pbv)
                pct_time = 100*np.nanmedian(pbv[mb]) if mb.sum() else np.nan
            else:
                pct_time = np.nan
            # stayed green ~ never closed >0.5% below entry (max_drawdown > -0.5%)
            md = mask & ~np.isnan(dd)
            stayed = 100*np.nanmean(dd[md] > -0.005) if md.sum() else np.nan
            rng = np.nanmedian((ru - dd)[mask & ~np.isnan(ru) & ~np.isnan(dd)]) * 100
            # P moved >= X% either way
            def pmove(p):
                uc, dc = f"first_cross_up_{p}pct_{h}", f"first_cross_down_{p}pct_{h}"
                if uc in df.columns and dc in df.columns:
                    u = df[uc].to_numpy(); d = df[dc].to_numpy()
                    mv = mask & ~np.isnan(u) & ~np.isnan(d)
                    return 100*np.nanmean((u[mv] > 0) | (d[mv] > 0)) if mv.sum() else np.nan
                return np.nan
            print(f"  {h:<8}{pg:>12.1f}%{pge:>12.1f}%{pct_time:>11.0f}%{stayed:>12.1f}%"
                  f"{rng:>10.2f}%{pmove(1):>6.0f}%{pmove(2):>6.0f}%{pmove(3):>6.0f}%")

    block("ALL deployable/liquid (buy any at 10:00)", np.ones(df.height, bool))
    block("MORNING MOMENTUM (top-quintile 0930-1000)", mom >= 0.8)

    print("\n--- reading ---")
    print("P(green end) = positive at horizon end (RAW; >50% = market drift/beta).")
    print("P(green exc) = beats SPY (the real 'direction' bet) — ~50% = coin flip.")
    print("stayed green = never closed >0.5% below entry (rare beyond intraday).")
    print("med range = typical peak-to-trough move = the MAGNITUDE signal, per timeframe.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
