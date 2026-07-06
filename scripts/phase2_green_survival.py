#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 2 — GREEN-SURVIVAL DEEP DIVE (conditional stay-green + REMAINING expectancy).

Entry @10:00. The key question is NOT "does it stay green" but: given the trade PROVED
ITSELF early (green by >= a buffer at checkpoint t), is holding from t to EOD +EV — or did
it just bank an early gain? A stock can stay green yet be a bad hold if it gives the move back.

For checkpoints {5m,15m,30m,60m,120m} × buffers {0, +0.25%, +0.5%, +1.0%}, condition on
ret(entry->checkpoint) >= buffer, then measure (using the discrete later path checkpoints):
  1. P(EOD green)                         P(ret_EOD > 0)
  2. P(stays above entry after ckpt)      P(min later ret >= 0)  [never revisits entry]
  3. P(draws down > X from ckpt price)     for X in 0.25/0.5/1.0%
  4/5. avg / median REMAINING return       ret_EOD - ret_ckpt   <-- the hold-from-here P&L
  6. avg total entry->EOD                  ret_EOD
  7. total excess over SPY (10:00->close)
  8. remaining net of 10 / 20 bp round-trip cost
Splits: above/below entry-VWAP, morning-momentum cohort vs all, market up/down, rel-volume.
Read-only. Deployable/liquid. Discrete-checkpoint approximation for intra-path extremes.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
ORDER = ["5m", "15m", "30m", "45m", "60m", "90m", "120m", "180m", "EOD"]
COND_CKPTS = ["5m", "15m", "30m", "60m", "120m"]
BUFFERS = [0.0, 0.0025, 0.0050, 0.0100]
DD_LEVELS = [0.0025, 0.005, 0.010]


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
    print("=" * 100)
    print("Phase 2 — GREEN-SURVIVAL DEEP DIVE: once proven green, is HOLDING from here +EV?  (entry 10:00)")
    print("=" * 100)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id"))
    path = pl.scan_parquet(wf("forward_path_short")).filter(pl.col("entry_offset") == OFFSET)
    retw = (path.filter(pl.col("path_checkpoint").is_in(ORDER))
            .select("day", "security_id", "path_checkpoint", "ret")
            .collect().pivot(values="ret", index=["day", "security_id"], on="path_checkpoint"))
    vw30 = (path.filter(pl.col("path_checkpoint") == "30m")
            .select("day", "security_id", "vwap_since_entry").collect())
    ep = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "entry_price").collect())
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "intraday_ret_0930_to_1000_percentile_today",
                  "premarket_volume_vs_20d_median").collect())
    mc = (pl.scan_parquet(wf("market_context_daily"))
          .select("day", "spy_ret_0930_to_1000", "spy_eod_open", "spy_eod_close").collect())

    df = (retw.join(cls.collect(), on=["day", "security_id"], how="inner")
          .join(vw30, on=["day", "security_id"], how="left")
          .join(ep, on=["day", "security_id"], how="left")
          .join(do, on=["day", "security_id"], how="left")
          .join(mc, on="day", how="left")
          .with_columns((pl.col("spy_eod_close") /
                         (pl.col("spy_eod_open") * (1 + pl.col("spy_ret_0930_to_1000"))) - 1).alias("spy_rem")))
    for c in ORDER:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(c))
    df = df.collect() if isinstance(df, pl.LazyFrame) else df

    R = {c: df[c].to_numpy() for c in ORDER}
    eod = R["EOD"]; spy_rem = df["spy_rem"].to_numpy()
    mompct = df["intraday_ret_0930_to_1000_percentile_today"].to_numpy()
    relvol = df["premarket_volume_vs_20d_median"].to_numpy()
    entry_price = df["entry_price"].to_numpy(); vwap30 = df["vwap_since_entry"].to_numpy()
    spy_morning = df["spy_ret_0930_to_1000"].to_numpy()
    n = df.height
    print(f"deployable trades with path @10:00: {n:,}\n")

    def later_cols(c):
        return ORDER[ORDER.index(c) + 1:]

    def metrics(cond, C):
        rc = R[C]
        m = cond & ~np.isnan(rc) & ~np.isnan(eod)
        if m.sum() < 200:
            return None
        rem = eod[m] - rc[m]
        lc = later_cols(C)
        if lc:
            later = np.vstack([R[k][m] for k in lc])                 # later returns-from-entry
            post_min = np.nanmin(later, axis=0)                      # min ret after ckpt
            post_min_ck = np.nanmin(later - rc[m][None, :], axis=0)  # min drawdown from ckpt price
        else:
            post_min = eod[m] - eod[m]; post_min_ck = np.zeros(m.sum())
        return dict(n=int(m.sum()),
                    p_eod_green=100*np.mean(eod[m] > 0),
                    p_stay_above=100*np.mean(post_min >= 0),
                    p_dd={x: 100*np.mean(post_min_ck <= -x) for x in DD_LEVELS},
                    avg_rem=rem.mean()*1e4, med_rem=np.median(rem)*1e4,
                    avg_tot=eod[m].mean()*1e4,
                    avg_exc=(eod[m] - spy_rem[m]).mean()*1e4,
                    net10=(rem.mean()*1e4 - 10), net20=(rem.mean()*1e4 - 20))

    print("── CORE: condition on ret(entry->checkpoint) >= buffer ──")
    print(f"  {'ckpt':<6}{'buffer':>8}{'n':>9}{'P(EODgrn)':>10}{'P(stay>e)':>10}"
          f"{'avgRem':>8}{'medRem':>8}{'net@10':>8}{'net@20':>8}{'avgTot':>8}{'DD>0.5%':>9}")
    for C in COND_CKPTS:
        for B in BUFFERS:
            r = metrics(R[C] >= B, C)
            if r is None:
                continue
            print(f"  {C:<6}{('+'+format(B*100,'.2f')+'%'):>8}{r['n']:>9,}{r['p_eod_green']:>9.1f}%"
                  f"{r['p_stay_above']:>9.1f}%{r['avg_rem']:>8.1f}{r['med_rem']:>8.1f}"
                  f"{r['net10']:>8.1f}{r['net20']:>8.1f}{r['avg_tot']:>8.1f}{r['p_dd'][0.005]:>8.1f}%")
        print()

    # splits at the canonical proven-early state: 30m, +0.5%
    C, B = "30m", 0.005
    base = (R[C] >= B)
    dist_vwap = R[C] - (vwap30 / entry_price - 1.0)
    splits = [("ALL proven (30m,+0.5%)", base),
              ("  + above entry-VWAP", base & (dist_vwap > 0)),
              ("  + below entry-VWAP", base & (dist_vwap < 0)),
              ("  + momentum cohort (top-quint)", base & (mompct >= 0.8)),
              ("  + market UP that morning", base & (spy_morning > 0)),
              ("  + market DOWN that morning", base & (spy_morning < 0)),
              ("  + high rel-volume", base & (relvol > np.nanmedian(relvol))),
              ("  + low rel-volume", base & (relvol <= np.nanmedian(relvol)))]
    print("── SPLITS at the 'proven' state (green >= +0.5% by 30m): does any give +EV remaining? ──")
    print(f"  {'condition':<34}{'n':>8}{'P(EODgrn)':>10}{'avgRem':>8}{'medRem':>8}"
          f"{'net@10':>8}{'avgExc':>8}")
    for lab, cond in splits:
        r = metrics(cond, C)
        if r is None:
            print(f"  {lab:<34} (insufficient)"); continue
        print(f"  {lab:<34}{r['n']:>8,}{r['p_eod_green']:>9.1f}%{r['avg_rem']:>8.1f}{r['med_rem']:>8.1f}"
              f"{r['net10']:>8.1f}{r['avg_exc']:>8.1f}")

    print("\n--- reading ---")
    print("USEFUL finding = a proven state where avgRem (remaining return) is clearly POSITIVE net")
    print("of cost (net@10/@20 > 0) AND P(EOD green) high AND P(stay>entry) high. That means holding")
    print("from there has real edge. If avgRem ~0 / negative net despite high P(EOD green), the trade")
    print("only BANKED an early gain — holding on from there has no expectancy (exit-or-hold is a wash).")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
