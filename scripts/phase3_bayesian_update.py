#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 3 — BAYESIAN UPDATE: P(stays green | already green + evidence at T), stacked positive
          evidence, WITH remaining expectancy (the metric that separates alpha from banked gain).

Not failure-states (done) — SUCCESS states, stacked. At entry P(win)=50%. After observing
positive evidence at checkpoint T, how does P(win) update — and, crucially, is the REMAINING
return from T positive net of cost, or is the high probability just a banked gain?

Checkpoints {5m,10m,15m,30m,60m}. Conditions (increasing positive evidence, incl. stacked):
  green(>0) · up>+0.5% · up>+1% · above-VWAP · RS-vs-SPY>0 · high-RVOL
  STACK-A: green + above-VWAP + RS>0
  STACK-B: up>+0.5% + above-VWAP + RS>0 + high-RVOL   (max evidence)
Metrics: P(finish green), P(never revisit entry), P(never lose >0.5% from T), mean/median
REMAINING return, EXCESS-over-SPY remaining (SPY intraday path is in the data), net@10/20bp.
Entry 10:00, deployable/liquid. Read-only.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
ORDER = ["5m", "10m", "15m", "20m", "30m", "45m", "60m", "90m", "120m", "180m", "EOD"]
COND_CKPTS = ["5m", "10m", "15m", "30m", "60m"]


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
    print("=" * 104)
    print("Phase 3 — BAYESIAN UPDATE: P(win | positive evidence at T) + REMAINING expectancy (entry 10:00)")
    print("=" * 104)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id", "addv_20d").collect())
    path = pl.scan_parquet(wf("forward_path_short")).filter(pl.col("entry_offset") == OFFSET)
    retw = (path.filter(pl.col("path_checkpoint").is_in(ORDER)).select("day", "security_id", "path_checkpoint", "ret")
            .collect().pivot(values="ret", index=["day", "security_id"], on="path_checkpoint"))
    vwapw = (path.filter(pl.col("path_checkpoint").is_in(COND_CKPTS))
             .select("day", "security_id", "path_checkpoint", "vwap_since_entry")
             .collect().pivot(values="vwap_since_entry", index=["day", "security_id"], on="path_checkpoint"))
    vwapw.columns = ["day", "security_id"] + [f"vw_{c}" for c in vwapw.columns[2:]]
    dvolw = (path.filter(pl.col("path_checkpoint").is_in(COND_CKPTS))
             .select("day", "security_id", "path_checkpoint", "dollar_volume_since_entry")
             .collect().pivot(values="dollar_volume_since_entry", index=["day", "security_id"], on="path_checkpoint"))
    dvolw.columns = ["day", "security_id"] + [f"dv_{c}" for c in dvolw.columns[2:]]
    # SPY intraday path -> per-day columns
    spy = (path.filter((pl.col("security_id") == "SPY") & pl.col("path_checkpoint").is_in(ORDER))
           .select("day", "path_checkpoint", "ret").collect()
           .pivot(values="ret", index="day", on="path_checkpoint"))
    spy.columns = ["day"] + [f"spy_{c}" for c in spy.columns[1:]]
    ep = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "entry_price").collect())

    df = (retw.join(cls, on=["day", "security_id"], how="inner")
          .join(vwapw, on=["day", "security_id"], how="left").join(dvolw, on=["day", "security_id"], how="left")
          .join(ep, on=["day", "security_id"], how="left").join(spy, on="day", how="left"))
    n = df.height
    eod = df["EOD"].to_numpy(); spy_eod = df["spy_EOD"].to_numpy()
    entry_price = df["entry_price"].to_numpy(); addv = df["addv_20d"].to_numpy()
    R = {c: df[c].to_numpy() for c in ORDER if c in df.columns}
    print(f"deployable trades: {n:,} · SPY intraday path joined for clean RS/excess\n")

    def later(c):
        return ORDER[ORDER.index(c) + 1:]

    def metrics(mask, C):
        rc = R[C]; m = mask & ~np.isnan(rc) & ~np.isnan(eod)
        if m.sum() < 300:
            return None
        rem = eod[m] - rc[m]
        spyC = df[f"spy_{C}"].to_numpy()[m]
        exrem = rem - (spy_eod[m] - spyC)
        lc = later(C)
        lat = np.vstack([R[k][m] for k in lc])
        post_min = np.nanmin(lat, axis=0)
        post_min_ck = np.nanmin(lat - rc[m][None, :], axis=0)
        return dict(n=int(m.sum()), p_green=100*np.mean(eod[m] > 0),
                    p_norevisit=100*np.mean(post_min >= 0),
                    p_nolose05=100*np.mean(post_min_ck >= -0.005),
                    mean_rem=rem.mean()*1e4, med_rem=np.median(rem)*1e4,
                    exc_rem=exrem.mean()*1e4, net10=rem.mean()*1e4-10, net20=rem.mean()*1e4-20)

    for C in COND_CKPTS:
        rc = R[C]
        vw = df[f"vw_{C}"].to_numpy(); dv = df[f"dv_{C}"].to_numpy()
        spyC = df[f"spy_{C}"].to_numpy()
        above_vwap = rc > (vw / entry_price - 1.0)
        rs = (rc - spyC) > 0
        rvol = dv / addv
        hi_rvol = rvol > np.nanmedian(rvol)
        conds = [("green (>0)", rc > 0), ("up >+0.5%", rc > 0.005), ("up >+1%", rc > 0.01),
                 ("above-VWAP", above_vwap), ("RS vs SPY >0", rs), ("high RVOL", hi_rvol),
                 ("STACK: grn+VWAP+RS", (rc > 0) & above_vwap & rs),
                 ("STACK: >0.5+VWAP+RS+RVOL", (rc > 0.005) & above_vwap & rs & hi_rvol)]
        print(f"── checkpoint {C} ──")
        print(f"  {'condition':<26}{'n':>8}{'P(win)':>8}{'P(noRev)':>9}{'P(noLose.5)':>12}"
              f"{'meanRem':>9}{'medRem':>8}{'excRem':>8}{'net@10':>8}{'net@20':>8}")
        for lab, cond in conds:
            r = metrics(cond, C)
            if r is None:
                print(f"  {lab:<26} (insufficient)"); continue
            print(f"  {lab:<26}{r['n']:>8,}{r['p_green']:>7.1f}%{r['p_norevisit']:>8.1f}%"
                  f"{r['p_nolose05']:>11.1f}%{r['mean_rem']:>9.1f}{r['med_rem']:>8.1f}"
                  f"{r['exc_rem']:>8.1f}{r['net10']:>8.1f}{r['net20']:>8.1f}")
        print()

    print("--- reading ---")
    print("P(win) SHOULD climb with stacked evidence (Bayesian update) — that part is real. The")
    print("ALPHA test is meanRem / excRem / net@10-20: if remaining expectancy is ~0 or negative net")
    print("of cost EVEN for the max-evidence STACK, the high P(win) is a BANKED gain, not a hold edge.")
    print("A genuine opportunity = a stacked state with P(win) high AND excRem clearly >0 net of cost.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
