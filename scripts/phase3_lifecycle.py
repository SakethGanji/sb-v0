#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 3 — TRADE LIFECYCLE INTELLIGENCE (loser identification + winner/loser divergence).

Not predicting the market — predicting our own trades. Green-survival (Part 1) showed holding
a PROVEN-GREEN trade has ~0 remaining edge (banked gain). This is Part 2 — the loser side:
  A. DIVERGENCE: average trajectory of eventual-winners vs eventual-losers at each checkpoint
     (where do they separate → where the exit decision belongs).
  B. FAILURE CURVES: condition on a BAD state at checkpoint C (down X%, below VWAP, weak vs SPY)
     -> P(recover to EOD green), and the honest metric: avg REMAINING return from C net of cost.
     Key test (per the user): is E[remaining | bad] clearly NEGATIVE (a real exit signal that
     ADDS expectancy) — or ~coin flip (cutting just locks the loss = no expectancy gain)?
  C. MAE & TIME-UNDERWATER: how much pain do winners endure vs losers; do losers stay underwater?
Entry 10:00, deployable/liquid. excess handled via raw remaining (intraday SPY drift ~0).
Read-only.
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
TRAJ = ["5m", "15m", "30m", "60m", "120m", "EOD"]
FAIL_CKPTS = ["15m", "30m", "60m"]
DOWN_BUFS = [-0.0025, -0.005, -0.0075]


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
    print("=" * 98)
    print("Phase 3 — TRADE LIFECYCLE: loser identification + winner/loser divergence (entry 10:00)")
    print("=" * 98)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id").collect())
    path = pl.scan_parquet(wf("forward_path_short")).filter(pl.col("entry_offset") == OFFSET)
    retw = (path.filter(pl.col("path_checkpoint").is_in(ORDER))
            .select("day", "security_id", "path_checkpoint", "ret")
            .collect().pivot(values="ret", index=["day", "security_id"], on="path_checkpoint"))
    vw30 = path.filter(pl.col("path_checkpoint") == "30m").select(
        "day", "security_id", "vwap_since_entry").collect()
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "entry_price", "max_drawdown_EOD", "max_runup_EOD",
                  "pct_bars_underwater_EOD").collect())
    df = (retw.join(cls, on=["day", "security_id"], how="inner")
          .join(vw30, on=["day", "security_id"], how="left")
          .join(fo, on=["day", "security_id"], how="left"))
    R = {c: df[c].to_numpy() for c in ORDER if c in df.columns}
    eod = R["EOD"]
    win = eod > 0; los = eod < 0
    print(f"deployable trades: {df.height:,} · eventual winners {100*np.nanmean(win):.1f}% "
          f"losers {100*np.nanmean(los):.1f}%\n")

    # A. divergence: mean ret at each checkpoint, winners vs losers
    print("── A. WINNER vs LOSER average trajectory (mean ret, bps) — where do they separate? ──")
    print(f"  {'outcome':<10}" + "".join(f"{c:>9}" for c in TRAJ))
    for lab, mask in [("winners", win), ("losers", los)]:
        row = f"  {lab:<10}"
        for c in TRAJ:
            v = R[c][mask]; row += f"{np.nanmean(v)*1e4:>9.1f}"
        print(row)
    row = f"  {'gap(W-L)':<10}"
    for c in TRAJ:
        row += f"{(np.nanmean(R[c][win])-np.nanmean(R[c][los]))*1e4:>9.1f}"
    print(row)
    print("  (winners are mechanically higher; watch how EARLY the gap opens — that's the signal window)")

    # B. failure curves — the honest remaining-expectancy test
    print("\n── B. FAILURE CURVES: condition on a BAD state at checkpoint -> recovery + REMAINING ──")
    print(f"  {'state':<26}{'n':>8}{'P(recover grn)':>15}{'avgRemain':>11}{'net@10':>9}{'net@20':>9}")
    entry_price = df["entry_price"].to_numpy(); vwap30 = df["vwap_since_entry"].to_numpy()
    for C in FAIL_CKPTS:
        rc = R[C]
        for B in DOWN_BUFS:
            m = (rc <= B) & ~np.isnan(rc) & ~np.isnan(eod)
            if m.sum() < 200:
                continue
            rem = eod[m] - rc[m]
            print(f"  {C+' down '+format(-B*100,'.2f')+'%':<26}{int(m.sum()):>8,}"
                  f"{100*np.mean(eod[m] > 0):>14.1f}%{rem.mean()*1e4:>11.1f}{rem.mean()*1e4-10:>9.1f}"
                  f"{rem.mean()*1e4-20:>9.1f}")
        # below-VWAP + weak states at this checkpoint
        dist = rc - (vwap30 / entry_price - 1.0) if C == "30m" else None
        if dist is not None:
            m = (dist < 0) & ~np.isnan(dist) & ~np.isnan(eod)
            rem = eod[m] - rc[m]
            print(f"  {'30m below-VWAP':<26}{int(m.sum()):>8,}{100*np.mean(eod[m] > 0):>14.1f}%"
                  f"{rem.mean()*1e4:>11.1f}{rem.mean()*1e4-10:>9.1f}{rem.mean()*1e4-20:>9.1f}")
        print()

    # C. MAE + time underwater, winners vs losers
    print("── C. MAX ADVERSE EXCURSION & TIME UNDERWATER (winners vs losers) ──")
    mdd = df["max_drawdown_EOD"].to_numpy(); mru = df["max_runup_EOD"].to_numpy()
    puw = df["pct_bars_underwater_EOD"].to_numpy()
    for lab, mask in [("winners", win), ("losers", los)]:
        print(f"  {lab:<10} median MAE {np.nanmedian(mdd[mask])*100:>6.2f}%  "
              f"median peak-runup {np.nanmedian(mru[mask])*100:>5.2f}%  "
              f"median %time underwater {100*np.nanmedian(puw[mask]):>4.0f}%")

    print("\n--- reading ---")
    print("The exit-decision test (B): a bad state ADDS expectancy ONLY if avgRemain is clearly")
    print("NEGATIVE net of cost (net@10/20 < 0 AND P(recover) low) -> cutting there beats holding.")
    print("If avgRemain ~0/positive (losers bounce = intraday reversal) then cutting LOCKS the loss")
    print("and forfeits the recovery = destroys expectancy (the win-rate trap). MAE(C) tells you the")
    print("pain a winner normally survives — a stop tighter than that cuts winners.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
