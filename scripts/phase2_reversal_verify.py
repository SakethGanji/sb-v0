#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 2 — REVERSAL VERIFICATION: is the "buy recent losers" long leg real, or a 2020-inflated,
          cost-eaten, illiquidity-concentrated artifact (the classic short-term-reversal trap)?

The momentum breakdown found robust short-term REVERSAL: recent losers (bottom decile of
trailing return) outperform recent winners. Long-only-tradeable leg = buy the losers, ~+70bp/21d
excess on paper. Before believing it, verify the 3 known ways this anomaly is a mirage:
  1. PER-YEAR — does 2020 (COVID crash->V-recovery) dominate the loser-leg? (era artifact)
  2. COST BY DECILE — the loser decile has wider spreads; net the leg by its OWN spread proxy,
     not the universe median.
  3. LIQUIDITY — does reversal survive in the MOST-liquid names, or only the 'normal' end?
     (short-term reversal classically lives in the harder-to-trade corner)
Signals: trailing-5d (hz 5d), trailing-21d (hz 21d). Offset 1000, deployable. excess-over-SPY.
Read-only.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS = ["mega", "large", "mid"]
LIQS = ["highly_liquid", "liquid", "normal"]
YEARS = [2016, 2017, 2018, 2019, 2020]
MINDEC = 10
CONFIGS = [("trail_5d", "5d", 6), ("trail_21d", "21d", 22)]   # (signal, horizon, shift-for-lookback)


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


def decile_daily(day, sig, out, want="bot"):
    """per-day forward mean of the loser (bottom) or winner (top) signal-decile."""
    m = ~np.isnan(sig) & ~np.isnan(out)
    d, s, o = day[m], sig[m], out[m]
    order = np.argsort(d, kind="stable"); d, s, o = d[order], s[order], o[order]
    u, idx = np.unique(d, return_index=True); ends = np.append(idx[1:], len(d))
    vals = []
    for i in range(len(u)):
        a, b = idx[i], ends[i]
        if b - a < MINDEC * 3:
            continue
        ss, oo = s[a:b], o[a:b]
        q = ss <= np.quantile(ss, 0.1) if want == "bot" else ss >= np.quantile(ss, 0.9)
        if q.sum() >= MINDEC:
            vals.append((u[i], oo[q].mean()))
    return np.array([v[0] for v in vals], dtype="datetime64[D]"), np.array([v[1] for v in vals])


def main():
    t0 = dt.datetime.now()
    print("=" * 96)
    print("Phase 2 — REVERSAL VERIFICATION: is 'buy recent losers' real, or 2020/cost/illiquidity?")
    print("=" * 96)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id", "liquidity_bucket"))
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "eod_day_close")
          .join(cls, on=["day", "security_id"], how="inner").sort(["security_id", "day"])
          .with_columns(
              (pl.col("eod_day_close").shift(1).over("security_id")
               / pl.col("eod_day_close").shift(6).over("security_id") - 1).alias("trail_5d"),
              (pl.col("eod_day_close").shift(1).over("security_id")
               / pl.col("eod_day_close").shift(22).over("security_id") - 1).alias("trail_21d")))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "entry_price", "entry_1m_range",
                  "ret_5d_excess_spy", "ret_21d_excess_spy"))
    df = (do.join(fo, on=["day", "security_id"], how="inner")
          .with_columns((pl.col("entry_1m_range") / pl.col("entry_price") * 1e4).alias("spread_bps"),
                        pl.col("day").dt.year().alias("yr")).collect())
    day = df["day"].to_numpy().astype("datetime64[D]"); yr = df["yr"].to_numpy()
    liq = df["liquidity_bucket"].to_numpy(); spread = df["spread_bps"].to_numpy()
    print(f"deployable @1000: {df.height:,}\n")

    for sigcol, hz, _ in CONFIGS:
        sig = df[sigcol].to_numpy(); out = df[f"ret_{hz}_excess_spy"].to_numpy()
        print(f"══ signal={sigcol}  horizon={hz}  (loser = bottom decile of trailing return) ══")
        # 1. per-year loser leg (isolate 2020)
        print("  per-year LOSER-leg forward excess (buy recent losers):")
        for y in YEARS:
            ym = yr == y
            d2, v2 = decile_daily(day[ym], sig[ym], out[ym], "bot")
            if len(v2):
                print(f"     {y}: {v2.mean()*1e4:>+7.1f}bp   (winner-leg {decile_daily(day[ym],sig[ym],out[ym],'top')[1].mean()*1e4:>+7.1f}bp, n_days={len(v2)})")
        # 2/3. by liquidity bucket (loser leg), full window
        print("  LOSER-leg by liquidity bucket (does it survive in the most liquid?):")
        for lb in LIQS:
            lm = liq == lb
            d2, v2 = decile_daily(day[lm], sig[lm], out[lm], "bot")
            # decile-specific cost: median spread of the loser decile in this bucket
            if len(v2):
                # approx loser-decile spread: median spread among bottom-decile-per-day rows
                print(f"     {lb:<15}: loser-leg {v2.mean()*1e4:>+7.1f}bp  n_days={len(v2)}")
        # decile cost: loser vs winner vs universe median spread
        # gather loser-decile membership spreads
        m = ~np.isnan(sig) & ~np.isnan(out) & ~np.isnan(spread)
        d, s, sp = day[m], sig[m], spread[m]
        order = np.argsort(d, kind="stable"); d, s, sp = d[order], s[order], sp[order]
        u, idx = np.unique(d, return_index=True); ends = np.append(idx[1:], len(d))
        losp, wisp = [], []
        for i in range(len(u)):
            a, b = idx[i], ends[i]
            if b - a < MINDEC * 3:
                continue
            ss = s[a:b]; spp = sp[a:b]
            lo = ss <= np.quantile(ss, 0.1); hi = ss >= np.quantile(ss, 0.9)
            losp.extend(spp[lo].tolist()); wisp.extend(spp[hi].tolist())
        lo_cost = np.nanmedian(losp); uni_cost = np.nanmedian(spread[~np.isnan(spread)])
        d2, v2 = decile_daily(day, sig, out, "bot")
        print(f"  COST: loser-decile median spread {lo_cost:.1f}bp vs universe {uni_cost:.1f}bp "
              f"(round-trip ~2x = {2*lo_cost:.0f}bp)")
        print(f"  NET loser-leg (full window): gross {v2.mean()*1e4:+.1f}bp - ~{2*lo_cost:.0f}bp round-trip "
              f"= {v2.mean()*1e4 - 2*lo_cost:+.1f}bp\n")

    print("--- verdict ---")
    print("REAL & TRADEABLE if: loser-leg positive in MOST years (not just 2020), survives in")
    print("highly_liquid names, and gross > loser-decile round-trip cost. MIRAGE if: 2020 dominates,")
    print("it vanishes in liquid names, or the loser-decile spread eats it (classic reversal trap).")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
