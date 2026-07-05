#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — MOMENTUM RELIABILITY: the core hypothesis, in raw numbers.

Question (verbatim): "given a stock with some capacity and a starting point, can we
reliably enter and exit at some point [for a gain]? Is there any level of stability
anywhere?" No verdict — just the numbers.

Setup: DEPLOYABLE / liquid universe (real capacity) — CS × cap∈{mega,large,mid} ×
liq∈{highly_liquid,liquid,normal}, enter at 10:00 (offset 1000). Cohorts by morning
momentum strength. Two views:
  A) RETURN DISTRIBUTION — win rate + mean/median, RAW and EXCESS-over-SPY, per horizon.
     (RAW shows market drift = beta; EXCESS strips the market to show selection edge.)
  B) ENTER-AND-EXIT-AT-A-GAIN — the materialized first_event_{+T before -S} outcomes:
     P(target first) / P(stop first) / P(neither), for symmetric and favorable pairs.
  + STABILITY — the headline momentum numbers broken out by year 2016-2020.

Read-only. Prints tables; no files written.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
HZ = ["1d", "2d", "5d", "21d"]
# (label, first_event column, +T, -S)  — T/S in the pair's own units (pct or atr)
PAIRS = [
    ("sym 1%/1%  EOD", "first_event_1pct_before_minus_1pct_EOD", 1.0, 1.0),
    ("sym 2%/2%  1d",  "first_event_2pct_before_minus_2pct_1d",  2.0, 2.0),
    ("sym 3%/3%  5d",  "first_event_3pct_before_minus_3pct_5d",  3.0, 3.0),
    ("fav 2%/1%  EOD", "first_event_2pct_before_minus_1pct_EOD", 2.0, 1.0),
    ("fav 2atr/1atr 5d",   "first_event_2atr_before_minus_1atr_5d",   2.0, 1.0),
    ("fav 3atr/1.5atr 21d", "first_event_3atr_before_minus_1_5atr_21d", 3.0, 1.5),
]


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


def wr(x):
    x = x[~np.isnan(x)]
    return (np.mean(x > 0) * 100, x.mean() * 1e4, np.median(x) * 1e4, len(x)) if len(x) else (np.nan,)*4


def event_probs(col):
    v = col.drop_nulls()
    v = v.filter(v.is_in(["target_first", "stop_first", "neither"]))
    n = v.len()
    if n == 0:
        return None
    t = (v == "target_first").sum(); s = (v == "stop_first").sum(); ne = (v == "neither").sum()
    return dict(n=n, pt=100*t/n, ps=100*s/n, pn=100*ne/n,
                tshare=(100*t/(t+s) if (t+s) else np.nan))


def main():
    print("=" * 90)
    print("MOMENTUM RELIABILITY — deployable/liquid universe, enter 10:00, exploration 2016-2020")
    print("=" * 90)

    fo_have = set(pl.scan_parquet(wf("forward_outcomes")).collect_schema().names())
    raw_hz = [f"ret_{h}" for h in HZ if f"ret_{h}" in fo_have]
    exc_hz = [f"ret_{h}_excess_spy" for h in HZ if f"ret_{h}_excess_spy" in fo_have]
    pairs = [(lab, c, T, S) for (lab, c, T, S) in PAIRS if c in fo_have]

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS")
                   & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select("day", "security_id"))
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "intraday_ret_0930_to_1000",
                  "intraday_ret_0930_to_1000_percentile_today"))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + raw_hz + exc_hz + [c for _, c, _, _ in pairs]))
    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner")
          .with_columns(pl.col("day").dt.year().alias("yr"))
          .collect())
    print(f"universe: {df.height:,} entries @10:00 across {df['day'].n_unique():,} days\n")

    mom = df["intraday_ret_0930_to_1000"].to_numpy()
    pct = df["intraday_ret_0930_to_1000_percentile_today"].to_numpy()
    cohorts = {
        "ALL (no selection)":        np.ones(df.height, bool),
        "UP this morning (>0)":      mom > 0,
        "STRONG (top-quintile)":     pct >= 0.8,
        "EXTREME (top-decile)":      pct >= 0.9,
    }

    # ---- A) return distribution: win rate + mean, RAW vs EXCESS ----
    for lens, cols in [("RAW return (incl. market drift = beta)", raw_hz),
                       ("EXCESS over SPY (market stripped out)", exc_hz)]:
        print(f"── A) {lens} ──")
        print(f"  {'cohort':<24}{'n':>10}   " + "".join(f"{c.replace('ret_','').replace('_excess_spy',''):>16}" for c in cols))
        print(f"  {'':<24}{'':>10}   " + "".join(f"{'win% / mean(bps)':>16}" for _ in cols))
        for name, mask in cohorts.items():
            sub = df.filter(mask)
            cells = []
            n0 = None
            for c in cols:
                w, m, md, n = wr(sub[c].to_numpy())
                n0 = n0 or n
                cells.append(f"{w:>5.1f}/{m:>+7.0f}")
            print(f"  {name:<24}{sub.height:>10,}   " + "".join(f"{x:>16}" for x in cells))
        print()

    # ---- B) enter-and-exit-at-a-gain: target-before-stop ----
    print("── B) ENTER-AND-EXIT-AT-A-GAIN  (P target-first / stop-first / neither; "
          "tgt-share = tgt/(tgt+stop)) ──")
    print(f"  {'cohort':<24}{'pair':<20}{'target%':>9}{'stop%':>8}{'neither%':>10}"
          f"{'tgt-share':>11}{'E[±] bps*':>11}")
    for name, mask in cohorts.items():
        sub = df.filter(mask)
        for lab, col, T, S in pairs:
            r = event_probs(sub[col])
            if r is None:
                continue
            exp_bps = (r["pt"]/100*T - r["ps"]/100*S) * 100  # neither→0 proxy, in bps
            print(f"  {name:<24}{lab:<20}{r['pt']:>8.1f}%{r['ps']:>7.1f}%{r['pn']:>9.1f}%"
                  f"{r['tshare']:>10.1f}%{exp_bps:>+11.0f}")
        print()
    print("  *E[±] proxy: target→+T, stop→-S, neither→0. >0 = favorable; it ignores the")
    print("   'neither' residual (real hold-to-horizon return), so read it as directional only.\n")

    # ---- STABILITY: momentum cohort headline by year ----
    print("── STABILITY: 'UP this morning' cohort, by year ──")
    hdr_hz = [h for h in ["1d", "5d"] if f"ret_{h}_excess_spy" in fo_have]
    sympair = next((p for p in pairs if p[0].startswith("sym 2%")), None)
    print(f"  {'year':<6}{'n':>9}   " +
          "".join(f"{'exc '+h+' win%/mean':>20}" for h in hdr_hz) +
          (f"{'2%/2% tgt-share':>18}" if sympair else ""))
    up = df.filter(mom > 0)
    for y in [2016, 2017, 2018, 2019, 2020]:
        s = up.filter(pl.col("yr") == y)
        cells = []
        for h in hdr_hz:
            w, m, md, n = wr(s[f"ret_{h}_excess_spy"].to_numpy())
            cells.append(f"{w:>6.1f}% /{m:>+7.0f}")
        extra = ""
        if sympair:
            r = event_probs(s[sympair[1]])
            extra = f"{(r['tshare'] if r else np.nan):>17.1f}%"
        print(f"  {y:<6}{s.height:>9,}   " + "".join(f"{x:>20}" for x in cells) + extra)

    print("\n(reading it: RAW win% high & positive = the market drifted up and you rode it;")
    print(" EXCESS win% ≈ 50 & mean ≈ 0 = momentum SELECTION added nothing beyond being long;")
    print(" tgt-share ≈ 50% = symmetric up/down = no reliable directional exit edge.)")


if __name__ == "__main__":
    main()
