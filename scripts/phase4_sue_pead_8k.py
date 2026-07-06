#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 4 — Step 5: TRUE-SUE PEAD on the CORRECTED (8-K announcement) clock.

The filing-date clock test (phase4_sue_pead.py) came back NULL, but the diagnostic showed
the dataset's earnings dates are 10-Q/K FILING dates — 0-14+ days after the press release.
This re-runs the same pre-registered test with events = 8-K item 2.02 dates.

Clock: day0 = first trading day >= 8-K filingDate (8-Ks filed after the close on D make the
news public the evening of D; day0=D is the reaction day, and entries start day1 at 10:00 —
always information-safe). PRIMARY entry window: trading days 1-5 after day0. Sensitivity: 2-6.

Everything else identical to phase4_sue_pead.py: top-SUE-quintile pooled long leg, 5d
excess-over-SPY, >=3 names/day, block bootstrap, placebo (within-day shuffle), quintile
ladder, era stability. PASS = net@15 > 0 with CI excl 0, >=4/5 eras, placebo ~0.
Train 2016-2020 only; validation 2021-22 only on PASS; holdout sealed.

Run: scripts/phase4_sue_pead_8k.py [--validate]
"""
from __future__ import annotations
import glob, sys, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

VALIDATE = "--validate" in sys.argv
EXP_START, EXP_END = ((dt.date(2021, 1, 1), dt.date(2022, 12, 31)) if VALIDATE
                      else (dt.date(2016, 6, 8), dt.date(2020, 12, 31)))
YEARS = sorted({EXP_START.year + i for i in range(EXP_END.year - EXP_START.year + 1)})
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
SEED = 20260706
N_BOOT = 4000
BLOCK = 5
MINQ = 3
COSTS = [15, 20]
MAX_EVENT_LAG = 120
PRIMARY_WIN = (1, 5)
SENS_WINS = [(2, 6), (1, 10), (6, 20)]


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


def block_ci(x, rng, block=BLOCK, B=N_BOOT):
    n = len(x)
    if n < block + 5:
        return (np.nan, np.nan)
    nb = int(np.ceil(n / block))
    idx = (rng.integers(0, n, size=(B, nb))[:, :, None] + np.arange(block)[None, None, :]) % n
    bm = x[idx.reshape(B, -1)[:, :n]].mean(axis=1)
    return float(np.quantile(bm, 0.025)), float(np.quantile(bm, 0.975))


def daily_quantile_series(day, sig, out, q_lo, q_hi, min_names=MINQ):
    m = ~np.isnan(sig) & ~np.isnan(out)
    d, s, o = day[m], sig[m], out[m]
    order = np.argsort(d, kind="stable"); d, s, o = d[order], s[order], o[order]
    u, idx = np.unique(d, return_index=True); ends = np.append(idx[1:], len(d))
    days, ser = [], []
    for i in range(len(u)):
        a, b = idx[i], ends[i]
        ss, oo = s[a:b], o[a:b]
        lo = np.quantile(ss, q_lo) if q_lo > 0 else -np.inf
        hi = np.quantile(ss, q_hi) if q_hi < 1 else np.inf
        sel = (ss >= lo) & (ss < hi) if q_hi < 1 else (ss >= lo)
        if sel.sum() >= min_names:
            days.append(u[i]); ser.append(oo[sel].mean())
    return np.array(days), np.array(ser)


def era_signs(days, ser):
    ss = []
    for y in YEARS:
        m = (days.astype("datetime64[Y]").astype(int) + 1970) == y
        if m.sum() >= 10:
            ss.append(np.sign(ser[m].mean()))
    pos = sum(x > 0 for x in ss)
    return f"{pos}/{len(ss)} pos"


def main():
    t0 = dt.datetime.now()
    tag = "VALIDATION 2021-2022" if VALIDATE else "TRAIN 2016-2020"
    print("=" * 96)
    print(f"Phase 4 — TRUE-SUE PEAD, CORRECTED 8-K CLOCK ({tag})")
    print("=" * 96)

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select("day", "security_id"))
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "eod_day_close")
          .join(cls, on=["day", "security_id"], how="inner"))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "ret_5d_excess_spy", "ret_21d_excess_spy"))
    panel = (do.join(fo, on=["day", "security_id"], how="inner")
             .sort(["security_id", "day"]).collect())

    sidmap = pl.read_parquet("data/phase1_analysis/edgar_sid_cik_map.parquet")
    ev8k = pl.read_parquet("data/phase1_analysis/edgar_8k_events.parquet")
    sue = pl.read_parquet("data/phase1_analysis/edgar_sue.parquet")
    panel = panel.join(sidmap, on="security_id", how="inner")

    # day0 per event: first trading day >= event_date, per security (asof join on the panel)
    ev = (ev8k.join(sidmap, on="cik", how="inner")
          .filter(pl.col("event_date").is_between(EXP_START - dt.timedelta(days=40), EXP_END))
          .select("security_id", "cik", "event_date").sort("event_date"))
    tdays = panel.select("security_id", "day").unique().sort("day")
    ev = ev.join_asof(tdays.rename({"day": "day0"}), left_on="event_date", right_on="day0",
                      by="security_id", strategy="forward").drop_nulls("day0")
    # SUE for each event: latest period_end within 120d before the 8-K date
    ev = (ev.join(sue.select("cik", "period_end", "sue"), on="cik", how="left")
          .filter(((pl.col("event_date") - pl.col("period_end")).dt.total_days()).is_between(0, MAX_EVENT_LAG))
          .sort("period_end", descending=True)
          .unique(subset=["security_id", "event_date"], keep="first")
          .select("security_id", "day0", pl.col("event_date").alias("ann_date"), "sue"))
    print(f"8-K events mapped to a trading day0 with SUE: {ev.height:,}")

    # trading-days-since-announcement: index panel rows per security after each day0
    panel = (panel.sort(["security_id", "day"])
             .with_columns(pl.int_range(pl.len()).over("security_id").alias("_ti")))
    ev_ti = (ev.join(panel.select("security_id", "day", "_ti"),
                     left_on=["security_id", "day0"], right_on=["security_id", "day"], how="inner")
             .rename({"_ti": "_ti0"}).select("security_id", "day0", "ann_date", "sue", "_ti0"))
    df = (panel.join(ev_ti, on="security_id", how="inner")
          .with_columns((pl.col("_ti") - pl.col("_ti0")).alias("tds"))
          .filter((pl.col("tds") >= 0) & (pl.col("tds") <= 25))
          # keep the most recent event per row
          .sort("tds").unique(subset=["security_id", "day"], keep="first"))

    skew = (df.filter(pl.col("tds") == 0)
            .select((pl.col("day0") - pl.col("ann_date")).dt.total_days().alias("gap")))
    print(f"day0 minus 8-K date (should be 0-2): p50={skew['gap'].median()} p95={skew['gap'].quantile(0.95)}")

    day = df["day"].to_numpy().astype("datetime64[D]")
    tds = df["tds"].to_numpy()
    s_sue = df["sue"].to_numpy().astype(float)
    o5 = df["ret_5d_excess_spy"].to_numpy().astype(float)
    rng = np.random.default_rng(SEED)

    def run_window(lo_d, hi_d, label, primary=False):
        m = (tds >= lo_d) & (tds <= hi_d)
        d_top, top = daily_quantile_series(day[m], s_sue[m], o5[m], 0.8, 1.0)
        if len(top) < 30:
            print(f"  {label}: insufficient days ({len(top)})"); return
        lo, hi = block_ci(top, rng)
        esig = era_signs(d_top, top)
        g = top.mean() * 1e4
        line = f"  {label}: gross {g:+7.1f}  CI[{lo*1e4:+6.1f},{hi*1e4:+6.1f}]  nd={len(top)}  eras {esig}"
        if primary:
            for c in COSTS:
                line += f"\n           net@{c}: {g-c:+7.1f}  CI[{lo*1e4-c:+6.1f},{hi*1e4-c:+6.1f}]" \
                        f"  {'** PASS candidate **' if lo*1e4-c > 0 else 'fails CI>0'}"
        print(line)
        return m

    print(f"\n── PRIMARY: top-SUE-quintile long leg, entry days {PRIMARY_WIN[0]}-{PRIMARY_WIN[1]} post-announcement, 5d excess ──")
    m_primary = run_window(*PRIMARY_WIN, f"days {PRIMARY_WIN[0]}-{PRIMARY_WIN[1]}", primary=True)

    print("\n── placebo (SUE shuffled within day, primary window) ──")
    m = m_primary
    s_shuf = s_sue[m].copy()
    dd = day[m]
    order = np.argsort(dd, kind="stable")
    u, idx = np.unique(dd[order], return_index=True); ends = np.append(idx[1:], len(order))
    tmp = s_shuf[order]
    for i in range(len(u)):
        seg = slice(idx[i], ends[i]); tmp[seg] = rng.permutation(tmp[seg])
    s_shuf[order] = tmp
    d_pl, pl_ser = daily_quantile_series(dd, s_shuf, o5[m], 0.8, 1.0)
    plo, phi = block_ci(pl_ser, rng)
    print(f"  placebo: {pl_ser.mean()*1e4:+6.1f}  CI[{plo*1e4:+6.1f},{phi*1e4:+6.1f}]")

    print("\n── quintile ladder (primary window, 5d gross bp) ──")
    for k in range(5):
        dq, q = daily_quantile_series(dd, s_sue[m], o5[m], k * 0.2, (k + 1) * 0.2 if k < 4 else 1.0)
        ql, qh = block_ci(q, rng)
        print(f"  Q{k+1}: {q.mean()*1e4:+7.1f}  CI[{ql*1e4:+6.1f},{qh*1e4:+6.1f}]  nd={len(q)}")

    print("\n── window sensitivity ──")
    for lo_d, hi_d in SENS_WINS:
        run_window(lo_d, hi_d, f"days {lo_d:>2}-{hi_d:<2}")

    print("\n── event-day reaction check (does SUE predict the day0-day1 move on this clock?) ──")
    m01 = tds <= 1
    d01 = df.filter(pl.col("tds") <= 1)
    r1 = (d01.sort(["security_id", "day"])
          .group_by("security_id", "day0").agg(pl.col("sue").first(),
                                               pl.col("ret_5d_excess_spy").first()))
    print("  (using 5d excess FROM day0/1 as reaction+drift blend)")
    s0 = r1["sue"].to_numpy().astype(float); o0 = r1["ret_5d_excess_spy"].to_numpy().astype(float)
    ok = ~np.isnan(s0) & ~np.isnan(o0)
    qs = np.quantile(s0[ok], [0.2, 0.4, 0.6, 0.8])
    for i, (ql_, qh_) in enumerate(zip([-np.inf] + list(qs), list(qs) + [np.inf])):
        mm = ok & (s0 >= ql_) & (s0 < qh_)
        print(f"  SUE Q{i+1}: mean 5d-from-event excess {np.nanmean(o0[mm])*1e4:+7.1f} bp (n={mm.sum():,})")

    print("\n--- pre-registered verdict rule (unchanged) ---")
    print("  PASS = primary net@15 > 0, CI excludes 0, >=4/5 eras pos, placebo ~0; else NULL")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
