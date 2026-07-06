#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 4 — Step 3: THE TEST. PEAD with a true (time-series) SUE — can the LONG leg clear cost?

Pre-registered design (frozen before running; see phase4 findings doc):
  * Universe/entry: deployable (CS, mega/large/mid, HL/L/N liq), enter 10:00, days_since 2-6.
  * Signal: point-in-time time-series SUE of the most recent earnings event (edgar_sue.parquet),
    event matched by period_end within 120d before the event date.
  * PRIMARY: pooled calendar-time TOP-QUINTILE long leg — per day, mean ret_5d_excess_spy of
    the top-SUE-quintile names (>=3 names); mean over days, block bootstrap (block=5) CI.
    PASS = net@15bp > 0 with CI excluding 0, positive in >=4/5 eras, placebo ~0.
    (Power gate: MDE95 ~23bp at this design — a null only rules out effects >~23bp.)
  * SECONDARY (context, not verdict): quintile ladder, decile spread (phase1 comparability),
    21d horizon, price-proxy comparison on the SAME matched rows, corr(SUE, proxy).
  * PLACEBO: SUE shuffled within day -> must collapse to ~0.
  * Train window ONLY (2016-06-08..2020-12-31). The 2021-2022 validation split is touched
    only if the train verdict is PASS.  --validate runs 2021-01..2022-12 with the same frozen
    construction. Holdout 2023+ stays sealed regardless.

Run: scripts/phase4_sue_pead.py [--validate]
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
MINQ = 3          # min names in the top-quintile portfolio for a day to count
COSTS = [15, 20]  # round-trip bp
MAX_EVENT_LAG = 120  # days: period_end must be within this window before the event


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
    """per-day mean(out) of names with sig in [q_lo,q_hi) quantile band; returns (days, series)."""
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
    return f"{pos}/{len(ss)} pos", pos, len(ss)


def main():
    t0 = dt.datetime.now()
    tag = "VALIDATION 2021-2022" if VALIDATE else "TRAIN 2016-2020"
    print("=" * 96)
    print(f"Phase 4 — TRUE-SUE PEAD ({tag}): does the long leg clear cost?")
    print("=" * 96)

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select("day", "security_id", "market_cap_bucket"))
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "days_since_last_earnings", "eod_day_close")
          .join(cls, on=["day", "security_id"], how="inner")
          .sort(["security_id", "day"])
          .with_columns((pl.col("eod_day_close") / pl.col("eod_day_close").shift(2).over("security_id") - 1.0)
                        .alias("r2"))
          .with_columns(pl.when(pl.col("days_since_last_earnings") == 1).then(pl.col("r2")).otherwise(None)
                        .alias("_react"))
          .with_columns(pl.col("_react").forward_fill().over("security_id").alias("reaction_ret"))
          # event date: day where days_since==0, forward-filled
          .with_columns(pl.when(pl.col("days_since_last_earnings") == 0).then(pl.col("day"))
                        .otherwise(None).alias("_ev"))
          .with_columns(pl.col("_ev").forward_fill().over("security_id").alias("event_date")))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "ret_5d_excess_spy", "ret_21d_excess_spy"))
    panel = (do.join(fo, on=["day", "security_id"], how="inner")
             .filter((pl.col("days_since_last_earnings") >= 2) & (pl.col("days_since_last_earnings") <= 6)
                     & pl.col("event_date").is_not_null())
             .collect())

    sidmap = pl.read_parquet("data/phase1_analysis/edgar_sid_cik_map.parquet")
    sue = pl.read_parquet("data/phase1_analysis/edgar_sue.parquet")
    panel = panel.join(sidmap, on="security_id", how="left")

    # match each (cik, event_date) to the latest SUE with period_end in (event-120d, event]
    ev = (panel.select("cik", "event_date").unique().drop_nulls()
          .join(sue.select("cik", "period_end", "filed", "sue"), on="cik", how="inner")
          .filter(((pl.col("event_date") - pl.col("period_end")).dt.total_days()).is_between(0, MAX_EVENT_LAG)
                  & (((pl.col("filed") - pl.col("event_date")).dt.total_days()) <= 90))
          .sort("period_end", descending=True)
          .unique(subset=["cik", "event_date"], keep="first")
          .select("cik", "event_date", "sue"))
    panel = panel.join(ev, on=["cik", "event_date"], how="left")

    n_ev_all = panel.select("security_id", "event_date").unique().height
    n_ev_sue = panel.filter(pl.col("sue").is_not_null()).select("security_id", "event_date").unique().height
    print(f"\npost-earnings entry rows: {panel.height:,} · unique events: {n_ev_all:,} · "
          f"with SUE: {n_ev_sue:,} ({100*n_ev_sue/max(n_ev_all,1):.1f}%)")
    mr = (panel.with_columns(pl.col("day").dt.year().alias("y"))
          .group_by("y").agg((pl.col("sue").is_not_null().mean() * 100).alias("match_pct"),
                             pl.len().alias("rows")).sort("y"))
    print(mr)

    df = panel.filter(pl.col("sue").is_not_null() & pl.col("ret_5d_excess_spy").is_not_null())
    day = df["day"].to_numpy().astype("datetime64[D]")
    s_sue = df["sue"].to_numpy().astype(float)
    s_prx = df["reaction_ret"].to_numpy().astype(float)
    o5 = df["ret_5d_excess_spy"].to_numpy().astype(float)
    o21 = df["ret_21d_excess_spy"].to_numpy().astype(float)
    rng = np.random.default_rng(SEED)

    ok = ~np.isnan(s_prx)
    print(f"\ncorr(SUE, price-proxy reaction): {np.corrcoef(s_sue[ok], s_prx[ok])[0,1]:+.3f}")

    print("\n── PRIMARY: pooled top-SUE-quintile long leg, 5d excess-over-SPY ──")
    d_top, top = daily_quantile_series(day, s_sue, o5, 0.8, 1.0)
    lo, hi = block_ci(top, rng)
    esig, _, _ = era_signs(d_top, top)
    g = top.mean() * 1e4
    print(f"  gross: {g:+7.1f} bp/5d  CI[{lo*1e4:+6.1f},{hi*1e4:+6.1f}]  nd={len(top)}  eras {esig}")
    for c in COSTS:
        print(f"  net@{c:>2}bp: {g-c:+7.1f} bp/5d  CI[{lo*1e4-c:+6.1f},{hi*1e4-c:+6.1f}]"
              f"  {'PASS candidate' if lo*1e4-c > 0 else 'fails CI>0'}")

    print("\n── placebo: SUE shuffled within day (must be ~0) ──")
    s_shuf = s_sue.copy()
    order = np.argsort(day, kind="stable")
    u, idx = np.unique(day[order], return_index=True); ends = np.append(idx[1:], len(order))
    tmp = s_shuf[order]
    for i in range(len(u)):
        seg = slice(idx[i], ends[i]); tmp[seg] = rng.permutation(tmp[seg])
    s_shuf[order] = tmp
    d_pl, pl_ser = daily_quantile_series(day, s_shuf, o5, 0.8, 1.0)
    plo, phi = block_ci(pl_ser, rng)
    print(f"  placebo top-quintile: {pl_ser.mean()*1e4:+6.1f} bp  CI[{plo*1e4:+6.1f},{phi*1e4:+6.1f}]")

    print("\n── quintile ladder (5d gross, bp) — monotone in SUE? ──")
    for k in range(5):
        dq, q = daily_quantile_series(day, s_sue, o5, k * 0.2, (k + 1) * 0.2 if k < 4 else 1.0)
        ql, qh = block_ci(q, rng)
        print(f"  Q{k+1} [{k*20:>2}-{(k+1)*20:>3}%]: {q.mean()*1e4:+7.1f}  CI[{ql*1e4:+6.1f},{qh*1e4:+6.1f}]  nd={len(q)}")

    print("\n── secondary ──")
    dd, dtop = daily_quantile_series(day, s_sue, o5, 0.9, 1.0)
    dl, dh = block_ci(dtop, rng)
    print(f"  top-DECILE long leg 5d : {dtop.mean()*1e4:+7.1f}  CI[{dl*1e4:+6.1f},{dh*1e4:+6.1f}]  nd={len(dtop)}")
    d21, t21 = daily_quantile_series(day, s_sue, o21, 0.8, 1.0)
    l21, h21 = block_ci(t21, rng)
    print(f"  top-quintile 21d       : {t21.mean()*1e4:+7.1f}  CI[{l21*1e4:+6.1f},{h21*1e4:+6.1f}]  nd={len(t21)}")
    dpx, px = daily_quantile_series(day, s_prx, o5, 0.8, 1.0)
    xl, xh = block_ci(px, rng)
    print(f"  price-proxy top-quint  : {px.mean()*1e4:+7.1f}  CI[{xl*1e4:+6.1f},{xh*1e4:+6.1f}]  nd={len(px)}"
          f"   (same matched rows)")
    # spread for phase1 comparability
    dts, tsp = daily_quantile_series(day, s_sue, o5, 0.9, 1.0)
    dbs, bsp = daily_quantile_series(day, s_sue, o5, 0.0, 0.1)
    both = np.intersect1d(dts, dbs)
    if len(both) > 20:
        sp = (tsp[np.isin(dts, both)] - bsp[np.isin(dbs, both)])
        sl, sh = block_ci(sp, rng)
        print(f"  decile SPREAD (T-B) 5d : {sp.mean()*1e4:+7.1f}  CI[{sl*1e4:+6.1f},{sh*1e4:+6.1f}]"
              f"  nd={len(sp)}   (phase1 proxy benchmark: +58.9)")

    print("\n--- pre-registered verdict rule ---")
    print("  PASS  = top-quintile net@15 > 0, CI(net) excludes 0, >=4/5 eras positive, placebo ~0")
    print("  else NULL (with the caveat: MDE ~23bp -> a null only rules out strong effects)")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
