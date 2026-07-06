#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 4 — Step 0: POWER GATE for the true-SUE PEAD test (run BEFORE building it).

Question: given the actual 2016-2020 deployable-universe earnings-event sample, what is the
minimum detectable long-leg effect (bp/5d, 95% CI excluding 0) for the planned test
  "rank post-earnings names (days_since 2-6) by true SUE, hold top decile 5d, excess-over-SPY"?

Method: the test statistic is the mean of the DAILY top-decile portfolio series (same
construction as phase1_pead_verify). Its sampling noise is estimated directly from the data
with the same block bootstrap (block=5) used for inference — so the MDE here is exactly the
effect the real test could declare. Signal values don't matter for the noise floor; we use
the existing reaction_ret ranking as the stand-in ranker (noise of a decile-mean series is
~invariant to which ranker picks the decile, since it is dominated by 5d idio vol).

Reported: nd (daily observations), per-day decile size, MDE95 for the long leg and the
spread; overall + mid-cap-only + SUE-coverage haircuts (70% / 50% of events matched to
EDGAR EPS). Verdict: can the test detect a plausible true-SUE long leg (~15-40bp/5d gross)?

Read-only. Run: scripts/phase4_pead_power_gate.py
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
SEED = 20260706
N_BOOT = 4000
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


def daily_top_series(day, sig, out, rng, coverage=1.0):
    """daily top-decile mean-outcome series (and spread series), optionally subsampling
    `coverage` fraction of names per day to model partial SUE match-rate."""
    m = ~np.isnan(sig) & ~np.isnan(out)
    d, s, o = day[m], sig[m], out[m]
    order = np.argsort(d, kind="stable"); d, s, o = d[order], s[order], o[order]
    u, idx = np.unique(d, return_index=True); ends = np.append(idx[1:], len(d))
    top, spr, sizes = [], [], []
    for i in range(len(u)):
        a, b = idx[i], ends[i]
        ss, oo = s[a:b], o[a:b]
        if coverage < 1.0:
            keep = rng.random(len(ss)) < coverage
            ss, oo = ss[keep], oo[keep]
        if len(ss) < MINDEC * 3:
            continue
        hi = ss >= np.quantile(ss, 0.9); lo = ss <= np.quantile(ss, 0.1)
        if hi.sum() >= MINDEC and lo.sum() >= MINDEC:
            top.append(oo[hi].mean()); spr.append(oo[hi].mean() - oo[lo].mean()); sizes.append(int(hi.sum()))
    return np.array(top), np.array(spr), np.array(sizes)


def block_se(series, rng, block=BLOCK, B=N_BOOT):
    n = len(series)
    if n < block + 5:
        return np.nan
    nb = int(np.ceil(n / block))
    idx = (rng.integers(0, n, size=(B, nb))[:, :, None] + np.arange(block)[None, None, :]) % n
    bm = series[idx.reshape(B, -1)[:, :n]].mean(axis=1)
    return float(bm.std(ddof=1))


def report(nm, day, sig, out, rng, coverage=1.0):
    top, spr, sizes = daily_top_series(day, sig, out, rng, coverage)
    if len(top) < 30:
        print(f"  {nm:<34} nd={len(top):>4}  (insufficient days — test NOT runnable)")
        return
    se_t, se_s = block_se(top, rng), block_se(spr, rng)
    print(f"  {nm:<34} nd={len(top):>4}  names/decile≈{np.mean(sizes):>5.1f}  "
          f"MDE95 long-leg {1.96*se_t*1e4:>5.1f}bp  spread {1.96*se_s*1e4:>5.1f}bp")


def main():
    t0 = dt.datetime.now()
    print("=" * 96)
    print("Phase 4 step 0 — POWER GATE for the true-SUE PEAD test (MDE from the real event sample)")
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
          .with_columns(pl.col("_react").forward_fill().over("security_id").alias("reaction_ret")))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "ret_5d_excess_spy"))
    df = do.join(fo, on=["day", "security_id"], how="inner").collect()

    day = df["day"].to_numpy().astype("datetime64[D]")
    dsle = df["days_since_last_earnings"].to_numpy()
    react = df["reaction_ret"].to_numpy()
    out = df["ret_5d_excess_spy"].to_numpy()
    cap = df["market_cap_bucket"].to_numpy()
    sid = df["security_id"].to_numpy()
    rng = np.random.default_rng(SEED)

    post = (dsle >= 2) & (dsle <= 6)
    ev = post & ~np.isnan(react) & ~np.isnan(out)
    uniq_events = len(set(zip(sid[ev].tolist(),
                              (df["day"].to_numpy()[ev] - dsle[ev].astype("timedelta64[D]")).tolist())))
    print(f"\npost-earnings entry rows (days_since 2-6): {ev.sum():,}   ~unique events: {uniq_events:,}")
    print(f"(each event contributes up to 5 entry days; inference is on the DAILY portfolio series)\n")

    print("── MDE95 (bp/5d, excess-over-SPY): the smallest effect the planned test could declare ──")
    report("full universe, 100% SUE match", day[post], react[post], out[post], rng)
    report("full universe,  70% SUE match", day[post], react[post], out[post], rng, coverage=0.70)
    report("full universe,  50% SUE match", day[post], react[post], out[post], rng, coverage=0.50)
    mid = post & (cap == "mid")
    report("mid-cap only,  100% SUE match", day[mid], react[mid], out[mid], rng)
    report("mid-cap only,   70% SUE match", day[mid], react[mid], out[mid], rng, coverage=0.70)

    print("\n── context ──")
    print("  plausible true-SUE long-leg effects (lit + our proxy): gross 15-40bp/5d; net@15-20bp cost: 0-25bp")
    print("  proxy PEAD long leg measured: +5.8bp gross (phase1) — true SUE must beat this to matter")
    print("\n--- verdict rule (pre-registered) ---")
    print("  PASS  : MDE95(long leg) <= ~20bp at realistic (70%) match rate -> test can answer the question")
    print("  MARGIN: MDE95 20-35bp -> only a strong effect is detectable; run but interpret nulls carefully")
    print("  FAIL  : MDE95 > ~35bp -> the 2016-2020 window cannot adjudicate the long leg; don't over-read a null")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
