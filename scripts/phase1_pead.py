#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — PEAD TEST: post-earnings-announcement drift — the potential DIRECTION flipper.

Red-team insight: "news gated" was read as "earnings gated," but the earnings DATE
(is_earnings_day / days_since_last_earnings) and the PRICE REACTION (returns) are BOTH in
the price/volume data — and NO prior experiment conditioned on earnings proximity. PEAD is
the most robust cross-sectional anomaly and it is DIRECTIONAL (the one thing we found null).

Signal = earnings-reaction return (surprise proxy) = the 2-day close-to-close return over the
announcement window (days_since ∈ {0,1}, capturing BMO/AMC). Then, among names 2-6 days
POST-earnings, rank by that reaction and test forward excess-over-SPY drift.
  PEAD predicts: big positive reaction -> positive forward drift (continuation), monotone,
  era-stable, and the TOP decile (positive surprise) is a LONG-ONLY tradeable leg.

CONTROL (the crucial test): the SAME construction on NON-earnings names (days_since large),
signal = trailing 2-day return. If post-earnings drift >> the non-earnings control, it's
genuine PEAD, not generic short-term momentum/reversal. Block bootstrap (block=horizon),
era-consistency, cost floor. Deployable/liquid, enter 10:00. Read-only.
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
POST_LO, POST_HI = 2, 6          # entry window: days after the earnings reaction
CTRL_LO, CTRL_HI = 30, 250       # control: well clear of any earnings
HORIZONS = ["5d", "21d"]
SEED = 20260705
N_BOOT = 2000
MINDEC = 10


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


def block_ci(spr, block, rng, B=N_BOOT):
    n = len(spr)
    if n < block + 5:
        return (np.nan, np.nan)
    nb = int(np.ceil(n / block))
    idx = (rng.integers(0, n, size=(B, nb))[:, :, None] + np.arange(block)[None, None, :]) % n
    bm = spr[idx.reshape(B, -1)[:, :n]].mean(axis=1)
    return float(np.quantile(bm, 0.025)), float(np.quantile(bm, 0.975))


def daily_decile(day, sig, out, yr=None, year=None):
    """top-decile minus bottom-decile of `out`, ranked by `sig`, per day. Returns (spread series,
    top-leg series, bot-leg series)."""
    m = ~np.isnan(sig) & ~np.isnan(out)
    if year is not None:
        m = m & (yr == year)
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


def era_signs(day, yr, sig, out):
    ss = []
    for y in YEARS:
        s, _, _ = daily_decile(day, sig, out, yr=yr, year=y)
        if len(s) >= 10:
            ss.append(np.sign(s.mean()))
    return f"{max(sum(x>0 for x in ss), sum(x<0 for x in ss))}/{len(ss)}" if ss else "n/a"


def main():
    t0 = dt.datetime.now()
    print("=" * 92)
    print("Phase 1 — PEAD TEST (post-earnings drift): the direction-verdict flipper")
    print("=" * 92)

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id"))
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "days_since_last_earnings", "is_earnings_day",
                  "eod_day_close", "prior_day_eod_close")
          .join(cls, on=["day", "security_id"], how="inner"))
    # per-security 2-day close-to-close return (reaction window)
    do = (do.sort(["security_id", "day"])
          .with_columns((pl.col("eod_day_close") / pl.col("eod_day_close").shift(2).over("security_id") - 1.0)
                        .alias("r2"))
          # reaction return set on days_since==1 (spans announcement day+after), then forward-filled
          .with_columns(pl.when(pl.col("days_since_last_earnings") == 1).then(pl.col("r2"))
                        .otherwise(None).alias("_react"))
          .with_columns(pl.col("_react").forward_fill().over("security_id").alias("reaction_ret")))

    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + [f"ret_{h}_excess_spy" for h in HORIZONS]))
    df = (do.join(fo, on=["day", "security_id"], how="inner")
          .with_columns(pl.col("day").dt.year().alias("yr")).collect())
    print(f"deployable panel: {df.height:,} rows\n")

    day = df["day"].to_numpy().astype("datetime64[D]"); yr = df["yr"].to_numpy()
    dsle = df["days_since_last_earnings"].to_numpy()
    react = df["reaction_ret"].to_numpy(); r2 = df["r2"].to_numpy()
    rng = np.random.default_rng(SEED)

    postmask = (dsle >= POST_LO) & (dsle <= POST_HI)
    ctrlmask = (dsle >= CTRL_LO) & (dsle <= CTRL_HI)
    print(f"post-earnings entries (days_since {POST_LO}-{POST_HI}): {postmask.sum():,}   "
          f"control (days_since {CTRL_LO}-{CTRL_HI}): {ctrlmask.sum():,}\n")

    for hz in HORIZONS:
        out = df[f"ret_{hz}_excess_spy"].to_numpy()
        print(f"── horizon {hz}: top-minus-bottom decile of forward excess, ranked by surprise ──")
        # A: PEAD (post-earnings, signal=reaction_ret)
        dA, sA, oA = day[postmask], react[postmask], out[postmask]
        yrA = yr[postmask]
        sprA, topA, botA = daily_decile(dA, sA, oA)
        loA, hiA = block_ci(sprA, {"5d":5,"21d":21}[hz], rng)
        eA = era_signs(dA, yrA, sA, oA)
        starA = "" if (loA <= 0 <= hiA) else "✓"
        print(f"  PEAD (earnings surprise)  {sprA.mean()*1e4:>8.1f}b  CI[{loA*1e4:>6.1f},{hiA*1e4:>6.1f}]{starA:>2}"
              f"  eras {eA}  long {topA.mean()*1e4:>+6.1f}b short {botA.mean()*1e4:>+6.1f}b  nd={len(sprA)}")
        # B: control (non-earnings, signal=trailing r2)
        dB, sB, oB = day[ctrlmask], r2[ctrlmask], out[ctrlmask]
        yrB = yr[ctrlmask]
        sprB, topB, botB = daily_decile(dB, sB, oB)
        loB, hiB = block_ci(sprB, {"5d":5,"21d":21}[hz], rng)
        eB = era_signs(dB, yrB, sB, oB)
        starB = "" if (loB <= 0 <= hiB) else "✓"
        print(f"  CONTROL (2d momentum)     {sprB.mean()*1e4:>8.1f}b  CI[{loB*1e4:>6.1f},{hiB*1e4:>6.1f}]{starB:>2}"
              f"  eras {eB}  long {topB.mean()*1e4:>+6.1f}b short {botB.mean()*1e4:>+6.1f}b  nd={len(sprB)}")
        print(f"  -> PEAD minus control spread: {(sprA.mean()-sprB.mean())*1e4:+.1f} bps\n")

    print("--- verdict ---")
    print("PEAD spread POSITIVE, era-stable (✓ + eras 4-5/5), MUCH larger than the control, and the")
    print("LONG leg (positive surprise) clears cost (~10-20bp round-trip over 5-21d) -> a REAL,")
    print("long-only-tradeable DIRECTIONAL edge in the data we already have — flips the verdict.")
    print("PEAD ~ control / CI incl 0 / long-leg ~0 -> earnings conditioning adds nothing; the")
    print("direction null holds even here.  (block bootstrap block=horizon; excess-over-SPY.)")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
