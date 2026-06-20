#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — FACTOR-SCAN VERIFICATION: is the consistent structure real, or junk+overlap?

The systematic scan found big era-stable cross-sectional spreads (vol/liquidity/size
factors), but with implausible magnitudes (−12%/21d). Two suspected artifacts:
  (1) the relaxed universe — extreme deciles full of micro-cap / leveraged-ETF junk;
  (2) multi-day overlap — day-resampling understates CIs (need block bootstrap).

This re-tests the top factor families on BOTH universes, with a circular BLOCK
bootstrap (block = horizon trading days), and reports the junk composition of the
extreme deciles. If the ordering survives on the DEPLOYABLE universe with block CIs,
it's real, clean, era-stable structure; if it collapses, it was contamination.
Read-only.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
YEARS = [2016, 2017, 2018, 2019, 2020]
SEED = 20260620
N_BOOT = 2000
MINDEC = 5
FEATURES = ["atr_14d", "realized_vol_21d_rank_today", "addv_20d",
            "intraday_volume_0930_to_1000", "pre_entry_ret_from_high",
            "days_since_last_10pct_move", "overnight_gap", "beta_spy_60d"]
HZ = {"1d": 1, "5d": 5, "21d": 21}
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]


def window_files(t):
    out = []
    for f in sorted(glob.glob(f"data/outputs/{t}/*.parquet")):
        try:
            d = dt.date.fromisoformat(Path(f).stem)
        except ValueError:
            continue
        if EXP_START <= d <= EXP_END:
            out.append(f)
    return out


def block_boot_ci(spr, block, rng, B=N_BOOT):
    n = len(spr); nb = int(np.ceil(n / block))
    idx = (rng.integers(0, n, size=(B, nb))[:, :, None] + np.arange(block)[None, None, :]) % n
    bm = spr[idx.reshape(B, -1)[:, :n]].mean(axis=1)
    return float(np.quantile(bm, 0.025)), float(np.quantile(bm, 0.975))


def daily_spread(day, q, o, yr=None, year=None):
    m = ~np.isnan(o)
    if year is not None:
        m = m & (yr == year)
    top = m & (q >= 0.9); bot = m & (q <= 0.1)
    def dm(mask):
        d = day[mask]; x = o[mask]
        order = np.argsort(d); ds, xs = d[order], x[order]
        u, idx = np.unique(ds, return_index=True); ends = np.append(idx[1:], len(xs))
        return u, np.array([xs[idx[i]:ends[i]].mean() for i in range(len(u))]), ends - idx
    ut, mt, ct = dm(top); ub, mb, cb = dm(bot)
    common = np.intersect1d(ut[ct >= MINDEC], ub[cb >= MINDEC])
    if len(common) < 20:
        return None
    return mt[np.isin(ut, common)] - mb[np.isin(ub, common)]


def run(df, label, rng):
    print(f"\n=== universe: {label}  (n={df.height:,}) ===")
    day = df["day"].to_numpy(); yr = df["yr"].to_numpy()
    print(f"  {'feature':<30}{'hz':>4}{'spread':>10}{'block95%CI':>22}{'eras':>6}")
    for f in FEATURES:
        q = df[f"__q_{f}"].to_numpy()
        for h, blk in HZ.items():
            o = df[f"ret_{h}_excess_spy"].to_numpy()
            spr = daily_spread(day, q, o)
            if spr is None:
                continue
            lo, hi = block_boot_ci(spr, blk, rng)
            signs = []
            for y in YEARS:
                s = daily_spread(day, q, o, yr, y)
                if s is not None:
                    signs.append(np.sign(s.mean()))
            ec = len(signs) == 5 and (all(x > 0 for x in signs) or all(x < 0 for x in signs))
            sig = "" if (lo <= 0 <= hi) else "✓"
            print(f"  {f:<30}{h:>4}{spr.mean()*1e4:>9.1f}b  [{lo*1e4:>7.1f},{hi*1e4:>7.1f}]"
                  f"{sig:>2}{(str(max(sum(s>0 for s in signs),sum(s<0 for s in signs)))+'/5'):>6}")


def main():
    t0 = dt.datetime.now()
    print("=" * 84)
    print("Phase 1 — FACTOR VERIFICATION (clean universe + block bootstrap)")
    print("=" * 84)
    cls = (pl.scan_parquet(window_files("security_classification_daily"))
           .select("day", "security_id", "ticker_type", "market_cap_bucket",
                   "liquidity_bucket", "is_etf", "is_leveraged_etf"))
    do = pl.scan_parquet(window_files("daily_observation")).select(
        "day", "security_id", "atr_14d", "realized_vol_21d_rank_today", "addv_20d",
        "intraday_volume_0930_to_1000", "days_since_last_10pct_move", "overnight_gap", "beta_spy_60d")
    fo = (pl.scan_parquet(window_files("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "pre_entry_ret_from_high",
                  "ret_1d_excess_spy", "ret_5d_excess_spy", "ret_21d_excess_spy"))
    base = fo.join(cls, on=["day", "security_id"], how="inner").join(
        do, on=["day", "security_id"], how="inner").with_columns(pl.col("day").dt.year().alias("yr"))

    # junk composition of extreme ATR decile (ALL universe)
    allu = base.with_columns(
        (pl.col("atr_14d").rank("average").over("day") / pl.col("atr_14d").count().over("day")).alias("aq")
    ).collect()
    topvol = allu.filter(pl.col("aq") >= 0.9)
    print(f"\n[composition] top-ATR-decile (ALL universe, n={topvol.height:,}):")
    for c, lab in [("is_leveraged_etf", "leveraged ETF"), ("is_etf", "any ETF")]:
        print(f"   {lab:<16}{100*topvol[c].fill_null(False).mean():>5.1f}%")
    print(f"   micro+small cap {100*topvol['market_cap_bucket'].is_in(['micro','small']).mean():>5.1f}%")
    print(f"   non-CS          {100*(topvol['ticker_type']!='CS').mean():>5.1f}%")

    rng = np.random.default_rng(SEED)
    qcols = [(pl.col(f).rank("average").over("day") / pl.col(f).count().over("day")).alias(f"__q_{f}")
             for f in FEATURES]
    full = base.with_columns(qcols).collect()
    run(full, "ALL (relaxed)", rng)

    clean = (base.filter((pl.col("ticker_type") == "CS")
                         & pl.col("market_cap_bucket").is_in(CAPS)
                         & pl.col("liquidity_bucket").is_in(LIQS))
             .with_columns(qcols).collect())
    run(clean, "DEPLOYABLE (CS, mega/large/mid, liquid/normal)", rng)

    print("\n--- verdict ---")
    print("If the vol/liquidity spreads stay large & era-consistent on DEPLOYABLE with")
    print("block CIs → real, clean, tradeable-universe structure (the low-vol/size factor).")
    print("If they collapse vs ALL → the scan's big numbers were junk-decile + overlap.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
