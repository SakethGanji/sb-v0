#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 2 — MOMENTUM-LENGTH BREAKDOWN: what is the persistent structure (train->OOS rank-corr
          +0.44) the config search found? Continuation or reversal, by lookback and horizon.

For each SIGNAL (momentum length) × HORIZON, pooled across all 17 entry offsets: the
cross-sectional top-decile-minus-bottom-decile forward excess spread (ranked HIGH signal at
top). POSITIVE = continuation (recent strength keeps winning); NEGATIVE = reversal (recent
strength gives back). Reported TRAIN (2016-18) and OOS (2019-20) with an OOS block-bootstrap
CI (block=horizon) + OOS era-signs, plus the long/short legs. Tells us which lookbacks the
data actually prefers directionally — and whether any clears a ~15-40bp cost. Read-only.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
TRAIN_YRS, OOS_YRS = [2016, 2017, 2018], [2019, 2020]
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
HORIZONS = ["EOD", "1d", "5d", "21d"]
BLOCK = {"EOD": 1, "1d": 1, "5d": 5, "21d": 21}
MINDEC = 10
N_BOOT = 2000
SEED = 20260706


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
    print("=" * 96)
    print("Phase 2 — MOMENTUM-LENGTH BREAKDOWN (continuation vs reversal, by lookback × horizon)")
    print("=" * 96)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id"))
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "overnight_gap", "consecutive_up_days_close_to_close", "eod_day_close")
          .join(cls, on=["day", "security_id"], how="inner").sort(["security_id", "day"])
          .with_columns(
              (pl.col("eod_day_close").shift(1).over("security_id")
               / pl.col("eod_day_close").shift(2).over("security_id") - 1).alias("trail_1d"),
              (pl.col("eod_day_close").shift(1).over("security_id")
               / pl.col("eod_day_close").shift(6).over("security_id") - 1).alias("trail_5d"),
              (pl.col("eod_day_close").shift(1).over("security_id")
               / pl.col("eod_day_close").shift(22).over("security_id") - 1).alias("trail_21d"))
          .select("day", "security_id", "overnight_gap",
                  pl.col("consecutive_up_days_close_to_close").alias("streak"),
                  "trail_1d", "trail_5d", "trail_21d"))
    exc = [f"ret_{h}_excess_spy" for h in HORIZONS]
    fo = (pl.scan_parquet(wf("forward_outcomes"))
          .select(["day", "security_id", "entry_offset", "pre_entry_ret_from_open"] + exc)
          .join(do, on=["day", "security_id"], how="inner")
          .with_columns((pl.col("day").dt.year()).alias("yr")).collect())
    print(f"rows {fo.height:,} · 17 offsets · TRAIN {TRAIN_YRS} OOS {OOS_YRS}\n")

    SIGNALS = [("pre_entry_ret_from_open", "intraday-so-far"), ("overnight_gap", "overnight-gap"),
               ("trail_1d", "trailing-1d"), ("trail_5d", "trailing-5d"),
               ("trail_21d", "trailing-21d"), ("streak", "up-day-streak")]
    yr = fo["yr"].to_numpy(); day = fo["day"].to_numpy().astype("datetime64[D]")
    off = fo["entry_offset"].to_numpy()
    train_m = np.isin(yr, TRAIN_YRS); oos_m = np.isin(yr, OOS_YRS)
    rng = np.random.default_rng(SEED)

    def spreads(sig, ret, mask):
        """per-(offset,day) top-minus-bottom decile spread + long/short legs, over mask."""
        m = mask & ~np.isnan(sig) & ~np.isnan(ret)
        d = day[m]; o = off[m]; s = sig[m]; r = ret[m]; y = yr[m]
        key = np.char.add(o.astype(str), d.astype("datetime64[D]").astype(str))
        order = np.argsort(key, kind="stable")
        key, s, r, dd, yy = key[order], s[order], r[order], d[order], y[order]
        uk, idx = np.unique(key, return_index=True); ends = np.append(idx[1:], len(key))
        spr, top, bot, yrs = [], [], [], []
        for i in range(len(uk)):
            a, b = idx[i], ends[i]
            if b - a < MINDEC * 3:
                continue
            ss, rr = s[a:b], r[a:b]
            hi = ss >= np.quantile(ss, 0.9); lo = ss <= np.quantile(ss, 0.1)
            if hi.sum() >= MINDEC and lo.sum() >= MINDEC:
                spr.append(rr[hi].mean() - rr[lo].mean()); top.append(rr[hi].mean())
                bot.append(rr[lo].mean()); yrs.append(yy[a])
        return np.array(spr), np.array(top), np.array(bot), np.array(yrs)

    def block_ci(x, blk):
        n = len(x)
        if n < blk + 5:
            return (np.nan, np.nan)
        nb = int(np.ceil(n / blk))
        idx = (rng.integers(0, n, size=(N_BOOT, nb))[:, :, None] + np.arange(blk)[None, None, :]) % n
        bm = x[idx.reshape(N_BOOT, -1)[:, :n]].mean(axis=1)
        return float(np.quantile(bm, 0.025)), float(np.quantile(bm, 0.975))

    print(f"  {'signal':<17}{'hz':>4}{'TRAIN sp':>10}{'OOS sp':>9}{'OOS 95%CI':>20}"
          f"{'OOSlong':>9}{'OOSshort':>9}{'eras':>6}  type")
    for col, lab in SIGNALS:
        sig = fo[col].to_numpy().astype(float)
        for h in HORIZONS:
            ret = fo[f"ret_{h}_excess_spy"].to_numpy()
            tr, _, _, _ = spreads(sig, ret, train_m)
            oo, oolong, oobot, ooyr = spreads(sig, ret, oos_m)
            if len(tr) < 20 or len(oo) < 20:
                continue
            lo, hi = block_ci(oo, BLOCK[h])
            esign = []
            for y in OOS_YRS:
                sy = oo[ooyr == y]
                if len(sy) >= 10:
                    esign.append(np.sign(sy.mean()))
            eras = f"{max(sum(x>0 for x in esign), sum(x<0 for x in esign))}/{len(esign)}" if esign else "-"
            typ = ("REVERSAL" if oo.mean() < 0 else "continu.") + (" ✓" if not (lo <= 0 <= hi) else "")
            print(f"  {lab:<17}{h:>4}{tr.mean()*1e4:>9.1f}b{oo.mean()*1e4:>8.1f}b"
                  f"  [{lo*1e4:>6.1f},{hi*1e4:>6.1f}]{oolong.mean()*1e4:>8.1f}{oobot.mean()*1e4:>8.1f}"
                  f"{eras:>6}  {typ}")
        print()

    print("--- reading ---")
    print("POSITIVE spread = continuation (buy recent strength); NEGATIVE = reversal (recent")
    print("strength gives back). A ✓ means the OOS CI excludes 0. 'eras' = OOS-year sign agreement.")
    print("Even a clean sign is only tradeable if the LONG leg (buy the favored decile) clears a")
    print("~15-40bp round-trip cost and the short leg isn't the whole story (short = gated).")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
