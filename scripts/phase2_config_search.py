#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 2 — CONFIG SEARCH: don't hardcode entry time / momentum length / hold length —
          SEARCH the whole grid and find the maximum HONESTLY (train-select -> OOS).

Nothing fixed. Grid:
  SIGNAL (momentum length): intraday-so-far (pre_entry_ret_from_open, offset-scaled),
     overnight gap, trailing 1d / 5d / 21d return, consecutive-up-days streak.
  ENTRY OFFSET: all 17 (0935 .. 1530).
  HOLD HORIZON: EOD, 1d, 5d, 21d.
For each (signal, offset, horizon): cross-sectional top-decile-minus-bottom-decile mean
forward excess-over-SPY return (a beta-neutral long-short), and the LONG leg (top decile).

THE KEY DISCIPLINE (max-operator optimism, E[max]>=max E): reporting the grid maximum is
overfitting. So: TRAIN = 2016-2018, OOS = 2019-2020 (holdout 2023+ untouched). We report
(a) the naive best-on-OOS (the overfit mirage), and (b) the HONEST max = pick best-on-TRAIN,
read its OOS value. If (b) ~ 0 / < cost, then even the fully-searched, nothing-hardcoded
config has no edge. Signals are strictly pre-entry (trailing returns through prior close).
DP note: the EXIT/hold decision is the optimal-stopping part; cross-fit DP (phase1_dp) already
added no OOS value, so here hold is a searched horizon, not a hardcoded 1d. Read-only.
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


def main():
    t0 = dt.datetime.now()
    print("=" * 96)
    print("Phase 2 — CONFIG SEARCH: search {signal × entry-offset × hold-horizon}, honest OOS max")
    print("=" * 96)

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id"))
    # per-day, offset-independent signals + trailing returns (strictly prior-close = no look-ahead)
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "overnight_gap", "consecutive_up_days_close_to_close",
                  "eod_day_close")
          .join(cls, on=["day", "security_id"], how="inner")
          .sort(["security_id", "day"])
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
          .with_columns(pl.col("day").dt.year().alias("yr")).collect())
    print(f"joined rows (all offsets): {fo.height:,} · offsets {fo['entry_offset'].n_unique()} · "
          f"train {TRAIN_YRS} / OOS {OOS_YRS}\n")

    SIGNALS = ["pre_entry_ret_from_open", "overnight_gap", "trail_1d", "trail_5d", "trail_21d", "streak"]
    SIGNAL_LABEL = {"pre_entry_ret_from_open": "intraday-so-far", "overnight_gap": "overnight-gap",
                    "trail_1d": "trailing-1d", "trail_5d": "trailing-5d", "trail_21d": "trailing-21d",
                    "streak": "up-day-streak"}
    offsets = sorted(fo["entry_offset"].unique().to_list())
    yr = fo["yr"].to_numpy()
    train_mask = np.isin(yr, TRAIN_YRS); oos_mask = np.isin(yr, OOS_YRS)
    day = fo["day"].to_numpy().astype("datetime64[D]")
    off = fo["entry_offset"].to_numpy()

    def decile_spread_mean(sig, ret, mask):
        """mean daily (top-decile - bottom-decile) of ret ranked by sig, within (offset,day), over mask."""
        m = mask & ~np.isnan(sig) & ~np.isnan(ret)
        d = day[m]; o = off[m]; s = sig[m]; r = ret[m]
        # group key = (offset, day)
        key = np.char.add(o.astype(str), d.astype(str))
        order = np.argsort(key, kind="stable")
        key, d2, s, r = key[order], d[order], s[order], r[order]
        uk, idx = np.unique(key, return_index=True); ends = np.append(idx[1:], len(key))
        spr, top = [], []
        for i in range(len(uk)):
            a, b = idx[i], ends[i]
            if b - a < MINDEC * 3:
                continue
            ss, rr = s[a:b], r[a:b]
            hi = ss >= np.quantile(ss, 0.9); lo = ss <= np.quantile(ss, 0.1)
            if hi.sum() >= MINDEC and lo.sum() >= MINDEC:
                spr.append(rr[hi].mean() - rr[lo].mean()); top.append(rr[hi].mean())
        return (np.mean(spr) if spr else np.nan, np.mean(top) if top else np.nan, len(spr))

    rows = []
    for sg in SIGNALS:
        sig = fo[sg].to_numpy().astype(float)
        for h in HORIZONS:
            ret = fo[f"ret_{h}_excess_spy"].to_numpy()
            for o in offsets:
                om = off == o
                tr_sp, tr_top, ntr = decile_spread_mean(sig, ret, train_mask & om)
                oo_sp, oo_top, noo = decile_spread_mean(sig, ret, oos_mask & om)
                if ntr >= 20 and noo >= 20:
                    rows.append(dict(signal=sg, offset=o, horizon=h,
                                     train_spread=tr_sp*1e4, oos_spread=oo_sp*1e4,
                                     train_long=tr_top*1e4, oos_long=oo_top*1e4))
    R = pl.DataFrame(rows)
    print(f"evaluated {R.height} configs (signal × offset × horizon)\n")

    # (a) naive best-on-OOS = the OVERFIT mirage
    bo = R.sort("oos_spread", descending=True).head(1).to_dicts()[0]
    print("── (a) NAIVE best-on-OOS (the overfit mirage — do NOT trust) ──")
    print(f"   {SIGNAL_LABEL[bo['signal']]:<16} offset {bo['offset']} hold {bo['horizon']:<4}"
          f"  OOS spread {bo['oos_spread']:+.1f}bp  (train {bo['train_spread']:+.1f}bp)")

    # (b) HONEST max = pick best-on-TRAIN, read its OOS
    bt = R.sort("train_spread", descending=True).head(1).to_dicts()[0]
    print("\n── (b) HONEST max: pick best config on TRAIN, evaluate OOS ──")
    print(f"   train-optimal: {SIGNAL_LABEL[bt['signal']]:<16} offset {bt['offset']} hold {bt['horizon']:<4}"
          f"  train {bt['train_spread']:+.1f}bp -> OOS {bt['oos_spread']:+.1f}bp "
          f"(long-leg OOS {bt['oos_long']:+.1f}bp)")

    # (b') more robust: pick top-5 on train, average their OOS (reduces single-pick luck)
    top5 = R.sort("train_spread", descending=True).head(5)
    print(f"   top-5-on-train -> avg OOS spread {top5['oos_spread'].mean():+.1f}bp  "
          f"(avg train {top5['train_spread'].mean():+.1f}bp)  | long-leg OOS {top5['oos_long'].mean():+.1f}bp")

    # in-sample vs OOS correlation across ALL configs — does train rank predict OOS at all?
    tr = R["train_spread"].to_numpy(); oo = R["oos_spread"].to_numpy()
    rank_corr = np.corrcoef(np.argsort(np.argsort(tr)), np.argsort(np.argsort(oo)))[0, 1]
    print(f"\n── generalization: rank-corr(train_spread, oos_spread) across {R.height} configs "
          f"= {rank_corr:+.3f}")
    print("   (~0 = train ranking is NOISE; a good config in-sample says nothing about OOS)")

    print("\n── best OOS-surviving LONG-only leg (tradeable), any config ──")
    best_long = R.filter((pl.col("train_long") > 0)).sort("oos_long", descending=True).head(3)
    for r in best_long.to_dicts():
        print(f"   {SIGNAL_LABEL[r['signal']]:<16} off {r['offset']} {r['horizon']:<4} "
              f"train-long {r['train_long']:+.1f} -> OOS-long {r['oos_long']:+.1f}bp")

    print("\n--- verdict ---")
    print("If the HONEST max (b) is ~0 / < a ~15-40bp round-trip cost, and rank-corr ~0, then even")
    print("the fully-searched, nothing-hardcoded config has NO edge — the grid maximum is pure")
    print("selection luck (exactly what train->OOS is designed to expose). If (b) is large, stable,")
    print("and rank-corr>0, a real config exists and deserves the full block-bootstrap + holdout.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
