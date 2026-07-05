#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 2D — SIMULATE TRADE MANAGEMENT: does a health-gated early exit beat hold-to-EOD?

Phase 2C: post-entry state predicts the REMAINING (30m->EOD) path with only rank-IC ~0.01
(beats noise, economically trivial; remaining path ~coin flip). This closes the loop the
honest way — SIMULATE the management overlay and measure EXPECTANCY, not IC:

  Base    : enter top-quintile morning momentum @10:00, HOLD to EOD  (return = ret_EOD)
  Managed : at 30m, consult the health model (OOS-predicted remaining 30m->EOD). If it
            predicts BAD (below a threshold), EXIT at 30m (return = ret_30m - exit_cost);
            else hold to EOD.

Compare mean return/trade, win%, avg winner, avg loser, PROFIT FACTOR, daily Sharpe — with
an exit-cost sweep. Walk-forward OOS (health model learned on past folds only). If Managed
does not beat Base after realistic exit cost, trade management fails -> Phase 3 decision.
Deployable/liquid, exploration window. Read-only.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CKPT, TGT = "30m", "EOD"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
TRAIN_TEST = [([2016, 2017], 2018), ([2016, 2017, 2018], 2019), ([2016, 2017, 2018, 2019], 2020)]
SEED = 20260705
EXIT_COSTS_BPS = [0.0, 5.0, 10.0]
STATE = ["ret", "high_ret_so_far", "low_ret_so_far", "close_max_ret_so_far", "close_min_ret_so_far",
         "pct_bars_profitable_so_far", "pct_bars_underwater_so_far", "volatility_within_trade",
         "rate_of_change", "current_ret_over_atr_14d", "volume_since_entry", "dollar_volume_since_entry"]


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


def stats(r, day):
    """mean bps, win%, avg win, avg loss, profit factor, daily Sharpe (ann)."""
    r = r[~np.isnan(r)]
    if len(r) == 0:
        return dict(mean=np.nan, win=np.nan, aw=np.nan, al=np.nan, pf=np.nan, sh=np.nan)
    w = r[r > 0]; l = r[r < 0]
    pf = w.sum() / -l.sum() if l.sum() < 0 else np.inf
    # daily-aggregated Sharpe
    order = np.argsort(day); d = day[order]; rr = r[order] if len(r) == len(day) else r
    return dict(mean=r.mean()*1e4, win=100*np.mean(r > 0), aw=w.mean()*1e4 if len(w) else 0,
                al=l.mean()*1e4 if len(l) else 0, pf=pf)


def main():
    t0 = dt.datetime.now()
    print("=" * 92)
    print("Phase 2D — SIMULATE MANAGEMENT: health-gated early exit vs hold-to-EOD")
    print("=" * 92)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id"))
    # momentum cohort filter needs the morning percentile from daily_observation
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "intraday_ret_0930_to_1000_percentile_today"))
    path = pl.scan_parquet(wf("forward_path_short")).filter(pl.col("entry_offset") == OFFSET)
    s30 = path.filter(pl.col("path_checkpoint") == CKPT).select(["day", "security_id"] + STATE
                                                                + [pl.col("ret").alias("ret_30")])
    eod = path.filter(pl.col("path_checkpoint") == TGT).select(["day", "security_id",
                                                                pl.col("ret").alias("ret_eod")])
    df = (s30.join(eod, on=["day", "security_id"], how="inner")
          .join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner")
          .filter(pl.col("intraday_ret_0930_to_1000_percentile_today") >= 0.8)     # momentum cohort
          .with_columns((pl.col("ret_eod") - pl.col("ret_30")).alias("remaining"))
          .with_columns(pl.col("day").dt.year().alias("yr")).sort("day").collect())

    feats = [f for f in STATE if df[f].drop_nulls().n_unique() >= 2]
    X = df.select(feats).to_numpy()
    rem = df["remaining"].to_numpy(); ret30 = df["ret_30"].to_numpy(); retEOD = df["ret_eod"].to_numpy()
    day = df["day"].to_numpy().astype("datetime64[D]"); yr = df["yr"].to_numpy()
    valid = ~np.isnan(rem) & ~np.isnan(retEOD) & ~np.isnan(ret30)
    print(f"momentum cohort with 30m+EOD path: {df.height:,}\n")

    # walk-forward OOS predicted remaining (health score)
    pred = np.full(df.height, np.nan)
    for tr_y, te_y in TRAIN_TEST:
        te = (yr == te_y) & valid; trn = np.isin(yr, tr_y) & valid
        if trn.any():
            trn = trn & (day <= day[trn].max() - np.timedelta64(2, "D"))
        if not trn.any() or not te.any():
            continue
        m = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
            min_samples_leaf=200, l2_regularization=1.0, early_stopping=True,
            validation_fraction=0.1, n_iter_no_change=20, random_state=SEED)
        m.fit(X[trn], rem[trn]); pred[te] = m.predict(X[te])

    oos = valid & ~np.isnan(pred)
    base = retEOD[oos]                      # hold to EOD
    p = pred[oos]; r30 = ret30[oos]; reod = retEOD[oos]; dd = day[oos]
    print(f"OOS trades (2018-2020): {oos.sum():,}\n")

    sB = stats(base, dd)
    print(f"  {'strategy':<34}{'mean bp':>9}{'win%':>7}{'avgW':>8}{'avgL':>8}{'PF':>7}")
    print(f"  {'BASE: hold to EOD':<34}{sB['mean']:>9.1f}{sB['win']:>6.1f}%{sB['aw']:>8.1f}{sB['al']:>8.1f}{sB['pf']:>7.2f}")

    # managed: exit at 30m if predicted-remaining below threshold. Sweep threshold + exit cost.
    for thr_q, thr_name in [(0.5, "exit worst 50%"), (0.25, "exit worst 25%"), (0.10, "exit worst 10%")]:
        thr = np.quantile(p, thr_q)
        exit_flag = p < thr
        for ec in EXIT_COSTS_BPS:
            managed = np.where(exit_flag, r30 - ec/1e4, reod)   # exit@30m pays exit cost
            sM = stats(managed, dd)
            print(f"  {'MANAGED '+thr_name+f' @{int(ec)}bp':<34}{sM['mean']:>9.1f}{sM['win']:>6.1f}%"
                  f"{sM['aw']:>8.1f}{sM['al']:>8.1f}{sM['pf']:>7.2f}"
                  f"   vs base {sM['mean']-sB['mean']:+.1f}bp")

    print("\n--- verdict ---")
    print("MANAGED beats BASE in mean/PF after realistic exit cost -> the health model adds value;")
    print("trade management is a real edge worth building out. MANAGED <= BASE (net of exit cost)")
    print("-> management does not help; combined with 2C's IC~0.01 this closes Phase 2: post-entry")
    print("info carries no tradeable edge -> Phase 3 (new data / pivot), per the roadmap.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
