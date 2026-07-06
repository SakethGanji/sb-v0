#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 6B — FEATURE-CONTINUATION / SIGNAL-DECAY test (see phase6-preregistration.md, 6B).

Q1: do favorable variables persist? (descriptive)
Q2: is persistence FORECASTABLE at entry from knowable features? (OOS AUC vs perm null)
Q3: does predicted persistence carry forward return net of cost? (the verdict)

Registered expectations: Q1 yes, Q2 yes (~0.65+ AUC, mechanical), Q3 ~0 net (null).

Run: scripts/phase6b_signal_decay.py
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingClassifier

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
SEED = 20260706
N_BOOT = 4000
BLOCK = 5
N_PERM = 8
EMBARGO_D = 7          # calendar days > 5 trading days
TRAIN_TEST = [([2016, 2017], 2018), ([2016, 2017, 2018], 2019), ([2016, 2017, 2018, 2019], 2020)]
FEATS = ["volpct", "voltrend", "trail21", "trail63", "volumetrend", "mom_depth",
         "overnight_gap", "intraday_ret_0930_to_1000", "beta_spy_60d",
         "consecutive_up_days_close_to_close", "days_since_last_5pct_move", "atr_14d"]


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


def auc(y, s):
    o = np.argsort(s)
    r = np.empty(len(s)); r[o] = np.arange(1, len(s) + 1)
    n1 = y.sum(); n0 = len(y) - n1
    if n1 == 0 or n0 == 0:
        return np.nan
    return (r[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)


def main():
    t0 = dt.datetime.now()
    print("=" * 96)
    print("Phase 6B — signal persistence: forecastable? and does the forecast pay?")
    print("=" * 96)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select("day", "security_id", "volatility_percentile_today", "beta_spy_60d", "atr_14d"))
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "eod_day_close", "yang_zhang_vol_5d", "yang_zhang_vol_42d",
                  "adv_5d", "adv_60d", "overnight_gap", "intraday_ret_0930_to_1000",
                  "consecutive_up_days_close_to_close", "days_since_last_5pct_move")
          .join(cls, on=["day", "security_id"], how="inner")
          .sort(["security_id", "day"])
          .with_columns([
              (pl.col("eod_day_close").shift(1) / pl.col("eod_day_close").shift(22) - 1)
              .over("security_id").alias("trail21"),
              (pl.col("eod_day_close").shift(1) / pl.col("eod_day_close").shift(64) - 1)
              .over("security_id").alias("trail63"),
              (pl.col("yang_zhang_vol_5d") / pl.col("yang_zhang_vol_42d")).alias("voltrend"),
              (pl.col("adv_5d") / pl.col("adv_60d")).alias("volumetrend"),
              pl.col("volatility_percentile_today").alias("volpct")]))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "ret_5d_excess_spy"))
    df = (do.join(fo, on=["day", "security_id"], how="inner")
          .drop_nulls(["trail21", "voltrend"]).collect()
          .with_columns([
              (pl.col("trail21").rank("ordinal") / pl.len()).over("day").alias("mom_pct"),
              (pl.col("voltrend").rank("ordinal") / pl.len()).over("day").alias("vt_pct")]))
    # persistence labels: still top-decile at t+1/3/5 (future ranks -> LABELS ONLY)
    df = (df.sort(["security_id", "day"])
          .with_columns([pl.col("mom_pct").shift(-k).over("security_id").alias(f"mom_pct_t{k}")
                         for k in (1, 3, 5)]
                        + [pl.col("vt_pct").shift(-k).over("security_id").alias(f"vt_pct_t{k}")
                           for k in (1, 3, 5)])
          .with_columns((pl.col("mom_pct") - 0.9).alias("mom_depth"),
                        pl.col("day").dt.year().alias("yr")))
    rng = np.random.default_rng(SEED)

    for setup_nm, pcol in [("MOMENTUM top-decile", "mom_pct"), ("VOL-TREND top-decile", "vt_pct")]:
        su = df.filter(pl.col(pcol) >= 0.9)
        print(f"\n### setup: {setup_nm} · {su.height:,} entries ###")
        print("── Q1 persistence: P(still top-decile at t+k) ──")
        for k in (1, 3, 5):
            p = (su[f"{pcol}_t{k}"] >= 0.9).mean()
            print(f"  t+{k}: {p*100:5.1f}%   (base rate 10%)")
        if setup_nm.startswith("VOL"):
            continue  # Q2/Q3 registered on the PRIMARY setup only

        lab = (su[f"{pcol}_t5"] >= 0.9).cast(pl.Int8).alias("persist")
        su = su.with_columns(lab).drop_nulls(["persist", "ret_5d_excess_spy"])
        X = su.select(FEATS).to_numpy()
        y = su["persist"].to_numpy()
        day = su["day"].to_numpy().astype("datetime64[D]")
        yr = su["yr"].to_numpy()
        ret = su["ret_5d_excess_spy"].to_numpy()

        print("── Q2 forecast: OOS AUC (walk-forward, embargo 7cd) vs permutation null ──")
        preds = np.full(len(y), np.nan)
        for tr_y, te_y in TRAIN_TEST:
            tr = np.isin(yr, tr_y); te = yr == te_y
            tmax = day[tr].max()
            tr &= day <= (tmax - np.timedelta64(EMBARGO_D, "D"))
            m = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05,
                                               min_samples_leaf=200, l2_regularization=1.0,
                                               random_state=SEED)
            m.fit(X[tr], y[tr])
            preds[te] = m.predict_proba(X[te])[:, 1]
        oos = ~np.isnan(preds)
        a_real = auc(y[oos], preds[oos])
        nulls = []
        for pi in range(N_PERM):
            prng = np.random.default_rng(SEED + 100 + pi)
            preds_n = np.full(len(y), np.nan)
            for tr_y, te_y in TRAIN_TEST:
                tr = np.isin(yr, tr_y); te = yr == te_y
                tmax = day[tr].max()
                tr &= day <= (tmax - np.timedelta64(EMBARGO_D, "D"))
                ysh = y[tr].copy()
                dtr = day[tr]; o = np.argsort(dtr, kind="stable")
                u, ix = np.unique(dtr[o], return_index=True); en = np.append(ix[1:], len(o))
                tmp = ysh[o]
                for i in range(len(u)):
                    seg = slice(ix[i], en[i]); tmp[seg] = prng.permutation(tmp[seg])
                ysh[o] = tmp
                m = HistGradientBoostingClassifier(max_iter=150, learning_rate=0.05,
                                                   min_samples_leaf=200, l2_regularization=1.0,
                                                   random_state=SEED)
                m.fit(X[tr], ysh)
                preds_n[te] = m.predict_proba(X[te])[:, 1]
            nulls.append(auc(y[oos], preds_n[oos]))
        print(f"  OOS AUC {a_real:.4f}  vs perm null mean {np.mean(nulls):.4f} / 95pct "
              f"{np.quantile(nulls, 0.95):.4f}  -> "
              f"{'FORECASTABLE' if a_real > np.quantile(nulls, 0.95) else 'inside null'}")

        print("── Q3 economics: forward 5d excess by predicted-persistence quintile (OOS) ──")
        dq, pq, rq, yq = day[oos], preds[oos], ret[oos], yr[oos]
        order = np.argsort(dq, kind="stable")
        dq, pq, rq = dq[order], pq[order], rq[order]
        u, ix = np.unique(dq, return_index=True); en = np.append(ix[1:], len(dq))
        qser = {k: ([], []) for k in range(5)}
        for i in range(len(u)):
            a, b = ix[i], en[i]
            if b - a < 15:
                continue
            pp, rr = pq[a:b], rq[a:b]
            qs = np.quantile(pp, [0.2, 0.4, 0.6, 0.8])
            for k, (lo_, hi_) in enumerate(zip([-np.inf] + list(qs), list(qs) + [np.inf])):
                m2 = (pp >= lo_) & (pp < hi_) if k < 4 else (pp >= lo_)
                if m2.sum() >= 2:
                    qser[k][0].append(u[i]); qser[k][1].append(rr[m2].mean())
        for k in range(5):
            days_k, s = np.array(qser[k][0]), np.array(qser[k][1])
            lo, hi = block_ci(s, rng)
            yrs = days_k.astype("datetime64[Y]").astype(int) + 1970
            signs = [np.sign(s[yrs == yy].mean()) for yy in (2018, 2019, 2020) if (yrs == yy).sum() >= 10]
            pos = sum(1 for v in signs if v > 0)
            g = s.mean() * 1e4
            tag = " <- PASS needs net@20 CI>0" if k == 4 else ""
            print(f"  Q{k+1}: gross {g:+7.1f}  CI[{lo*1e4:+6.1f},{hi*1e4:+6.1f}]  "
                  f"net@20 {g-20:+7.1f} CI[{lo*1e4-20:+6.1f},{hi*1e4-20:+6.1f}]  "
                  f"nd={len(s)}  eras {pos}/{len(signs)}{tag}")

    print("\n--- registered verdict rule: PASS = Q5 net@20 > 0, CI excl 0, >=4/5 eras, monotone ---")
    print(f"WALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
