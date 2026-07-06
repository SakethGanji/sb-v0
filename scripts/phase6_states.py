#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 6 — Steps 1+2: frozen market states + stability check (see phase6-preregistration.md).

States: k-means k=6 on [vol percentile, vol-trend 5/42, trail-21d, trail-63d, volume-trend
5/60], standardized with 2016-06..2018-12 stats, FIT ON TRAIN ONLY, centroids frozen, seeded.
Stability: occupancy / transition matrix / durations, 2016-18 vs 2019-20.
Registered prediction: stable, vol/liquidity-ordered (GARCH rediscovery).

Writes: data/phase1_analysis/phase6_states.parquet (security_id, day, state)
        data/phase1_analysis/phase6_centroids.parquet

Run: scripts/phase6_states.py
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl
from sklearn.cluster import KMeans

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
TRAIN_END = dt.date(2018, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
K = 6
SEED = 20260706
FEATS = ["volpct", "voltrend", "trail21", "trail63", "volumetrend"]


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
    print(f"Phase 6 — FROZEN STATES (k={K}, train-fit 2016-06..2018-12) + stability")
    print("=" * 96)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select("day", "security_id", "volatility_percentile_today"))
    do = (pl.scan_parquet(wf("daily_observation"))
          .select("day", "security_id", "eod_day_close", "yang_zhang_vol_5d",
                  "yang_zhang_vol_42d", "adv_5d", "adv_60d")
          .join(cls, on=["day", "security_id"], how="inner")
          .sort(["security_id", "day"])
          .with_columns([
              (pl.col("eod_day_close").shift(1) / pl.col("eod_day_close").shift(22) - 1)
              .over("security_id").alias("trail21"),
              (pl.col("eod_day_close").shift(1) / pl.col("eod_day_close").shift(64) - 1)
              .over("security_id").alias("trail63"),
              (pl.col("yang_zhang_vol_5d") / pl.col("yang_zhang_vol_42d")).alias("voltrend"),
              (pl.col("adv_5d") / pl.col("adv_60d")).alias("volumetrend"),
              pl.col("volatility_percentile_today").alias("volpct"),
          ]))
    df = do.select(["day", "security_id"] + FEATS).drop_nulls().collect()
    print(f"panel rows with full state vector: {df.height:,}")

    tr = df.filter(pl.col("day") <= TRAIN_END)
    X_tr = tr.select(FEATS).to_numpy()
    mu, sd = X_tr.mean(0), X_tr.std(0)
    km = KMeans(n_clusters=K, n_init=10, random_state=SEED)
    rng = np.random.default_rng(SEED)
    fit_idx = rng.choice(len(X_tr), min(300_000, len(X_tr)), replace=False)
    km.fit((X_tr[fit_idx] - mu) / sd)
    X_all = (df.select(FEATS).to_numpy() - mu) / sd
    state = km.predict(X_all)
    df = df.with_columns(pl.Series("state", state))

    cent = pl.DataFrame({"state": range(K)} | {f: km.cluster_centers_[:, i] for i, f in enumerate(FEATS)})
    print("\n── centroids (z-scores; read the signature) ──")
    print(cent.with_columns([pl.col(f).round(2) for f in FEATS]))

    # occupancy + transitions, train vs confirm
    print("\n── stability: occupancy % (2016-18 vs 2019-20) ──")
    for nm, lo, hi in [("train", EXP_START, TRAIN_END),
                       ("confirm", dt.date(2019, 1, 1), EXP_END)]:
        seg = df.filter(pl.col("day").is_between(lo, hi))
        occ = seg.group_by("state").len().sort("state")
        tot = occ["len"].sum()
        print(f"  {nm:>8}: " + "  ".join(f"S{r[0]}:{100*r[1]/tot:4.1f}%" for r in occ.iter_rows()))

    print("\n── daily transition matrix P(state_t+1 | state_t), train → confirm drift ──")
    tm = {}
    for nm, lo, hi in [("train", EXP_START, TRAIN_END),
                       ("confirm", dt.date(2019, 1, 1), EXP_END)]:
        seg = (df.filter(pl.col("day").is_between(lo, hi))
               .sort(["security_id", "day"])
               .with_columns(pl.col("state").shift(-1).over("security_id").alias("nxt"))
               .drop_nulls("nxt"))
        M = np.zeros((K, K))
        for (s, n), c in (seg.group_by("state", "nxt").len().iter_rows(named=False) and
                          [((r[0], r[1]), r[2]) for r in seg.group_by("state", "nxt").len().iter_rows()]):
            M[int(s), int(n)] = c
        M = M / M.sum(1, keepdims=True)
        tm[nm] = M
        print(f"  {nm}: diag (persistence) = " + " ".join(f"{M[i,i]:.2f}" for i in range(K)))
    dmax = np.abs(tm["train"] - tm["confirm"]).max()
    dmean = np.abs(tm["train"] - tm["confirm"]).mean()
    print(f"  |ΔP| train→confirm: max {dmax:.03f}  mean {dmean:.03f}  "
          f"({'STABLE' if dmax < 0.10 else 'DRIFTED'} by the <0.10 max-cell rule)")

    df.select("security_id", "day", "state").write_parquet("data/phase1_analysis/phase6_states.parquet")
    cent.write_parquet("data/phase1_analysis/phase6_centroids.parquet")
    print(f"\nwrote phase6_states.parquet ({df.height:,} rows) + centroids")
    print(f"WALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
