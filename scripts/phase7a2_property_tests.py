#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 7 A2 — property tests (phase7a2-preregistration.md, frozen).
A2a residualized volume-profile HHI persistence (t+1/5/21), A2c residualized avg
dollar trade size persistence (t+1/5/21), A2b monthly Hill tail persistence (power
gate first at (stock, month)). PASS floor per prereg: residual rank AC >= 0.10 at the
shortest horizon, clustered CI excluding 0, sign-stable 5/5 eras.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
STATIC_VOL = ["atr_5d", "atr_14d", "atr_42d", "realized_vol_21d",
              "yang_zhang_vol_5d", "yang_zhang_vol_14d", "yang_zhang_vol_21d", "yang_zhang_vol_42d"]
LIQ_BLOCK = ["adv_20d", "addv_20d"]
FLOOR = 0.10


def wf(t, base="data/outputs"):
    out = []
    for f in sorted(glob.glob(f"{base}/{t}/*.parquet")):
        try:
            d = dt.date.fromisoformat(Path(f).stem)
        except ValueError:
            continue
        if EXP_START <= d <= EXP_END:
            out.append(f)
    return out


def load_panel(with_tails=False):
    cols = ["day", "security_id", "profile_hhi", "modal_share", "avg_trade_size", "avg_dollar_trade"]
    if with_tails:
        cols += ["top_r", "bot_r"]
    prof = pl.scan_parquet(wf("volume_profile_daily", base="data/phase1_analysis")).select(cols)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id"))
    do = pl.scan_parquet(wf("daily_observation")).select(["day", "security_id"] + STATIC_VOL + LIQ_BLOCK)
    df = (prof.join(cls, on=["day", "security_id"], how="inner")
              .join(do, on=["day", "security_id"], how="inner").collect())
    print(f"  panel rows {df.height:,} · days {df['day'].n_unique()}")
    return df


def rank_cols(df, cols):
    return df.with_columns([(pl.col(c).rank("average").over("day") / pl.col(c).count().over("day"))
                            .alias(f"rk_{c}") for c in cols])


def residualize(df, measure, regs, by="day"):
    """per-group cross-sectional OLS of rk_measure on rk_regs; returns residual re-rank."""
    out = np.full(df.height, np.nan)
    y_all = df[f"rk_{measure}"].to_numpy()
    X_all = np.column_stack([df[f"rk_{r}"].to_numpy() for r in regs])
    gid = df[by].rank("dense").to_numpy().astype(np.int64) - 1
    order = np.argsort(gid, kind="stable")
    bounds = np.searchsorted(gid[order], np.arange(gid.max() + 2))
    for g in range(gid.max() + 1):
        ix = order[bounds[g]:bounds[g + 1]]
        y, X = y_all[ix], X_all[ix]
        ok = ~np.isnan(y) & ~np.isnan(X).any(1)
        if ok.sum() < 50:
            continue
        Xo = np.column_stack([np.ones(ok.sum()), X[ok]])
        beta, *_ = np.linalg.lstsq(Xo, y[ok], rcond=None)
        res = y[ok] - Xo @ beta
        rr = (np.argsort(np.argsort(res)) + 1) / len(res)
        out[ix[ok]] = rr
    return df.with_columns(pl.Series("resid_rank", out))


def persistence(df, lags, era_col="yr", group="security_id", time_col="day"):
    """per-time-unit Spearman of resid_rank vs its lead at each lag; clustered stats."""
    df = df.sort([group, time_col])
    df = df.with_columns([pl.col("resid_rank").shift(-k).over(group).alias(f"lead{k}") for k in lags])
    res = {}
    for k in lags:
        per = (df.select(time_col, era_col, "resid_rank", f"lead{k}").drop_nulls()
               .filter(pl.col("resid_rank").is_not_nan() & pl.col(f"lead{k}").is_not_nan())
               .group_by(time_col, era_col).agg(pl.corr("resid_rank", f"lead{k}").alias("ac"))
               .drop_nulls().filter(pl.col("ac").is_not_nan()))
        ac = per["ac"].to_numpy()
        mean, se = float(ac.mean()), float(ac.std(ddof=1) / np.sqrt(len(ac)))
        eras = per.group_by(era_col).agg(pl.col("ac").mean()).sort(era_col)
        signs = {int(r[0]): float(r[1]) for r in eras.iter_rows()}
        stable = len({np.sign(v) for v in signs.values()}) == 1
        res[k] = (mean, se, signs, stable)
    return res


def report(name, res, shortest):
    mean, se, signs, stable = res[shortest]
    for k, (m, s, sg, st) in res.items():
        era = " ".join(f"{y}:{v:+.3f}" for y, v in sg.items())
        print(f"  {name} t+{k:<3} AC {m:+.4f} ± {s:.4f}  eras[{era}]  stable {st}")
    ok = mean >= FLOOR and mean - 1.96 * se > 0 and stable
    print(f"  {name} VERDICT: {'PASS (property exists)' if ok else 'FAIL'}"
          f"  (floor {FLOOR}, shortest-horizon AC {mean:+.4f})")
    return ok


def hill(vals, k=100):
    v = np.abs(np.asarray([x for x in vals if x is not None], dtype=float))
    v = np.sort(v[np.isfinite(v)])[::-1]
    v = v[v > 0][:k]
    if len(v) < 30:
        return np.nan
    return len(v) / np.sum(np.log(v / v[-1]))


def main():
    t0 = dt.datetime.now()
    print("=" * 96)
    print("Phase 7 A2 · property tests — orthogonalized persistence (pre-registered floors)")
    print("=" * 96)
    print("\n--- A2a/A2c: daily measures ---")
    df = load_panel()
    df = rank_cols(df, ["profile_hhi", "avg_dollar_trade", "avg_trade_size", "modal_share"]
                   + STATIC_VOL + LIQ_BLOCK)
    df = df.with_columns(pl.col("day").dt.year().alias("yr"))
    verdicts = {}
    for name, measure in [("A2a HHI", "profile_hhi"), ("A2c $trade", "avg_dollar_trade")]:
        d = residualize(df, measure, STATIC_VOL + LIQ_BLOCK)
        res = persistence(d, [1, 5, 21])
        verdicts[name] = report(name, res, 1)

    print("\n--- A2b: monthly Hill tails (power gate first) ---")
    dft = load_panel(with_tails=True).with_columns(
        pl.col("day").dt.strftime("%Y-%m").alias("month"), pl.col("day").dt.year().alias("yr"))
    mo = (dft.group_by("security_id", "month", "yr")
          .agg(pl.col("top_r").flatten().alias("up"), pl.col("bot_r").flatten().alias("dn"),
               pl.len().alias("ndays"), *[pl.col(c).mean() for c in STATIC_VOL])
          .filter(pl.col("ndays") >= 15))
    up = np.array([hill(v) for v in mo["up"].to_list()])
    dn = np.array([hill(v) for v in mo["dn"].to_list()])
    mo = mo.with_columns(pl.Series("hill_up", up), pl.Series("hill_dn", dn)).drop(["up", "dn"])
    # power gate: MDE for monthly residual rank-AC from cross-section sizes + month count
    per_m = mo.group_by("month").len().sort("month")
    ns, M = per_m["len"].to_numpy(), per_m.height - 1
    mde = 2.80 * np.sqrt(np.mean(1.0 / np.maximum(ns - 1, 1))) / np.sqrt(max(M, 1))
    print(f"  months {per_m.height} · median names/month {int(np.median(ns))}"
          f" · MDE95@80% for monthly rank-AC = {mde:.4f} (floor {FLOOR})")
    if mde > FLOOR:
        print("  A2b VERDICT: UNANSWERABLE at (stock, month) — per prereg, coarsen only if gate demands")
    else:
        mo = rank_cols(mo.rename({"month": "day"}), ["hill_up", "hill_dn"] + STATIC_VOL).rename({"day": "month"})
        for name, measure in [("A2b hill_up", "hill_up"), ("A2b hill_dn", "hill_dn")]:
            d = residualize(mo, measure, STATIC_VOL, by="month")
            res = persistence(d, [1], time_col="month")
            verdicts[name] = report(name, res, 1)
    print(f"\nA2 SUMMARY: " + "  ".join(f"{k}={'PASS' if v else 'FAIL'}" for k, v in verdicts.items()))
    print("Monetization map (frozen): any PASS routes ONLY to a Branch B extension; no delta-one branch.")
    print(f"WALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
