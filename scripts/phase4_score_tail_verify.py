#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 4 — SCORE-TAIL VERIFICATION: is the 5d top-0.1-0.5% tail (+33bp net, 50% win) real
structure or the vol/skew mirage wearing a new hat?

Battery on the pooled OOS tail picks (5d target): per-year gross/net, day-clustered block
bootstrap CI, crisis-window share, name concentration, vol/cap fingerprint of the picks,
and the win/loss payoff asymmetry (skew decomposition). Read-only.

Run: scripts/phase4_score_tail_verify.py
"""
from __future__ import annotations
import datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

import importlib.util as _ilu
_spec = _ilu.spec_from_file_location("jp", Path(__file__).parent / "phase1_joint_probe.py")
jp = _ilu.module_from_spec(_spec); _spec.loader.exec_module(jp)

TARGET = "ret_5d_excess_spy"
TAILS = [0.005, 0.001]
SEED = 20260706
BLOCK = 5
N_BOOT = 4000


def block_ci(x, rng, block=BLOCK, B=N_BOOT):
    n = len(x)
    if n < block + 5:
        return (np.nan, np.nan)
    nb = int(np.ceil(n / block))
    idx = (rng.integers(0, n, size=(B, nb))[:, :, None] + np.arange(block)[None, None, :]) % n
    bm = x[idx.reshape(B, -1)[:, :n]].mean(axis=1)
    return float(np.quantile(bm, 0.025)), float(np.quantile(bm, 0.975))


def main():
    t0 = dt.datetime.now()
    print("=" * 92, flush=True)
    print("Phase 4 — SCORE-TAIL VERIFICATION (5d, top 0.5% / 0.1%)", flush=True)
    print("=" * 92, flush=True)
    do_have = set(pl.scan_parquet(jp.wf("daily_observation")).collect_schema().names())
    fo_have = set(pl.scan_parquet(jp.wf("forward_outcomes")).collect_schema().names())
    cl_have = set(pl.scan_parquet(jp.wf("security_classification_daily")).collect_schema().names())
    mc_have = set(pl.scan_parquet(jp.wf("market_context_daily")).collect_schema().names())
    do_f, fo_f = jp.keep(jp.DO_FEATS, do_have), jp.keep(jp.FO_FEATS, fo_have)
    cl_f, mc_f = jp.keep(jp.CL_FEATS, cl_have), jp.keep(jp.MC_FEATS, mc_have)
    feats = do_f + fo_f + cl_f + mc_f

    cls = (pl.scan_parquet(jp.wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS")
                   & pl.col("market_cap_bucket").is_in(jp.CAPS)
                   & pl.col("liquidity_bucket").is_in(jp.LIQS))
           .select(["day", "security_id"] + cl_f))
    do = pl.scan_parquet(jp.wf("daily_observation")).select(["day", "security_id"] + do_f)
    mc = pl.scan_parquet(jp.wf("market_context_daily")).select(["day"] + mc_f)
    fo = (pl.scan_parquet(jp.wf("forward_outcomes")).filter(pl.col("entry_offset") == jp.OFFSET)
          .select(["day", "security_id"] + fo_f + [TARGET]))
    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner")
          .join(mc, on="day", how="left")
          .with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in feats])
          .with_columns(pl.col("day").dt.year().alias("yr"))
          .sort("day").collect())
    feats = [f for f in feats if df[f].drop_nulls().n_unique() >= 2]
    day_arr = df["day"].to_numpy().astype("datetime64[D]")
    yr_arr = df["yr"].to_numpy()
    rng = np.random.default_rng(SEED)

    # regenerate pooled OOS predictions, keep row identity via an index column trick:
    # jp.walk_forward returns (day, pred, actual) pooled in fold order; we need sid + vol too.
    # Re-implement the fold loop here with full row indices.
    X = df.select(feats).to_numpy()
    y = df[TARGET].to_numpy()
    ok = ~np.isnan(y)
    sid = df["security_id"].to_numpy()
    volpct = df["volatility_percentile_today"].to_numpy()
    cap = df["market_cap"].to_numpy()
    parts = []
    for train_yrs, test_yr in jp.TRAIN_TEST:
        te = (yr_arr == test_yr) & ok
        tr = np.isin(yr_arr, train_yrs) & ok
        tmax = day_arr[tr].max()
        tr = tr & (day_arr <= tmax - np.timedelta64(jp.EMBARGO_DAYS, "D"))
        pr = jp.fit_predict(X[tr], y[tr], X[te], jp.SEED)
        parts.append((np.where(te)[0], pr))
    idx_all = np.concatenate([p[0] for p in parts])
    pred = np.concatenate([p[1] for p in parts])
    td, ac = day_arr[idx_all], y[idx_all]
    print(f"pooled OOS rows: {len(idx_all):,} (2018-2020)", flush=True)

    order = np.argsort(td, kind="stable")
    for q in TAILS:
        d, s, a = td[order], pred[order], ac[order]
        si, vp, cp = sid[idx_all][order], volpct[idx_all][order], cap[idx_all][order]
        u, idx = np.unique(d, return_index=True)
        ends = np.append(idx[1:], len(d))
        sel = np.zeros(len(d), dtype=bool)
        for i in range(len(u)):
            lo, hi_ = idx[i], ends[i]
            k = max(1, int(round((hi_ - lo) * q)))
            top = np.argpartition(s[lo:hi_], -k)[-k:]
            sel[lo + top] = True
        aa, dd, ssid, vv, cc = a[sel], d[sel], si[sel], vp[sel], cp[sel]
        # daily portfolio series for clustered CI
        ud, uidx = np.unique(dd, return_index=True)
        uends = np.append(uidx[1:], len(dd))
        daily = np.array([aa[x:e].mean() for x, e in zip(uidx, uends)])
        lo_ci, hi_ci = block_ci(daily, rng)
        print(f"\n── tail {q*100:.1f}% : n={sel.sum():,} rows, {len(ud)} days ──", flush=True)
        print(f"  pooled gross {aa.mean()*1e4:+.1f}bp  net@20 {aa.mean()*1e4-20:+.1f}", flush=True)
        print(f"  DAILY-series gross {daily.mean()*1e4:+.1f}bp  CI[{lo_ci*1e4:+.1f},{hi_ci*1e4:+.1f}]"
              f"  net@20 CI[{lo_ci*1e4-20:+.1f},{hi_ci*1e4-20:+.1f}]", flush=True)
        for e in [2018, 2019, 2020]:
            m = (dd.astype("datetime64[Y]").astype(int) + 1970) == e
            if m.sum():
                print(f"    {e}: gross {aa[m].mean()*1e4:+7.1f}bp  n={m.sum():,}  "
                      f"win {(aa[m]>0).mean()*100:.1f}%", flush=True)
        crisis = (dd >= np.datetime64("2020-02-15")) & (dd <= np.datetime64("2020-04-30"))
        print(f"  crisis (2020-02-15..04-30) share of rows: {crisis.mean()*100:.1f}%  "
              f"crisis-rows gross {aa[crisis].mean()*1e4 if crisis.any() else float('nan'):+.1f}bp", flush=True)
        ex_crisis = aa[~crisis]
        dd_ex = dd[~crisis]
        ud2, uidx2 = np.unique(dd_ex, return_index=True)
        uends2 = np.append(uidx2[1:], len(dd_ex))
        daily_ex = np.array([ex_crisis[x:e].mean() for x, e in zip(uidx2, uends2)])
        lo2, hi2 = block_ci(daily_ex, rng)
        print(f"  EX-CRISIS daily gross {daily_ex.mean()*1e4:+.1f}bp  CI[{lo2*1e4:+.1f},{hi2*1e4:+.1f}]"
              f"  net@20 CI[{lo2*1e4-20:+.1f},{hi2*1e4-20:+.1f}]", flush=True)
        vals, cnts = np.unique(ssid, return_counts=True)
        top5 = np.sort(cnts)[::-1][:5].sum()
        print(f"  names: {len(vals)} unique, top-5 = {100*top5/len(ssid):.1f}% of picks", flush=True)
        print(f"  fingerprint: mean vol-percentile {np.nanmean(vv):.2f}, median mktcap "
              f"${np.nanmedian(cc)/1e9:.1f}B", flush=True)
        w = aa > 0
        print(f"  payoff decomposition: win {w.mean()*100:.1f}%  avg win {aa[w].mean()*1e4:+.1f}bp"
              f"  avg loss {aa[~w].mean()*1e4:+.1f}bp  (skew ratio {abs(aa[w].mean()/aa[~w].mean()):.2f})",
              flush=True)

    print("\n--- reading ---", flush=True)
    print("Real if: daily-clustered net CI > 0, positive ALL THREE years, survives ex-crisis,", flush=True)
    print("not name-concentrated. A 2020-only / crisis-driven / skew-only result = vol mirage #5.", flush=True)
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s", flush=True)


if __name__ == "__main__":
    main()
