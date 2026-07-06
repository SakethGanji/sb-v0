#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 4 — SCORE-TAIL CHECK (companion to the PRIM subset test).

The joint probe (phase1_joint_probe.py) verdicted on rank-IC and DECILE spread — a 1%-support
pocket is diluted 10:1 in a decile. This re-runs the identical leakage-hardened walk-forward
GBM, pools the OOS predictions, and evaluates the EXTREME TAIL of the model's own ranking:
top 5% / 1% / 0.5% / 0.1% of scores within each day — pooled mean excess, win rate, net@20.
Null: the same tail metrics after shuffling predictions within day (50 shuffles, 95th pct).

If a high-P(win) pocket exists and is learnable by a global model, it must appear here.
Same universe/features/embargo as the joint probe. Train 2016-2020 only. Read-only.

Run: scripts/phase4_score_tail.py
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor

import importlib.util as _ilu
_spec = _ilu.spec_from_file_location("jp", Path(__file__).parent / "phase1_joint_probe.py")
jp = _ilu.module_from_spec(_spec); _spec.loader.exec_module(jp)  # reuse feature lists + wf

TARGET = "ret_1d_excess_spy"
TARGET5 = "ret_5d_excess_spy"
TAILS = [0.05, 0.01, 0.005, 0.001]
N_SHUF = 50
SEED = 20260706


def main():
    t0 = dt.datetime.now()
    print("=" * 92, flush=True)
    print("Phase 4 — SCORE-TAIL: does the joint GBM's extreme tail hide a high-P(win) pocket?",
          flush=True)
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
          .select(["day", "security_id"] + fo_f + [TARGET, TARGET5]))
    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner")
          .join(mc, on="day", how="left")
          .with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in feats])
          .with_columns(pl.col("day").dt.year().alias("yr"))
          .sort("day").collect())
    feats = [f for f in feats if df[f].drop_nulls().n_unique() >= 2]
    print(f"panel: {df.height:,} rows · {len(feats)} features", flush=True)
    day_arr = df["day"].to_numpy().astype("datetime64[D]")
    yr_arr = df["yr"].to_numpy()
    rng = np.random.default_rng(SEED)

    for target in [TARGET, TARGET5]:
        print(f"\n### target: {target} (walk-forward OOS 2018-2020, embargo 2d) ###", flush=True)
        res = jp.walk_forward(df, feats, target, day_arr, yr_arr, seed=jp.SEED)
        td, pr, ac = res
        cost = 20e-4

        def tail_metrics(scores):
            """per-day top-q% pooled outcomes for each tail size."""
            out = {}
            order = np.argsort(td, kind="stable")
            d, s, a = td[order], scores[order], ac[order]
            u, idx = np.unique(d, return_index=True)
            ends = np.append(idx[1:], len(d))
            for q in TAILS:
                sel = np.zeros(len(d), dtype=bool)
                for i in range(len(u)):
                    lo, hi_ = idx[i], ends[i]
                    k = max(1, int(round((hi_ - lo) * q)))
                    top = np.argpartition(s[lo:hi_], -(k))[-k:]
                    sel[lo + top] = True
                aa = a[sel]
                out[q] = (aa.mean(), (aa > 0).mean(), len(aa))
            return out

        real = tail_metrics(pr)
        # within-day shuffle null of the same tail statistic
        nulls = {q: [] for q in TAILS}
        for _ in range(N_SHUF):
            sh = pr.copy()
            order = np.argsort(td, kind="stable")
            u, idx = np.unique(td[order], return_index=True)
            ends = np.append(idx[1:], len(order))
            tmp = sh[order]
            for i in range(len(u)):
                seg = slice(idx[i], ends[i])
                tmp[seg] = rng.permutation(tmp[seg])
            sh[order] = tmp
            nm = tail_metrics(sh)
            for q in TAILS:
                nulls[q].append(nm[q][0])
        print(f"  {'tail':>6} {'n':>7} {'gross bp':>9} {'net@20':>7} {'win%':>6} "
              f"{'null95 bp':>10}  verdict", flush=True)
        for q in TAILS:
            g, w, n = real[q]
            n95 = np.quantile(nulls[q], 0.95) * 1e4
            verdict = "BEATS null" if g * 1e4 > n95 else "inside null"
            print(f"  {q*100:>5.1f}% {n:>7,} {g*1e4:>+9.1f} {g*1e4-20:>+7.1f} {w*100:>6.1f} "
                  f"{n95:>+10.1f}  {verdict}", flush=True)

    print("\n--- reading ---", flush=True)
    print("A real pocket must show: gross beats null95 AND win% >> 50 AND survives net@20 at", flush=True)
    print("the SAME tail across both targets. Tail-vs-null noise at 0.1% is expected (n small).", flush=True)
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s", flush=True)


if __name__ == "__main__":
    main()
