#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — CROSS-FIT DP TEST (does a path-dependent exit policy add OOS value?).

The honest test of the DP/exit question (§7.3), with the max-operator optimism bias
controlled by cross-fitting (§7.7.1: "exploration-set V on its own training data is
never a test statistic").

Intraday optimal stopping: 10:00 entry → exit at some checkpoint by close, using the
materialized intraday path (ret_to_1030 … ret_to_close). Tabular backward-induction
DP on binned state (current_ret bin × drawdown bin × checkpoint). Cost = 5 bps
round-trip at exit. 2-FOLD CROSS-FIT: learn policy on fold A, VALUE it on held-out
fold B, and vice versa; every trade is valued out-of-sample.

Compare OOS DP value to:
  - hold-to-close (the trivial fixed policy), and
  - best FIXED-TIME exit (state-blind control — the "1-line rule" of §7.7.1).
Day-clustered bootstrap CI on the DP−baseline differences. If DP doesn't beat the
state-blind baseline OOS, the path STATE adds nothing — exits can't manufacture edge.

Representative cells (deployable, signal-firing momentum). Read-only.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CHKPTS = ["ret_to_1030", "ret_to_1100", "ret_to_1130", "ret_to_1200",
          "ret_to_1300", "ret_to_1400", "ret_to_1500", "ret_to_close"]
COST = 5.0 / 1e4
RET_BINS, DD_BINS = 5, 3
MIN_STATE = 30
SEED = 20260620
N_BOOT = 2000
CELLS = [("large", "liquid", "above_100"), ("mid", "normal", "20_to_100"),
         ("small", "normal", "5_to_20")]


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


def learn_policy(paths, days, ret_edges, dd_edges):
    """paths: (n, K) cumulative return at each checkpoint (col0..K-1, last=close).
    Backward induction → policy[k] dict {(rb,db): 'exit'/'hold'} + per-trade OOS-able
    bin function. Returns policy table + V (not used downstream)."""
    n, K = paths.shape
    full = np.hstack([np.zeros((n, 1)), paths])          # prepend entry ret=0
    maxsf = np.maximum.accumulate(full, axis=1)
    dd = maxsf - full                                    # drawdown from running max
    # bins per checkpoint position (use the value AT that checkpoint)
    rb = np.clip(np.digitize(full, ret_edges), 0, RET_BINS - 1)   # (n,K+1)
    db = np.clip(np.digitize(dd, dd_edges), 0, DD_BINS - 1)
    V = np.empty(n)
    Vnext = full[:, K] - COST                            # at close: forced exit
    V[:] = Vnext
    policy = {}
    for k in range(K - 1, 0, -1):                        # k=1..K-1 (k=0 is entry)
        exit_now = full[:, k] - COST
        # group trades by (rb,db) at checkpoint k
        key = rb[:, k] * DD_BINS + db[:, k]
        newV = Vnext.copy()
        pol_k = {}
        for b in np.unique(key):
            m = key == b
            if m.sum() < MIN_STATE:
                ev, hv = exit_now[m].mean(), Vnext[m].mean()  # sparse → still decide
            else:
                ev, hv = exit_now[m].mean(), Vnext[m].mean()
            do_exit = ev >= hv
            pol_k[int(b)] = do_exit
            newV[m] = exit_now[m] if do_exit else Vnext[m]
        policy[k] = pol_k
        Vnext = newV
    return policy


def value_policy(paths, ret_edges, dd_edges, policy):
    """Apply a learned policy to held-out trades; return realized exit return per trade."""
    n, K = paths.shape
    full = np.hstack([np.zeros((n, 1)), paths])
    maxsf = np.maximum.accumulate(full, axis=1)
    dd = maxsf - full
    rb = np.clip(np.digitize(full, ret_edges), 0, RET_BINS - 1)
    db = np.clip(np.digitize(dd, dd_edges), 0, DD_BINS - 1)
    out = full[:, K] - COST                              # default: held to close
    done = np.zeros(n, bool)
    for k in range(1, K):
        pol_k = policy.get(k, {})
        key = rb[:, k] * DD_BINS + db[:, k]
        ex = np.array([pol_k.get(int(b), False) for b in key]) & ~done
        out[ex] = full[ex, k] - COST
        done |= ex
    return out


def day_boot_diff(d, x, rng, B=N_BOOT):
    """Day-clustered one-sided LB (5th pct) of the mean of x (per-trade diff)."""
    order = np.argsort(d); ds, xs = d[order], x[order]
    uniq, idx = np.unique(ds, return_index=True)
    ends = np.append(idx[1:], len(xs))
    daily = np.array([xs[idx[i]:ends[i]].mean() for i in range(len(uniq))])
    nb = len(daily)
    bm = daily[rng.integers(0, nb, size=(B, nb))].mean(axis=1)
    return float(daily.mean()), float(np.quantile(bm, 0.05))


def main():
    t0 = dt.datetime.now()
    print("=" * 84)
    print("Phase 1 — CROSS-FIT DP TEST (path-dependent intraday exit, OOS)")
    print("=" * 84)
    cls = (pl.scan_parquet(window_files("security_classification_daily"))
           .filter(pl.col("ticker_type") == "CS")
           .select("day", "security_id", "market_cap_bucket", "liquidity_bucket", "price_bucket"))
    sig = (pl.scan_parquet(window_files("daily_observation"))
           .filter(pl.col("intraday_ret_0930_to_1000") > 0.0).select("day", "security_id"))
    df = (pl.scan_parquet(window_files("forward_outcomes"))
          .filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + CHKPTS)
          .join(cls, on=["day", "security_id"], how="inner")
          .join(sig, on=["day", "security_id"], how="inner")
          .drop_nulls(CHKPTS).collect())
    print(f"signal-firing trades w/ full intraday path: {df.height:,}")
    print(f"cost {COST*1e4:.0f}bps round-trip · state=(ret×{RET_BINS}, dd×{DD_BINS}, "
          f"chkpt×{len(CHKPTS)}) · 2-fold cross-fit\n")
    rng = np.random.default_rng(SEED)

    print(f"  {'cell':<26}{'n':>7}{'hold-close':>11}{'best-fixed':>11}{'DP(OOS)':>10}"
          f"{'DP−fixed':>10}{'LB':>9}")
    for cell in CELLS:
        sub = df.filter((pl.col("market_cap_bucket") == cell[0])
                        & (pl.col("liquidity_bucket") == cell[1])
                        & (pl.col("price_bucket") == cell[2]))
        n = sub.height
        if n < 2000:
            print(f"  {'/'.join(cell):<26}{n:>7}  (too few — skip)")
            continue
        paths = sub.select(CHKPTS).to_numpy()
        days = sub["day"].to_numpy()
        # global bin edges from full data (quantiles of per-checkpoint values & drawdown)
        full = np.hstack([np.zeros((n, 1)), paths])
        maxsf = np.maximum.accumulate(full, axis=1)
        ret_edges = np.quantile(full, np.linspace(0, 1, RET_BINS + 1))[1:-1]
        dd_edges = np.quantile(maxsf - full, np.linspace(0, 1, DD_BINS + 1))[1:-1]

        # 2-fold cross-fit (split by day so train/value are disjoint in time-clusters)
        uniq_days = np.unique(days)
        fold_of_day = {d: (i % 2) for i, d in enumerate(uniq_days)}
        fold = np.array([fold_of_day[d] for d in days])
        dp_oos = np.empty(n)
        for f in (0, 1):
            tr, va = fold != f, fold == f
            pol = learn_policy(paths[tr], days[tr], ret_edges, dd_edges)
            dp_oos[va] = value_policy(paths[va], ret_edges, dd_edges, pol)
        hold = paths[:, -1] - COST                       # hold-to-close
        # best FIXED-time exit chosen on fold-0, valued on fold-1 (and vice versa), OOS
        bf = np.empty(n)
        for f in (0, 1):
            tr, va = fold != f, fold == f
            kbest = int(np.argmax([(paths[tr, k] - COST).mean() for k in range(len(CHKPTS))]))
            bf[va] = paths[va, kbest] - COST

        m_hold, m_bf, m_dp = hold.mean(), bf.mean(), dp_oos.mean()
        diff = dp_oos - bf
        _, lb = day_boot_diff(days, diff, rng)
        print(f"  {'/'.join(cell):<26}{n:>7}{m_hold*1e4:>10.1f}b{m_bf*1e4:>10.1f}b"
              f"{m_dp*1e4:>9.1f}b{(m_dp-m_bf)*1e4:>9.1f}b{lb*1e4:>8.1f}b")

    print("\n--- verdict ---")
    print("DP(OOS) is the cross-fit value of the state-dependent exit policy. 'best-fixed'")
    print("is the state-BLIND control (best single exit time). If DP−fixed ≤ 0 (or its")
    print("day-clustered LB ≤ 0), the path STATE adds no out-of-sample value — a")
    print("state-dependent exit does not beat a 1-line rule, and exits cannot manufacture")
    print("an edge the entry doesn't have (§7.3). All returns RAW net-of-cost; the")
    print("DP-vs-fixed comparison holds the same names so beta largely cancels.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
