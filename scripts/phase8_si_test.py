#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 8 — REGISTERED TEST: short-interest positioning (phase8-preregistration.md).
Gate verdict (phase8_si_gate.py): mean-IC estimands ANSWERABLE (MDE95 0.012/0.015 vs
bar 0.03); decile spreads UNANSWERABLE (MDE 22bp/5d, 61bp/21d) -> spreads are
DESCRIPTIVE ONLY below, not adjudicated.
Family: 3 signals (DTC, SIR, dSI) x 2 horizons (5d/21d excess-SPY) = 6 mean-IC tests.
PASS per test = BY-FDR(q=0.10)-surviving block-bootstrap CI excluding 0 AND era
sign-stability 3/3 (2018/2019/2020) AND |mean IC| > within-date permutation 95th pct.
Registered priors: null / weakly-negative-gated / null.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

SET_START, SET_END = dt.date(2017, 12, 29), dt.date(2020, 11, 15)
LAG_CAL_DAYS = 11
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
BLOCK, N_BOOT, N_PERM, SEED = 2, 4000, 200, 20260707
COST_RT = 15e-4
Q_FDR = 0.10


def trading_days():
    out = []
    for f in sorted(glob.glob("data/outputs/forward_outcomes/*.parquet")):
        try:
            out.append(dt.date.fromisoformat(Path(f).stem))
        except ValueError:
            pass
    return out


def spearman(a, b):
    ra = np.argsort(np.argsort(a)).astype(float)
    rb = np.argsort(np.argsort(b)).astype(float)
    return float(np.corrcoef(ra, rb)[0, 1])


def block_boot_ci(x, rng):
    """moving-block bootstrap (block=2) mean CI + two-sided p for H0: mean=0."""
    n = len(x)
    nb = int(np.ceil(n / BLOCK))
    starts = rng.integers(0, n - BLOCK + 1, size=(N_BOOT, nb))
    idx = (starts[:, :, None] + np.arange(BLOCK)[None, None, :]).reshape(N_BOOT, -1)[:, :n]
    m = x[idx].mean(1)
    lo, hi = np.quantile(m, 0.025), np.quantile(m, 0.975)
    p = 2 * min((m <= 0).mean(), (m >= 0).mean())
    return float(lo), float(hi), float(max(p, 1 / N_BOOT))


def by_fdr(pvals, q=Q_FDR):
    m = len(pvals)
    c = sum(1.0 / i for i in range(1, m + 1))
    order = np.argsort(pvals)
    passed = np.zeros(m, dtype=bool)
    thresh = 0
    for rank, i in enumerate(order, start=1):
        if pvals[i] <= q * rank / (m * c):
            thresh = rank
    passed[order[:thresh]] = True
    return passed


def main():
    t0 = dt.datetime.now()
    print("=" * 96)
    print("Phase 8 · REGISTERED TEST — short-interest positioning (mean rank-IC family)")
    print("=" * 96)
    days = trading_days()
    si = (pl.scan_parquet("data/reference/short_interest.parquet")
          .filter(pl.col("security_id").is_not_null())
          .select("security_id", pl.col("settlement_date").cast(pl.Date), "short_interest")
          .collect())
    sets_ = sorted(s for s in si["settlement_date"].unique().to_list() if SET_START <= s <= SET_END)
    prev_of = {s: p for p, s in zip(sets_[:-1], sets_[1:])}

    rows = []   # per (date, signal, horizon) IC; plus decile spreads for descriptive read
    for s in sets_:
        avail = s + dt.timedelta(days=LAG_CAL_DAYS)
        t = next((d for d in days if d >= avail), None)
        if t is None:
            continue
        cur = si.filter(pl.col("settlement_date") == s).select("security_id", "short_interest")
        base = (pl.scan_parquet(f"data/outputs/security_classification_daily/{t}.parquet")
                .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                        & pl.col("liquidity_bucket").is_in(LIQS))
                .select("security_id", "market_cap")
                .join(pl.scan_parquet(f"data/outputs/daily_observation/{t}.parquet")
                      .select("security_id", "adv_20d", "prior_day_eod_close"), on="security_id")
                .join(pl.scan_parquet(f"data/outputs/forward_outcomes/{t}.parquet")
                      .filter(pl.col("entry_offset") == OFFSET)
                      .select("security_id", "ret_5d_excess_spy", "ret_21d_excess_spy"), on="security_id")
                .collect()
                .join(cur, on="security_id", how="inner"))
        if s in prev_of:
            prev = si.filter(pl.col("settlement_date") == prev_of[s]).select(
                "security_id", pl.col("short_interest").alias("si_prev"))
            base = base.join(prev, on="security_id", how="left")
        else:
            base = base.with_columns(pl.lit(None, dtype=pl.UInt32).alias("si_prev"))
        base = base.with_columns(
            (pl.col("short_interest") / pl.col("adv_20d")).alias("DTC"),
            (pl.col("short_interest") / (pl.col("market_cap") / pl.col("prior_day_eod_close"))).alias("SIR"),
            (pl.col("short_interest").cast(pl.Float64) / pl.col("si_prev")).log().alias("dSI"))
        for sig in ("DTC", "SIR", "dSI"):
            for hz, col in (("5d", "ret_5d_excess_spy"), ("21d", "ret_21d_excess_spy")):
                d = base.select(sig, col).drop_nulls().drop_nans()
                if d.height < 100:
                    continue
                x, y = d[sig].to_numpy(), d[col].to_numpy()
                k = max(1, len(x) // 10)
                top = np.argpartition(x, -k)[-k:]; bot = np.argpartition(x, k)[:k]
                rows.append({"date": t, "yr": t.year if t.year >= 2018 else 2018,
                             "signal": sig, "hz": hz, "ic": spearman(x, y),
                             "spread": float(y[top].mean() - y[bot].mean()), "n": len(x),
                             "xy": (x, y)})
    df_keys = [(sig, hz) for sig in ("DTC", "SIR", "dSI") for hz in ("5d", "21d")]
    rng = np.random.default_rng(SEED)

    results = []
    for sig, hz in df_keys:
        r = [row for row in rows if row["signal"] == sig and row["hz"] == hz]
        r.sort(key=lambda z: z["date"])
        ics = np.array([z["ic"] for z in r])
        lo, hi, p = block_boot_ci(ics, rng)
        eras = {}
        for y in (2018, 2019, 2020):
            e = [z["ic"] for z in r if z["yr"] == y]
            eras[y] = np.mean(e) if e else np.nan
        era_ok = len(set(np.sign(v) for v in eras.values() if not np.isnan(v))) == 1
        # permutation placebo: shuffle signal within each date
        perm = []
        for _ in range(N_PERM):
            perm.append(np.mean([spearman(rng.permutation(z["xy"][0]), z["xy"][1]) for z in r]))
        p95 = float(np.quantile(np.abs(perm), 0.95))
        spread = float(np.mean([z["spread"] for z in r]))
        results.append({"signal": sig, "hz": hz, "dates": len(r), "mean_ic": float(ics.mean()),
                        "lo": lo, "hi": hi, "p": p, "eras": eras, "era_ok": era_ok,
                        "perm95": p95, "spread_gross": spread})
    passed = by_fdr([x["p"] for x in results])
    print(f"{'signal':<5} {'hz':<4} {'D':>3} {'meanIC':>8} {'CI':>18} {'p':>7} {'FDR':>4}"
          f" {'eras 18/19/20':>22} {'perm95':>7}  {'spread(desc)':>12}  verdict")
    for ok_fdr, x in zip(passed, results):
        clear_perm = abs(x["mean_ic"]) > x["perm95"]
        verdict = "PASS" if (ok_fdr and x["era_ok"] and clear_perm
                             and (x["lo"] > 0 or x["hi"] < 0)) else "NULL"
        e = "/".join(f"{x['eras'][y]:+.3f}" for y in (2018, 2019, 2020))
        print(f"{x['signal']:<5} {x['hz']:<4} {x['dates']:>3} {x['mean_ic']:>+8.4f}"
              f" [{x['lo']:+.4f},{x['hi']:+.4f}] {x['p']:>7.4f} {str(ok_fdr)[0]:>4}"
              f" {e:>22} {x['perm95']:>7.4f}  {x['spread_gross']*1e4:>+9.1f}bp  {verdict}")
    print("\nNotes: spreads are DESCRIPTIVE (gate: UNANSWERABLE at registered bars);")
    print("any negative-signal result is SHORT-GATED (borrow/margin wall) per prereg;")
    print(f"long-leg cost floor for context: {COST_RT*1e4:.0f}bp RT at 21d horizon.")
    n_pass = sum(1 for ok_fdr, x in zip(passed, results)
                 if ok_fdr and x["era_ok"] and abs(x["mean_ic"]) > x["perm95"]
                 and (x["lo"] > 0 or x["hi"] < 0))
    print(f"\nREGISTERED VERDICT: {n_pass}/6 tests PASS."
          + (" A PASS requires 2021-22 confirmation (§7.7) before belief." if n_pass else
         " Positioning (short interest) joins the closed map: no adjudicable edge at this window."))
    print(f"WALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
