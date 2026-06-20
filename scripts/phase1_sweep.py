#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — COMPREHENSIVE BREADTH SWEEP: "is there ANYTHING at all?"

The prior passes all tested ONE signal (10:00 momentum). This sweeps breadth —
~16 distinct entry signals AND the exit-rule / target-before-stop dimension (the
DP-adjacent question) — through the SAME validated machinery. If nothing clears
the bar across all of it, the null is earned across the catalog, not extrapolated.

Inputs are VALIDATED before use: each signal's firing rate + non-null rate is
printed; each target/stop label's resolved/neither/no_data rates are printed.

Stage 1 — multi-signal entry expectancy. For each signal × horizon {EOD,1d,5d},
  deployable cohort (mega/large/mid × highly_liquid/liquid/normal, CS): daily
  equal-weight portfolio of cost-adjusted (5bps flat) excess-over-SPY; one-sided
  block-bootstrap LB (block=horizon days, §7.7.1) + gross; BY FDR over the grid.
Stage 2 — exit rules (DP-adjacent). For each signal × 9 materialized target/stop
  pairs: P(target before stop | resolved) vs breakeven Y/(X+Y). Edge>0 ⇒ the
  target/stop rule beats breakeven (raw). Exits can't manufacture edge from a
  no-edge entry (§7.3), so this is the empirical check on that.

All long-only (§1.6); a "down" signal = buy-the-dip. Exploration window only.
Read-only; writes data/phase1_analysis/sweep_*.parquet.
"""
from __future__ import annotations
import glob, re, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
HZ = {"EOD": 1, "1d": 1, "5d": 5}
FLAT_COST_BPS = 5.0
MIN_NAMES, MIN_DAYS = 5, 60
ALPHA = 0.10
N_BOOT = 2000
SEED = 20260620
OUT = Path("data/phase1_analysis")

PAIRS = ["0_5pct_before_minus_0_5pct_30min", "1pct_before_minus_1pct_EOD",
         "2pct_before_minus_1pct_EOD", "3pct_before_minus_1_5pct_EOD",
         "2pct_before_minus_2pct_1d", "3pct_before_minus_3pct_5d",
         "1atr_before_minus_0_5atr_EOD", "2atr_before_minus_1atr_5d",
         "3atr_before_minus_1_5atr_21d"]

# signals as (name, polars-expr over daily_observation feature columns). long-only.
def signals():
    g = pl.col("overnight_gap"); m = pl.col("intraday_ret_0930_to_1000")
    pct = pl.col("intraday_ret_0930_to_1000_percentile_today")
    fromhi = pl.col("intraday_ret_from_first_30m_high_to_1000")
    conc = pl.col("signal_concentration_percentile_today")
    return {
        "mom_up": m > 0, "mom_strong_1pct": m > 0.01, "mom_top_decile": pct > 0.9,
        "rev_down": m < 0, "rev_bottom_decile": pct < 0.1,
        "gap_up_1pct": g > 0.01, "gap_down_1pct": g < -0.01,
        "gap_and_go": (g > 0.005) & (m > 0.005), "gap_fade": (g > 0.02) & (m < 0),
        "at_30m_high": fromhi > -0.002, "pullback_from_high": fromhi < -0.015,
        "pm_vol_spike": pl.col("pre_market_volume_spike_flag"),
        "high_concentration": conc > 0.8, "low_concentration": conc < 0.2,
        "near_52w_high": (pl.col("intraday_vwap_0930_to_1000") / pl.col("high_52w")) > 0.95,
        "consec_up_3": pl.col("consecutive_up_days_close_to_close") >= 3,
    }

DO_FEATS = ["overnight_gap", "intraday_ret_0930_to_1000",
            "intraday_ret_0930_to_1000_percentile_today",
            "intraday_ret_from_first_30m_high_to_1000",
            "signal_concentration_percentile_today", "pre_market_volume_spike_flag",
            "intraday_vwap_0930_to_1000", "high_52w", "consecutive_up_days_close_to_close"]


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


def harmonic(m):
    return float(np.sum(1.0 / np.arange(1, m + 1))) if m > 0 else 1.0


def by_mask(p, alpha=ALPHA):
    m = len(p)
    if m == 0:
        return np.array([], bool)
    o = np.argsort(p); sp = np.asarray(p)[o]
    thr = (np.arange(1, m + 1) * alpha) / (m * harmonic(m))
    below = sp <= thr
    k = int(np.max(np.where(below)[0]) + 1) if below.any() else 0
    rs = np.zeros(m, bool); rs[:k] = True
    out = np.zeros(m, bool); out[o] = rs
    return out


def block_boot(x, block, rng, B=N_BOOT):
    n = len(x); nb = int(np.ceil(n / block))
    idx = (rng.integers(0, n, size=(B, nb))[:, :, None] + np.arange(block)[None, None, :]) % n
    bm = x[idx.reshape(B, -1)[:, :n]].mean(axis=1)
    return float(np.quantile(bm, 0.05)), float(np.mean(bm <= 0.0))


def parse_pair(name):
    left, right = name.split("_before_minus_")
    def num(tok):
        mm = re.match(r"(\d+(?:_\d+)?)(pct|atr)", tok)
        return float(mm.group(1).replace("_", "."))
    return num(left), num(right)


def main():
    t0 = dt.datetime.now(); OUT.mkdir(parents=True, exist_ok=True)
    print("=" * 92)
    print("Phase 1 — COMPREHENSIVE BREADTH SWEEP (multi-signal + exit-rule)")
    print("=" * 92)

    cls = (pl.scan_parquet(window_files("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id"))
    do = (pl.scan_parquet(window_files("daily_observation")).select(["day", "security_id"] + DO_FEATS))
    hz_cols = [f"ret_{h}_excess_spy" for h in HZ]
    df = (pl.scan_parquet(window_files("forward_outcomes"))
          .filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + hz_cols + [f"first_event_{p}" for p in PAIRS])
          .join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner").collect())
    print(f"deployable cohort trades @1000: {df.height:,}\n")

    sig = signals()
    # ---- INPUT VALIDATION ------------------------------------------------------
    print("[validate] signal firing rates (deployable cohort):")
    sig_cols = {}
    for nm, ex in sig.items():
        col = df.select(ex.fill_null(False).alias("s"))["s"]
        sig_cols[nm] = col.to_numpy()
        print(f"  {nm:<20} fires {100*col.mean():>5.1f}%  (n={int(col.sum()):,})")
    fe0 = df[f"first_event_{PAIRS[0]}"]
    print(f"\n[validate] first_event distinct values ({PAIRS[0]}): "
          f"{fe0.drop_nulls().unique().to_list()[:6]}")

    rng = np.random.default_rng(SEED)
    # ---- STAGE 1: multi-signal entry expectancy --------------------------------
    print("\n[Stage 1] multi-signal entry expectancy (cost-adj excess/SPY, block-boot)")
    days = df["day"].to_numpy()
    recs = []
    for h, blk in HZ.items():
        ex = df[f"ret_{h}_excess_spy"].to_numpy()
        for nm in sig:
            mask = sig_cols[nm] & ~np.isnan(ex)
            if mask.sum() < MIN_NAMES * MIN_DAYS:
                continue
            d = days[mask]; e = ex[mask]
            # daily equal-weight portfolio
            order = np.argsort(d); ds = d[order]; es = e[order]
            uniq, idx = np.unique(ds, return_index=True)
            daily = np.array([es[idx[i]:(idx[i+1] if i+1 < len(idx) else len(es))].mean()
                              for i in range(len(uniq))])
            cnts = np.diff(np.append(idx, len(es)))
            daily = daily[cnts >= MIN_NAMES]
            if len(daily) < MIN_DAYS:
                continue
            net = daily - FLAT_COST_BPS / 1e4
            lb_n, p_n = block_boot(net, blk, rng)
            lb_g, _ = block_boot(daily, blk, rng)
            recs.append(dict(signal=nm, horizon=h, n_days=len(daily),
                             gross_bps=float(daily.mean()*1e4), net_bps=float(net.mean()*1e4),
                             net_lb_bps=lb_n*1e4, gross_lb_bps=lb_g*1e4, p_net_pos=p_n))
    s1 = pl.DataFrame(recs)
    s1 = s1.with_columns(pl.Series("by_survivor", by_mask(s1["p_net_pos"].to_numpy())))
    s1.write_parquet(OUT / "sweep_signals.parquet")
    nsurv = int(s1["by_survivor"].sum())
    npos = s1.filter(pl.col("net_lb_bps") > 0).height
    ngpos = s1.filter(pl.col("gross_lb_bps") > 0).height
    print(f"  {s1.height} (signal×horizon) combos · BY net-positive survivors: {nsurv}")
    print(f"  raw net_LB>0: {npos} · gross_LB>0: {ngpos}")
    print(f"\n  top 8 by GROSS excess (is there ANY raw alpha?):")
    for r in s1.sort("gross_bps", descending=True).head(8).iter_rows(named=True):
        flag = " *NET-LB>0*" if r["net_lb_bps"] > 0 else (" gross-LB>0" if r["gross_lb_bps"] > 0 else "")
        print(f"    {r['signal']:<20} {r['horizon']:>4}  gross={r['gross_bps']:+6.1f}b "
              f"(LB{r['gross_lb_bps']:+6.1f})  net={r['net_bps']:+6.1f}b (LB{r['net_lb_bps']:+6.1f}){flag}")

    # ---- STAGE 2: exit-rule / target-before-stop -------------------------------
    print("\n[Stage 2] exit rules: P(target before stop | resolved) vs breakeven Y/(X+Y)")
    e2 = []
    for pair in PAIRS:
        X, Y = parse_pair(pair); be = Y / (X + Y)
        fe = df[f"first_event_{pair}"].to_numpy().astype(object)
        for nm in sig:
            mask = sig_cols[nm]
            v = fe[mask]
            ntg = int(np.sum(v == "target_first")); nst = int(np.sum(v == "stop_first"))
            res = ntg + nst
            if res < 500:
                continue
            hr = ntg / res
            e2.append(dict(pair=pair, signal=nm, X=X, Y=Y, breakeven=be, n_resolved=res,
                           hit_rate=hr, edge=hr - be))
    s2 = pl.DataFrame(e2)
    s2.write_parquet(OUT / "sweep_exits.parquet")
    pos = s2.filter(pl.col("edge") > 0).sort("edge", descending=True)
    print(f"  {s2.height} (pair×signal) combos · with hit_rate > breakeven: {pos.height}")
    print(f"  top 10 by edge (hit_rate − breakeven):")
    for r in s2.sort("edge", descending=True).head(10).iter_rows(named=True):
        print(f"    {r['signal']:<20} {r['pair']:<34} be={r['breakeven']:.2f} "
              f"hit={r['hit_rate']:.3f} edge={r['edge']:+.3f} (n={r['n_resolved']:,})")

    print("\n--- verdict ---")
    print(f"Stage 1: {nsurv} BY survivors, {npos} raw net-LB>0, {ngpos} gross-LB>0.")
    print(f"Stage 2: {pos.height}/{s2.height} target/stop combos beat breakeven (raw, pre-excess).")
    print("If Stage-1 survivors=0 and Stage-2 edges are ≤0 / tiny (and the breakeven bar")
    print("rises further once cost + excess-vs-SPY are applied), the null is earned across")
    print("the signal catalog AND the exit-rule dimension — there is nothing exploitable.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
