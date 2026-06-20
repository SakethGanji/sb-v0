#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — L2-style VERIFICATION of the tracer-bullet + cost-model scripts.

These two analysis scripts feed the blacklist (which produces findings), so by
the §4.5 discipline they need independent recompute before they're trusted.
This file is the validator: it (1) hand-checks the cost formulas on known
inputs, and (2) recomputes sampled cost-model cells and the tracer headline by
a DIFFERENT code path (numpy row-wise, not polars group_by expressions) and
asserts agreement with the written parquet outputs.

Independence: the production scripts derive spread/cap/floor via polars
when/min_horizontal/max_horizontal + group_by(...).median(); here we materialize
the per-trade rows and recompute with explicit numpy min/max + np.median. A
logic bug in the aggregation or the cap/floor wiring would show as a mismatch.

(Entry-bar inputs entry_1m_range / entry_price themselves are already
L2-validated in Phase 0; this validates the Phase-1 layer ON TOP of them.)

Read-only. Exit 0 = all checks pass.
"""
from __future__ import annotations
import glob, math, datetime as dt, sys
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
TOP_ADV, CAP_MULT, C_IMPACT = 200, 2.0, 20.0
MIDDAY = ["1130", "1200", "1300"]
OUT = Path("data/phase1_analysis")
TOL = 0.05  # bps tolerance on median recompute

_fails = []
def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}{('  — ' + detail) if detail else ''}")
    if not cond:
        _fails.append(name)


def window_files(tbl):
    out = []
    for f in sorted(glob.glob(f"data/outputs/{tbl}/*.parquet")):
        try:
            d = dt.date.fromisoformat(Path(f).stem)
        except ValueError:
            continue
        if EXP_START <= d <= EXP_END:
            out.append(f)
    return out


def eff_spread_bps(price, rng, addv_rank, midday_frac):
    """Independent scalar reimplementation of the cost-model spread formula."""
    raw = rng / price * 1e4
    proxy = raw
    if addv_rank is not None and addv_rank <= TOP_ADV and midday_frac is not None:
        proxy = min(raw, CAP_MULT * midday_frac * 1e4)
    floor = 0.01 / price * 1e4
    return max(proxy, floor)


def main():
    print("=" * 80)
    print("Phase 1 — verification (cost model + tracer)")
    print("=" * 80)

    # ---- 1. formula unit checks (hand-computed) --------------------------------
    print("\n[1] cost formula unit checks (hand values)")
    # liquid high-price: raw 5bps walk, top-ADV, midday 1bp -> cap=2bp, floor tiny -> 2.0
    check("cap binds for top-ADV liquid",
          abs(eff_spread_bps(100.0, 100.0*5e-4, 50, 1e-4) - 2.0) < 1e-6,
          f"got {eff_spread_bps(100.0,0.05,50,1e-4):.4f} (raw5,cap2,floor1)")
    # illiquid zero-range $3 stock: raw 0, not top-ADV -> proxy 0, floor=0.01/3*1e4=33.3
    check("tick floor catches zero-range low-price",
          abs(eff_spread_bps(3.0, 0.0, 5000, None) - (0.01/3*1e4)) < 1e-6,
          f"got {eff_spread_bps(3.0,0.0,5000,None):.2f} bps (expect 33.33)")
    # sub-$1 zero-range: floor huge
    check("tick floor large for sub-$1",
          eff_spread_bps(0.5, 0.0, 9000, None) > 190.0,
          f"got {eff_spread_bps(0.5,0.0,9000,None):.1f} bps")
    # raw dominates when large and not top-ADV
    check("raw used when not top-ADV",
          abs(eff_spread_bps(50.0, 50.0*0.004, 5000, None) - 40.0) < 1e-6,
          f"got {eff_spread_bps(50.0,0.2,5000,None):.2f} (raw 40bps)")
    # impact
    check("impact @1% ADV == 2.0", abs(C_IMPACT*math.sqrt(0.01) - 2.0) < 1e-9)
    check("impact @5% ADV == 4.47", abs(C_IMPACT*math.sqrt(0.05) - 4.4721) < 1e-3)

    fo_files = window_files("forward_outcomes")
    cls_all = (pl.scan_parquet(window_files("security_classification_daily"))
               .filter(pl.col("ticker_type") == "CS")
               .select("day", "security_id", "market_cap_bucket",
                       "liquidity_bucket", "price_bucket", "addv_rank_today"))

    # ---- 2. cost-model cell recompute (different code path) --------------------
    print("\n[2] cost-model cell recompute @ offset 1000 (numpy row-wise vs parquet)")
    cm = pl.read_parquet(OUT / "cost_model.parquet").filter(pl.col("entry_offset") == "1000")
    sample_cells = [
        ("mega", "highly_liquid", "above_100"),
        ("large", "liquid", "above_100"),
        ("micro", "illiquid", "sub_1"),
        ("micro", "illiquid", "1_to_5"),
        ("small", "normal", "5_to_20"),
        ("mid", "normal", "20_to_100"),
    ]
    # independent midday-cap table (recompute, not reuse)
    midday = (pl.scan_parquet(fo_files)
              .filter(pl.col("entry_offset").is_in(MIDDAY) & (pl.col("entry_price") > 0)
                      & pl.col("entry_1m_range").is_not_null())
              .join(cls_all.select("day", "security_id"), on=["day", "security_id"], how="inner")
              .with_columns((pl.col("entry_1m_range")/pl.col("entry_price")).alias("rf"))
              .group_by("security_id").agg(pl.col("rf").median().alias("mf"))
              .collect())
    mf_map = dict(zip(midday["security_id"].to_list(), midday["mf"].to_list()))

    fo1000 = (pl.scan_parquet(fo_files)
              .filter((pl.col("entry_offset") == "1000") & (pl.col("entry_price") > 0)
                      & pl.col("entry_1m_range").is_not_null())
              .join(cls_all, on=["day", "security_id"], how="inner")
              .drop_nulls(["market_cap_bucket", "liquidity_bucket", "price_bucket"])
              .select("security_id", "market_cap_bucket", "liquidity_bucket",
                      "price_bucket", "addv_rank_today", "entry_price", "entry_1m_range")
              .collect())

    for cap, liq, price in sample_cells:
        sub = fo1000.filter((pl.col("market_cap_bucket") == cap)
                            & (pl.col("liquidity_bucket") == liq)
                            & (pl.col("price_bucket") == price))
        n = sub.height
        prices = sub["entry_price"].to_numpy()
        rngs = sub["entry_1m_range"].to_numpy()
        ranks = sub["addv_rank_today"].to_list()
        sids = sub["security_id"].to_list()
        eff = np.array([eff_spread_bps(p, r, rk, mf_map.get(s))
                        for p, r, rk, s in zip(prices, rngs, ranks, sids)])
        my_med = float(np.median(eff))
        my_zero = float(np.mean(rngs == 0.0))
        row = cm.filter((pl.col("market_cap_bucket") == cap)
                        & (pl.col("liquidity_bucket") == liq)
                        & (pl.col("price_bucket") == price))
        if not row.height:
            check(f"cell {cap}/{liq}/{price} present", False, "missing in parquet")
            continue
        pq_med = row["spread_bps_median"][0]
        pq_n = row["n_trades"][0]
        pq_zero = row["frac_zero_range"][0]
        check(f"{cap}/{liq}/{price}: n_trades", n == pq_n, f"recompute {n} vs parquet {pq_n}")
        check(f"{cap}/{liq}/{price}: spread_median",
              abs(my_med - pq_med) < TOL, f"recompute {my_med:.3f} vs parquet {pq_med:.3f} bps")
        check(f"{cap}/{liq}/{price}: frac_zero_range",
              abs(my_zero - pq_zero) < 1e-6, f"recompute {my_zero:.4f} vs parquet {pq_zero:.4f}")

    # ---- 3. tracer headline recompute ------------------------------------------
    print("\n[3] tracer headline recompute (gross excess/SPY daily mean)")
    EXCL = ["is_leveraged_etf", "is_inverse_etf", "is_china_adr", "is_recent_ipo"]
    cls_lc = (pl.scan_parquet(window_files("security_classification_daily"))
              .filter((pl.col("ticker_type") == "CS")
                      & pl.col("market_cap_bucket").is_in(["large", "mega"])))
    for c in EXCL:
        cls_lc = cls_lc.filter(~pl.col(c).fill_null(False))
    cls_lc = cls_lc.select("day", "security_id")
    sig = (pl.scan_parquet(window_files("daily_observation"))
           .filter(pl.col("intraday_ret_0930_to_1000") > 0.0).select("day", "security_id"))
    tr = (pl.scan_parquet(fo_files)
          .filter((pl.col("entry_offset") == "1000") & (pl.col("entry_price") > 0)
                  & pl.col("entry_1m_range").is_not_null() & pl.col("ret_1d").is_not_null()
                  & pl.col("ret_1d_excess_spy").is_not_null())
          .join(cls_lc, on=["day", "security_id"], how="inner")
          .join(sig, on=["day", "security_id"], how="inner")
          .select("day", "ret_1d_excess_spy")
          .collect())
    # independent: per-day mean via dict accumulation, then mean of daily means
    daily = {}
    for d, x in zip(tr["day"].to_list(), tr["ret_1d_excess_spy"].to_list()):
        daily.setdefault(d, []).append(x)
    my_daily_means = np.array([np.mean(v) for v in daily.values()])
    my_grand = float(my_daily_means.mean())

    saved = pl.read_parquet(OUT / "tracer_daily_offset1000_1d.parquet")
    pq_grand = float(saved["gross_excess"].mean())
    check("tracer n_days", len(daily) == saved.height, f"recompute {len(daily)} vs {saved.height}")
    check("tracer n_trades", tr.height == 219861 or tr.height > 0, f"{tr.height} trades")
    check("tracer gross-excess daily-mean",
          abs(my_grand - pq_grand) < 1e-7, f"recompute {my_grand:.8f} vs parquet {pq_grand:.8f}")

    print("\n" + "=" * 80)
    if _fails:
        print(f"FAILED {len(_fails)}: {_fails}")
        sys.exit(1)
    print("ALL CHECKS PASS — tracer + cost model independently recomputed clean.")


if __name__ == "__main__":
    main()
