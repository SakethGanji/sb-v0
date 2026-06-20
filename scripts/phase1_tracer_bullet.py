#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — §5.1 step-0 TRACER BULLET (the first real number from the data).

Runs the ENTIRE discipline on ONE pre-frozen question with plain conditional
sorts — NO MI/BN/DP/HB. Purpose (per phase1-research-strategy.md §5.1 step 0):
  (1) prove the I/O discipline on the ~300 GB forward_outcomes table
      (lazy scan + column projection + window/offset pushdown);
  (2) sanity-check the dividend total-return columns (§3.3);
  (3) exercise the whole plumbing end-to-end and produce the first number.

FROZEN QUESTION (single cell, §5.1 step 0):
  entry_offset = 1000 (10:00 ET) · horizon = 1d · large-cap CS · signal > 0
  signal := intraday_ret_0930_to_1000 > 0   (the §1.6 default, known at 10:00)
  exclusions := leveraged/inverse ETF, China ADR, recent IPO (pre-reg template)

COST MODEL (frozen 2026-06-20, retail — see strategy §6 RESOLVED):
  round-trip cost ≈ effective spread = (entry_1m_high - entry_1m_low)/entry_price
  i.e. entry_1m_range / entry_price, in bps. Commission ~0. No top-200-ADV cap
  yet (deferred to the real §6.1 cost-model step — flagged below); large-cap
  names keep the raw proxy modest. Also reported at flat 0/5/10/20 bps for context.

HEADLINE METRIC (frozen headline rule, §1.5 / §7.7.1):
  daily equal-weight portfolio of cost-adjusted EXCESS-over-SPY return →
  day-clustered one-sided bootstrap CI lower bound. >0 ? (MDE pass already says
  a true Sharpe-0.5 cell can't clear this — so a null here is the EXPECTED,
  pipeline-validating result, not a disappointment.)

Window: exploration only, 2016-06-08 → 2020-12-31. Holdout untouched.
Read-only on data/outputs/; writes data/phase1_analysis/.
"""
from __future__ import annotations
import glob, math, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START = dt.date(2016, 6, 8)
EXP_END = dt.date(2020, 12, 31)
HEADLINE_OFFSET = "1000"
SURFACE_OFFSETS = ["1000", "1005", "1010", "1015", "1020", "1030",
                   "1045", "1100", "1130", "1200", "1300", "1530"]  # >= 10:00 (signal known)
SURFACE_HORIZONS = ["1d", "2d", "3d", "5d"]
LARGE_BUCKETS = ["large", "mega"]
EXCLUDE_FLAGS = ["is_leveraged_etf", "is_inverse_etf", "is_china_adr", "is_recent_ipo"]
TRADING_DAYS_YR = 252
N_BOOT = 10000
SEED = 20260620
OUT = Path("data/phase1_analysis")


def window_files(tbl: str) -> list[str]:
    out = []
    for f in sorted(glob.glob(f"data/outputs/{tbl}/*.parquet")):
        try:
            d = dt.date.fromisoformat(Path(f).stem)
        except ValueError:
            continue
        if EXP_START <= d <= EXP_END:
            out.append(f)
    return out


def cls_lazy() -> pl.LazyFrame:
    lf = (pl.scan_parquet(window_files("security_classification_daily"))
          .filter((pl.col("ticker_type") == "CS")
                  & pl.col("market_cap_bucket").is_in(LARGE_BUCKETS)))
    for c in EXCLUDE_FLAGS:
        lf = lf.filter(~pl.col(c).fill_null(False))
    return lf.select("day", "security_id")


def sig_lazy() -> pl.LazyFrame:
    return (pl.scan_parquet(window_files("daily_observation"))
            .filter(pl.col("intraday_ret_0930_to_1000") > 0.0)
            .select("day", "security_id"))


def boot_lower_bound(daily: np.ndarray, q: float = 0.05) -> tuple[float, float]:
    """One-sided day-clustered bootstrap. Returns (mean, lower q-quantile of
    resampled means). Day-resampling is correct for the 1d horizon (negligible
    overlap); multi-day horizons would need a block bootstrap (§7.7.1)."""
    rng = np.random.default_rng(SEED)
    n = len(daily)
    idx = rng.integers(0, n, size=(N_BOOT, n))
    means = daily[idx].mean(axis=1)
    return float(daily.mean()), float(np.quantile(means, q))


def ann_sharpe(daily: np.ndarray) -> float:
    sd = daily.std(ddof=1)
    return float(daily.mean() / sd * math.sqrt(TRADING_DAYS_YR)) if sd > 0 else float("nan")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    t0 = dt.datetime.now()
    print("=" * 84)
    print("Phase 1 — TRACER BULLET  (§5.1 step 0)")
    print("=" * 84)
    print(f"Window     : {EXP_START} → {EXP_END}  (exploration only; holdout untouched)")
    print(f"Cell       : offset={HEADLINE_OFFSET} · horizon=1d · large-cap CS · signal>0")
    print(f"Cost       : retail, round-trip = entry_1m_range/entry_price (bps); commission 0")
    print()

    fo_files = window_files("forward_outcomes")
    print(f"forward_outcomes files in window: {len(fo_files)}  (projecting ~8 of 657 cols)")

    # ---- HEADLINE collect: offset 1000, 1d horizon -----------------------------
    fo = (pl.scan_parquet(fo_files)
          .filter(pl.col("entry_offset") == HEADLINE_OFFSET)
          .select("day", "security_id", "entry_price", "entry_1m_range",
                  "ret_1d", "ret_1d_total", "ret_1d_excess_spy",
                  "days_with_missing_forward_bars"))
    trades = (fo.join(cls_lazy(), on=["day", "security_id"], how="inner")
                .join(sig_lazy(), on=["day", "security_id"], how="inner")
                .filter((pl.col("entry_price") > 0)
                        & pl.col("entry_1m_range").is_not_null()
                        & pl.col("ret_1d").is_not_null()
                        & pl.col("ret_1d_excess_spy").is_not_null())
                .with_columns(
                    (pl.col("entry_1m_range") / pl.col("entry_price") * 1e4).alias("cost_bps"))
                .collect())

    n_trades = trades.height
    n_days = trades["day"].n_unique()
    years = n_days / TRADING_DAYS_YR
    cb = trades["cost_bps"]
    print(f"\nqualifying trades: {n_trades:,} over {n_days} days  "
          f"(~{n_trades/max(n_days,1):.0f} names/day, {years:.2f} yr)")
    print(f"spread-proxy cost_bps (round-trip): median={cb.median():.1f}  "
          f"p75={cb.quantile(0.75):.1f}  p95={cb.quantile(0.95):.1f}  "
          f"max={cb.max():.0f}  [UNCAPPED — top-200-ADV cap deferred to §6.1]")

    # dividend sanity (§3.3): how much does total-return differ from price-return?
    dd = trades.select(
        (pl.col("ret_1d_total") - pl.col("ret_1d")).alias("div_delta")).drop_nulls()
    nz = dd.filter(pl.col("div_delta").abs() > 1e-9).height
    print(f"dividend check: ret_1d_total vs ret_1d — {nz:,}/{dd.height:,} trades differ "
          f"({100*nz/max(dd.height,1):.1f}%); mean Δ on those = "
          f"{dd.filter(pl.col('div_delta').abs()>1e-9)['div_delta'].mean() if nz else 0:+.6f}")

    # daily equal-weight portfolio of cost-adjusted EXCESS return
    daily = (trades.with_columns(
                (pl.col("ret_1d_excess_spy") - pl.col("cost_bps") / 1e4).alias("net_excess"),
                (pl.col("ret_1d") - pl.col("cost_bps") / 1e4).alias("net_raw"))
             .group_by("day").agg(
                 pl.col("ret_1d_excess_spy").mean().alias("gross_excess"),
                 pl.col("net_excess").mean().alias("net_excess"),
                 pl.col("net_raw").mean().alias("net_raw"),
                 pl.len().alias("n"))
             .sort("day"))
    OUT_path = OUT / "tracer_daily_offset1000_1d.parquet"
    daily.write_parquet(OUT_path)

    g = daily["gross_excess"].to_numpy()
    ne = daily["net_excess"].to_numpy()
    nr = daily["net_raw"].to_numpy()
    print("\n--- HEADLINE (daily-portfolio, day-clustered) ---")
    for name, arr in [("raw (net of cost)", nr),
                      ("excess/SPY (gross)", g),
                      ("excess/SPY (net of cost)", ne)]:
        mean, lb = boot_lower_bound(arr)
        sh = ann_sharpe(arr)
        t = sh * math.sqrt(years)
        print(f"  {name:26s}  mean_daily={mean:+.5f}  ann={mean*252:+.3%}  "
              f"Sharpe={sh:+.2f}  t≈{t:+.2f}  boot5%LB={lb:+.5f} "
              f"{'>0 ✓' if lb > 0 else '≤0 (null)'}")

    # cost sensitivity on the excess series (flat bps, §1.6 Category-B spirit)
    print("\n--- cost sensitivity (flat round-trip bps, on excess/SPY) ---")
    gross_daily_ex = daily["gross_excess"].to_numpy()
    # gross_excess above already has NO cost; subtract flat cost per trade == per name,
    # but daily portfolio is equal-weight mean so flat per-trade bps subtracts directly.
    for bps in (0, 5, 10, 20):
        adj = gross_daily_ex - bps / 1e4
        _, lb = boot_lower_bound(adj)
        print(f"  {bps:2d} bps : mean_daily={adj.mean():+.5f}  Sharpe={ann_sharpe(adj):+.2f}  "
              f"boot5%LB={lb:+.5f} {'>0 ✓' if lb > 0 else '≤0'}")

    # ---- SURFACE diagnostic (§2.4): offsets>=10:00 × horizons, point estimates only ---
    print("\n--- SURFACE diagnostic: mean cost-adj excess/SPY by (offset × horizon) ---")
    print("    (point estimates only; smooth gradient = real, isolated spike = suspect)")
    hcols = [f"ret_{h}_excess_spy" for h in SURFACE_HORIZONS]
    have = set(pl.scan_parquet(fo_files[0]).collect_schema().names())
    hcols = [c for c in hcols if c in have]
    surf = (pl.scan_parquet(fo_files)
            .filter(pl.col("entry_offset").is_in(SURFACE_OFFSETS))
            .select(["day", "security_id", "entry_price", "entry_1m_range",
                     "entry_offset"] + hcols)
            .join(cls_lazy(), on=["day", "security_id"], how="inner")
            .join(sig_lazy(), on=["day", "security_id"], how="inner")
            .filter((pl.col("entry_price") > 0) & pl.col("entry_1m_range").is_not_null())
            .with_columns([(pl.col(c) - pl.col("entry_1m_range") / pl.col("entry_price")).alias(c)
                           for c in hcols])
            .group_by("entry_offset")
            .agg([pl.col(c).mean().alias(c) for c in hcols] + [pl.len().alias("n")])
            .collect())
    surf = surf.with_columns(
        pl.col("entry_offset").cast(pl.Utf8)).sort("entry_offset")
    hdr = "  offset  " + "".join(f"{h:>11}" for h in SURFACE_HORIZONS) + f"{'n/day':>10}"
    print(hdr)
    nday = max(n_days, 1)
    for row in surf.iter_rows(named=True):
        line = f"  {row['entry_offset']:<8}"
        for c in hcols:
            v = row[c]
            line += f"{v*1e4:>10.1f}b" if v is not None else f"{'—':>11}"
        line += f"{row['n']/nday:>10.0f}"
        print(line)
    surf.write_parquet(OUT / "tracer_surface_offset_x_horizon.parquet")

    print("\n" + "=" * 84)
    print(f"wrote {OUT_path.name} + tracer_surface_offset_x_horizon.parquet")
    print(f"WALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")
    print("Interpretation: a ≤0 lower bound here CONFIRMS the §1.5.1 power reality")
    print("(this window can't see Sharpe-0.5 mean edges). The point of the tracer is")
    print("that the PLUMBING ran clean end-to-end, not that this cell is tradeable.")


if __name__ == "__main__":
    main()
