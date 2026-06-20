#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — §7.8 step 3 / §6.1 COST MODEL derivation (frozen, cost-side only).

Builds data/phase1_analysis/cost_model.parquet: per (entry_offset ×
classification cell) cost in bps, frozen BEFORE discovery and built from
cost-side inputs ONLY (never from the edge distribution — §6.1).

Cell = (market_cap_bucket × liquidity_bucket × price_bucket), CS universe,
exploration window 2016-06-08 → 2020-12-31, all 17 entry offsets.

COMPONENTS (§6.1, retail frozen 2026-06-20):
  1. Spread proxy = (entry_1m_high - entry_1m_low)/entry_price = entry_1m_range/
     entry_price, in bps. Reported median (typical) + p75 (conservative).
     TOP-200-ADV CAP (the tracer-validated fix): for names with
     addv_rank_today <= 200, cap at min(raw_bps, 2 × stock's midday-median 1m
     range bps), because the raw high-low reflects the price walk, not the
     round-trip cost, and overstates spread for the most liquid names.
     Midday-median 1m range ≈ median of entry_1m_range/entry_price at the
     quiet midday offsets {1130,1200,1300} per security over the window.
     TICK FLOOR (added after the v1 run showed the high-low proxy reads 0.0
     bps for illiquid/thin names — they barely trade, so the entry minute has
     high==low → fake-free, exactly backwards for the widest-spread names).
     effective spread = max(capped high-low proxy, 1-cent round-trip floor =
     0.01/entry_price in bps). Frozen on a hard fact (min tick), directionally
     correct for low-price/illiquid cells.
  2. Market impact = c·sqrt(trade_size/addv), c=20 (§6.1 midpoint). At a FIXED
     %-of-ADV size this is cell-invariant; reported as flat adds at 0.1/1/5% ADV.
     (Dollar-fixed sizing would make it cell-varying — deferred to capacity work.)
  3. Commission = 0 (retail).
  + frac_zero_range per cell: share of entry bars with zero range (no trading) —
    an exitability flag feeding the §3.4 non-exitable cohort, independent of cost.

  total_cost_bps(size) = spread_proxy_capped_median + impact_bps(size) + 0

Read-only on data/outputs/; writes data/phase1_analysis/.
"""
from __future__ import annotations
import glob, math, datetime as dt
from pathlib import Path
import polars as pl

EXP_START = dt.date(2016, 6, 8)
EXP_END = dt.date(2020, 12, 31)
MIDDAY_OFFSETS = ["1130", "1200", "1300"]      # quiet-period sample for the cap
TOP_ADV_RANK = 200                              # §6.1 cap applies to top-200-ADV names
CAP_MULT = 2.0                                  # §6.1: 2× midday median range
C_IMPACT = 20.0                                 # §6.1 sqrt-impact coefficient (midpoint)
SIZE_FRACS = {"0.1pct": 0.001, "1pct": 0.01, "5pct": 0.05}
CELL = ["market_cap_bucket", "liquidity_bucket", "price_bucket"]
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


def cls_lazy(extra: list[str]) -> pl.LazyFrame:
    return (pl.scan_parquet(window_files("security_classification_daily"))
            .filter(pl.col("ticker_type") == "CS")
            .select(["day", "security_id"] + extra))


def impact_bps(frac: float) -> float:
    return C_IMPACT * math.sqrt(frac)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    t0 = dt.datetime.now()
    print("=" * 88)
    print("Phase 1 — COST MODEL derivation  (§7.8 step 3 / §6.1, retail, capped)")
    print("=" * 88)
    fo_files = window_files("forward_outcomes")
    print(f"window {EXP_START}→{EXP_END} · {len(fo_files)} forward_outcomes files · CS universe")
    print(f"cell = cap × liq × price · cap: top-{TOP_ADV_RANK} ADV → min(raw, {CAP_MULT}× midday range)")
    for s, f in SIZE_FRACS.items():
        print(f"  impact @ {s:>6} ADV = {impact_bps(f):.2f} bps (flat, c={C_IMPACT:g})")
    print()

    # ---- Pass 1: per-security midday-median 1m range fraction (the cap input) ----
    cap_tbl = (pl.scan_parquet(fo_files)
               .filter(pl.col("entry_offset").is_in(MIDDAY_OFFSETS)
                       & (pl.col("entry_price") > 0)
                       & pl.col("entry_1m_range").is_not_null())
               .join(cls_lazy([]), on=["day", "security_id"], how="inner")
               .with_columns((pl.col("entry_1m_range") / pl.col("entry_price")).alias("rf"))
               .group_by("security_id")
               .agg(pl.col("rf").median().alias("midday_range_frac"))
               .collect())
    print(f"pass 1: midday-range cap computed for {cap_tbl.height:,} securities")

    # ---- Pass 2: capped spread proxy per (offset × cell) ------------------------
    raw_bps = (pl.col("entry_1m_range") / pl.col("entry_price") * 1e4)
    cap_bps = (CAP_MULT * pl.col("midday_range_frac") * 1e4)
    tick_floor_bps = (0.01 / pl.col("entry_price") * 1e4)   # 1-cent round-trip floor
    capped = (pl.when((pl.col("addv_rank_today") <= TOP_ADV_RANK)
                      & pl.col("midday_range_frac").is_not_null())
              .then(pl.min_horizontal(raw_bps, cap_bps))
              .otherwise(raw_bps))

    rows = (pl.scan_parquet(fo_files)
            .filter((pl.col("entry_price") > 0) & pl.col("entry_1m_range").is_not_null())
            .join(cls_lazy(["market_cap_bucket", "liquidity_bucket", "price_bucket",
                            "addv_rank_today"]),
                  on=["day", "security_id"], how="inner")
            .drop_nulls(CELL)
            .join(cap_tbl.lazy(), on="security_id", how="left")
            .with_columns(raw_bps.alias("raw_bps"),
                          capped.alias("proxy_capped_bps"),
                          tick_floor_bps.alias("tick_floor_bps"),
                          (pl.col("addv_rank_today") <= TOP_ADV_RANK).alias("is_top_adv"),
                          (pl.col("entry_1m_range") == 0).alias("zero_range"))
            .with_columns(
                pl.max_horizontal("proxy_capped_bps", "tick_floor_bps").alias("spread_bps"))
            .with_columns((pl.col("proxy_capped_bps") < pl.col("raw_bps") - 1e-9).alias("cap_bound"),
                          (pl.col("tick_floor_bps") > pl.col("proxy_capped_bps") + 1e-9).alias("floor_bound"))
            .group_by(["entry_offset"] + CELL)
            .agg(
                pl.len().alias("n_trades"),
                pl.col("security_id").n_unique().alias("n_secs"),
                pl.col("spread_bps").median().alias("spread_bps_median"),
                pl.col("spread_bps").quantile(0.75).alias("spread_bps_p75"),
                pl.col("raw_bps").median().alias("spread_bps_median_uncapped"),
                pl.col("cap_bound").mean().alias("frac_cap_bound"),
                pl.col("floor_bound").mean().alias("frac_floor_bound"),
                pl.col("zero_range").mean().alias("frac_zero_range"),
            )
            .collect())

    # add impact + commission + total columns
    rows = rows.with_columns(
        pl.lit(0.0).alias("commission_bps"),
        *[pl.lit(impact_bps(f)).alias(f"impact_bps_{s}") for s, f in SIZE_FRACS.items()],
    )
    rows = rows.with_columns(
        *[(pl.col("spread_bps_median") + impact_bps(f)).alias(f"total_cost_bps_{s}_median")
          for s, f in SIZE_FRACS.items()]
    ).with_columns(pl.col("entry_offset").cast(pl.Utf8)).sort(
        ["entry_offset"] + CELL)

    path = OUT / "cost_model.parquet"
    rows.write_parquet(path)
    print(f"pass 2: {rows.height:,} (offset × cell) rows written → {path}")

    # ---- summary: representative cells at offset 1000 ---------------------------
    print("\n--- effective spread @ offset 1000 by cell (raw → final bps) ---")
    print("    raw = uncapped high-low · final = max(capped proxy, 1c tick floor)")
    show = (rows.filter((pl.col("entry_offset") == "1000")
                        & (pl.col("n_trades") >= 200))
            .sort(["market_cap_bucket", "liquidity_bucket", "price_bucket"]))
    print(f"  {'cap':<6}{'liq':<14}{'price':<11}{'n_sec':>6}"
          f"{'raw':>8}{'final':>8}{'%cap':>6}{'%flr':>6}{'%0rng':>7}{'tot@1%':>8}")
    for r in show.iter_rows(named=True):
        print(f"  {str(r['market_cap_bucket']):<6}{str(r['liquidity_bucket']):<14}"
              f"{str(r['price_bucket']):<11}{r['n_secs']:>6}"
              f"{r['spread_bps_median_uncapped']:>8.1f}{r['spread_bps_median']:>8.1f}"
              f"{100*r['frac_cap_bound']:>5.0f}%{100*r['frac_floor_bound']:>5.0f}%"
              f"{100*r['frac_zero_range']:>6.0f}%{r['total_cost_bps_1pct_median']:>8.1f}")

    liq = rows.filter(pl.col("liquidity_bucket").is_in(["liquid", "highly_liquid"]))
    illq = rows.filter(pl.col("liquidity_bucket").is_in(["illiquid", "thin"]))
    if liq.height:
        print(f"\nliquid/highly-liquid: raw median={liq['spread_bps_median_uncapped'].median():.1f}b "
              f"→ final={liq['spread_bps_median'].median():.1f}b "
              f"(cap bound {100*liq['frac_cap_bound'].mean():.0f}%) "
              f"— upper bound, pending §6.1 calibration.")
    if illq.height:
        print(f"illiquid/thin     : raw median={illq['spread_bps_median_uncapped'].median():.1f}b "
              f"(fake-cheap; {100*illq['frac_zero_range'].mean():.0f}% zero-range bars) "
              f"→ final={illq['spread_bps_median'].median():.1f}b "
              f"(floor bound {100*illq['frac_floor_bound'].mean():.0f}%) — the 0-bps bug, fixed.")

    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
