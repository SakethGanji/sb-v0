#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with scipy --with numpy python3
"""
Phase 1 — Step -1: Power / Minimum-Detectable-Effect (MDE) pass.

Implements `phase1-research-strategy.md` §7.8 step -1 and the §1.5.1 power
reality. Answers, BEFORE any discovery run and using NO engine output beyond
the already-written classification + regime tables:

  For a candidate BY discovery family of size m over the exploration window,
  what is the minimum annualized Sharpe a cell must have to be DETECTABLE
  after Benjamini-Yekutieli FDR control? And how does that detectable floor
  compare to the §2.8 required-edge hurdle (Sharpe 0.5)?

If the detectable floor sits far above the hurdle for most of the family,
the family must be coarsened BEFORE discovery (per §1.5.1) — otherwise the
phase is a guaranteed "characterized null" regardless of truth.

Method (matches the doc):
  * Day-clustered inference => a cell collapses to a daily portfolio return
    series, so t ≈ Sharpe_annual × √years   (§1.5.1).
  * One-sided test (headline rule is a one-sided lower-bound > 0, §7.8 step 9).
  * BY threshold under arbitrary positive dependence: the per-cell p-value
    cutoff ranges from the most-stringent  α / (m · H_m)   (rank 1)
    to the least-stringent  α / H_m   (rank m), where H_m = Σ_{i=1..m} 1/i.
    We report the detectable Sharpe at BOTH ends of that band.
  * MDE Sharpe for a cell with N active days:  z* / √(N/252).

Frozen constants (see §1.6): α_discovery = 0.10, hurdle Sharpe = 0.5,
exploration window 2016-06-08 → 2020-12-31, long-only CS universe.

Per-cell N here = distinct days the cell is ACTIVE (≥ MIN_NAMES names). This
is an UPPER BOUND on effective N: the long-only signal filter
(intraday_ret_0930_to_1000 > 0) only removes days/names, so true power is
≤ what this reports. We also print a 0.5× signal-haircut row to bracket it.

Outputs:
  * stdout: the family-granularity ladder + detectable-Sharpe band per level,
    and the fraction of cells reachable at the hurdle.
  * data/outputs/analysis/power_mde_<primary>.parquet: per-cell detectable map
    for the chosen primary granularity.

Run:  scripts/power_mde_pass.py
"""

from __future__ import annotations
import math
from pathlib import Path
import numpy as np
import polars as pl
from scipy.stats import norm

# ---- frozen constants -------------------------------------------------------
ALPHA = 0.10                      # discovery FDR level (§1.6)
HURDLE_SHARPE = 0.5               # §2.8 required-edge hurdle (default)
EXPLORATION_START = "2016-06-08"
EXPLORATION_END = "2020-12-31"    # §2.5 exploration split
TRADING_DAYS_YR = 252
MIN_NAMES = 5                     # a (day, cell) counts as active with >= this many names
MIN_DAYS = 20                     # a cell is "testable" only with >= this many active days
SIGNAL_HAIRCUT = 0.5             # long-only signal-fire fraction (bracketing assumption)

CLS_GLOB = "data/outputs/security_classification_daily/**/*.parquet"
REGIME_PARQUET = "data/outputs/regime_definitions.parquet"
# NOTE: read-only against data/outputs/ (owned by the Phase 0 / B6 writer).
# Analysis output lives OUTSIDE that tree so it never collides with B6.
OUT_DIR = Path("data/phase1_analysis")

# classification × regime granularity ladder (coarse → fine)
CLS_LADDER = {
    "cap":              ["market_cap_bucket"],
    "cap×liq":          ["market_cap_bucket", "liquidity_bucket"],
    "cap×liq×price":    ["market_cap_bucket", "liquidity_bucket", "price_bucket"],
    "cap×liq×price×vol":["market_cap_bucket", "liquidity_bucket", "price_bucket", "volatility_bucket"],
}
REG_LADDER = {
    "none":                 [],
    "vix":                  ["vix_level"],
    "vix×trend":            ["vix_level", "spy_trend"],
    "vix×trend×breadth":    ["vix_level", "spy_trend", "breadth"],
}
# the level we treat as the candidate PRIMARY coarse family (§7.7.1)
PRIMARY = ("cap×liq", "vix×trend")


def harmonic(m: int) -> float:
    """H_m = Σ 1/i, exact for small m, asymptotic for large."""
    if m <= 0:
        return 1.0
    if m <= 100_000:
        return float(np.sum(1.0 / np.arange(1, m + 1)))
    return math.log(m) + 0.5772156649 + 1.0 / (2 * m)


def by_z_band(m: int, alpha: float = ALPHA) -> tuple[float, float]:
    """One-sided z-thresholds at the stringent (rank-1) and loose (rank-m)
    ends of the BY rejection region. Returns (z_stringent, z_loose)."""
    Hm = harmonic(m)
    p_stringent = alpha / (m * Hm)     # rank k=1
    p_loose = alpha / Hm               # rank k=m  => k·α/(m·Hm)
    z_stringent = float(norm.ppf(1.0 - p_stringent))
    z_loose = float(norm.ppf(1.0 - p_loose))
    return z_stringent, z_loose


def mde_sharpe(z: float, n_days: float) -> float:
    """Minimum detectable annualized Sharpe for a cell with n_days obs."""
    if n_days <= 1:
        return float("inf")
    return z / math.sqrt(n_days / TRADING_DAYS_YR)


def load_panel() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (classification panel over the FULL exploration window,
    wide regime table over whatever days it currently covers).

    The two are kept separate so classification-only families use the full
    window; only regime-conditioned families are limited by regime coverage.
    """
    cls = (
        pl.scan_parquet(CLS_GLOB)
        .filter(
            (pl.col("day") >= pl.lit(EXPLORATION_START).str.to_date())
            & (pl.col("day") <= pl.lit(EXPLORATION_END).str.to_date())
            & (pl.col("ticker_type") == "CS")
        )
        .select(["day", "security_id", "market_cap_bucket", "liquidity_bucket",
                 "price_bucket", "volatility_bucket"])
        # drop unclassified rows — a None bucket is not a testable cell
        .drop_nulls(["market_cap_bucket", "liquidity_bucket", "price_bucket", "volatility_bucket"])
        .collect()
    )
    reg = (
        pl.read_parquet(REGIME_PARQUET)
        .filter(pl.col("taxonomy").is_in(["vix_level", "spy_trend", "breadth", "liquidity"]))
        .pivot(values="regime_name", index="day", on="taxonomy", aggregate_function="first")
    )
    return cls, reg


def family_stats(cls: pl.DataFrame, reg: pl.DataFrame,
                 cls_cols: list[str], reg_cols: list[str]):
    """Return (m, per-cell DataFrame[cell_key, n_days, med_names], n_days_used)."""
    keys = cls_cols + reg_cols
    panel = cls
    if reg_cols:
        panel = cls.join(reg, on="day", how="inner").drop_nulls(reg_cols)
    n_days_used = panel["day"].n_unique() if panel.height else 0
    per_day_cell = (
        panel.group_by(keys + ["day"])
        .agg(pl.len().alias("n_names"))
        .filter(pl.col("n_names") >= MIN_NAMES)
    )
    cells = (
        per_day_cell.group_by(keys)
        .agg(
            pl.col("day").n_unique().alias("n_days"),
            pl.col("n_names").median().alias("med_names"),
        )
        .filter(pl.col("n_days") >= MIN_DAYS)
        .sort("n_days", descending=True)
    )
    return cells.height, cells, n_days_used


def main() -> None:
    cls, reg = load_panel()
    n_days_total = cls["day"].n_unique()
    reg_days = reg["day"].n_unique()
    years = n_days_total / TRADING_DAYS_YR
    print("=" * 78)
    print("Phase 1 — Power / MDE pass  (§7.8 step -1, §1.5.1)")
    print("=" * 78)
    print(f"Exploration window : {EXPLORATION_START} → {EXPLORATION_END}")
    print(f"Trading days swept : {n_days_total}  (~{years:.2f} years)  [classification]")
    print(f"Regime coverage    : {reg_days} days  "
          f"{'(FULL)' if reg_days >= n_days_total*0.9 else '⚠ SMOKE ARTIFACT — regime-conditioned rows below are coverage-limited until B6'}")
    print(f"Universe           : ticker_type=CS, fully-classified rows only")
    print(f"Frozen             : α={ALPHA} (discovery), hurdle Sharpe={HURDLE_SHARPE}")
    print()

    # --- §1.5.1 reproduction: single-cell detectability (no multiplicity) ---
    t_hurdle = HURDLE_SHARPE * math.sqrt(years)
    p_hurdle = 1.0 - norm.cdf(t_hurdle)
    print(f"[§1.5.1] A true Sharpe-{HURDLE_SHARPE} cell gives one-sided "
          f"t ≈ {t_hurdle:.2f} (p ≈ {p_hurdle:.3f}) — "
          f"{'clears' if p_hurdle < 0.05 else 'CANNOT clear'} even an "
          f"UNCORRECTED α=0.05 over this window.")
    print()

    # --- family-granularity ladder -----------------------------------------
    print(f"{'family':<26}{'m':>6}{'med N':>8}"
          f"{'MDE@strict':>12}{'MDE@loose':>11}{'%≤0.5':>8}{'%≤1.0':>8}  note")
    print("-" * 92)
    primary_cells = None
    for cname, ccols in CLS_LADDER.items():
        for rname, rcols in REG_LADDER.items():
            m, cells, nd_used = family_stats(cls, reg, ccols, rcols)
            if m == 0:
                continue
            cov = "" if (not rcols or nd_used >= n_days_total * 0.9) else "⚠ regime-smoke"
            z_strict, z_loose = by_z_band(m)
            nd = cells["n_days"].to_numpy().astype(float)
            mde_strict = z_strict / np.sqrt(nd / TRADING_DAYS_YR)
            mde_loose = z_loose / np.sqrt(nd / TRADING_DAYS_YR)
            med_n = float(np.median(nd))
            med_mde_strict = z_strict / math.sqrt(med_n / TRADING_DAYS_YR)
            med_mde_loose = z_loose / math.sqrt(med_n / TRADING_DAYS_YR)
            frac_05 = float(np.mean(mde_strict <= HURDLE_SHARPE)) * 100
            frac_10 = float(np.mean(mde_strict <= 1.0)) * 100
            tag = "PRIMARY" if (cname, rname) == PRIMARY else cov
            print(f"{cname+'×'+rname:<26}{m:>6}{med_n:>8.0f}"
                  f"{med_mde_strict:>12.2f}{med_mde_loose:>11.2f}"
                  f"{frac_05:>7.0f}%{frac_10:>7.0f}%  {tag}")
            if (cname, rname) == PRIMARY:
                primary_cells = (cells, z_strict, z_loose, m)
    print()

    # --- per-cell detectable map for the chosen primary family --------------
    if primary_cells is not None:
        cells, z_strict, z_loose, m = primary_cells
        nd = cells["n_days"].to_numpy().astype(float)
        out = cells.with_columns(
            pl.lit(m).alias("family_size_m"),
            pl.Series("mde_sharpe_strict", z_strict / np.sqrt(nd / TRADING_DAYS_YR)),
            pl.Series("mde_sharpe_loose", z_loose / np.sqrt(nd / TRADING_DAYS_YR)),
            # 0.5× signal-haircut: fewer effective days => higher (worse) MDE
            pl.Series("mde_sharpe_strict_haircut",
                      z_strict / np.sqrt((nd * SIGNAL_HAIRCUT) / TRADING_DAYS_YR)),
            (pl.Series("mde_sharpe_strict", z_strict / np.sqrt(nd / TRADING_DAYS_YR))
             <= HURDLE_SHARPE).alias("reachable_at_hurdle"),
        )
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        path = OUT_DIR / f"power_mde_{PRIMARY[0]}_x_{PRIMARY[1]}.parquet".replace("×", "x")
        out.write_parquet(path)
        reach = out["reachable_at_hurdle"].sum()
        print(f"[PRIMARY = {PRIMARY[0]} × {PRIMARY[1]}]  m={m} testable cells, "
              f"H_m={harmonic(m):.2f}")
        print(f"  detectable-Sharpe band (median cell): "
              f"{z_strict/math.sqrt(float(np.median(nd))/TRADING_DAYS_YR):.2f} (strict) … "
              f"{z_loose/math.sqrt(float(np.median(nd))/TRADING_DAYS_YR):.2f} (loose)")
        print(f"  cells reachable at hurdle {HURDLE_SHARPE}: {reach}/{m} "
              f"({100*reach/m:.0f}%)  [upper bound — signal haircut makes this worse]")
        print(f"  per-cell map written: {path}")
        print()
    print("Interpretation: if MDE@strict >> the hurdle for most of the family,")
    print("COARSEN the family before discovery (§1.5.1) — or accept that the")
    print("phase tests second-moment/structure claims, not mean-expectancy edges.")


if __name__ == "__main__":
    main()
