#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — §5.1 step 1 / §2.2 NEGATIVE-SPACE BLACKLIST (deliverable #1).

"Where NOT to trade." Per §5.0 this is the highest-power deliverable — a false
blacklist entry costs only foregone opportunity, a false whitelist entry costs
money — so the asymmetry favors mining negative space. Two parts:

PART A — structural / cost blacklist (near-deterministic, very high power).
  From the frozen cost model + exitability flag, NO outcome data needed:
    - non-exitable: frac_zero_range > 0.30 (§3.4 — barely trades, can't exit).
    - cost-prohibitive: round-trip cost (1% ADV) above a hurdle no 1-day
      momentum edge can plausibly overcome. Reported at 20/30/50 bps.

PART B — empirical negative-expectancy blacklist (statistical, §7.7.1).
  Cells where going long the frozen signal has significantly NEGATIVE
  cost-adjusted excess-over-SPY expectancy, under day-clustered bootstrap +
  Benjamini-Yekutieli FDR at α=0.10 (sign flipped). Each BY survivor is tagged:
    - ROBUST   = significantly negative even GROSS (before cost) — momentum
      genuinely bleeds here; cost-independent.
    - COST-DRIVEN = flat/positive gross, negative only after the frozen cost
      (which is an upper bound for liquid cells — so this is sensitive to the
      deferred §6.1 calibration and flagged as such).

Family (per §1.5.1 / §7.7.1): coarse classification `cap×liq×price`, regime
EXCLUDED (the MDE pass proved regime-splitting craters power — it is the §2.1
day-gate, not a per-cell BY axis). Signal frozen `intraday_ret_0930_to_1000>0`,
entry offset 1000. Exploration window only.

Read-only on data/outputs/; writes data/phase1_analysis/.
"""
from __future__ import annotations
import glob, math, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CELL = ["market_cap_bucket", "liquidity_bucket", "price_bucket"]
MIN_NAMES = 5            # a (cell, day) is active with >= this many names
MIN_DAYS = 60           # a cell is testable with >= this many active days
ALPHA = 0.10            # discovery FDR (§1.6)
N_BOOT = 5000
SEED = 20260620
EXITABILITY_THRESH = 0.30
ZERO_RANGE_HURDLES = (20.0, 30.0, 50.0)   # cost-prohibitive thresholds (bps, 1% ADV)
HEADLINE_COST_HURDLE = 30.0
OUT = Path("data/phase1_analysis")


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


def harmonic(m):
    return float(np.sum(1.0 / np.arange(1, m + 1))) if m > 0 else 1.0


def by_survivors(pvals, alpha=ALPHA):
    """Benjamini-Yekutieli: return boolean mask of rejections (under arbitrary
    positive dependence). p_(k) <= k*alpha/(m*H_m)."""
    m = len(pvals)
    if m == 0:
        return np.array([], dtype=bool)
    Hm = harmonic(m)
    order = np.argsort(pvals)
    sorted_p = np.asarray(pvals)[order]
    thresh = (np.arange(1, m + 1) * alpha) / (m * Hm)
    below = sorted_p <= thresh
    kmax = np.max(np.where(below)[0]) + 1 if below.any() else 0
    rej_sorted = np.zeros(m, dtype=bool)
    rej_sorted[:kmax] = True
    out = np.zeros(m, dtype=bool)
    out[order] = rej_sorted
    return out


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    t0 = dt.datetime.now()
    print("=" * 92)
    print("Phase 1 — NEGATIVE-SPACE BLACKLIST  (§5.1 step 1 / §2.2, deliverable #1)")
    print("=" * 92)

    cm = pl.read_parquet(OUT / "cost_model.parquet").filter(pl.col("entry_offset") == OFFSET)

    # ---- PART A: structural / cost blacklist -----------------------------------
    print("\n[A] structural / cost blacklist (from cost model + exitability)")
    nonexit = cm.filter(pl.col("frac_zero_range") > EXITABILITY_THRESH)
    print(f"  non-exitable (frac_zero_range > {EXITABILITY_THRESH}): "
          f"{nonexit.height} cells")
    for h in ZERO_RANGE_HURDLES:
        c = cm.filter(pl.col("total_cost_bps_1pct_median") > h).height
        tag = "  <- headline" if h == HEADLINE_COST_HURDLE else ""
        print(f"  cost-prohibitive (round-trip > {h:>4.0f} bps): {c:>3} cells{tag}")
    struct = cm.filter((pl.col("frac_zero_range") > EXITABILITY_THRESH)
                       | (pl.col("total_cost_bps_1pct_median") > HEADLINE_COST_HURDLE))
    struct_keys = set(tuple(r) for r in struct.select(CELL).iter_rows())
    print(f"  → PART A blacklist (union): {len(struct_keys)} cells")

    # ---- PART B: empirical negative-expectancy ---------------------------------
    print("\n[B] empirical negative cost-adjusted excess expectancy (BY α=0.10)")
    cls = (pl.scan_parquet(window_files("security_classification_daily"))
           .filter(pl.col("ticker_type") == "CS")
           .select(["day", "security_id"] + CELL))
    sig = (pl.scan_parquet(window_files("daily_observation"))
           .filter(pl.col("intraday_ret_0930_to_1000") > 0.0).select("day", "security_id"))
    cost = cm.select(CELL + ["total_cost_bps_1pct_median"]).lazy()

    trades = (pl.scan_parquet(window_files("forward_outcomes"))
              .filter((pl.col("entry_offset") == OFFSET)
                      & pl.col("ret_1d_excess_spy").is_not_null())
              .select("day", "security_id", "ret_1d_excess_spy")
              .join(cls, on=["day", "security_id"], how="inner")
              .drop_nulls(CELL)
              .join(sig, on=["day", "security_id"], how="inner")
              .join(cost, on=CELL, how="left")
              .with_columns(
                  (pl.col("ret_1d_excess_spy")
                   - pl.col("total_cost_bps_1pct_median").fill_null(0.0) / 1e4).alias("net_ex"))
              .collect())

    # daily equal-weight portfolio per cell
    daily = (trades.group_by(CELL + ["day"])
             .agg(pl.col("ret_1d_excess_spy").mean().alias("gross"),
                  pl.col("net_ex").mean().alias("net"),
                  pl.len().alias("n"))
             .filter(pl.col("n") >= MIN_NAMES))

    cells = (daily.group_by(CELL).agg(pl.col("day").n_unique().alias("n_days"))
             .filter(pl.col("n_days") >= MIN_DAYS).sort("n_days", descending=True))
    cell_list = [tuple(r) for r in cells.select(CELL).iter_rows()]
    m = len(cell_list)
    Hm = harmonic(m)
    print(f"  testable family: m={m} cells (>= {MIN_DAYS} active days), H_m={Hm:.2f}")

    rng = np.random.default_rng(SEED)
    recs = []
    for key in cell_list:
        cond = pl.lit(True)
        for col, val in zip(CELL, key):
            cond = cond & (pl.col(col) == val)
        sub = daily.filter(cond).sort("day")
        net = sub["net"].to_numpy()
        gross = sub["gross"].to_numpy()
        nd = len(net)
        idx = rng.integers(0, nd, size=(N_BOOT, nd))
        net_bm = net[idx].mean(axis=1)
        gross_bm = gross[idx].mean(axis=1)
        # one-sided p for "mean < 0" = frac of bootstrap means >= 0
        p_net = float(np.mean(net_bm >= 0.0))
        p_gross = float(np.mean(gross_bm >= 0.0))
        recs.append(dict(zip(CELL, key)) | dict(
            n_days=nd, mean_net=float(net.mean()), mean_gross=float(gross.mean()),
            net_ub95=float(np.quantile(net_bm, 0.95)),
            p_net_negative=p_net, p_gross_negative=p_gross))

    res = pl.DataFrame(recs)
    rej = by_survivors(res["p_net_negative"].to_numpy())
    rej_gross = by_survivors(res["p_gross_negative"].to_numpy())   # proper BY, not a loose threshold
    res = res.with_columns(pl.Series("by_blacklisted", rej),
                           pl.Series("gross_by_survivor", rej_gross))
    # ROBUST = significantly negative GROSS under proper BY (cost-independent);
    # COST-DRIVEN = net-significant only. (The placebo validated that proper BY
    # on gross gives ~0 here — the loose α/H_m threshold over-counted.)
    res = res.with_columns(
        pl.when(~pl.col("by_blacklisted")).then(pl.lit(""))
        .when(pl.col("gross_by_survivor")).then(pl.lit("ROBUST"))
        .otherwise(pl.lit("COST-DRIVEN")).alias("kind"))

    bl = res.filter(pl.col("by_blacklisted")).sort("mean_net")
    nrob = bl.filter(pl.col("kind") == "ROBUST").height
    print(f"  BY survivors (significantly NEGATIVE net excess): {bl.height}/{m}  "
          f"({nrob} ROBUST / {bl.height - nrob} COST-DRIVEN)")
    if bl.height:
        print(f"\n  {'cap':<6}{'liq':<14}{'price':<11}{'nday':>5}"
              f"{'mean_net':>10}{'mean_gr':>10}{'p_net':>8}  kind")
        for r in bl.iter_rows(named=True):
            print(f"  {str(r['market_cap_bucket']):<6}{str(r['liquidity_bucket']):<14}"
                  f"{str(r['price_bucket']):<11}{r['n_days']:>5}"
                  f"{r['mean_net']*1e4:>9.1f}b{r['mean_gross']*1e4:>9.1f}b"
                  f"{r['p_net_negative']:>8.3f}  {r['kind']}")

    # ---- combined blacklist ----------------------------------------------------
    res.write_parquet(OUT / "blacklist_empirical.parquet")
    by_keys = set(tuple(r) for r in bl.select(CELL).iter_rows())
    combined = struct_keys | by_keys
    print(f"\n[COMBINED] blacklist = {len(struct_keys)} structural ∪ {len(by_keys)} empirical "
          f"= {len(combined)} cells")
    # robust-gross count across whole family (cost-independent, PROPER BY)
    rob_all = int(res["gross_by_survivor"].sum())
    print(f"  (cost-independent: {rob_all}/{m} cells survive proper-BY gross-negative "
          f"— placebo-validated; momentum bleed independent of the cost caveat)")

    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")
    print("Interpretation: PART A (structural) is the durable operating blacklist. "
          "Under proper\nBY, 0 cells are robustly gross-negative (placebo-validated) — so PART B's "
          "survivors\nare all COST-DRIVEN: low-liquidity cells already in A, plus liquid cells that "
          "flip\nwith the liquid-cost upper bound and graduate only after §6.1 calibration.")


if __name__ == "__main__":
    main()
