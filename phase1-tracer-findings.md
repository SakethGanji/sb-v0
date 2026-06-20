# Phase 1 — Tracer-Bullet Findings (§5.1 step 0)

**Date:** 2026-06-20 · **Script:** `scripts/phase1_tracer_bullet.py` (read-only on
`data/outputs/`, writes `data/phase1_analysis/`). **Status:** PASS — plumbing
validated, power reality reproduced, one cost-model action item surfaced.

The first real number from the data. Runs the entire discipline (cost,
raw + excess, day-clustered bootstrap, surface) on ONE frozen cell with plain
conditional sorts — no MI/BN/DP/HB — per the strategy doc's mandate that the
next move be "a number computed from real data."

**Frozen cell:** offset `1000` (10:00 ET) · 1d horizon · large-cap CS · signal
`intraday_ret_0930_to_1000 > 0` · exclude leveraged/inverse ETF, China ADR,
recent IPO. Window: exploration only (2016-06-08 → 2020-12-31).

---

## What ran
- **219,861 trades over 1,150 days** (~191 names/day, 4.56 yr).
- Lazy projected scan of **1,151** `forward_outcomes` files (~8 of 657 cols),
  joined to classification + signal tables. **Wall: 173s.** The I/O discipline
  (projection + join, one day's worth of columns at a time) is what every later
  pass will use.

## The four checks
1. **Plumbing ✅** — end-to-end clean in 173s; no memory blowup, projection +
   pushdown working.
2. **Dividend columns ✅ (§3.3)** — `ret_1d_total` vs `ret_1d`: **1.2%** of trades
   differ, mean Δ **+0.59%** on those. Correct sign and magnitude (one ex-div day
   inside a 1d window ≈ a quarterly large-cap yield). Total-return columns behave.
3. **Power reality reproduced ✅ (§1.5.1)** — gross excess/SPY Sharpe **+0.25**,
   t≈0.53, bootstrap 5% LB **≤0 → null**. The window cannot see a Sharpe-0.5 mean
   edge, exactly as the MDE pass said. A null here is the *expected, validating*
   outcome — the tracer tests the pipeline, not this cell's tradeability.
4. **Surface smooth ✅ (§2.4)** — (offset × horizon) grid is a clean monotonic
   gradient (1d: −14.4b @10:00 → −5.3b @13:00), no isolated spikes. But the
   gradient is mostly the entry-bar cost proxy shrinking through the session, and
   the 1d≈2d≈3d≈5d invariance shows no multi-day continuation alpha.

## Action item surfaced (the real finding)
**The §6.1 top-200-ADV spread-proxy cap is load-bearing — implement it in the
cost-model step.** The *uncapped* high-low proxy gives a **median 11.3 bps
round-trip for large-caps** (p75 18.4, p95 37.7) — implausible; real large-cap
effective spread is 1–3 bps. §6.1 warns of exactly this ("the bar range reflects
the price walk, not the round-trip cost"). Consequence: the tracer's "net Sharpe
−5.59" is a **cost artifact, not signal**. The cost-sensitivity row brackets the
truth — at a realistic ~5 bps the cell is still negative-excess (−1.74), at 0 bps
it's the +0.25 null — so the no-edge conclusion holds regardless, but the cap is
mandatory before any cost-adjusted ranking is trustworthy.

## Outputs
- `data/phase1_analysis/tracer_daily_offset1000_1d.parquet` — daily portfolio series.
- `data/phase1_analysis/tracer_surface_offset_x_horizon.parquet` — the §2.4 surface.

## Reproduce
```
scripts/phase1_tracer_bullet.py
```
