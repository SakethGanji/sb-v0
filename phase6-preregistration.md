# Phase 6 pre-registration — event-conditioned state dynamics

**Frozen:** 2026-07-06, before any Phase 6 code ran. **Purpose: dynamics, not alpha.**

## Registered null expectation (written first, per protocol)

> If the learned states reduce to volatility/liquidity regimes and do not improve net
> directional expectancy, this **confirms the prior null** (volatility clustering / GARCH,
> rediscovered) rather than opening a new branch. The unconditional event effects are
> already known: true SUE is priced within ~1 day; insider cluster-buys carry negative,
> short-gated drift. The ONLY new content this phase can produce is the **interaction**:
> does the same knowable event carry different forward drift depending on the state it
> lands in? Everything else is description.

## Design (frozen)

### Step 1 — states
- Per (name, day) state vector, all knowable at 10:00 via prior-close data:
  `volatility_percentile_today`, vol-trend = yang_zhang_5d/42d, trailing 21d return,
  trailing 63d return, volume-trend = adv_5d/adv_60d.
- k-means, k=6, standardized with 2016-06..2018-12 stats, fit on train only, **centroids
  frozen**, seeded (20260706). States labeled post-hoc from centroid signatures.

### Step 2 — stability (the descriptive verdict)
- Occupancy, daily transition matrix, expected durations: 2016-18 vs 2019-20.
- Registered prediction: stable, vol-dominated. Stability alone = GARCH rediscovery, NOT news.

### Step 3 — event × state cells (the only alpha-shaped part)
- Events (knowable at decision time): SUE top-quintile (8-K clock), SUE bottom-quintile,
  insider cluster-buy filing (≥2 O/D, trailing 30d first-flag day), any 8-K 2.02.
- Cell = event × state-at-entry (6) × horizon (5d, 21d excess-over-SPY), entry next trading
  day 10:00. ~48 cells → **BY-FDR across all cells**; per-cell block bootstrap
  (block=horizon); placebo = event flag shuffled among same-day same-state names.
- **Power gate per cell first**: cells with MDE95 > 60bp/21d (or >35bp/5d) are declared
  UNANSWERABLE and excluded from any verdict (reported as such, not as null).
- PASS for a long cell: net@20 > 0, BY-significant, ≥3 of available eras positive, placebo ~0.
  Anything else = null. Short cells (negative drift) are reported but pre-labeled GATED
  (wall #2) regardless of significance.
- Train window only (2016-06..2020-12). Validation 2021-22 only if a cell PASSes. Holdout sealed.

## What would count as a discovery
1. A transition-matrix regime that is NOT explainable by vol/liquidity ordering (vs prediction).
2. An event whose forward drift **differs across states** with BY-significance and era
   stability — i.e., the market misprices an event conditional on state, not the event itself.
