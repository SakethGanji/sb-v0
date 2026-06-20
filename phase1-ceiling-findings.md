# Phase 1 — Predictability-Ceiling Map Findings (§2.10 / §7.1, deliverable #2)

**Date:** 2026-06-20 · **Script:** `scripts/phase1_ceiling.py` · **Output:**
`data/phase1_analysis/ceiling_map.parquet`. **Status:** FINAL.

Per coarse cell: the MI-implied upper bound on how much ANY model could extract
from the recorded pre-entry features about the 1d beat-SPY outcome (Fano). Outcome
y = (ret_1d_excess_spy > 0); 16 pre-entry ML-safe features; binned plug-in MI
corrected against a 50-permutation null; signal-firing CS trades @ offset 1000,
exploration window. 996,939 trades, 34 cells with ≥2,000 trades.

## Result: the ceiling is near-zero everywhere
- Units = **millibits**; H(y) ≈ 1 bit (base rates 45–51%). So 1m ≈ 0.1% of outcome
  uncertainty.
- **Best cell** (micro·illiquid·above_100): **7.33m = 0.7% of H(y)** — the *most*
  predictable cell, and it's already structurally blacklisted.
- **15/34 cells at the noise floor** (best corrected MI < 1m).
- **Tradeable liquid cells ≈ zero**: large·liquid·above_100 = 0.27m,
  mega·liquid·20_to_100 = 0.28m.

A perfect model on these features could shave <1% off the beat-SPY uncertainty in
any cell — a fraction-of-a-percent accuracy lift over the ~50% base rate.

**Conservative caveat:** reported best-MI is the max over 16 features and is NOT
corrected for that selection, so the true ceiling is ≤ shown — even closer to zero.
The near-zero conclusion only strengthens.

## Feature ranking (§7.1; mean corrected MI across cells, millibits)
1. signal_concentration_percentile_today — 0.85m
2. signal_concentration_hhi_today — 0.50m
3. realized_vol_21d_rank_today — 0.31m
4. intraday_ret_0930_to_1000 (the signal magnitude) — 0.29m
5. intraday_ret_0930_to_0950 — 0.26m · overnight_gap — 0.23m
(crowding ranks highest, but all are negligible in absolute terms.)

## Decision-relevant conclusions
1. **No meta-labeling headroom (§5.0 deliverable #4 / Phase 4).** Ceiling <1% of
   outcome uncertainty ⇒ the §2.10 gap (ceiling − captured) is ~0 because the
   ceiling itself is ~0. An ML meta-labeler would be modeling noise. **Do not build
   Phase 4 on this data/horizon/feature set** — a research-budget call made on a number.
2. **Coherent with the whole phase.** Third independent axis agreeing: MDE null
   (no Sharpe<1.5 mean edge) ↔ blacklist (negative space is cost, not signal) ↔
   ceiling (features don't predict outcome). The anticipated "characterized null."
3. **§2.10 caveat:** "random w.r.t. the recorded features + binary 1d outcome,"
   NOT random absolutely. Unrecorded drivers (news, order flow) are invisible here;
   the ceiling is conditional on the feature set.

## Reproduce
```
scripts/phase1_ceiling.py
```
