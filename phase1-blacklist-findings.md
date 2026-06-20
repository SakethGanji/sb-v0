# Phase 1 — Negative-Space Blacklist Findings (§5.1 step 1 / §2.2, deliverable #1)

**Date:** 2026-06-20 · **Script:** `scripts/phase1_blacklist.py` · **Outputs:**
`data/phase1_analysis/blacklist_empirical.parquet`. **Status:** Part A FINAL
(structural); Part B **provisional — pending the §7.8 step-0.5 placebo validation**
of the FDR machinery before it is treated as confirmed.

Family: coarse classification `cap×liq×price`, regime EXCLUDED (MDE pass: regime is
the §2.1 day-gate, not a BY axis). Signal `intraday_ret_0930_to_1000>0`, offset
1000, exploration window. Cost = frozen retail model (per-cell, 1% ADV).

## Part A — structural / cost blacklist (durable, calibration-independent)
55 cells = non-exitable (33, `frac_zero_range > 0.30`) ∪ cost-prohibitive (27,
round-trip > 30 bps; 14 at >50, 39 at >20). All low-liquidity / thin / sub-$1 /
low-price junk — structurally untradeable independent of any edge or the deferred
calibration. This is the §2.2 core ("friction reduction is most of the realized
alpha").

## Part B — empirical negative cost-adjusted excess expectancy (BY α=0.10)
Testable family m=35 (≥60 active days), H_m=4.15. **23/35 BY-significant negative,
but only 5 ROBUST.** The ROBUST/COST-DRIVEN split is the load-bearing distinction:

- **5 ROBUST** (gross-negative *before* cost) — genuine negative-momentum cohorts:
  micro·illiquid·{5_to_20, 20_to_100}, small·illiquid·20_to_100, small·thin·above_100
  (mostly already in Part A), **plus `mega·liquid·20_to_100`** (gross −10.3b,
  n_days=210 — a rare cohort, treat cautiously / re-check on validation).
- **18 COST-DRIVEN — NOT actionable.** Flat/positive gross, negative only after the
  frozen cost. Critically these include liquid large/mega cells: `large·liquid·above_100`
  is **+1.1 bps gross** (slightly positive!), blacklisted only by the ~12-bps liquid
  cost that §6.1 says is an overstated upper bound. With realistic ~2–3 bps these
  flip. **Blacklisting them would exclude the most-tradeable cells most likely to
  hold a real edge — a serious false positive.** Held out pending §6.1 calibration.

## The operating blacklist
- **NOW (calibration-independent) ≈ 58 cells** = Part A (55) ∪ Part B ROBUST (5).
- **Held back = 18 COST-DRIVEN cells** (incl. liquid large-caps ~break-even gross).
- **Only 5/35 cells are gross-negative** — momentum is not broadly negative; the
  negative space is overwhelmingly cost/liquidity friction (§2.2 confirmed). Liquid
  large-caps are ~break-even gross, consistent with the §1.5.1 MDE null — neither
  edge nor structural avoid.

## Caveats / next
- **Part B is provisional until the placebo run (§7.8 step 0.5)** confirms the
  bootstrap+BY machinery isn't over-rejecting (23/35 is a high hit rate — plausible
  given costs, but unvalidated). Part A needs no such check (deterministic).
- COST-DRIVEN cells graduate (to blacklist or to whitelist candidates) only after
  §6.1 spread calibration tightens the liquid-cost upper bound.

## Reproduce
```
scripts/phase1_blacklist.py
```
