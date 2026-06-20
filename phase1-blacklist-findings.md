# Phase 1 — Negative-Space Blacklist Findings (§5.1 step 1 / §2.2, deliverable #1)

**Date:** 2026-06-20 · **Script:** `scripts/phase1_blacklist.py` · **Outputs:**
`data/phase1_analysis/blacklist_empirical.parquet`. **Status:** Part A FINAL
(structural); Part B **placebo-validated and CORRECTED (2026-06-20)** — the
placebo (`scripts/phase1_placebo.py`, §7.8 step 0.5) caught that the original
"ROBUST" tag used a loose per-cell threshold (`p_gross ≤ α/H_m`) instead of the
proper Benjamini-Yekutieli step-up. Under correct BY, **0 cells are robustly
gross-negative** (matching the placebo: real 0 ≈ placebo mean 0.4, max 1), so
all empirical survivors are cost-driven. Findings below reflect the correction.

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
Testable family m=35 (≥60 active days), H_m=4.15. **23/35 BY-significant negative
NET expectancy — but 0 ROBUST under proper BY** (corrected after the placebo):

- **0 ROBUST.** No cell survives the Benjamini-Yekutieli step-up on the *gross*
  (pre-cost) test — the smallest gross p-value (≈0.001+) exceeds the rank-1
  threshold `α/(m·H_m)=0.000688`. The placebo confirms this is correct: under the
  real signal proper BY gives 0, identical to the shuffled-signal placebo (mean
  0.4, max 1). So **none of the negativity is signal-driven momentum bleed** that
  survives rigorous FDR. (The original "5 ROBUST" used a loose `p≤α/H_m` per-cell
  threshold — over-lenient; that label is retracted.)
- **All 23 survivors are COST-DRIVEN** — significantly negative only *after* the
  frozen cost. They split into (a) low-liquidity cells already in Part A, and
  (b) liquid large/mega cells that are ~break-even GROSS (`large·liquid·above_100`
  = +1.1 bps gross) and negative only under the ~12-bps liquid cost that §6.1 says
  is an overstated upper bound. **Blacklisting (b) would exclude the most-tradeable
  cells most likely to hold a real edge — a false positive.** Held out pending
  §6.1 calibration.

## The operating blacklist
- **= Part A structural (55 cells), full stop.** Part B contributes no independent
  robust findings: 0 cells are rigorously gross-negative, and the cost-driven
  survivors are either already-structural (low-liquidity) or calibration-sensitive
  liquid cells held back.
- **The honest headline:** the negative space is **entirely cost/liquidity/structural**.
  There is no statistically robust signal-driven negative-momentum cohort at this
  granularity/power — consistent with the §1.5.1 MDE null (no Sharpe-0.5 effect
  visible in *either* direction on this window).

## Validation & next
- **Placebo (§7.8 step 0.5): PASS + bug caught.** Real gross-negative survivors
  (0) ≈ placebo (mean 0.4) → FDR calibrated, not manufacturing signal-specific
  discoveries. It also caught the loose-threshold ROBUST mislabel, now fixed.
- COST-DRIVEN liquid cells graduate (to blacklist or to whitelist candidates) only
  after §6.1 spread calibration tightens the liquid-cost upper bound.

## Reproduce
```
scripts/phase1_blacklist.py
```
