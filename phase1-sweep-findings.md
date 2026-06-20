# Phase 1 — Comprehensive Breadth Sweep Findings (multi-signal + exit-rule)

**Date:** 2026-06-20 · **Script:** `scripts/phase1_sweep.py` · **Outputs:**
`data/phase1_analysis/sweep_signals.parquet`, `sweep_exits.parquet`. **Status:**
FINAL — the null is now EARNED across the signal catalog AND the exit/DP dimension,
not extrapolated from the single momentum signal.

Deployable cohort (mega/large/mid × highly_liquid/liquid/normal, CS), offset 1000,
exploration window, 994,694 trades. All 16 signals validated firing 1.7–49%.

## Stage 1 — 16 entry signals × 3 horizons (48 combos): NULL
Signals: momentum (up/strong/top-decile), reversal (down/bottom-decile), gap
(up/down/and-go/fade), breakout (at-30m-high), pullback, pm-vol-spike, concentration
(high/low), near-52w-high, consec-up. Cost-adj excess/SPY, block-boot LB, BY.
- **0/48 net-positive · 0 BY survivors · 0 raw net-LB>0.**
- Only 1/48 has a positive GROSS lower bound (rev_bottom_decile EOD: +4.2 bps, LB
  +1.1 — a tiny intraday mean-reversion bounce) and it **dies after cost** (net −0.8).
- Largest point estimates (gap_down 5d +9.5b, high_concentration 5d +9.2b) have LBs
  below zero — noise.
→ No entry signal, in any direction, has a deployable excess edge.

## Stage 2 — exit rules / target-before-stop (9 pairs × 16 signals): beta, not alpha
48/144 combos beat the naive breakeven Y/(X+Y), top edge +0.039 — BUT this is
market drift, proven by two tells:
- **Cross-signal invariance:** the 3atr/1.5atr_21d pair gives ~+0.026–0.039 edge for
  mom_up, pullback_from_high, at_30m_high, gap_and_go, high_concentration alike —
  near-opposite signals, same edge ⇒ it's the horizon, not the signal.
- **Unconditional baseline (decisive):** with NO signal, the same pair's edge is
  **+0.025** (21d) / +0.021 (3%/3% 5d) — identical to the signal-conditioned values.
  And the intraday pair (0.5%/0.5% 30min, negligible drift) goes **−0.007**.
→ The apparent exit "edge" is bull-market drift over the holding window (§2.3
beta-in-disguise), pre-cost and pre-excess. Signal/exit conditioning adds nothing.
Consistent with §7.3: exits can't manufacture excess edge from a no-excess-edge entry.

## Verdict
**There is no exploitable strategy across 16 entry signals × 3 horizons × 9 exit
rules.** The positive-looking numbers are sub-cost (Stage 1) or pure market drift
identical without any signal (Stage 2). Combined with the MDE null, the blacklist,
the near-zero predictability ceiling, and the era sweep, the Phase-1 edge question
is exhausted across breadth. Remaining untested are characterization/structure
questions (bounded by the same power + predictability ceilings), not edge-hunts.

## Reproduce
```
scripts/phase1_sweep.py
```
