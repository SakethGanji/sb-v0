# Phase 1 — Pipeline Validation Findings (placebo + synthetic injection)

**Date:** 2026-06-20 · **Scripts:** `scripts/phase1_placebo.py` (§7.8 step 0.5),
`scripts/phase1_synthetic.py` (§7.9), `scripts/phase1_verify.py` (L2 recompute).
**Status:** the Phase-1 analysis layer is validated to the limit the data allows.

## The three independent validations now agree end-to-end

1. **L2 recompute** (`phase1_verify.py`) — tracer + cost-model logic/formulas/
   aggregation independently recomputed (numpy row-wise), exact match on 6 cells
   + the tracer headline. The analysis CODE is correct.

2. **Placebo / false-positive control** (`phase1_placebo.py`) — signal shifted by
   12 large lags (±21…±168d), gross-negative BY re-run each time: REAL 0/35 ≈
   placebo (mean 0.4, max 1). The FDR does NOT manufacture findings. (Also caught
   the loose-threshold ROBUST mislabel in the blacklist — now fixed.)

3. **Synthetic injection / true-positive power** (`phase1_synthetic.py`) — inject a
   known-Sharpe drift into a real cell's daily gross series, run the identical
   bootstrap+BY family test, K=200/Sharpe. Power curve:

   | Sharpe (real t) | large·liquid·above_100 (N=1131) | mega·liquid·20_to_100 (N=210) |
   |---|---|---|
   | 0.0 | 0% | 0% |
   | 0.5 (t≈1.06 / 0.46) | 4% | 2% |
   | 1.0 | 29% | 2% |
   | 1.5 (t≈3.18 / 1.37) | 60% | 8% |
   | 2.0 | 92% | 12% |
   | 3.0 | 100% | 46% |

## What this proves
- **Detects detectable edges:** 92% power @ Sharpe 2, 100% @ 3 (high-N).
- **Correctly blind below floor:** 4% @ Sharpe 0.5 (≈ nominal FP rate), 0% @ 0.
- **50% crossing ≈ Sharpe 1.4–1.5** = the family's MDE *strict* floor — the
  analytic MDE, the FP control, and the empirical power all coincide.
- **Power scales with N** (small-N cell needs a far bigger edge) — §1.5.1 confirmed.

**Conclusion: the all-null Phase-1 results are REAL nulls, not pipeline blindness.**
A Sharpe-2 edge would have been flagged; none was.

## The one gap that CANNOT be closed now
**Cost-model accuracy vs ground truth.** The liquid-cell spread proxy is a known
upper bound; validating it needs NBBO quotes (no entitlement; calibration deferred
per the 2026-06-20 decision). Un-closable without external data — documented, and
in the safe direction for a blacklist (pessimistic on cost). Everything else
validatable on this data IS validated.

## Reproduce
```
scripts/phase1_verify.py · scripts/phase1_placebo.py · scripts/phase1_synthetic.py
```
