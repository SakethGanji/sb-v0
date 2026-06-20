# Phase 1 — Cross-Fit DP Exit-Policy Test Findings

**Date:** 2026-06-20 · **Script:** `scripts/phase1_dp.py`. **Status:** FINAL — closes
the DP/exit question as a RESULT (cross-fit, OOS, significance-tested), not a gate.

The prior conclusion gated DP out via the predictability ceiling — but that ceiling
measured PRE-ENTRY features, whereas DP exploits WITHIN-TRADE path state, a different
conditioning set. This test closes that gap directly.

## Method
Intraday optimal stopping: 10:00 entry → exit by close, on the materialized path
(ret_to_1030 … ret_to_close). Tabular backward-induction DP on binned state
(current_ret×5, drawdown×3, checkpoint×8), 5 bps round-trip cost. **2-fold cross-fit**
(split by day): learn the policy on one fold, VALUE it on the held-out fold — controls
the max-operator optimism bias (§7.3/§7.7.1). Benchmarks: hold-to-close, and best
FIXED-time exit (the state-blind control, chosen on train / valued OOS). Day-clustered
bootstrap LB on (DP − best-fixed). 3 representative deployable cells, signal-firing.

## Result — path state adds NO out-of-sample value
| cell | n | hold-close | best-fixed | DP (OOS) | DP−fixed | LB |
|---|---:|---:|---:|---:|---:|---:|
| large/liquid/above_100 | 56,344 | −5.0b | −4.9b | −4.9b | −0.0b | −1.3b |
| mid/normal/20_to_100 | 183,483 | −6.1b | −5.2b | −5.1b | +0.1b | −1.2b |
| small/normal/5_to_20 | 28,407 | +1.8b | −2.5b | +0.6b | +3.1b | **−2.6b** |

- **All three: day-clustered LB on (DP − best-fixed) ≤ 0** → no significant OOS gain.
- The `small/normal` case is the instructive one: DP−fixed looks +3.1b but (a) LB=−2.6
  (insignificant) and (b) **DP (+0.6) < hold-to-close (+1.8)** — the "best-fixed"
  benchmark overfit a bad exit time on train. Exactly the optimism mirage the cross-fit
  exposes; the point estimate is noise.

## Conclusion
A state-dependent intraday exit policy does **not** beat a one-line rule out-of-sample
in any representative cell. Confirms §7.3 empirically: exits cannot manufacture an edge
the entry lacks. The DP gate is now an earned result. (Raw net-of-cost; the DP-vs-fixed
comparison holds the same names so beta largely cancels — and in excess terms, where
the entry edge is already ~0/negative, it is no better.)

## Reproduce
```
scripts/phase1_dp.py
```
