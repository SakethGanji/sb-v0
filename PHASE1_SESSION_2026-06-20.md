# Phase 1 — Session Status & Handoff (2026-06-20)

Single cold-read for the next session. Pairs with `phase1-research-strategy.md`
(the frozen plan) and the per-step findings docs. **Phase 0 engine is complete &
validated** (406,190 checks pass; see git ≤ 1511a3d). This session ran Phase 1.

## State at a glance
| Step | Status |
|---|---|
| §7.8 step −1 power/MDE | ✅ `phase1-power-mde-findings.md` — no Sharpe<1.5 edge detectable on the window |
| §5.1 step-0 tracer | ✅ `phase1-tracer-findings.md` — plumbing + power + dividend cols validated |
| L2 verifier | ✅ `scripts/phase1_verify.py` — tracer+cost recompute-clean |
| §6.1 cost model | ✅ `phase1-cost-model-findings.md` — frozen retail (capped spread + 1c tick floor + frac_zero_range) |
| #1 blacklist | ✅ `phase1-blacklist-findings.md` — 55-cell structural; 0 robust signal-bleed |
| placebo + synthetic | ✅ `phase1-validation-findings.md` — FDR calibrated + detection power confirmed |
| #2 ceiling map | ✅ `phase1-ceiling-findings.md` — features carry <1% of outcome info; no meta-label headroom |
| #3 stability | ✅ `phase1-stability-findings.md` — null across entry/horizon/era; 2020 = meme-era inflation, insignificant |

## THE CRITICAL SCOPE CAVEAT
**Everything above tested exactly ONE signal:** `intraday_ret_0930_to_1000 > 0`
(frozen momentum, 10:00 entry). It is null across cells/offsets/horizons/eras.
**NOT yet run:** other signal definitions, the DP/exit-rule layer, the
target-before-stop materialized labels (`first_event_*`, 9 pairs), the ~135-question
backlog (`phase1-candidate-questions.md`). The "no strat" conclusion is EARNED for
the momentum signal and EXTRAPOLATED (via the power+predictability ceilings, which
are signal/method-agnostic) to the rest — extrapolation, not result. **The breadth
sweep below is what earns it across the catalog.**

## Validated machinery (all `uv run --with polars --with pyarrow --with numpy python3 scripts/X`)
- Daily-portfolio cost-adjusted excess/SPY + day-clustered (block) bootstrap LB + BY FDR.
- Validated 3 ways: L2 recompute (`phase1_verify`), placebo FP-control (`phase1_placebo`,
  caught a real BY-labeling bug), synthetic power (`phase1_synthetic`: 92% power @ Sharpe2,
  4% @ 0.5 — crosses 50% at the MDE floor ~1.4).

## Frozen decisions (Category-A; see strategy §6 RESOLVED + memory)
- Cost: retail ~0 commission; capped high-low proxy + 1c tick floor; NO NBBO calibration
  (deferred — the one un-closable validation gap). Capacity weighting retail-scale.
- Primary family: coarse `cap×liq×price` (regime EXCLUDED — MDE proved it craters power;
  regime is the §2.1 day-gate). Sharpe hurdle 0.5; ≤20 pre-reg hypotheses.
- Holdout 2023-01→2026-06 UNTOUCHED. Nothing pre-registered (no positive candidate found).

## Data / env
- Outputs on T7: `data/outputs/{forward_outcomes,daily_observation,security_classification_daily,
  market_context_daily,regime_definitions,...}` — forward_outcomes 657 cols × 2,513 days.
- Exploration window = 2016-06-08→2020-12-31 (all Phase-1 work uses this only).
- Analysis outputs land in `data/phase1_analysis/`. System python lacks polars → use uv.

## NEXT (in progress this session): comprehensive breadth sweep
`scripts/phase1_sweep.py` — answers "is there ANYTHING at all" across breadth:
- Stage 1: multi-signal entry-expectancy (~15 signals × horizons, deployable cohort,
  cost-adj excess + gross, block-boot LB, BY). Input-validated (firing rate / nulls per signal).
- Stage 2: exit-rule / DP-adjacent — target-before-stop hit-rate vs breakeven Y/(X+Y)
  for the 9 `first_event_*` pairs × signals. Tests whether any target/stop rule beats breakeven.
Then: if all null → write consolidated characterization (closes cycle 1, §1.7).
If a survivor appears → surface/excess-recheck/era-check before any claim.
