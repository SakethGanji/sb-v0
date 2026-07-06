# Phase 7 — final implementation plan

**Date:** 2026-07-06 · Governs everything after `FINAL_REPORT.md`. Companion docs:
`phase7-magnitude-program.md` (design space + closure theorem) ·
`phase7-preregistration.md` (Test 7.1, frozen) · `phase7-stoploss-closure.md`.

This is the complete remaining work of the project. Four steps, strictly ordered, each
with a run command or a freeze-first requirement, a decision rule, and a deliverable.
When step 4 completes, the project is closed — either with the exhaustion statement or
with a PASS routed through the confirmation protocol. Nothing else gets run.

---

## Standing rules (apply to every step)

- Freeze pre-registration before code runs; power gate before any test; costs first;
  BY-FDR within family; placebo/permutation where a search exists; day-clustered or
  block-bootstrap inference; train 2016-06→2020-12; validation 2021-22 only on a
  registered PASS; 2023+ holdout sealed.
- Every proposal must name its (channel, hatch) cell per `phase7-magnitude-program.md`
  §4. Closed cell → cite the closure, zero compute. No delta-one expectancy backtests
  under any framing (policy grammars, exit architectures, invalidation pockets, hedged
  structures — all Channel E, all closed).
- Risk-tool verdicts and alpha verdicts are never merged. No PASS is believed without
  the 2021-22 confirmation pass.

## Step 1 — RUN Test 7.1 (now; free; one overnight on the data machine)

**What:** `scripts/phase7_forecast_value.py` (written, syntax-checked, pushed).
Pre-registration frozen in `phase7-preregistration.md`. Cell: Channel G / hatch H1.

```
cd <repo on data machine>
scripts/phase7_forecast_value.py            # all arms; or --arm A|B|C
```

- Arm A: vol-targeted SPY, ML σ̂ vs RV21/BLEND. Arm B: inverse-vol portfolio, ML vs
  atr_14d. Arm C: harvest(ML basket) vs harvest(ATR basket).
- The script resolves the `forward_outcomes` next-day-return column defensively and
  prints its choice; record any contract substitution verbatim in the findings doc.
- **PASS criterion (economic utility, never alpha):** better realized Sharpe /
  drawdown / geometric growth **at equal risk** versus the naive-trailing-vol version
  of the same portfolio — ΔSharpe is scale-invariant; Δgrowth is compared with both
  return series rescaled to a common 10% realized vol, and average exposure is
  reported, so higher exposure in a bull window cannot masquerade as forecast value.
  **A PASS means "better risk tool," never "trading edge"** (pre-registration,
  pre-run amendment of 2026-07-06).
- **Decision rule (frozen):** NULL on all arms → the forecast is persistence-in-a-
  costume for every portfolio use; Channel G closes at the margin; naive trailing vol
  remains the recommended risk input. Any arm PASS → risk-tool verdict only → run the
  2021-22 confirmation for that arm before adoption.
- **Deliverable:** `phase7-findings.md` §7.1 with the printed verdict lines, plus a
  one-line update to `MASTER_FINDINGS.md` §6.

## Step 2 — A2 property sprint: orthogonal invariants (free; ~2 evenings; freeze first)

**What:** the one net-new item adopted from the external roadmap review. Two property
tests — NOT strategies. Cell: none (pure science); the monetization map is pre-committed
below so a positive result cannot be mistaken for an alpha lead.

**Freeze `phase7a2-preregistration.md` before code, containing exactly:**

- **A2a — liquidity-concentration persistence.** Build a daily volume-profile table
  from `bars_1m_raw/` (one DuckDB/polars pass: per (security_id, day), HHI of volume
  across the day's price bins + share-in-modal-bin). Property test: cross-sectional
  rank persistence of concentration at t+1/t+5/t+21, **after partialling out both the
  vol block (8 STATIC_VOL features) AND the liquidity block (adv/addv)** — raw
  concentration trivially persists via ADV; only the residual counts.
- **A2b — tail-shape persistence.** Per (security_id, month), Hill tail index (both
  tails) on 1-min returns; Hill is scale-invariant so vol orthogonality is structural,
  but partial out the vol block anyway. Property test: month-over-month cross-sectional
  rank persistence of the residual index. Registered prior: weak/none — equity tail
  indices are noisy and roughly universal; the power gate may already say UNANSWERABLE
  at fine granularity (gate at (stock, month) first; coarsen to quarter only if the
  gate demands and say so).
- **PASS (property exists) per test:** residual rank autocorrelation CI excludes 0
  (day/month-clustered), sign-stable 5/5 eras, and above a floor frozen in the pre-reg
  (suggest ≥ 0.10 at the shortest horizon — below that a "property" is decorative).
- **Pre-committed monetization map:** a PASS routes to exactly one place — a Branch B
  extension (does our tail/concentration forecast beat option-implied skew/smile?),
  i.e. it can raise Step 3's priority or add one registered comparison to it. It does
  NOT open any delta-one branch (theorem), and it does not reopen Channel E under any
  narrative. A FAIL on both closes the question: price/volume contains one robust
  invariant — volatility — and nothing else discovered.
- **Deliverable:** `phase7-findings.md` §A2 + the new `volume_profile_daily` table
  documented as engine output #9 if built.

## Step 3 — Test 7.2 / Branch B: realized vs implied (paid, ~$50–100; when funded)

**What:** the only expectancy-shaped door (Channel C / hatch H2). Unchanged from
`FINAL_REPORT.md` §8; A2 may add one registered skew comparison, nothing else.

1. Buy one month of IV history (ThetaData or ORATS tier, ~$50–100). Liquid-optionable
   subset of the deployable universe.
2. **Power gate first** (its own script + freeze): can the sample adjudicate whether
   (our forecast − IV) predicts (realized − IV) at the size option spreads require?
   If the gate FAILs, the verdict is UNANSWERABLE-at-this-budget, reported as such,
   and the decision to buy more history is a new, explicit bet.
3. Single pre-registered test on gate PASS: rank-IC of (forecast − IV) on
   (realized − IV) + delta-hedged straddle economics net of quoted spreads.
   Registered prior: null (+0.008 forecast lift over persistence; persistence is IV's
   own first input).
4. **Deliverable:** `phase7-findings.md` §7.2. A PASS goes to 2021-22 confirmation
   per §7.7 before a dollar moves.

## Step 3.5 — live cost-calibration book (optional; tiny capital; runs alongside Step 3)

**What:** the one measurement history cannot provide (adopted from the Program C review,
`phase7-magnitude-program.md` §7): a small live book of trades sized to **measure fills,
not to profit** — market and limit orders in the deployable universe, logged against the
quoted/arrival price. Purpose: replace the frozen cost model's deliberate liquid-cell
upper bound (~10–17bp assumed vs ~1–3bp believed true) with a measured number.

- Not a strategy; no expectancy claim possible or sought. Position direction randomized,
  flat by close, size at the smallest measurable increment.
- **Deliverable:** a measured spread/impact table for the liquid cells →
  `phase1-cost-model-findings.md` amendment. Effect: every existing net verdict
  sharpens; if true costs are ~1–3bp, a handful of "real gross, dead net" structures
  (e.g. reversal's −15.8bp/5d net at ~30bp round-trip) deserve one re-read at measured
  cost — as recomputation of existing results, not new searches.

## Step 4 — Seal (one writing session; no code)

Write `FINAL_REPORT_ADDENDUM.md`:

- Results of 7.1, A2, 7.2 with exact numbers.
- **If all nulls — the earned exhaustion statement** (verbatim skeleton in
  `phase7-magnitude-program.md` §6): magnitude prediction cannot be converted into
  stock-trading alpha at retail in any of the four channels that partition the design
  space; it remains a validated risk tool (wide stops, vol-aware sizing, drawdown
  control); the informational boundary of price/volume is now mapped on both the
  prediction side (Phases 0–6B) and the decision side (Phase 7).
- Standing recommendation unchanged unless a PASS survived confirmation: index the
  capital; optional vol-targeting overlay (naive input unless 7.1 said otherwise);
  reopen only for a genuinely new information source (options flow, text, revisions,
  positioning) via the EDGAR-pattern pipeline.
- Close the branch. Anything after this is a new project with a new spec.

---

## Sequencing and effort

| step | cost | wall time | blocked by |
|---|---|---|---|
| 1 · Test 7.1 | $0 | one overnight run + one write-up session | nothing — ready now |
| 2 · A2 sprint | $0 | freeze (½ session) + profile build + 2 property tests | nothing; parallelizable with 1 |
| 3 · Branch B | ~$50–100 | gate + one test, ~2 sessions | funding decision |
| 3.5 · cost calibration | tiny live book | passive, weeks of logging | broker access; optional |
| 4 · Seal | $0 | one session | 1–3 complete (or 3 explicitly declined) |

Explicitly out of scope, permanently, with the citation that closes each: policy
grammars and strategy-composition searches (theorem, `phase7-magnitude-program.md` §2);
any stop/exit/management architecture (`phase7-stoploss-closure.md`); conditional-entry
or invalidation-pocket scans (Phase 4 PRIM pipeline null); new directional
models/indicators on the same data (Phases 1–6B, closed as a class); Channel X execution
work (no quote data); "Program C" mechanism/network/disagreement modeling on price/volume
data (`phase7-magnitude-program.md` §7 — collapses into A/B; its role is choosing the
next dataset if the project reopens); tax-loss harvesting (real, direction-free,
volatility-loving — but engineering against one's tax bill, not market research).
Declining Branch B is allowed; it converts one line of the
exhaustion statement from "measured" to "untested by choice, prior poor" — the addendum
must then say exactly that.
