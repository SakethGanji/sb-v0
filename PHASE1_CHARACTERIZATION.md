# Phase 1 — Characterization (Cycle 1 Close)

**Date:** 2026-06-20 · **Status:** CYCLE 1 COMPLETE. This is the standing record of
what Phase 1 found. It consolidates the seven step-level findings docs (linked at
the end) into one authoritative artifact, per `phase1-research-strategy.md` §1.7.

---

## 0. Verdict (plain words)

**There is no deployable trading strategy in this data for the signals tested —
and that conclusion is rigorously earned, not assumed.**

Simple-to-moderate intraday/short-horizon rules ("buy morning strength," "buy the
dip," gap/breakout/concentration plays, with/without stops and targets) do **not**
produce a positive cost-adjusted, market-excess return on 2016–2020 US equities.
What Phase 1 *did* produce: a durable **structural blacklist** (where not to trade),
a **near-zero predictability ceiling** (the recorded features carry almost no
information about forward outcomes), and a **fully validated engine + analysis
pipeline**. This is the "characterized null" the plan named as a valid, and likely,
endpoint (§1.4, §1.5.1) — not a failure of execution.

---

## 1. What was tested (scope)

- **Universe:** CS equities, 2016-06-08 → 2020-12-31 (the exploration split). The
  2023-2026 holdout was **never touched**.
- **Signals:** 16 distinct entry signals — momentum (up / strong / top-decile),
  reversal (down / bottom-decile), gap (up / down / and-go / fade), breakout
  (at-30m-high), pullback, premarket-volume-spike, concentration (high / low),
  near-52w-high, consecutive-up.
- **Timeframes:** entry offsets 10:00 → 15:30 × horizons 60min, EOD, 1d, 2d, 5d, 21d.
- **Exit rules:** all 9 materialized target-before-stop pairs (the DP-adjacent
  dimension), fixed-% and ATR-based.
- **Eras:** each exploration year 2016–2020 (isolating the 2020 COVID/meme regime).
- **Cells:** classification cross `cap×liq×price` (~46 cells); regime as the day-gate.

The edge-hunting subset of the ~135-question backlog is covered by the above; the
remainder are characterization/structure questions, bounded by the ceilings below.

---

## 2. The deliverables

### 2.1 Power reality (§7.8 step −1 — `phase1-power-mde-findings.md`)
The exploration window (~4.5 yr, day-clustered) can only **detect** edges of
annualized Sharpe ≈ **1.4–2** after BY FDR; a true Sharpe-0.5 cell gives t≈1.06
(undetectable). 0% of cells in any family granularity are reachable at the 0.5
hurdle. **Regime-splitting craters power** (one VIX axis ≈ 2× the MDE), which is why
regime is the §2.1 day-gate, not a per-cell BY axis. *Everything below is read
against this floor: "null" means "no edge ≥ ~Sharpe 1.5," not "provably zero."*

### 2.2 Deliverable #1 — Structural blacklist (`phase1-blacklist-findings.md`)
**55 cells** are untradeable by cost/exitability alone (illiquid / thin / sub-$1 /
non-exitable, `frac_zero_range > 0.3` or round-trip cost > 30 bps). This is durable,
calibration-independent, and compounds across every future variant (§2.2). The
empirical layer found **0 cells with robustly negative *gross* expectancy** (after
the placebo-caught BY correction) — i.e. the negative space is **cost/liquidity
friction, not signal-driven momentum bleed**.

### 2.3 Deliverable #2 — Predictability ceiling (`phase1-ceiling-findings.md`)
MI of 16 pre-entry features vs the 1d beat-SPY outcome, permutation-corrected:
**near-zero everywhere.** Best cell 7.33 millibits = **0.7% of outcome uncertainty**
(and already blacklisted); 15/34 cells at the noise floor; tradeable liquid cells
≈ 0.27m. **No meta-labeling headroom** ⇒ Phase 4 ML is not warranted on this
data/horizon. Feature ranking (all negligible): concentration > realized-vol-rank >
momentum magnitude.

### 2.4 Deliverable #3 — Stability across timeframe & era (`phase1-stability-findings.md`)
Swept the deployable cohort across 12 offsets × 6 horizons × 5 years. **0/72
(offset×horizon) combos positive** (net or gross). **2020 (COVID/meme)** shows an
elevated gross reading (+5–8 bps/day) where 2018–19 are negative — the §2.5
pooling-masks-an-era pattern — **but 0/72 2020 combos are statistically significant
even gross**, it's net-negative, and non-persistent: the §1.3.20 meme-era inflation
artifact, not an edge. The structural blacklist is era-independent.

### 2.4b Systematic factor scan — consistent structure, but untradeable (`phase1-factor-findings.md`)
A signal-agnostic scan of every pre-entry feature × horizon (cross-sectional decile
spreads, era-stability, BY) **did find consistent structure**: low-volatility / smaller-
size / lower-liquidity / pullback factors predict forward excess return, **5/5 eras**.
BUT verification (block-bootstrap + clean universe) shows it's concentrated in
**small/micro-cap, high-vol, illiquid names** (36% of the extreme decile) — the
blacklisted corner — and **collapses to noise on the deployable universe** (atr 21d
−8.5b, CI incl. 0). It's the known low-vol/size anomaly in the friction corner, and a
long-short factor (short leg gated). The tradeable cross-section is efficient. Mirrors
the blacklist: structure concentrates where friction prevents capture.

### 2.5 Breadth sweep — signals & exit rules (`phase1-sweep-findings.md`)
- **Entry (16 signals × 3 horizons = 48):** 0 net-positive, 0 BY survivors. The one
  gross-positive blip (buy extreme intraday losers → EOD) dies after cost.
- **Exit rules (9 pairs × 16 signals):** 48/144 "beat breakeven," but proven to be
  **market drift (beta), not alpha** — the unconditional no-signal baseline gives the
  identical edge (+0.025 vs +0.026–0.039), and the no-drift intraday pair goes
  negative. Exits cannot manufacture excess edge from a no-excess-edge entry (§7.3).

---

## 3. Validation record (`phase1-validation-findings.md`)

The analysis layer is validated to the limit the data allows — three independent ways:
- **L2 recompute** (`phase1_verify.py`): tracer + cost model recomputed by an
  independent numpy path; exact match on 6 cells + the headline.
- **Placebo / false-positive control** (`phase1_placebo.py`): signal shuffled across
  12 lags → real survivors (0) ≈ placebo (0.4). FDR doesn't invent findings. **Caught
  a real BY-labeling bug** in the blacklist (loose threshold vs proper step-up), fixed.
- **Synthetic / true-positive power** (`phase1_synthetic.py`): injected known-Sharpe
  edges → 4% power @ Sharpe 0.5, 92% @ 2, 100% @ 3, 50%-crossing at the MDE floor
  ~1.4. **The pipeline would have detected a real edge. There wasn't one.**

The three coincide: analytic MDE ↔ false-positive control ↔ true-positive power.

---

## 4. Limits & honest caveats

1. **"Null" = undetectable, not provably zero.** A real edge below ~Sharpe 1.5 could
   exist and be invisible on this window. The power floor is a property of the data
   (N days), not the signals.
2. **Conditional on the recorded features + the chosen outcomes.** No news, order
   flow, options, or fundamentals. "Unpredictable given what we measure" ≠ "random."
3. **Cost model accuracy is the one un-closable gap** — the liquid-cell spread is an
   upper bound; tightening it needs NBBO quotes (no entitlement; calibration deferred).
   This is in the *safe* direction for a blacklist (pessimistic on cost).
4. **Exploration-set only.** The holdout (2023-01 → 2026-06) is untouched and intact.

---

## 5. Frozen decisions (Category-A; do not drift without a version bump)

- Cost model: retail (~0 commission); capped high-low proxy + 1¢ tick floor;
  **no NBBO calibration** (deferred). Capacity weighting retail-scale.
- Primary BY family: coarse `cap×liq×price`; **regime excluded** (it's the day-gate).
- Sharpe hurdle 0.5; ≤20 pre-registered hypotheses; BY α=0.10 discovery / 0.05 holdout.
- **Nothing was pre-registered** — no positive candidate cleared the bar, so the
  holdout was correctly never read. It remains available for a future cycle.

---

## 6. The heavy stack — gated, and DP additionally tested directly

- **DP / optimal-stopping exits:** first gated by the ceiling, then **tested directly**
  to close the gap (the ceiling measured pre-entry features; DP uses within-trade path
  state — a different conditioning set). A **cross-fit DP exit policy** on 3
  representative cells (`phase1-dp-findings.md`) adds **no significant out-of-sample
  value** over a state-blind fixed-time exit (all DP−fixed lower bounds ≤ 0; the one
  positive point estimate was an overfit mirage the cross-fit exposed). DP confirmed
  as a result, not a gate.
- **Hierarchical Bayes (Stage C):** gated out — nothing to pool (no positive cells).
- **Phase 4 meta-labeling:** ruled out by the near-zero ceiling (#2).

---

## 7. What is reusable (the durable assets)

- A validated 657-column Phase-0 data engine (8 tables, 2016–2026, all checks green).
- A validated Phase-1 analysis pipeline: cost model + daily-portfolio cost-adjusted
  excess + block-bootstrap + BY FDR + power/placebo/synthetic validators
  (`scripts/phase1_*.py`), reusable for any future signal or dataset.
- The 55-cell structural blacklist — transferable friction-avoidance.
- Proof that naive-to-moderate signal trading does not clear costs here — i.e. money
  not lost to an overfit backtest.

---

## 8. Cycle-1 close & the cycle-2 decision (§1.7)

**Cycle 1 is complete.** Three valid endpoints existed (positive cell / characterized
null / inconclusive); the outcome is a **characterized null + structural blacklist**.

A **cycle 2** is permitted but must be justified by specific written lessons, not
"look harder" (§1.7). The only genuinely-distinct untested work:
- **§1.3.21–23 exploratory tiers:** swing/multi-day continuation, short-side
  *patterns* (long-avoid filters; short execution still gated), setup-archetype
  clustering. **All face the same power ceiling** → low prior on a deployable edge.
- **New data** that would lift the ceiling (news, order flow, options, fundamentals),
  or a quotes month to calibrate the cost model — a spend decision, not analysis.

Absent one of those, the honest move is to **stop here with the documented null**,
and treat the validated engine + blacklist as the deliverable.

---

## 9. Index

**Findings docs:** `phase1-power-mde-findings.md` · `phase1-tracer-findings.md` ·
`phase1-cost-model-findings.md` · `phase1-blacklist-findings.md` ·
`phase1-validation-findings.md` · `phase1-ceiling-findings.md` ·
`phase1-stability-findings.md` · `phase1-sweep-findings.md`
**Scripts:** `scripts/phase1_{power_mde_pass,tracer_bullet,cost_model,verify,
blacklist,placebo,synthetic,ceiling,stability,sweep}.py`
**Plan:** `phase1-research-strategy.md` (frozen) · **Session handoff:**
`PHASE1_SESSION_2026-06-20.md`
**Analysis outputs:** `data/phase1_analysis/*.parquet` (T7)
