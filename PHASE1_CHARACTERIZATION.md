# Phase 1 — Characterization (Cycle 1 Close)

**Date:** 2026-06-20 · **Amended 2026-07-05** (multivariate / meta-label layer added —
see §0 third bullet, §2.3, §6). **Status: CYCLE 1 CLOSED.** This is the standing record
of what Phase 1 found. It consolidates the step-level findings docs (linked at the end)
into one authoritative artifact, per `phase1-research-strategy.md` §1.7.

---

## 0. Verdict (plain words)

**No deployable strategy in the tradeable universe — but the data is not empty:
it holds real, era-stable cross-sectional factor structure that sits exactly where
friction prevents its capture.** Both halves are rigorously earned, not assumed.

- **Null where you can trade.** Simple-to-moderate rules ("buy morning strength,"
  "buy the dip," gap/breakout/concentration, with/without stops/targets, 16 signals)
  and path-dependent DP exits do **not** produce a positive cost-adjusted, market-
  excess return on liquid/deployable 2016–2020 US equities. The cross-section of
  deployable names is **efficient** (the factor scan finds no era-stable structure
  there).
- **Real-but-uncapturable structure where you can't.** A systematic cross-sectional
  scan *did* find consistent, 5/5-era-stable structure — the classic **low-volatility
  / size / liquidity factor family** — but it is **concentrated in small/micro-cap,
  high-vol, illiquid names** (the blacklisted friction corner) and is a **long-short
  factor** (capturing it needs shorting the high-vol leg, which is gated). It is
  documented anomaly behavior, not novel alpha, and it is **behind glass**: friction
  and the long-only constraint prevent harvesting it.
- **The learnable structure is volatility, not direction** (added 2026-07-05,
  `phase1-metalabel-findings.md`). The multivariate / conditional / meta-label layer —
  the thing the univariate scans could not see — was tested directly. A joint model
  (95 features incl. market-state) carries only a **faint, untradeable** directional
  trace at 1d (rank-IC 0.007, beats a noise null but not cost/CI; gone by 5d). A direct
  **meta-labeling** classifier is more revealing: the *directional* labels ("beat SPY")
  are null (AUC ≈ 0.50), but the *trade-quality* label "+2% before −2%" is **strongly,
  stably, model-agnostically predictable** (OOS AUC 0.63, GBM ≈ linear). It is a
  **volatility / barrier-resolution** signal (top-decile target:stop ≈ 1.05 — a coin
  flip on *which* barrier; importances dominated by realized-vol / dispersion / market
  activity) and it **does not monetize** net of cost, flipping sign by year. These
  features are rich in *variance* information, poor in *directional* information —
  exactly why meta-labeling reorganizes the variance beautifully yet yields no edge.

Every result points the same way and is mutually consistent. What Phase 1 produced:
a durable **structural blacklist**, a **near-zero predictability ceiling** in liquid
names, a **map of where the real structure lives (and why it's uncapturable)**, and a
**fully validated engine + analysis pipeline**. This is the "characterized null +
characterization" the plan named as a valid, and likely, endpoint (§1.4, §1.5.1) —
the project working as designed, not a failure of execution.

**CYCLE 1 CLOSED — 2026-06-20.** A cycle 2 (§1.7) is permitted only with a written,
specific justification — realistically that means new data (news/order-flow/options/
fundamentals/borrow) or a long-short + small-cap-access mandate to attempt the factor,
not more analysis of this data. Absent that, this characterization is the deliverable.

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
≈ 0.27m. Feature ranking (all negligible): concentration > realized-vol-rank >
momentum magnitude. **CAVEAT (2026-07-05): this ceiling is _univariate_** — it caps
the MI of any *single* feature, not *joint* models. The earlier gloss "no meta-labeling
headroom ⇒ Phase 4 ML not warranted" was **overstated** and is superseded by the direct
multivariate / meta-label test (§0 third bullet, §6, `phase1-metalabel-findings.md`):
joint models and a meta-label classifier *do* find learnable structure — but it is
**volatility, not directional edge**, and does not monetize. The practical conclusion
(no tradeable directional edge on this data) stands; the *reason* is now measured
directly rather than inferred from a univariate ceiling.

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
- **Phase 4 meta-labeling:** **directly tested** (2026-07-05, `phase1-metalabel-findings.md`),
  not just gated by the ceiling. A joint multivariate probe (95 features incl. market-state)
  and a meta-label classifier (accept/reject on "+2% before −2%") were run walk-forward OOS
  with permutation nulls. Result: real, model-agnostic **volatility / barrier-resolution**
  structure (OOS AUC 0.63, GBM ≈ linear) but **no directional edge** and **no monetization**
  net of cost (top-decile barrier P&L ≈ 0 gross, negative net, flips by year). Return-ranking
  was null (rank-IC ~0.007). Meta-labeling confirmed as a *result*, not merely a gate.

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
`phase1-stability-findings.md` · `phase1-sweep-findings.md` ·
`phase1-dp-findings.md` · `phase1-factor-findings.md` · `phase1-metalabel-findings.md`
**Scripts:** `scripts/phase1_{power_mde_pass,tracer_bullet,cost_model,verify,
blacklist,placebo,synthetic,ceiling,stability,sweep,dp,factor_scan,factor_verify,
joint_probe,metalabel,metalabel_economic,metalabel_control,momentum_reliability}.py`
**Plan:** `phase1-research-strategy.md` (frozen) · **Session handoff:**
`PHASE1_SESSION_2026-06-20.md`
**Analysis outputs:** `data/phase1_analysis/*.parquet` (T7)
