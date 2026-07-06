# Phase 7 program — the magnitude-conversion design space

**Status:** PROGRAM DESIGN, frozen before any Phase 7 code runs. Not findings.
**Date:** 2026-07-06 · Prior record: `FINAL_REPORT.md`, `MASTER_FINDINGS.md`.
**Question under design:** *Can the one thing this dataset provably predicts — movement
magnitude (stock-normalized AUC 0.65–0.74) — be systematically converted into a trading
edge without ever solving direction?* And, critically: *can that design space be exhausted
with confidence, so a global negative is an honest conclusion rather than a failure of
imagination?*

This document does five things:

1. Audits the proposal against Phases 0–6B — **most of the proposed idea list is already
   tested and closed**, and the closures must be inherited, not re-run.
2. States the **closure theorem**: why the largest region of the design space (stock-only
   position/exit policies) closes *as a class*, by measurement plus arithmetic, not
   idea-by-idea.
3. Enumerates the complete taxonomy of magnitude-conversion families, with the economic
   mechanism, status, and killing evidence for each.
4. Specifies the search framework and pre-registration protocol for the few genuinely
   open cells.
5. Defines the exhaustion criterion — the exact statement we will be entitled to make if
   the open cells fail.

---

## 1. Audit: the proposal vs. the existing record

The continuation prompt proposes exploring: dynamic stop-losses, asymmetric stop/target
logic, volatility-conditioned sizing, trailing/partial exits, conditional entry rules,
regime conditioning, hedged long/short structures, adaptive exposure. **Seven of those
nine are closed doors with exact numbers already in this repo:**

| Proposed idea | Where it was tested | Killing number |
|---|---|---|
| Dynamic stops / exit rules / trailing logic | Phase 2D simulate, Phase 3 lifecycle & failure curves | managed vs hold: wash at 0 cost, strictly negative at any real cost; every rule raised win% and truncated winners equally (`phase2-findings.md`, `phase3-findings.md` §B) |
| Asymmetric stop/take-profit | Phase 1 metalabel barrier economics | top-decile magnitude: target:stop hit ratio **1.05** (coin flip); favorable-asymmetric 2:1 **loses** E −8…−16bp (`MASTER_FINDINGS.md` Phase 1 frontier) |
| Exit on trade-health deterioration | Phase 2C | post-entry state rank-IC on *remaining* return +0.0103 — beats noise, order of magnitude below tradeable |
| Hold-the-winner conditioning | Phase 3 green-survival + Bayesian stack | P(win)→85% real, **excess-remaining within ±1.5bp of 0**, negative net |
| Conditional entry subsets | Phase 4 PRIM + score-tail | best valid OOS box −31.6bp vs permutation-null 95pct +12.7 — *inside the null* |
| Regime/state conditioning | Phase 6 | states = GARCH; 0/6 answerable event×state cells survive BY-FDR |
| "This will keep being a high-magnitude setup" conditioning | Phase 6B | persistence forecastable (AUC 0.741), top-quintile forward **−27.5bp gross** |
| Volatility-conditioned sizing / adaptive exposure | Phase 5 vol-targeting | 12/12 configs: Sharpe +0.13…+0.64, maxDD −50…−82%, **every ΔSharpe CI includes 0** → risk tool, not provable alpha |
| Long/short hedged structures | Phase 2.5/3 reversal + factor scan | the only cross-sectional spreads found (reversal, low-vol) are crisis-driven / cost-eaten / illiquid-corner |

The decisive measured fact, which every new proposal must confront: **the project already
conditioned on the magnitude signal itself and measured direction and expectancy inside
that conditioning.** Given top-decile predicted magnitude: P(up) ≈ 0.5 at every barrier
threshold ("direction contributes exactly 0.5"), high-RVOL P(win) = 50.5%, barrier
economics gross +4.1bp / net −15.9. The premise "suppose we know a stock is about to move
a lot" was granted, at full strength, repeatedly. Knowing a coin will be flipped harder
does not tell you which side lands up, and it was *measured* not to.

So Phase 7 must not re-run these. Its only legitimate content is: (a) the closure
argument that generalizes those point results to the whole policy class, and (b) the
small set of families that genuinely escape the argument.

---

## 2. The closure theorem (why the biggest region closes as a class)

**Premise (measured, not assumed).** Let F_t be the information available at time t from
price/volume (the 657-column feature set, post-entry path state, market state, and any
function of these — including every magnitude forecast we can build). Phases 1–6B
measured, across ~45 experiments:

> E[ r_excess(t→t+h) | F_t ] ≈ 0, uniformly over every conditioning tested,
> with MDE floor ~20–25bp/5d for the main designs; and strictly < 0 after retail costs
> (cost floor ~15–20bp round trip).

This was measured pre-entry (Phase 1), post-entry (Phase 2), across the lifecycle
(Phase 3), in searched subsets with full-pipeline permutation nulls (Phase 4), across
states (Phase 6), and conditioned on predicted persistence (Phase 6B). Excess price is,
to the resolution of this window, a **martingale with respect to the price/volume
filtration — and a supermartingale net of costs.**

**Theorem (arithmetic, not finance).** Any stock-only ("delta-one") strategy is a
position process w_t adapted to F_t — this includes every stop, trailing stop, partial
exit, profit target, re-entry rule, health gate, sizing rule, hedge ratio, and long/short
combination, however clever, however dynamic. Its expected excess P&L is

  E[ Σ_t w_t · r_excess(t→t+1) ] = Σ_t E[ w_t · E[ r_excess | F_t ] ] ≈ Σ_t E[w_t] · 0 = 0,

minus costs proportional to Σ|Δw_t|. Stops and targets are the special case of stopping
rules; the optional stopping theorem says no stopping rule changes the expectation of a
martingale. **Therefore: no delta-one policy built on price/volume information can have
positive expected excess return, and every active one is strictly dominated by its
passive benchmark after costs.** Magnitude knowledge enters only through w_t — it can
reshape the *distribution* of P&L (variance, skew, drawdown, win rate) arbitrarily; it
cannot move the mean off zero-minus-costs.

This is exactly the recurring empirical pattern of the whole project ("win rate up,
expectancy flat-to-worse", "banked gains, not a hold edge", "P(win) 85%, remaining ±1.5bp
of 0") — now recognized as a theorem instance rather than a coincidence to be re-tested
per idea. The project independently confirmed the theorem's prediction at least five
times (doors 3, 4, 5, 10, 18 in `FINAL_REPORT.md` §4). Further delta-one exit/sizing
backtests have the same status as testing whether a new stopping rule beats a fair coin:
**they are not experiments, they are noise generators**, and running them would only
manufacture multiplicity.

**What the theorem does NOT close — the exhaustive list of escape hatches.** The argument
has exactly four load-bearing assumptions; each one that can be relaxed defines a branch
of design space that remains legitimately open:

- **(H1) "Expected value" — the objective.** The theorem fixes arithmetic expectancy.
  Objectives that are *nonlinear in the P&L distribution* — geometric growth (log
  wealth), Sharpe, drawdown — CAN be improved by pure magnitude knowledge, because
  g ≈ μ − σ²/2: cutting σ when μ>0 raises compound growth without touching μ. This is
  the risk-management channel. It is real, and it is **not alpha** — the μ being
  harvested is the market's, not ours. Phase 5 already measured its flagship (vol-
  targeted SPY) and found exactly what the math predicts: better Sharpe/DD, unprovable Δμ.
- **(H2) "Delta-one" — the instrument.** Options have payoffs *convex in price*;
  magnitude is first-order in their price. A correct RV forecast that beats implied
  vol converts directly into expectancy through straddles/strangles. This is Branch B —
  the theorem's unique expectancy-shaped loophole, and it is exactly where the project
  already said the one open door is.
- **(H3) "Costs are exogenous" — execution.** The theorem takes the cost line as given.
  Magnitude prediction could in principle *reduce* costs (limit-order fill probability
  and adverse-selection risk both scale with expected magnitude). Cost reduction is real
  money but bounded by the cost line itself (~15–20bp) and requires quote/NBBO data we
  do not have. Out of scope at current entitlement; recorded, not testable.
- **(H4) The premise's resolution.** E[r|F_t] ≈ 0 is measured with MDE ~20–25bp/5d.
  Sub-floor effects may exist — but they are below retail cost by construction, so the
  conclusion "not convertible into *retail* alpha" is unconditional. (This is FINAL_REPORT
  §7's caveat, inherited.)

**These four hatches span the design space.** Any proposed magnitude strategy must name
which hatch it exits through; if it exits through none, it is a delta-one expectancy
claim and is closed by the theorem plus the measured premise. This naming requirement is
the anti-brute-force device the continuation prompt asked for: the design space is
searched at the level of *assumption relaxations* (4 of them), not at the level of
*strategies* (unbounded).

---

## 3. The complete taxonomy

Every economically plausible magnitude-conversion family, organized by which escape hatch
it uses. "CLOSED" = measured in this repo or implied by the theorem given the measured
premise; re-testing requires new *data*, not a new rule.

### Channel E — delta-one expectancy (hatch: none) — **CLOSED as a class**

| # | Family | Mechanism claimed | Status |
|---|---|---|---|
| E1 | Stops / targets / trailing / partial exits / re-entries | truncate the loss tail of high-magnitude names | CLOSED — theorem (optional stopping) + doors 3–5 measured |
| E2 | Health-gated / Bayesian dynamic management | act on post-entry evidence | CLOSED — Phase 2C/2D/3D measured (IC 0.01; wash) |
| E3 | Conditional entry (subsets, score tails, filters) | enter only where magnitude + context is favorable | CLOSED — Phase 4 PRIM/score-tail, inside permutation null |
| E4 | Regime/state/persistence conditioning | same rules, better states | CLOSED — Phases 6/6B measured (0/6 cells; persistence pays −27.5bp) |
| E5 | Long/short pairs, market-neutral structures around high-magnitude names | direction cancels, "capture the move" | CLOSED — a linear combination of delta-one positions is delta-one; both legs have E≈0; costs double. (Plus: every cross-sectional spread found was uncapturable — doors 6, 7.) |
| E6 | Buy-the-dip / add-on-drawdown within high-magnitude names | intra-position mean reversion | CLOSED — this is short-term reversal re-represented (door 7: crisis mirage, net −15.8bp/5d) |
| E7 | Volatility breakout / expansion entry (long the move once it starts) | magnitude begets magnitude | CLOSED — true (AUC 0.65–0.74) and directionless at every threshold; the 0.63 metalabel decomposition |

### Channel G — growth/risk efficiency on a positive-drift host (hatch H1) — **risk tools, one marginal test open**

The mechanism is always g ≈ μ − σ²/2 or its portfolio analogue (diversification return
= (σ̄²_components − σ²_portfolio)/2). Magnitude forecasts improve the σ estimate; μ is
the equity premium, not ours. These can never be "alpha" but are legitimate engineering.

| # | Family | Mechanism | Status |
|---|---|---|---|
| G1 | Vol-targeted index exposure | cut σ in high-vol states, μ mostly survives | TESTED — Phase 5: 12/12 Sharpe up, maxDD −50–82%, 0/12 provable as return edge. Standing verdict: use if drawdowns matter; not alpha. |
| G2 | Cross-sectional inverse-vol / risk-parity weighting of a stock portfolio | equalize risk contributions → higher portfolio Sharpe by diversification math | Known beta (low-vol tilt = door 6's factor, which lives in the illiquid corner). The only *new* question is **G2′ below**. |
| G2′ | **Marginal value of OUR magnitude forecast over trailing vol** for G1/G2 weights | if the ML forecast (RV IC 0.612) beats naive persistence (0.576) *enough*, weights improve measurably | **OPEN — the one cheap, honest delta-one-adjacent test left.** Written prior: ~null (lift over persistence +0.036 rank-IC; the oft-quoted +0.0076 is over the informed baseline — pre-reg amendment v2 correction). Pre-registered as Test 7.1. |
| G3 | Volatility-harvesting rebalancing (Shannon's-demon) among top-decile predicted-magnitude names | diversification return scales with σ²; predicted-high-σ basket maximizes it | OPEN in principle, but the mechanism pays *geometric* not arithmetic return, is already captured by any fixed-weight rebalanced portfolio, and the harvest (≈σ²/2 per period on the traded fraction) must beat round-trip costs on the *highest-spread* names. Folded into Test 7.1 as a secondary arm rather than its own program. |
| G4 | Kelly / dynamic leverage on predicted magnitude | same math as G1 with leverage | CLOSED as duplicate of G1 (no-leverage constraint binds at retail anyway). |

### Channel C — convexity (hatch H2) — **the only expectancy-shaped door, already identified**

| # | Family | Mechanism | Status |
|---|---|---|---|
| C1 | Long straddle/strangle where forecast RV > IV | buy underpriced realized variance | **OPEN = Branch B.** Requires ~$50–100 of IV history. Written prior: poor (+0.0076 forecast lift over the informed baseline, +0.036 over raw persistence — and IV embeds more than persistence). Power gate first. |
| C2 | Short premium where IV > forecast RV | sell overpriced variance | Same test, other tail — but pre-labeled GATED (margin, assignment, tail risk = wall-class) exactly like short-side equity findings. Measured for the record if Branch B data is bought; not deployable at retail regardless. |
| C3 | "Synthetic gamma" — delta-one rebalancing to replicate option convexity | gamma-scalp without options | **CLOSED by identity**: rebalancing P&L nets to (realized variance × traded notional) *minus the same trading costs*, with no option premium received/paid on the other side. Without an options counterparty it is Channel G3 in disguise; with one it is C1/C2. It is a re-representation, exactly the class the prompt asked to eliminate. |

### Channel X — execution/cost (hatch H3) — **out of scope, recorded**

| # | Family | Mechanism | Status |
|---|---|---|---|
| X1 | Limit-vs-market order policy conditioned on predicted magnitude | fill probability and adverse selection scale with vol; spread capture on quiet names, aggression on movers | UNANSWERABLE at current entitlement (no NBBO/quotes). Bounded upside = the cost line itself. Reopen only if quote data ever becomes free. |
| X2 | Don't-trade filter in predicted-explosion states | avoid the worst spread/impact days | Risk/cost tool; trivially adoptable inside any live process; not alpha; no experiment needed. |

**Taxonomy completeness argument.** A strategy is (instrument, information, objective,
cost model). Information is fixed (price/volume — that's the project). Instruments
available to the account: stock (delta-one) or options (convex) → Channels E/G vs C.
Objectives: arithmetic expectancy vs distribution-shaped → E vs G. Costs: exogenous vs
endogenous → X. The four channels are therefore a partition, not a list of examples —
a new proposal lands in exactly one cell, and 12 of the 14 rows above are closed or
out of scope. **The design space reduces to two pre-registerable tests.**

---

## 4. The Phase 7 protocol (framework, not optimizer)

The prompt asks for systematic search without brute force. The answer is that the search
happens at the taxonomy level (§3), which is finite and now enumerated; *experiments* are
run only for open cells, under the standing protocol (`phase1-research-strategy.md`,
unchanged):

1. **Cell claim.** Every proposal must first name its (channel, hatch) cell. Closed cell
   → cite the closure, done, zero compute. This step replaces idea-by-idea backtesting
   and is what makes exhaustion possible.
2. **Power gate before code.** MDE95 for the registered estimand vs plausible effect
   size; UNANSWERABLE is a verdict, not an invitation.
3. **Pre-registration frozen in-repo before code** (this document + a per-test freeze
   like `phase6-preregistration.md`): hypothesis, estimand, PASS/FAIL, null expectation.
4. **Costs first, BY-FDR within family, placebo/permutation, era stability, day-clustered
   or block-bootstrap CIs** — the existing stack, reused as-is.
5. **Splits unchanged**: train 2016-06→2020-12; validation 2021–22 touched only on a
   registered PASS; 2023+ holdout sealed.
6. **Objective honesty rule (new, specific to Channel G):** a Channel G test may PASS as
   a *risk tool* (distribution improved, μ not provably changed) without being booked as
   alpha. The two verdicts are pre-separated so a Sharpe improvement can never be
   quietly promoted into an expectancy claim — the exact confusion Phase 5 was designed
   to prevent.

**Models for searching the space:** none needed beyond what exists. The magnitude
forecast itself is already built (RV IC 0.612 full-ML vs 0.576 naive). Channel G tests
consume it as an input; Channel C tests compare it to IV. Any "strategy-search" ML over
rules (RL on exits, genetic rule search, etc.) is a delta-one policy search inside
Channel E — closed as a class, and doubly closed by the Phase 4 lesson that search
procedures must be judged against full-pipeline permutation nulls, which is precisely
where PRIM died.

---

## 5. Phase 7 test queue (complete, ordered, terminal)

### Test 7.1 — marginal forecast value for risk overlays (free, one overnight)
**Cell:** Channel G (hatch H1). **Registered question:** does the full-ML magnitude
forecast improve a vol-managed portfolio *over the same portfolio built on naive trailing
vol*? Arms: (a) vol-targeted SPY per Phase 5, weights from ML forecast vs RV21 baseline;
(b) inverse-vol-weighted liquid-universe portfolio, ML vs trailing weights, monthly
rebalance, costs at the frozen model; (c) secondary: fixed-weight rebalanced basket of
top-decile predicted-magnitude names vs buy-and-hold same basket (the G3 harvest), net.
**Estimands:** ΔSharpe, Δlog-growth, ΔmaxDD (ML − naive), block-bootstrap CIs.
**PASS (risk-tool):** ML beats naive with CI excluding 0 on ΔSharpe or Δgrowth.
**PASS (alpha): not available to this design by construction — pre-declared.**
**Registered null expectation:** ML ≈ naive (the forecast lift over trailing-vol
persistence is +0.036 rank-IC — pre-reg amendment v2 corrected the v1 "+0.0076" figure,
which is the lift over the informed baseline; persistence is already in the naive
weights). Either way the "our forecast is special" question ends with a number.

### Test 7.2 — Branch B: realized vs implied (paid, ~$50–100, one month of IV history)
**Cell:** Channel C (hatch H2). Exactly as specified in `FINAL_REPORT.md` §8 and
`MASTER_FINDINGS.md` §6: power gate first; then one pre-registered test of whether
(our forecast − IV) predicts (realized − IV) net of option spreads, delta-hedged
straddle economics. **Registered prior: null.** This is the only test in the queue that
can produce expectancy. If funding is declined, Branch B is recorded as "untested by
choice, prior poor" — the conclusion in §6 weakens from "measured" to "measured except
the options channel," and honesty requires saying so.

### Test 7.3 — none. The queue is intentionally terminal.
Channel E needs no test (theorem + five measured confirmations). Channel X is
entitlement-gated. Anything else proposed must name a cell (§4 step 1), and there are no
unclaimed cells.

> **Amended 2026-07-06 (late):** one addition adopted after review of an external
> roadmap — the **A2 property sprint** (liquidity-concentration persistence, tail-shape
> persistence; property tests only, vol/liquidity-orthogonalized, monetization map
> pre-committed to Branch B or nothing). It is science, not a candidate edge, and does
> not alter the terminal character of the queue. Full sequencing now lives in
> `phase7-implementation-plan.md`, which governs execution.

---

## 6. The exhaustion statement (what we may claim, and when)

The continuation prompt's ask #7 — "can I honestly conclude the space is exhausted?" —
gets a conditional answer, available now:

> **If Test 7.1 lands on its registered null and Test 7.2 (Branch B) lands on its
> registered null**, then: historical price/volume magnitude prediction cannot be
> converted into stock-trading alpha at retail execution, in any of the four channels
> that exhaust the design space — (E) closed by the martingale/optional-stopping argument
> on the measured E[r|F_t]≈0, at five-times-confirmed empirical strength; (G) closed as
> alpha by construction and as marginal engineering by 7.1; (C) closed by 7.2's
> measurement against the option market's own forecast; (X) bounded by the cost line and
> untestable at current entitlement. Confidence is bounded by the stated MDE floor
> (~20–25bp/5d), below which effects are untradeable at retail cost anyway.
> Magnitude prediction remains what Phase 5/6 established: a genuine, validated **risk
> and sizing tool** — wide stops, vol-aware sizing, drawdown control — with permanent
> defensive value and zero offensive value.

If either test PASSes, the standing confirmation protocol
(`phase1-research-strategy.md` §7.7) applies unchanged: validation split, then decide.

---

## 7. Program C disposition (added 2026-07-06, late — closure, not dismissal)

A third framing was considered and evaluated: stop modeling price and model the market's
own decision process — mechanisms, information propagation networks, disagreement,
information-seeking trades ("Program C", vs A = predict the market, B = optimal policy
given predictions). Recorded here so the question is answered once, by argument, and
does not re-enter as drift.

**The valid core.** Program C's central question — *what economic activity becomes
profitable when direction is unknowable but magnitude is knowable?* — has a classical
answer: **selling insurance against movement**. The market maker and the option writer
are paid *because* direction is unknowable, and both price their service off expected
magnitude. The natural consumer of a magnitude forecast is a liquidity provider, not a
direction guesser. The map, restated through that lens:

| who needs magnitude | route | status |
|---|---|---|
| options market (IV, straddles, vol selling) | Channel C — **Branch B** | the one retail-accessible seat; Step 3, upgraded meaning |
| liquidity provision (spreads, reversal, limit orders) | doors 7 / X1 | measured (reversal = slow-motion market making, real gross, cost-wall net) or quote-data-gated |
| risk tools (sizing, drawdown control) | Channel G | Phase 5 validated; margin = Test 7.1 |
| tax engineering (harvest volatility) | out of scope | real, direction-free, retail-accessible — engineering, not market alpha |

This re-derives the existing map from economic first principles — Grossman-Stiglitz as
observed in `FINAL_REPORT.md` §5: the insurance premium survives only behind walls that
keep capital out. It also upgrades Branch B: not "try options," but the single
retail-accessible test of whether our forecast can price movement risk better than the
market that sells it.

**The rest of Program C, sorted by the standing principle** (the theorem binds an
information set × instrument, never a narrative): mechanism modeling / "model the middle
step" on price/volume data is the same filtration wearing an economic story — Phase 6 is
the executed cautionary case (dynamics framing, honest run, GARCH rediscovered); to model
the middle step requires the middle step's data (orders, signed trades, book states) =
the reopening condition of `FINAL_REPORT.md` §8. Network/propagation trades require
trading the lagging name directionally (direction re-entering by the side door; sector/
breadth/beta features already contributed ~nothing, and intraday lead-lag is HFT's core
product); the propagation that is real here is vol spillover — magnitude again.
Disagreement detection: the price/volume proxies (volume-without-move, dispersion,
correlation breakdown) were in the 657 tested columns and predict magnitude, not
direction; the genuinely new disagreement axis is options-vs-stock, i.e. **Branch B is
the first disagreement detector**. Information-gain trading is dominated by free
backtests on ten years of history, with one exception adopted below.

**Two kernels adopted:** (1) a **live cost-calibration book** — tiny trades sized to
measure fills, not to profit — is the only measurement history cannot provide; it
tightens the frozen cost model's deliberate upper bound (liquid cells ~10–17bp assumed
vs ~1–3bp true) and mechanically sharpens every existing net verdict
(implementation plan Step 3.5). (2) **Tax-loss harvesting** noted for the record as the
one guaranteed direction-free positive expectancy at retail — volatility-loving (the
forecast says where harvests will appear) — but alpha against one's tax bill, not the
market: engineering, permanently out of research scope.

**Standing disposition:** Program C is not a third research program on current data —
on price/volume it collapses into Programs A/B under the information-set argument. Its
durable role is the layer above the reopening condition: mechanism questions are how
the *next dataset* gets chosen (order flow vs options flow vs text) by economic
reasoning rather than availability, if the project ever reopens.

---

What this program deliberately is **not**: a strategy-search loop. The single deepest
lesson of Phases 0–6B is that this dataset punishes searches and rewards arguments —
every search (config sweep, PRIM, score-tail) found only its own multiplicity, and every
closed-form question got a stable answer. Phase 7 keeps the project on the right side of
that line: two experiments, both pre-registered, both with written null priors, and a
closure theorem doing the work that ten thousand backtests could only counterfeit.
