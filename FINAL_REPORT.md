# FINAL REPORT — Price/Volume Alpha Research (Phases 0–6B)

**Project window:** 2026-06 → 2026-07-06 · **Data studied:** US equities 2016-06 → 2020-12
(train), 2021–22 validation reserved, 2023+ holdout sealed and never read.
**Total external data cost: $0.** Exact numbers for every claim: `MASTER_FINDINGS.md`;
per-phase detail in `phase*-findings.md`; every test reproducible from `scripts/`.

---

## 1. The verdict

**There is no tradeable edge in liquid US equities available from price/volume data at
retail execution costs — directional, conditional, event-driven, or state-dependent — in
2016–2020.** This conclusion was not assumed; it was earned across ~45 experiments with
validated power to detect the effects it rules out, and it survived every reframing the
researchers could construct, including: joint machine learning, meta-labeling, subgroup
discovery with permutation nulls, true earnings surprises on a corrected announcement
clock, insider-filing events, market-state machines, and signal-persistence forecasting.

What the data does contain — robustly, stably, and measured to the decimal — is
**magnitude**: how much a stock will move, without direction. That signal is real
(stock-normalized AUC 0.65–0.74), is ~volatility persistence, and is already the first
input of every option market maker's pricing model. It is a risk-management tool, not
alpha.

**The standing conclusion: index the capital.** Optionally overlay vol-targeting if
drawdowns matter (Phase 5: all 12 configurations cut max drawdown 50–82%; none provable
as a return edge). Apply the risk rules in §6 to any trading ever done anyway.

---

## 2. The rules the project played by

Every result below was produced under, and owes its credibility to:

- **Frozen splits**: discover on 2016-06→2020-12; validate on 2021–22 only when a
  pre-registered test passed (none did); 2023+ holdout untouched, still clean today.
- **Pre-registration**: hypotheses, kill criteria, and null expectations written before
  code ran (Phases 4–6B literally have the frozen documents in-repo).
- **Power gates before tests**: three questions were halted *before* testing because the
  window could not answer them — reported as "unanswerable," never as null.
- **Multiplicity control**: BY-FDR across every searched family; full-pipeline permutation
  nulls for search procedures (the entire PRIM pipeline re-run 50 times on shuffled labels).
- **Costs from day one**: retail cost model frozen in Phase 1; every "edge" quoted net.
- **Leakage audit**: all ML scripts audited; the one real leak found (a PEAD control) was
  fixed; since leakage inflates signal, every null is conservative.
- **Placebos and synthetic power**: the stack detects planted signals at 92% power
  (Sharpe 2) and collapses on shuffled labels — the instrument was proven before trusted.

## 3. What was built (all reusable, all $0)

- A validated **657-column Rust data engine**: 8 parquet tables, minute-level inputs,
  2016–2026, deterministic and checksummed.
- The **analysis stack**: cost model, day-clustered/block-bootstrap inference, BY-FDR,
  power/MDE machinery, placebo/synthetic validators, walk-forward ML, PRIM subgroup
  search, lifecycle/path analysis.
- A free **SEC EDGAR pipeline** (Phase 4–5): 431k point-in-time EPS facts → split-immune
  SUE; 97k 8-K earnings-announcement dates (which exposed and fixed the dataset's
  filing-date clock); 96.8k open-market insider purchases.
- Frozen **market-state definitions** (Phase 6) and the persistence-forecast machinery
  (Phase 6B).

## 4. Every door, and how it closed

| # | Door | Verdict | Killing number |
|---|---|---|---|
| 1 | Pre-entry direction, univariate (657 cols) | null | best MI 0.27 millibits |
| 2 | Joint ML, all features + market state | null | OOS IC +0.007 (~3bp, cost floor ~15-20bp) |
| 3 | Meta-labeling / barrier labels | magnitude, not direction | direction AUC 0.5101 vs null 0.5106; target:stop 1.05 |
| 4 | Trade management / exits / lifecycle | null | every rule: win-rate up, expectancy flat-to-worse |
| 5 | Bayesian evidence stacking post-entry | banked gains | P(win)→85%, remaining ±1.5bp of 0 |
| 6 | Low-vol/size factor | real, uncapturable | lives in illiquid micro corner |
| 7 | Short-term reversal | crisis mirage | 2017/18 negative; net −15.8bp/5d |
| 8 | PEAD, price-proxy | real, uncapturable | long leg +5.8bp gross |
| 9 | PEAD, **true SUE**, both clocks ($0, Phase 4) | priced within ~1 day | event-window swing −43→+72bp; earliest safe entry −9.5bp net |
| 10 | Conditional subsets (PRIM, 25-perm pipeline null) | inside the null | best valid OOS −31.6bp vs null 95pct +12.7; zero ≤4-condition box ever valid |
| 11 | GBM score tail (top 0.1%) | skew, not signal | win 50.0%, CI[−107,+189] |
| 12 | Vol-targeting overlay (Phase 5) | risk tool, unprovable alpha | ΔSharpe +0.13..+0.64, all CIs incl 0 |
| 13 | Long-horizon (6-12mo) momentum | **unanswerable** | MDE 56–168bp vs plausible 10–90bp |
| 14 | Insider cluster buying (long) | null | −81.4bp/21d — wrong direction |
| 15 | Insider cluster buying (short side) | suggestive, gated | −85.4bp vs decline-matched CI[−141,−31]; post-hoc, wall #2 |
| 16 | Market-state machine (Phase 6) | GARCH rediscovered | states = vol regimes; OOS transition drift 0.033 |
| 17 | Event × state interactions | null / unanswerable | 42/48 cells underpowered; 0/6 survive BY |
| 18 | Signal-persistence forecast (Phase 6B) | forecastable, worthless | persistence AUC **0.741**; top-quintile forward −27.5bp |
| 19 | Options/IV (Branch B) | **untested** — the one open door | requires ~$50–100 paid data; written prior: poor (+0.008 forecast lift over persistence) |

Doors 13's and 17's "unanswerable" verdicts are as final as the nulls: the 2016–2020
window cannot resolve them at any effort level; testing anyway would only manufacture noise.

## 5. The one recurring discovery

Every high number this data ever produced was one of two things:

1. **Volatility wearing a costume** — the 0.63 metalabel AUC, the 0.85 barrier AUC, the
   score-tail's +33bp, the PRIM boxes, the state machine, the "70% mover prediction."
   Decomposed, each contained direction = 0.50 exactly.
2. **The past wearing a costume** — the 85% Bayesian P(win), the 90.6% "stays green from
   +1%," the AUC-0.74 persistence forecast. Each described gains already banked or
   appearances already formed; the *forward* expectancy from the moment of knowing was ~0.

Direction was only ever moved by *information* (earnings, insiders) — and each information
source was either priced within a day of becoming public or blocked by a structural wall
(shorting, liquidity, crisis concentration). This is Grossman-Stiglitz economics observed
in the wild: public-data edges survive only behind walls that keep capital out, and a
retail account is on the wrong side of every wall found.

## 6. Durable rules (the part with permanent value)

1. **Magnitude yes, direction no.** Position-size by expected range (median 1d range 2.8%;
   ±1% crossed ~90% of days); never act on a directional hunch from price action — its
   measured win rate is 50.0%.
2. **Stops wide or not at all.** Winners' median adverse excursion is ~0.35%; tight stops
   harvest losers and donate spread. No exit rule tested added expectancy.
3. **Optimize expectancy, never win rate.** Every win-rate improvement found was paid for
   in truncated winners.
4. **"Green" is banked, not predictive.** From +1%, P(finish green)=90.6% and
   E[remaining]≈−0 net. High conditional win probabilities describe the past.
5. **A setup continuing to "look good" (predictable, AUC 0.74) is not the stock going up
   (coin flip).** The market lets you forecast appearances and charges you for returns.
6. **Any future test: power gate → pre-register → costs-first → FDR → placebo.** The
   machinery is in this repo and it caught every mirage that fooled the naked eye.

## 7. Scope — what this report does NOT claim

- Nothing about other asset classes, other execution tiers (HFT/institutional), other
  eras, or non-price/volume information not tested here (news text, analyst revisions,
  options flow, positioning data).
- Effects smaller than the measured MDEs (~20–25bp/5d for the main designs) could exist
  below the detection floor — but at retail costs they would also be untradeable, which is
  why the search was allowed to end.
- The 2021–22 validation split and 2023+ holdout remain clean. If any pre-registered test
  ever passes (e.g., Branch B), the confirmation protocol in
  `phase1-research-strategy.md` §7.7 still applies unchanged.

## 8. The one open door, and reopening conditions

**Branch B** (realized-vs-implied volatility): one month of ThetaData/ORATS history
(~$50–100), power gate first, then a single pre-registered test of whether
`(our forecast − IV)` predicts `(realized − IV)` net of option spreads. Expected outcome:
null (our forecast's lift over naive persistence is +0.008 rank-IC). Value either way:
the last "monetize magnitude" argument ends with a measurement.

Beyond that, this project reopens only if a **genuinely new information source** becomes
cheaply available — not a new representation of price/volume (Phases 1–6B closed those as
a class), but new bits: options flow, machine-readable news, revisions, borrow data. The
EDGAR pipeline pattern (bulk download → point-in-time construction → power gate →
pre-registered test) is the template; the cost of an honest answer is now about one evening.

## 9. Closing

The project set out to find an edge and instead produced something rarer: a complete,
honest, reproducible map of where edges are not — plus working machinery, four (now six)
documented real-but-uncapturable structures, a corrected dataset clock, and a set of risk
rules that are true whether or not anyone trades. No capital was deployed against an
overfit backtest at any point. The holdout was never opened, because nothing earned it.

That is a finished research program, closed on purpose.

*— compiled 2026-07-06, Phases 0–6B, all results committed at `master`.*
