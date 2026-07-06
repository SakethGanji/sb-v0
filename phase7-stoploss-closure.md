# Stop-loss design space — coverage audit and closure argument

**Date:** 2026-07-06 · Companion to `phase7-magnitude-program.md` §2/§3 (Channel E).
**The question:** "Optimal stop loss — what have we actually tested? There are many ways
to size and evaluate a stop. How do we know we have concluded?"

The answer has three layers: (1) the point tests actually run, (2) the class-level
measurement that covers every stop rule *not* explicitly run, (3) the decomposition that
shows what "optimal stop" questions remain meaningful — and where they went.

---

## 1. What was explicitly tested (the point evidence)

| Stop family | Where tested | Result |
|---|---|---|
| Breakeven stop (the Phase 0 spec's own rule) | Phase 0 engine, frozen spec | the base experiment; every later analysis inherits its paths |
| Fixed-% symmetric target/stop pairs | Phase 1 momentum sweep (× **9 exit pairs**), metalabel economics | all null; top magnitude decile hits target:stop 1.05 (coin flip) |
| Fixed-% asymmetric (2:1 favorable target:stop) | `phase1_metalabel_economic.py` | **loses**, E −8…−16bp — asymmetry harvests the vol, pays the spread |
| ATR-scaled (vol-adjusted) barriers, 8 widths | `phase1_barrier_sweep.py` | which barrier gets hit is predictable (AUC 0.65–0.74); *which side* is 0.500 at every width |
| Time stops (hold-horizon exits) | Phase 2 config search, 408 configs, honest train→OOS | best train→OOS +0.8bp; naive best was an overfit mirage |
| Threshold stops at checkpoints (−0.50%, −0.75%, below-VWAP at 30m/60m) | `phase3_lifecycle.py` failure curves | E[remaining \| triggered] = −3.6 to **+4.2bp** — the 60m stop fires exactly when the bounce is coming; net@10 always negative |
| Model-based dynamic stop (exit worst-K% by full ML health score) | `phase2d_simulate.py` | wash at 0 cost, strictly negative at any real cost; win% up 48.3→50.9, winners truncated 122→83bp |
| Trailing / giveback logic | covered by class argument (§2): drawdown-from-peak was one of the eight tested state features | its information about remaining return is inside IC 0.0103 |
| Profit-taking stops (sell the proven winner) | `phase2_green_survival.py`, `phase3_bayesian_update.py` | remaining from any green/stacked state ≈ 0 (±1.5bp), so taking profit ≈ not taking it, minus a spread |
| Stop-width frontier (MAE) | Phase 3 | winners' median adverse excursion −0.34%, losers −1.25% → any stop tighter than ~1.25% mostly harvests winners |

## 2. Why the class is closed, not just the tested points

A stop loss — any stop loss: fixed, ATR, trailing, giveback, time, model-gated, partial,
re-entering — is a rule that maps the **post-entry path state** to the decision
"flatten now." Its entire effect on expected P&L is:

> ΔE = − E[ remaining return | the state that triggers it ] − exit costs.

So the universe of stop rules is closed the moment you know E[remaining | state] for
*every reachable state* — you do not need to enumerate rules, you need to measure that
one conditional surface. **That is exactly what Phase 2 did.** The state vector
(return-so-far, drawdown-from-peak, VWAP distance, %bars profitable, within-trade vol,
rate-of-change, ret/ATR, volume-since-entry — the sufficient statistics every stop rule
in the literature is built from) was fed, whole, to a walk-forward ML model predicting
remaining return, on 994,687 trades. The ceiling it found: **rank-IC 0.0103** — and the
descriptive surface: remaining ≈ −1bp / P ≈ 50% in **every** state cut (`phase2-findings.md`
2B/2C). The lifecycle checks (§1 rows 6, 9) then verified the surface at the exact
corners where stop rules trigger, including the one *positive* corner (down-states
bounce) that makes stops locally worse than nothing.

Layered on that measurement is the arithmetic (`phase7-magnitude-program.md` §2): with
E[remaining | F_t] ≈ 0, the optional stopping theorem says **no stopping rule can move
the mean** — it can only reshape the distribution and pay costs. The empirical point
tests and the theorem agree everywhere they meet, five independent times. A stop rule
that "works" in this dataset would require the conditional surface to be wrong somewhere
the ML model, the descriptive cuts, AND the corner tests all missed — while all three
found each other consistent.

**This is the answer to "how do we know we've concluded":** the search was run at the
level of the conditional-expectation surface (one measurable object, measured), not at
the level of rules (unbounded, unmeasurable). Any new stop proposal must claim the
surface is ≠ 0 at its trigger states — a claim the existing measurement already prices
at ≤ IC 0.01 with an MDE floor of ~20–25bp/5d, below the ~15–20bp cost of acting.

## 3. "Many ways to size and evaluate" — the decomposition that exhausts them

A stop question is always (trigger policy) × (evaluation objective). §1–2 close the
trigger axis. The objective axis has exactly two values:

- **Expectancy.** Closed (above). There is no optimal stop for expectancy; the optimum
  is *no stop* (every tested and theorizable rule ≤ hold, net of costs).
- **Distribution** (variance, drawdown, ruin, growth rate). Here stops are genuinely
  useful and never claimed otherwise — but note what the question becomes: a stop at
  k×ATR with "risk R per trade" IS position sizing (w = R / (k·ATR)) — inverse-vol
  sizing wearing an order ticket. The "optimal stop width" for risk purposes is
  therefore a **Channel G sizing question**, and its empirical content is:
  1. width: wider than the ~1.25% MAE frontier (Phase 3) or the stop mostly harvests
     winners and donates spread;
  2. sizing input: predicted magnitude — where the marginal question ("does OUR
     forecast beat trailing vol as the sizing input?") is precisely **Test 7.1 Arm B**,
     pre-registered in `phase7-preregistration.md`.

So the stop-loss thread does not dangle: its expectancy half is closed by measurement +
theorem; its risk half reduces to sizing and is the live, registered test.

## 4. Honest coverage boundaries (what this closure does NOT cover)

- **Domain measured:** 10:00 entries, intraday→21d horizons, liquid US equities
  2016–2020, bar-level idealized fills (`README.md` §6.4), MDE ~20–25bp/5d. Effects
  below that floor could exist — and would be untradeable at retail cost anyway.
- **Catastrophe insurance is not adjudicated by expectancy.** A wide disaster stop
  (or position sizing that makes one unnecessary) protects against the ruin tail
  (halts, gaps, fat fingers). Its expected cost is the small negative measured here;
  its value is distributional. Durable rule 2 stands: wide or not at all — and sized.
- **Other execution tiers / instruments / eras:** inherited scope limits of
  `FINAL_REPORT.md` §7.

**Bottom line.** "What is the optimal stop loss?" has a measured answer in this dataset:
*for making money, none — the question is ill-posed on a martingale, and we verified the
martingale at every state a stop can read. For controlling risk, wide (>~1.25%) and
volatility-sized — and whether our forecast improves that sizing is Test 7.1, currently
queued to run.*
