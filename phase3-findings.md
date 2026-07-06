# Phase 3 — Trade Lifecycle Intelligence + Reversal Verification Findings

**Date:** 2026-07-06 · **Scripts:** `phase2_green_survival.py`, `phase3_lifecycle.py`,
`phase2_reversal_verify.py` (+ `phase2_momentum_breakdown.py` that motivated the reversal check).
**Status:** COMPLETE. Reframe: not predicting the market — predicting *our own trades*.

## Motivation
The config search found the persistent structure is **reversal, not momentum**
(`phase2_momentum_breakdown.py`): recent winners underperform recent losers across nearly every
lookback × horizon, OOS, both years, CIs excluding 0 (up to −80 bp/21d spread). The long-only leg
(buy recent losers) looked large (~+70 bp/21d). Separately, the lifecycle framing asks whether
winners/losers can be told apart *after entry* well enough to manage trades dynamically.

## A. Green survival — holding a PROVEN-GREEN trade has no remaining edge (`phase2_green_survival.py`)
Condition on green ≥ buffer at each checkpoint (5/15/30/60/120m); measure the REMAINING return to EOD.
- **"Stays green" is true** — green +1% by 120m → 90.6% finish green, 90.3% never break entry.
- **But avgRemain ~0/negative everywhere** (−0.4 to −10.6 bp) and **net of cost always negative**
  (net@10bp −7 to −31). The big total return is the **banked** early gain, not a hold edge.
- No split helps: above-VWAP −1.9, momentum-cohort −4.4, high-vol −3.9, market-down +1.1 (net −8.9).
**Holding from proven-green is a wash-to-negative** — you can't act on the banked gain at entry.

## B. Loser identification / failure curves — no exit rule adds expectancy (`phase3_lifecycle.py`)
- **Winner/loser trajectories** separate at 5m (11 bp gap) and diverge ~symmetrically to 209 bp at
  EOD — **no special divergence point**; the split is mechanical (winners = the ones that went up).
- **Failure curves** (condition on a BAD state → recovery + remaining):
  | state | P(recover green) | avg remaining | net@10bp |
  |---|---|---|---|
  | 30m down 0.50% | 24.6% | −3.6 | −13.6 |
  | 30m down 0.75% | 21.6% | −5.5 | −15.5 |
  | 30m below-VWAP | 38.5% | −1.2 | −11.2 |
  | 60m down 0.75% | 17.1% | **+4.2** | −5.8 |
  Losers finish red (low recovery) — but the **remaining return from any down-state is ~0** (even
  slightly POSITIVE at 60m: losers bounce = intraday reversal echo). So **cutting locks the loss +
  forfeits recoveries = the win-rate trap**; no stop/exit rule adds expectancy.
- **MAE & time-underwater** (hindsight-clean, ex-ante-useless): winners median MAE −0.34% / 9% time
  underwater; losers −1.25% / 90%. Practical: winners rarely dip past ~0.35% → **a tight stop cuts
  winners; only a stop wider than ~1.25% avoids them.** (Risk-sizing, not alpha.)

## C. Reversal verification — the "buy recent losers" lead is a mirage (`phase2_reversal_verify.py`)
The breakdown's headline reversal long-leg, stress-tested three ways:
- **Per-year: crisis-driven & inconsistent.** trailing-21d loser-leg: 2016 +98 / 2017 −4 / 2018 −41 /
  2019 +37 / 2020 +105 bp. 2020 (COVID V-recovery) + 2016 (post-selloff) dominate; 2017–18 negative.
  Not era-stable across the full window (the "2/2 OOS" masked this).
- **Cost eats it.** Loser-decile spread 14.7–14.9 bp vs 11.3 universe (beaten-down = wider). Round-trip
  ~30 bp → trailing-5d gross +14 → **net −15.8 bp**; trailing-21d gross +31 → **net +1.7 bp** (~zero).
- **Doesn't survive in liquid names** (stronger in mid-liquidity than the least-liquid bucket; highly-
  liquid too thin to measure). Textbook short-term-reversal: real gross, gone net of cost + crises.

## Verdict
The trade lifecycle is **legible in hindsight, not predictable in real time.** Winners and losers
look different (trajectory, MAE, time-underwater) *only because those features ARE the outcome
unfolding* — conditioning on any real-time-observable state gives ~0 remaining expectancy, so **no
dynamic management (exit, stop, health score) improves expectancy over holding the plan.** This
matches Phase 2C (post-entry IC 0.01) and 2D (management = wash). And the one directional lead
(reversal) dies at the cost/crisis wall — the 4th time real gross structure is uncapturable net of
friction (with PEAD, the factor scan, and now reversal).

**The one durable, useful nugget:** winners take little heat (MAE −0.34%), losers bleed (−1.25%, 90%
underwater) — so **stops should be WIDE (>~1.25%), not tight**; tight stops cut winners and, since the
remaining from a down-state is ~0, add no expectancy. Risk control, not alpha.

Overall: price/volume is exhausted for a tradeable stock edge — pre-entry (Phase 1), post-entry /
lifecycle (Phase 2–3), and the reversal lead. The remaining path is NEW data (PEAD + real EPS-surprise
is the cheapest sharp directional test) or index.

## Reproduce
```
scripts/phase2_momentum_breakdown.py · scripts/phase2_reversal_verify.py
scripts/phase2_green_survival.py · scripts/phase3_lifecycle.py
```
