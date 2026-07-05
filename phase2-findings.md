# Phase 2 — Trade Management (post-entry information) Findings

**Date:** 2026-07-05 · **Scripts:** `phase2_trade_health.py`, `phase2d_simulate.py`
(+ audit of all Phase-1 scripts). **Status:** COMPLETE — post-entry info carries no tradeable
edge; Phase-3 decision point reached.

Phase 1 used only PRE-entry information (no directional edge; only volatility magnitude, which
is trivial persistence). Phase 2 is the first test of POST-ENTRY information — a genuinely
different prediction problem: *I've already entered; does how the trade behaves after entry tell
me what happens next?*

## 2A — Leakage audit (highest priority; a dedicated auditor pass)
Audited all 10 Phase-1 experiment scripts for look-ahead / OOS / embargo / barrier construction.
- **6 CLEAN, 3 MINOR, 1 LEAK.** The one real leak = the *first-pass PEAD control* (`phase1_pead.py`:
  trailing return used the same-day close while entering at 10:00) — already superseded by
  `phase1_pead_verify.py`'s prior-day-shifted control; the PEAD *signal* was always clean.
- MINOR: embargo (2d) < horizon (5d) on the two 5d-label scripts (joint_probe, metalabel-L3) — a
  few train days per year-boundary overlap the test year.
- **Crucial:** leakage/short-embargo *inflate* signal, so they bias toward FALSE POSITIVES. Every
  null we produced is therefore CONSERVATIVE — the flaws would only have helped, and the results
  were still null. `high_52w`/`low_52w` verified prior-close-based (13.5% of rows sit below the
  day's intraday high → excludes today = safe). **No conclusion is a leakage artifact.**

## 2B/2C — Trade-health / remaining-path predictability (`phase2_trade_health.py`)
Enter @10:00; at the 30m checkpoint use the intra-trade state (`forward_path_short`: return-so-far,
drawdown-from-peak, VWAP distance, %bars profitable, within-trade vol, rate-of-change, ret/ATR,
volume-since-entry) to predict the **REMAINING** return 30m→EOD (target excludes the banked gain,
so it dodges the "winners are already up" tautology). Deployable/liquid, 994,687 trades, walk-forward
OOS, day-clustered rank-IC + permutation null.

- **Descriptive (2B):** remaining return + P(remaining>0) are **~−1 bp / ~50%** in EVERY state cut
  (up/down, above/below VWAP, rising/falling). The post-entry state does NOT separate remaining
  winners from losers. (Faint *mean-reversion* tilt if anything: up/rising names do slightly worse.)
- **OOS (2C):** FULL post-entry state rank-IC **+0.0103**, CI[+0.0033,+0.0172], **beats the null**
  (+0.0040) and beats ret-so-far (+0.0033) — but it's the same "beats noise, not cost" pattern as
  the joint probe (0.007): an order of magnitude below tradeable (~0.03–0.05), mostly `rate_of_change`
  (mean-reversion), and the unconditional remaining expectancy is already slightly negative.

## 2D — Simulated management: health-gated exit vs hold (`phase2d_simulate.py`)
Base = momentum cohort, hold to EOD. Managed = at 30m, exit early if the OOS health model predicts
a bad remaining path; else hold. Exit-cost sweep. 162,599 OOS trades (2018–2020).

| strategy | mean bp | win% | avg W | avg L | PF |
|---|---|---|---|---|---|
| BASE hold-to-EOD | −5.6 | 48.3 | 122.3 | −126.4 | 0.91 |
| MANAGED (exit worst 50%) @0bp | −5.2 | 50.9 | 82.8 | −97.8 | 0.89 |
| MANAGED (exit worst 50%) @5bp | −7.7 | 49.5 | 82.2 | −96.4 | 0.84 |
| MANAGED (exit worst 25%) @10bp | −8.0 | 51.4 | 93.7 | −116.6 | 0.86 |

**Management does not add expectancy.** At *zero* exit cost it's a wash (best +0.4 bp); at any
realistic exit cost it strictly hurts. It raises WIN RATE (48.3→50.9%) but truncates winners as
much as losers (avg W 122→83) — the "optimize win-rate not expectancy" trap made concrete. PF < 1
throughout.

## Verdict — Phase 3 decision point reached
Post-entry information — the last untested source in the price/volume dataset — carries **no
tradeable edge, in selection (2C, IC 0.01) OR management (2D, no expectancy lift)**. Combined with
Phase 1 (no pre-entry directional edge; volatility magnitude is trivial persistence), the dataset
is **exhausted for a tradeable stock strategy, pre- AND post-entry.** The evidence is overwhelming.

**Phase 3:** the only way forward is NEW information, not a new model on the same features.
Ranked (see `phase1-frontier-findings.md`): (1) directional data — real earnings-surprise (SUE) +
analyst revisions [cheapest sharp test: re-run PEAD with true SUE, target the long leg], (2) options/IV
[downgraded — our vol signal is ~persistence, unlikely to beat IV], (3) index baseline as the honest
default. The one in-data lead is PEAD (real directional drift, but long-only-null / short-gated).

## Reproduce
```
scripts/phase2_trade_health.py · scripts/phase2d_simulate.py
```
