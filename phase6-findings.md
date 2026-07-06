# Phase 6 findings — event-conditioned state dynamics

**Date:** 2026-07-06 · Pre-registration: `phase6-preregistration.md` (frozen before code) ·
Scripts: `phase6_states.py`, `phase6_event_state.py` · Cost: $0.

## Registered prediction #1 — CONFIRMED (GARCH rediscovered)

k=6 states, train-fit 2016-18, frozen. The centroids are exactly vol/liquidity/trend
regimes: quiet (31%), run-up, drawdown, chop, vol-expansion, explosion (4.8%). Persistence
0.73–0.90; transition matrix essentially identical out-of-sample (max cell drift **0.033**,
mean 0.009, vs the pre-registered <0.10 stability rule). The "market state machine" is
**volatility clustering with narrative labels** — stable, real, and already known to
econometrics since ARCH/GARCH. No discovery, as registered.

## Registered discovery criterion #2 — event × state interactions: NULL / UNANSWERABLE

4 knowable events (SUE top/bottom quintile on the 8-K clock, insider cluster-buy flag day,
any 8-K 2.02) × 6 states × 2 horizons = 48 cells, entry next trading day 10:00.

- **Power gate: 42/48 cells UNANSWERABLE** (MDE95 above the 35bp/5d / 60bp/21d limits) —
  splitting a few thousand events six ways shreds the sample; the interaction question is
  mostly beyond this window's resolution, and is reported as such rather than as null.
- **BY-FDR (α=0.10) over the 6 answerable cells: 0 survive.** Nearest misses were
  *negative* post-announcement drift for generic 8-Ks landing in quiet/expansion states
  (S3 5d −28.8bp raw p=0.020; S0 5d −22.6 p=0.038) — wrong side of multiplicity, wrong side
  of the shorting wall, and era-unstable (1/5, 2/5).
- SUE-top in the one answerable state: +11.7bp, p=0.51 — nothing.

## Verdict (per the registered rule)

> "If states reduce to volatility/liquidity regimes and do not improve net directional
> expectancy, this confirms the prior null rather than opening a new branch."

**Both conditions hit. The branch does not open.** Phase 6's value is the closure itself:
the "movie vs snapshot" / state-machine reframing was executed with frozen definitions and
honest multiplicity, and it landed where the information ceiling said it must — the
dynamics of price/volume states are real, stable, and already priced; the events that could
redirect them are either priced within a day (SUE), gated (insider short side), or split
too thin to measure (most interaction cells).

## Phase 6B — feature-continuation / signal-decay (the user's exact question, registered form)

"Can we forecast signal persistence BEFORE seeing it, and does the forecast pay?"
(`phase6b_signal_decay.py`; pre-registered in `phase6-preregistration.md` §6B.)

- **Q1 (descriptive): favorable variables DO persist.** Top-decile momentum stays top-decile
  80.2% at t+1, 65.1% at t+3, 54.7% at t+5 (base 10%). Vol-trend decays much faster
  (72% → 12% by t+5) — vol *expansion* is a spike, momentum *rank* is sticky.
- **Q2: persistence is FORECASTABLE.** Walk-forward OOS AUC **0.741** vs permutation null
  0.576 — you genuinely can tell, at entry, which momentum setups will still look good
  next week.
- **Q3: the forecast pays NOTHING — mildly negative.** Forward 5d excess by predicted-
  persistence quintile (OOS 2018-2020): Q1 −2.2bp … **Q5 −27.5bp gross** CI[−58.2,−0.6],
  net@20 −47.5, eras 0/3 positive. The most-confidently-persistent setups did *worst*.
  **Registered null confirmed** (and the sign echoes the lottery/overextension pattern —
  the stocks most certain to keep "looking good" are the most crowded/extended ones).

This is the Phase 2/3 result reproduced at daily scale and in its strongest form: the
market lets you predict *that the setup will continue to look good* (AUC 0.74!) and pays
you nothing for knowing it, because "looking good" was never the same thing as "about to
go up." Signal persistence ≠ return continuation — now measured, not argued.

## Project status after Phase 6

Free queue: **empty** (Phases 4–6 spent it: SUE both clocks, subsets, score-tail,
vol-targeting, momentum gate, insiders, state dynamics). Remaining moves: **Branch B**
(paid IV slice, one pre-registered test, written prior poor) or **final report + seal**.
