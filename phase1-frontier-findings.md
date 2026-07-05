# Phase 1 — Frontier / "What's Next" Findings (autonomous session, 2026-07-05)

**Scripts:** `phase1_vol_forecast.py`, `phase1_pead.py`, `phase1_pead_verify.py`.
**Method:** 3 idea-generation/red-team agents → 3 decisive experiments on the two live
threads (the vol-magnitude signal; the untested earnings-conditioning). **Status:** FINAL.

Starting point (see `phase1-metalabel-findings.md`): direction is null five ways; the one
real signal is stock-normalized volatility *magnitude*. Two live questions: (A) is the vol
signal *non-trivial* (would it beat implied vol → is an options cycle-2 justified)? (B) is
there ANY untested directional structure in the data we already have?

## Agent idea-generation (3 parallel agents)
- **Red-team the null:** the standout untested hole = **PEAD** — "news gated" was wrongly read
  as "earnings gated," but the earnings *date* + *price reaction* are in the data and were never
  conditioned on. Also flagged: beta/sector-residual mean-reversion, overnight/intraday
  decomposition (both likely "behind glass"), lead-lag & calendar (near-dead).
- **Vol monetization:** AUC-0.65 is *not yet* non-triviality — must beat a HAR-RV/persistence
  baseline (what IV prices). Crispest test = incremental OOS R² over static-vol + today's obvious
  info, earnings/VIX stripped. Without options, a vol signal has **no standalone alpha** (sizing/
  execution/avoidance are overlays on a directional book we lack; stock has no gamma).
- **Cycle-2 data scoping:** concrete vendors + minimal go/no-go tests (below).

## Finding A — the vol signal is trivial (path 2 weakened) · `phase1_vol_forecast.py`
Predict realized 1d range (deployable, 994k rows), OOS cross-sectional rank-IC, models of
rising sophistication:

| model | OOS rank-IC |
|---|---|
| STATIC-VOL (historical vol only) | 0.576 |
| INFORMED (+ opening range/gap/earnings/VIX ≈ what a 10:00 option knows) | 0.604 |
| FULL-ML (all 65 feats) | 0.612 |

**Full-ML beats the INFORMED baseline by +0.0076 IC (+1% relative), stable across years.**
Realized vol is highly predictable (~0.58) but **~96% static persistence** — exactly what
implied vol prices. Our model adds ~nothing over "static vol + today's obvious info," and that
sliver would not survive an options bid-ask spread. **The vol-expansion signal is real but
trivial (already in every option premium) → an options/straddle cycle-2 is NOT supported by our
own data.** (Only pursue path 2 if a cheap real-IV slice directly shows realized > implied.)

## Finding B — PEAD: the first real DIRECTIONAL signal, but "behind glass" · `phase1_pead*.py`
Surprise proxy = 2-day earnings-reaction return (days_since ∈ {0,1}, forward-filled); among
names 2–6 days POST-earnings, rank by surprise → forward 5d excess-over-SPY decile spread.

- **PEAD 5d spread +58.9 bps, block-CI [8.0, 112.0] ✓, 5/5 eras.** Clean and significant.
- **Validated:** placebo (shuffled surprise) −12.4 bps (null); **look-ahead-safe momentum control
  −24.1 bps (null)** — so it is NOT generic momentum (generic 2d momentum is null once the
  look-ahead is removed, consistent with the whole project); persists→decays with event distance
  (2–6d +58.9 → 3–8d +48.7 → 4–10d +32.2 n.s. — textbook PEAD); broad (624 names, top-5 = 3.9%);
  concentrated in **mid-caps** (+73.5) vs large (null). 21d not significant (CI incl 0).
- **BUT asymmetric and uncapturable:** the spread is almost all SHORT leg — negative surprises
  drift **−53.0 bps/5d** (needs shorting = GATED); the long leg (positive surprises) is **+5.8
  bps/5d → −9.2 net of ~15bp cost** (long-only NULL).

**Significance:** conditioning on the earnings event *does* unlock directional predictability that
raw price/volume lacks — the first directional structure found. It does **not** flip the no-long-
only-edge verdict (long leg is arbitraged away in liquid names and attenuated by the price-based
surprise proxy), but it is the strongest cycle-2 lead: a **real EPS-surprise measure (SUE) +
analyst revisions** would sharpen the signal and could restore a tradeable long leg.

## Remaining untested price/volume ideas (red-team, lower prior)
- **Beta/sector-residual short-horizon mean-reversion** (stat-arb on neutralized residuals): MED
  it exists / LOW-MED it trades — historically lives in the illiquid corner + short-leg gated.
- **Overnight vs intraday return decomposition** as separate targets: MED / LOW-MED — largely a
  market-wide beta effect, overnight execution frictions.
Both are genuinely untested *as constructed* but the prior is they resurface the same "real but
behind glass" pattern. Worth a cheap check before any data spend; not verdict-changers.

## Cycle-2 scoping & recommendation (agent-sourced, verify pricing — knowledge dated)
- **Path 3 (directional / earnings) — now the top lead.** Cheapest sharp test: real earnings
  surprise + analyst revisions (e.g. Financial Modeling Prep, ~hundreds/yr; SEC/FINRA short
  interest free). **Minimal go/no-go:** re-run PEAD with true SUE (not the price proxy) + a
  revision-momentum event study; measure OOS net long-short IC & the *long*-leg net of cost. Our
  price-proxy PEAD already gives a clean drift → higher prior that a real surprise measure yields
  a tradeable long leg. This is the smallest spend with the most direct read.
- **Path 2 (options/vol) — downgraded** by Finding A. Only worth a ThetaData/ORATS slice (~hundreds)
  → delta-hedged straddle test *if* you first accept the vol signal is ~persistence; the decisive
  metric is realized−implied vol net of the option spread, and our data says the edge is thin.
- **Baseline to beat: just index.** Honest default if the cheap path-3 test is null net of cost.

**Bottom line:** the data is exhausted for a long-only price/volume edge (now including the two
most-promising untested angles). The one genuinely new lead is **PEAD with real earnings-surprise
data** — a small, well-scoped, evidence-motivated cycle-2, not "look harder."

## Reproduce
```
scripts/phase1_vol_forecast.py · scripts/phase1_pead.py · scripts/phase1_pead_verify.py
```
