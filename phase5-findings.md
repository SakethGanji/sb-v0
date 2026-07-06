# Phase 5 findings — vol-targeting, the momentum power gate, and insider cluster-buying

**Date:** 2026-07-06 · Scripts: `scripts/phase5_*.py` · Data: `/mnt/atlas/edgar/form345/` +
`data/phase1_analysis/form4_purchases.parquet`. All at $0.

## 1. Vol-targeted SPY (`phase5_vol_target.py`) — real risk overlay, unprovable alpha

4 vol signals (RV21 / VIX / EWMA10 / blend) × 3 rules (inverse-vol, inverse-variance,
binary top-quintile filter), σ*=15%, no leverage, 2bp/turnover, cash at 0.

- **All 12 configs improved Sharpe** (+0.13…+0.64) and **cut maxDD 50–82%** (best: −6.4% vs
  SPY's −34.6% through COVID).
- **0/12 formal PASS**: every bootstrap CI on ΔSharpe includes 0 (4.5yr with two vol events
  cannot prove it; the literature needed decades), and the payoff is insurance-shaped —
  wins only in 2018/2020, ties-minus-costs otherwise — so the "better in ≥4/5 years"
  criterion is structurally unreachable for this strategy class (a design lesson, noted).
- Verdict: **use it as a risk overlay if drawdowns matter; do not book it as alpha.**
  Validation 2021-22 untouched.

## 2. Long-horizon momentum power gate (`phase5_momentum_power_gate.py`) — UNANSWERABLE

The one timeline never tested (all prior momentum work was ≤21d lookback / ≤21d hold;
classic momentum is 6-12mo lookback, 1-3mo hold; ret_42d/63d existed unused).

| design | MDE95 long leg | plausible post-2010 effect |
|---|---|---|
| 12-1 × 21d | 55.9bp | ~10–30bp |
| 12-1 × 63d | 155.2bp | ~30–90bp |
| 6-1 × 63d | 168.2bp | ~30–90bp |

A 252d lookback burns the first year (entries start 2017-07) → ~14 independent 63d
observations. **Gate FAIL: the test was not run.** Status is "unanswerable in this window,"
not "momentum is null." (Third possible outcome of the discipline, now exercised.)

## 3. Insider cluster-buying (`phase5_form4_extract.py`, `phase5_insider_test.py`)

The last free directional information source: SEC Form 345 structured data, 22 quarterly
zips, 96,780 open-market purchases (code P, acquired, 79% officer/director), 2015q3–2020q4.
Signal: O/D purchase filings in trailing 30 calendar days; entry next day 10:00; hold 21d
excess-over-SPY, calendar-time portfolio.

- **Power gate: MARGIN** (MDE95: ANY 63.1bp / CLUSTER 74.9bp vs plausible 20–60bp; the
  $100k tier FAILed at 86bp and is excluded from verdicts). Ran with the caveat stated.
- **Pre-registered hypothesis (long edge): NULL.** ANY −20.2bp gross (CI incl 0);
  CLUSTER **−81.4bp** gross CI[−154.8, −5.3], eras 1/5 pos, placebo clean (+7.1).
- **Post-hoc surprise (unregistered, suggestive only):** the negative is NOT a decline
  artifact. Cluster names had fallen (−4.6% trailing 21d vs +1.4% universe — insiders are
  contrarian buyers), but decline-MATCHED controls earned **+2.1bp** while cluster names
  earned −83.3bp: **cluster-minus-control −85.4bp/21d, CI[−140.8, −30.5]**. In liquid mid+
  caps 2016-2020, officer/director cluster buying marked *idiosyncratically troubled* names
  that kept underperforming — the classic small-cap-positive literature result inverted.
- **Status: real-but-behind-glass candidate #6 (short-gated).** It is a SHORT signal
  (shorting = wall #2); as a long-only avoid-filter it touches ~2% of rows (~1.6bp/21d of
  portfolio impact — negligible). Post-hoc + 3 tiers tested + marginal power ⇒ it would
  need its own pre-registered confirmation before being believed; per protocol the
  validation split was NOT touched (the registered hypothesis failed).

## 4. Where this leaves the project

The free queue is now empty:

| door | status |
|---|---|
| price/volume direction (all granularities) | null, measured |
| conditional subsets (PRIM + score-tail) | inside the null |
| earnings surprise (true SUE, both clocks) | priced within ~1 day |
| long-horizon momentum | unanswerable in this window |
| insider cluster-buying (long) | null; (short) suggestive, gated |
| vol-targeting overlay | real risk tool, unprovable alpha |
| options/IV (Branch B) | untested, paid, prior poor (+0.008 forecast lift) |

Remaining moves: fund Branch B (~$50–100/mo, one month, one pre-registered test), or write
the final report and seal the project with "index + risk rules" as the standing conclusion.
