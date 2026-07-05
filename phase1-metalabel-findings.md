# Phase 1 — Joint-Model & Meta-Labeling Findings (the multivariate / conditional layer)

**Date:** 2026-07-05 · **Scripts:** `phase1_joint_probe.py`, `phase1_metalabel.py`,
`phase1_metalabel_economic.py`, `phase1_metalabel_control.py`.
**Outputs:** `data/phase1_analysis/joint_probe.parquet`, `metalabel.parquet`. **Status:** FINAL.

## Why this exists
Every earlier Phase-1 script is **univariate** (one feature/signal at a time), and the
predictability ceiling (`phase1_ceiling.py`) that underwrote "no ML headroom" is **also
univariate** — it caps the MI of any *single* feature. Two features each ~0 alone can be
jointly predictive. This layer closes that gap: it tests the **joint / interaction /
conditional** predictability the 657-column aggregate was built to enable, including the
**market-state** features (VIX / breadth / dispersion / sector) that *no prior script ever
loaded*. It then tests **meta-labeling** — a conditional accept/reject *classification*
objective, which is distinct from return-ranking and can add value even when ranking is weak.

**Model:** sklearn histogram GBM (`HistGradientBoosting{Regressor,Classifier}`;
`max_iter=300, lr=0.05, max_leaf_nodes=31, min_samples_leaf=200, l2=1.0`, early stopping),
native NaN. Expanding **walk-forward** by year (train ≤Y−1 → test Y), 2-day embargo,
2018–2020 OOS. Nulls: label/y **permutation** (shuffle within train days, refit) + day-clustered
bootstrap CI. Universe: deployable/liquid (CS, cap∈{mega,large,mid}, liq∈{highly_liquid,liquid,normal}),
enter 10:00.

## 1. Joint return-ranking probe (`phase1_joint_probe.py`)
95 joint features → continuous excess return, OOS rank-IC. 994,694 rows.
- **1d excess:** OOS daily rank-IC **+0.0070**, day-clustered CI **[−0.0037, +0.0183]**
  (includes 0), **beats the permutation null** (null 95th = +0.0023). Gross decile spread
  **+3.2 bps/day**, CI [−6.2, +13.6].
- **5d excess:** rank-IC −0.0019, CI [−0.0148, +0.0109], **inside the null**; nothing.

**Read:** a **faint but non-random** trace at 1d — it beats the noise null, so joint models
*do* carry slightly more than any single feature (the univariate "no-headroom" claim was
overstated). But it is **untradeable**: rank-IC ~0.007 is an order of magnitude below tradeable
(~0.03–0.05), the day-clustered CI includes 0, and ~3 bps/day gross is well under the cost floor.
Gone by 5d.

## 2. Direct meta-labeling test (`phase1_metalabel.py`)
Primary signal = top-quintile morning momentum (228,297 trades). Binary trade-quality labels,
classifier on 95 features, OOS AUC vs label-permutation null + decile lift.

| Label | OOS AUC | vs null | top-decile win-lift | monotone | year-stable |
|---|---|---|---|---|---|
| L1 beat-SPY 1d | 0.5048 | inside ✗ | +0.5pp | — | no |
| **L2 +2% before −2% (1d)** | **0.6310** | **BEATS ✓** (0.552) | **+15.0pp** | **yes** | **yes** (+10.7/+15.5/+12.5pp) |
| L3 beat-SPY 5d | 0.5022 | inside ✗ | −2.6pp | no | no |

**Read:** the *directional* labels (L1/L3, "beat SPY") are **null** — coin flips, consistent
with the ranking probe. But the *trade-quality* label L2 ("+2% before −2%") is **strongly,
monotonically, year-stably predictable** (AUC 0.63, D0 12.6% → D9 47.8% target-first). Return-
ranking completely missed this. **However**, the top decile's buy-hold-1d excess was **−2.9 bps**
(worse than −1.9 base) — the win-rate lift did *not* convert to return under a hold-to-close exit,
flagging that L2 predicts *path geometry*, not money, measured under the wrong exit.

## 3. L2 economic test — its own barrier exit (`phase1_metalabel_economic.py`)
Barrier P&L: target_first→+2%, stop_first→−2%, neither→hold-to-close (`ret_1d`), net-of-cost sweep.

- **Top-decile mix: 47.8% target / 45.4% stop / 6.9% neither** → target:stop ≈ **1.05 (coin flip)**.
  Down the deciles, target% and stop% **rise in lockstep** while "neither" collapses 70.8% → 6.9%:
  the score sorts on **whether a barrier is hit at all** (a variance question), not which one.
- **Gross barrier P&L stays ~0 across all deciles** (−3.3 … +4.1 bps). Gated top-decile **+4.1 bps
  gross → −15.9 net@20bp**; "all momentum" control ≈ **−0.2 gross** (coin-flip baseline).
- **Flips by year:** top-decile gross **−31.0 (2018) / +10.2 (2019) / +21.6 (2020)** bps — negative
  in the volatile down-year, positive in drift years. Regime, not edge.

## 4. Controls — model-agnostic + mechanism (`phase1_metalabel_control.py`)
- **Linear cross-check:** L2 logistic AUC **0.6291** ≈ GBM **0.6310**. The structure is a **simple
  monotone gradient**, not GBM-only interactions — so the GBM null on *direction* is not a
  capacity artifact. Neither monetizes (GBM top-decile +4.1 gross; Linear −11.5 gross; both deeply
  negative net of cost).
- **Feature importance (mechanism):** GBM permutation importance (OOS 2020) is dominated by
  `universe_total_dollar_volume`, `cross_sectional_ret_iqr/dispersion_at_1000`,
  `breadth_count_movers_above_5pct`, and `yang_zhang_vol_{5,21,42}d` — **volatility, dispersion,
  market activity** (several are *day-level*: "is today a volatile day"). Linear |coef| agrees:
  `volatility_percentile_today`, `realized_vol_21d_rank` push toward target; momentum-magnitude
  features enter with **mixed signs** (no clean directional predictor).

## Verdict
The multivariate / conditional / meta-label layer is **not empty, but not edge**:
- Joint models carry a faint, untradeable directional trace at 1d (beats noise, not cost/CI).
- The meta-labeling objective is **real and model-agnostic** (AUC 0.63, GBM ≈ linear) — but it
  predicts **barrier RESOLUTION (volatility), not direction**, and **does not monetize** net of
  cost, flipping sign by year.

This confirms the caveat exactly: *meta-labeling reorganizes existing information; it cannot
create information that isn't there.* These features are **rich in variance information, poor in
directional information** — so a classifier reorganizes the variance beautifully, and it isn't
tradeable (net of cost, and long-only / no options). The price-volume hypothesis is now closed
four ways: univariate null → multivariate near-null → meta-label directional null → model-agnostic
confirmation the only learnable structure is volatility.

## Caveats
- OOS is exploration-window 2018–2020; the 2023+ holdout remains untouched.
- Fixed hyperparameters, no tuning sweep; GBM≈linear makes tuning-sensitivity low-prior.
- Barrier P&L assumes fills at the exact ±2% touch (optimistic on the target leg); it *still*
  fails to clear cost, so the negative conclusion is robust to that optimism.

## Reproduce
```
scripts/phase1_joint_probe.py · scripts/phase1_metalabel.py ·
scripts/phase1_metalabel_economic.py · scripts/phase1_metalabel_control.py
```
