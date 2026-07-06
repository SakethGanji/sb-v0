# Phase 7 findings

**Date:** 2026-07-06 · Pre-registration: `phase7-preregistration.md` (frozen 2026-07-06,
amendment v2 applied pre-run). Program: `phase7-magnitude-program.md` ·
Execution doc: `phase7-implementation-plan.md`.

---

## §7.1 — Test 7.1: marginal forecast value for risk overlays → **NULL on all three arms**

**The registered question:** does the full-ML magnitude forecast improve any
volatility-managed portfolio over the SAME portfolio built on naive trailing vol?
**Registered expectation:** null. **Result: null, on every arm, against every baseline.**

Run record: `scripts/phase7_forecast_value.py` at commit `f102eea` (amendment v2),
all arms, OOS 2018-01-01..2020-12-31, seed 20260706, wall 164s on the data machine.
The verdict lines, verbatim:

```
ARM A — vol-targeted SPY: ML σ̂ vs RV21 / BLEND (INV-VOL rule frozen from Phase 5)
  forecast RMSE vs fwd-5d realized (OOS): ML 0.1544  RV21 0.1446  BLEND 0.1255
  ML σ̂ OOS dispersion (std of forecast): 0.0682
  eval days: 751 of 756 OOS days  (5 dropped for missing σ̂ in any strategy — paired-sample rule)
  avg exposure ⟨w⟩: ML 0.79 vs RV21 0.84  [exposure gap >0.05 — disclose with verdict]
  ML vs RV21    Sharpe  0.33 vs  0.60  ΔSh CI[-0.72,+0.19]  g@10%vol +2.75/+5.54%  Δg CI[-7.27%,+1.96%]/yr  maxDD -29.8/-17.2%  -> null
  avg exposure ⟨w⟩: ML 0.79 vs BLEND 0.83
  ML vs BLEND   Sharpe  0.33 vs  0.60  ΔSh CI[-0.70,+0.16]  g@10%vol +2.75/+5.54%  Δg CI[-7.12%,+1.70%]/yr  maxDD -29.8/-17.0%  -> null
  ARM A verdict: NULL

ARM B — cross-sectional inverse-vol portfolio: ML σ̂ vs atr_14d/price (21d rebalance, 15bp RT)
  training rows 994,694 · portfolio-universe OOS rows 223,848
  ML / ATR: zero-filled (delist/gap) name-days 20 each · uninvested days 0
  inv-ML vs inv-ATR/price (registered)  Sharpe  0.55 vs  0.55  ΔSh CI[-0.02,+0.04]  g@10%vol +5.05/+4.95%  Δg CI[-0.18%,+0.35%]/yr  maxDD -35.8/-35.4%  -> null
  context only: equal-weight Sharpe 0.56, growth +9.51%/yr, maxDD -36.6%
  ARM B verdict: NULL

ARM C — volatility-harvest basket: harvest(ML top-decile) vs harvest(ATR top-decile)
  ML   net harvest raw +0.74%/yr · matched@10%vol +0.25%/yr · basket bh vol 32.8% (k=0.30)
  ATR  net harvest raw +0.65%/yr · matched@10%vol +0.22%/yr · basket bh vol 34.4% (k=0.29)
  registered estimand Δharvest@10%vol (ML − ATR): +0.04%/yr  CI[-0.14,+0.22]
  ARM C verdict: NULL
```

### Reading, arm by arm

- **Arm A is a mechanical null**, resolved by the registered secondary read: the ML
  model's OOS forecast RMSE (0.1544) is **worse** than both naive baselines (RV21
  0.1446, BLEND 0.1255). At the market level there is nothing to convert — the ML
  forecast does not out-predict trailing vol in the first place. The overlay
  consequences (Sharpe 0.33 vs 0.60, maxDD −29.8% vs −17.2%, at ⟨w⟩ 0.79 vs 0.84,
  disclosed) point *against* ML, though no CI excludes 0.
- **Arm B is the clean statement of the registered question**: the forecast's +0.036
  rank-IC lift over persistence, pushed through real inverse-vol weights, monthly
  rebalancing, and the frozen 15bp cost model, is worth ΔSharpe ≈ 0.00 (CI
  [−0.02, +0.04]) and Δgrowth@10%vol ≈ +0.1%/yr (CI [−0.18%, +0.35%]). Invisible,
  exactly as registered.
- **Arm C: the rebalancing harvest exists but is decorative** — +0.65…0.74%/yr raw,
  +0.22…0.25%/yr at matched risk, barely surviving 15bp costs on the highest-vol
  decile — and the ML-vs-ATR selection difference is +0.04%/yr (CI [−0.14, +0.22]).
  Shannon's demon is real physics and irrelevant economics at retail scale.

### Contract record (per the anti-goals clause)

- `[contract] daily_observation: expected columns ABSENT and dropped: ['entry_1m_range',
  'entry_range_vs_atr_14d', 'vix_open']` — expected; these features load from
  `forward_outcomes` / `market_context_daily` respectively, as in Phase 1. No other
  substitution occurred. The v1 script's defects and their pre-run fixes are enumerated
  in `phase7-preregistration.md` Amendment v2 (crash, ret_1d overlap, dollar-ATR
  baseline, calendar-day embargo, NaN policy, matched-risk CIs, seeds). Two of the
  fixed defects were biased **toward a false PASS**, so this null is a clean null.

### Verdict, per the frozen decision rule

NULL on all arms → **the forecast is persistence-in-a-costume for every portfolio
use; Channel G closes at the margin; naive trailing vol remains the recommended risk
input.** No PASS occurred, so the 2021-22 confirmation protocol is not triggered and
the validation split stays sealed. The Phase 5 standing verdict is unchanged (vol
targeting is a real risk tool if drawdowns matter) — it simply needs no ML model to
run: RV21 or BLEND is the input.

The "our forecast is special" question is now closed with a number, in economic units,
three ways.

### Status of the Phase 7 queue after 7.1

- Step 1 — **DONE, NULL** (this section).
- Step 2 — A2 property sprint: unblocked, $0, data local (`bars_1m_raw` on this
  machine). Freeze `phase7a2-preregistration.md` first.
- Step 3 — Branch B (realized vs implied): unchanged; funding decision. Note Arm A's
  RMSE result is additional prior *against*: the market-level ML forecast lost to
  BLEND = sqrt(RV·VIX), i.e. to a crude implied-vol blend, before any option spread
  was paid.
- Step 4 — seal after 2 (and 3 or its explicit decline).
