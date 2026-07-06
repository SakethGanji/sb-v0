# Phase 7 pre-registration — Test 7.1: marginal forecast value for risk overlays

**Frozen:** 2026-07-06, before any Phase 7 result was seen. Script:
`scripts/phase7_forecast_value.py` (written alongside this freeze; not yet run — the
data lives on the analysis machine, not in this session's container).
**Cell claim (per `phase7-magnitude-program.md` §4 step 1):** Channel G, hatch H1
(objective = growth/risk efficiency). **By construction this test CANNOT produce an
alpha verdict** — its only available PASS is "risk-tool improved." Pre-declared.

## The single question

Everything "magnitude" can do at the portfolio level, naive trailing volatility already
does (that is what Phase 5 used). The ONLY thing our months of ML work added is a
forecast whose lift over persistence is **+0.0076 rank-IC** (0.604 informed → 0.612
full-ML, `phase1_vol_forecast.py`). Test 7.1 asks, once, in economic units:

> **Does the full-ML magnitude forecast improve any volatility-managed portfolio over
> the SAME portfolio built on naive trailing vol?**

If no: the forecast is confirmed as persistence-in-a-costume for every portfolio use,
and Channel G closes at the margin (naive vol remains a fine risk tool; our model adds
nothing). If yes: we have a validated better risk input — still not alpha.

## Registered expectation (written first)

**Null on all three arms.** The forecast's information beyond trailing vol is ~1% relative;
portfolio weights are a heavily smoothed functional of σ̂; a +0.008 IC difference should
be invisible in Sharpe/growth after costs. A PASS would be a genuine surprise requiring
the §7.7 confirmation protocol before belief.

## Common frozen choices

- **Window:** forecasts trained expanding walk-forward by year (train from 2016-06-08),
  evaluated **OOS 2018-01-01..2020-12-31 only** (the years with OOS predictions).
  Validation 2021-22 untouched unless a PASS; holdout sealed.
- **Costs:** SPY 2bp per unit turnover (Phase 5 convention). Single stocks: **15bp
  round-trip × turnover** (frozen cost model, liquid-cell conservative upper bound,
  `phase1-cost-model-findings.md`).
- **Inference:** joint block bootstrap (block=21, B=4000, seed 20260706) on the paired
  daily return series — the SAME resampled blocks price both strategies, so the CI is on
  the difference and correlation is preserved.
- **Estimands per arm:** ΔSharpe and Δlog-growth (ML − naive), each with 95% CI; maxDD
  reported descriptively.
- **PASS (risk-tool verdict) per arm:** CI(ΔSharpe) excludes 0 **or** CI(Δlog-growth)
  excludes 0, in favor of ML, against **every** naive baseline in that arm. Anything
  else = NULL. Three arms tested → a single marginal PASS is noise; only a PASS that
  survives all baselines in its arm counts at all, and belief further requires 2021-22
  per protocol.

> **Amended 2026-07-06 (pre-run — frozen before any result was seen; data not yet
> available to this session).** The PASS criterion is *economic utility at matched
> risk*, never return: better realized Sharpe / drawdown / geometric growth **at equal
> risk** versus the naive-trailing-vol version of the same portfolio. Concretely:
> ΔSharpe is scale-invariant and stands as-is; **Δlog-growth is compared after both
> net return series are rescaled to a common realized volatility** (10% annualized),
> so a forecast that merely runs systematically higher average exposure in a bull
> window cannot manufacture a growth PASS. Average exposure ⟨w⟩ is reported per
> strategy and any material gap is disclosed with the verdict. A PASS under this
> criterion is a **risk tool, not a trading edge** — restating, with teeth, what the
> cell claim already made unavailable.

## Arm A — vol-targeted SPY: ML σ̂ vs naive σ̂ (time-series)

- ML σ̂_t: HistGB regression, features = market-level knowables through t−1
  (spy_realized_vol_21d, vix, EWMA10, |spy ret|, overnight gap, breadth, dispersion,
  IQR, movers, log dollar volume — whichever exist in `market_context_daily`), target =
  **forward 5d realized vol of SPY daily returns, annualized** (t+1..t+5). Expanding
  walk-forward by year, 5d embargo.
- Naive baselines (identical lag discipline, info through t−1): **RV21** and
  **BLEND** = sqrt(RV21·VIX) — Phase 5's two best-behaved signals.
- Rule (fixed, from Phase 5, no re-tuning): INV-VOL w = min(1, 0.15/σ̂), long-only,
  no leverage, cash at 0, 2bp/turnover.
- Registered secondary read: OOS time-series correlation of each σ̂ with realized —
  if ML's forecast RMSE does not even beat RV21's, the portfolio null is mechanical.

## Arm B — cross-sectional inverse-vol portfolio: ML σ̂ vs trailing σ̂

- Universe per rebalance day: CS, market_cap ∈ {mega, large}, liquidity ∈
  {highly_liquid, liquid} (the deployable corner — deliberately excludes the illiquid
  low-vol-factor corner so the known factor cannot masquerade as forecast value).
- ML σ̂ per (day, stock): the Phase 1 FULL-ML forecast of realized 1d range
  (identical features/model/walk-forward/embargo to `phase1_vol_forecast.py`),
  computed once and reused by Arm C.
- Naive σ̂: **atr_14d** (the persistence baseline in the same range-like units).
- Portfolio: rebalance every 21 trading days; weights ∝ 1/σ̂ normalized; holdings
  drift within the block (compounded daily via per-stock next-day returns at the 10:00
  clock); names whose returns disappear mid-block (delist) contribute their last value
  at 0 further return, logged. Cost charged on one-way turnover at each rebalance:
  15bp × ½·Σ|w_new − w_drifted|.
- Comparators: (i) **inverse-ATR14** — the registered marginal question; (ii)
  **equal-weight** — descriptive context only (whether inverse-vol beats EW is a known
  beta, not this test's question, and cannot PASS anything).

## Arm C — volatility-harvest basket: does the ML forecast select a better harvest?

- Mechanism under test: diversification/rebalancing return ≈ (σ̄²_components −
  σ²_portfolio)/2 scales with component vol → a basket of predicted-high-magnitude
  names maximizes the harvest. This pays **geometric** growth, never expectancy
  (pre-declared).
- On each 21d rebalance day: top-decile predicted 1d range among the Arm B universe →
  equal-weight basket. Two implementations of the SAME basket per block: rebalanced
  daily to equal weight (daily turnover costed) vs buy-and-hold within block (block-
  boundary costs only). Harvest = Δlog-growth (rebalanced − buy-hold), net.
- **Primary registered estimand:** harvest(ML-selected basket) − harvest(ATR14-selected
  basket) — the marginal-forecast question, consistent with the rest of 7.1.
- Secondary descriptive: absolute net harvest of each basket (is the premium even
  positive after 15bp costs on high-spread names).

## Anti-goals (frozen)

- No re-tuning of σ*, rebalance frequency, rule form, universe, or cost level after
  seeing results — the grid above is the whole grid. Any variant run later is a NEW
  registration.
- No expectancy claims from any arm, PASS or not.
- If data-contract mismatches force column-name substitutions at run time, they are
  recorded in the findings doc verbatim; no other deviation is permitted silently.
