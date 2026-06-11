# Phase 1 — Research Strategy & Reframings

**Status:** Working notes for the analytical layer that consumes the Phase 0
outputs. Not a frozen spec — a living document refined before any cell-by-cell
research begins. **Feature-frozen 2026-06-11** after external review: the
next change to this plan must be motivated by a number computed from real
data (start with the §5 step-0 tracer bullet), not by additional
methodology ideas.

**Amended 2026-06-11 (post-freeze; defect repair + emphasis only, no new
methodology):** overlapping-horizon bootstrap fix (§7.7.1), walk-forward
given a concrete home in the implementation order (§2.5 ↔ §7.8 step 6.5),
deliverable-hierarchy note (§5.0), BH→BY consistency fix (§7.4),
recording-gap additions from the schema audit (§3.6), v2 implementation
checklist + audit-closing decisions (§3.6–§3.7). **Implementation starts
from §3.7.**

**Read after:** `momentum-hold-phase0-frozen-v2.md`, `phase0-observation-pivot-rfc.md`.

---

## 0. Why this document exists

The RFC v6 schema is correct and dynamic — 615 columns × 17 entry offsets ×
23 path checkpoints means almost nothing is hardcoded. But "having the data"
and "extracting the right insight from the data" are different problems.

Default analytical approach to a backtest dataset = "find the best cell by
expected return after costs." That approach produces overfit garbage 95% of
the time because it's selection-on-outcome plus multiple comparisons plus
era-pooling.

This document is the **reframing layer** — 10 structural changes to how Phase 1
queries the data, ordered by leverage. Each is a small query- or model-layer
change on top of what's already being built. None requires more raw ingest.

This document is meant to be **almost standalone**: §1 below covers what's
already on disk, what the Phase 0 engine will produce, and the full catalog of
research questions the dataset is designed to answer. A reader cold-opening
this file should be able to understand the project scope from §1 alone, then
read §2+ for the analytical strategy.

---

## 1. Project context: data on disk, engine outputs, and question catalog

### 1.1 Raw data inventory (already pulled, validated, on disk)

Phase 0 ingest is complete. Every piece below is in `data/` with file-level
Parquet metadata stamps and 67 cross-table integrity checks passing
(`scripts/validate_reference_data.py`).

**Equity bars** — primary input
```
data/bars_1m_raw/YYYY-MM-DD.parquet
```
- 2,513 trading days, 2016-06-08 → 2026-06-05.
- 1-minute OHLCV for every ticker that traded each day, ~5,000-8,000 names per day.
- Pre-market 04:00–09:30 ET, RTH 09:30–16:00, after-hours 16:00–20:00 — full
  extended-hours coverage.
- Includes SPY, QQQ, IWM (index ETFs needed for `market_context_daily`).
- Per-day storage, sorted `(display_symbol, t)`. Read pattern is day-major;
  rename resolution via `figi_map` at read time.
- Bars are **raw unadjusted tape**; split adjustment is applied at read time
  by `BarReader` against a pinned `splits.parquet` snapshot. Dividends are
  stored as events, not folded into prices.

**Reference data** — 11 Parquet tables under `data/reference/`

| File | Rows | Purpose |
|---|---|---|
| `tickers.parquet` | 11,801 | Raw universe (active + delisted), CS only |
| `tickers_enriched.parquet` | 11,801 | + FIGI fill-in (79% active, 43% delisted), + `delisted_utc` |
| `tickers_classified.parquet` | 11,801 | Full details (SIC, market_cap snapshot, list_date, employees, shares outstanding) |
| `figi_map.parquet` | 6,480 | `(security_id, display_symbol, valid_from, valid_to)` — rename windows |
| `ticker_events.parquet` | ~12k | Raw rename events (FB → META, etc.) |
| `splits.parquet` | 26,961 | Historical splits, with `splits_snapshot_date` pin |
| `dividends.parquet` | 709,125 | Cash dividends, joined as events (NOT folded into bars) |
| `short_interest.parquet` | 3.7M | FINRA bi-weekly short interest, 2017-12 → 2026-05 |
| `financials.parquet` | 251,549 | SEC filings (raw); 10-Q/10-K, used to derive earnings calendar + per-day market cap |
| `acceptance_datetime_backfill.parquet` | 189,632 | SEC EDGAR backfill for missing acceptance timestamps (99.8% coverage) |
| `vix_daily.parquet` | 2,650 | Daily VIX close, from FRED |

**What's NOT on disk and won't be in Phase 0** (RFC explicitly defers):
- Float / historical shares outstanding (proxy via fundamentals when needed)
- Index membership history (S&P 500, Russell, etc.)
- Catalyst events (FDA, M&A, analyst revisions, guidance, offerings)
- News feed
- Tick data / NBBO quotes (also not entitled on current Massive plan)
- Options chain / implied vol

These are explicitly out-of-scope per the RFC; the schema reserves joins for
them as future Phase 1+ additions.

### 1.2 Phase 0 engine outputs (8 Parquet tables the engine will write)

Once the writer engine ships (~5-6 weeks of work from now), these tables are
produced by deterministic aggregation over the raw data above. No further API
calls.

| Table | Grain | Approx size | What's in it |
|---|---|---|---|
| `daily_observation.parquet` | (day, security_id) | ~50-85 GB | Bars + intraday signal snapshots + first-30m/first-hour shape + premarket + overnight context + rolling vol/liquidity/betas + earnings proximity + data-quality flags + calendar |
| `market_context_daily.parquet` | (day) | <200 MB | SPY/QQQ/IWM intraday + EOD + overnight gaps + VIX + breadth (A/D, %-green, mover counts) |
| `forward_outcomes.parquet` | (day, security_id, entry_offset) | ~200-300 GB | 17 entry offsets × 13 horizons × 9 outcome stats + 13×7×2 fixed-% crossings + 13×6×2 ATR crossings + day-0 segments + next-day + gap-vs-RTH days 1-5 + materialized target-before-stop labels + terminal events (615 columns total) |
| `forward_path_short.parquet` | (day, security_id, entry_offset, checkpoint) | ~150-220 GB | 23 path checkpoints (1m → 5d_close) per trade, with running ret / high_ret_so_far / drawdown / bars_elapsed / pct_bars_profitable_so_far |
| `regime_definitions.parquet` | (day, taxonomy, regime_id) | <50 MB | 5 taxonomies: era, spy_trend, vix_level, breadth, liquidity |
| `earnings_calendar.parquet` | (security_id, earnings_date) | <100 MB | Derived from `financials.parquet` + EDGAR backfill; point-in-time via `announced_at_date` |
| `sector_aggregates_daily.parquet` | (day, sector_id) | <100 MB | Sector-level intraday + EOD returns, %-green, rank |
| `security_classification_daily.parquet` | (day, security_id) | ~3-5 GB | Per-day point-in-time tags: ticker_type, sector/industry/sub_industry, market_cap_bucket, liquidity_bucket, price_bucket, volatility_bucket, style_bucket, plus 10 behavioral tags (is_mega_cap_tech, is_biotech, is_low_float_candidate, is_meme_candidate, etc.) |

**Total output: 450-650 GB.** Lives on the research machine; never
redistributed. Schema is frozen at v6 with versioned-metadata stamps so
downstream readers refuse mismatched joins.

### 1.3 Question catalog — what the dataset is designed to answer

The 13 original question categories + 6 additional ones I'd flag as worth
asking. Each row points at the algorithmic layer that answers it (full
algorithm explanation in §7).

**Notation:** *MI* = mutual information (Layer 1), *BN* = Bayesian network
structure (Layer 2), *DP* = Bellman dynamic programming (Layer 3), *HB* =
hierarchical Bayes (Layer 4). *XGB* = gradient-boosted trees for Phase 4
meta-labeling.

#### 1.3.1 Entry timing optimization
| Question | Algorithm | Strategy answer |
|---|---|---|
| Optimal entry time between 9:35 and 15:30? | DP | `π*(state) → entry_offset`; varies by state, not "10:00 always" |
| Does 10:00 vs 9:45 change win rate? | DP + MI | Compare `V(state, 1000)` vs `V(state, 0945)` per cell; MI on `(entry_offset, ret)` |
| Is there a "don't trade" window? | DP | Offsets where `V < cost` for all states |
| Does optimal entry vary by security type? | DP per cell + HB | Per-classification policy + shrinkage |
| Does pre-entry volume predict better entry? | MI + BN | `I(pre_entry_volume; ret \| entry_offset)` |

#### 1.3.2 Exit rule design (profit targets / stops)
| Question | Algorithm | Strategy answer |
|---|---|---|
| Would +1% target have hit before -0.5% stop? | DP | Derived from policy + transition probabilities |
| Optimal profit target for 1-day holds? | DP | State-conditional target, not fixed |
| Fixed-% stop vs ATR-based stop? | DP | Compare V under each as state variable |
| Does trailing stop improve results? | DP | `max_ret_so_far` is a state var; trailing falls out |
| P(+2% before -1% within 5 days)? | DP / direct label | Materialized label + DP transition probabilities |
| Move stop to breakeven after +1%? | DP | Special case of state-dependent policy |

#### 1.3.3 Holding period selection
| Question | Algorithm | Strategy answer |
|---|---|---|
| 1d vs 5d Sharpe? | DP across horizons | `V(state, 1d)` vs `V(state, 5d)` |
| Best horizon by security class? | DP per cell | `argmax_horizon V(state)` per classification |
| Do gains reverse after day 3? | DP value over time | Where `V` decreases as `t` increases |
| Optimal holding for post-earnings drift? | DP + earnings as state | `π*` conditional on `days_since_earnings` |

#### 1.3.4 Security type / classification conditioning
| Question | Algorithm | Strategy answer |
|---|---|---|
| Momentum: mega-cap tech vs biotech? | DP per cell + HB | Separate policies, shrinkage on differences |
| Are leveraged ETFs tradeable? | DP + cost-adjusted reward | Policy refuses if `V < 0` after costs |
| ADRs gap differently? | Conditional MI + BN | `I(next_day_gap; is_adr)` |
| Low-float: bigger moves, higher costs? | DP per `liquidity_bucket` | Net of costs in reward |
| Market cap affects momentum persistence? | DP per `market_cap_bucket` | Per-bucket policy comparison |
| Recent IPOs too risky? | DP per `is_recent_ipo` | Policy refuses if vol too high |

#### 1.3.5 Market regime interaction
| Question | Algorithm | Strategy answer |
|---|---|---|
| Momentum only when VIX < 15? | DP + VIX in state | Policy branches on VIX if real |
| SPY trend matter? | DP + regime in state | Policy branches on trend if real |
| Breadth predictive? | MI + BN | `I(breadth_ad; ret)`; check BN edges |
| Momentum fades in high vol? | DP per vol bucket | V comparison across buckets |
| Small caps in certain regimes? | DP + `(cap × regime)` state | Conditional policy |

#### 1.3.6 Intraday path / shape analysis
| Question | Algorithm | Strategy answer |
|---|---|---|
| Positive early then fade? | DP value function | `V(state, t)` shape over time |
| Typical path to +1%? | `forward_path_short` + DP | Empirical trajectory + expected stopping time |
| Time to hit profit target? | DP | `E[τ \| π*]` |
| Dead zone after entry? | DP value heatmap | Low-V regions in (current_ret, bars_elapsed) |
| Trade recovers after drawdown? | DP transition probs | `P(reach +X% \| current_ret < -Y%)` |
| Max consecutive losing bars? | Direct stat | `max_consecutive_bars_underwater_<H>` column |

#### 1.3.7 Pre-entry / signal quality filters
| Question | Algorithm | Strategy answer |
|---|---|---|
| Pre-market volume predicts intraday? | MI + BN | `I(premarket_volume; ret_<H>)`; check BN edges |
| Require gap up (>1%)? | MI + DP | Add `overnight_gap` to state, see if policy uses it |
| First 30-min shape matters? | MI + BN | MI on shape features; BN structural test |
| Avoid stocks at recent high? | MI + DP | `pre_entry_ret_from_high` in state |
| Wick size predicts slippage? | MI | `I(wick_pct; entry_slippage_proxy)` |
| Order size feasible? | Direct | `entry_participation_capacity_*` columns |

#### 1.3.8 Gap vs RTH decomposition
| Question | Algorithm | Strategy answer |
|---|---|---|
| Edge from gaps or intraday? | BN structure | Which is the parent of `ret_<H>` in DAG |
| Gaps reverse next day? | Conditional MI | `I(next_day_gap; overnight_gap)` sign |
| Fade open after large gap? | DP + gap in state | Policy on `next_day_fade_from_open` |
| Holding overnight improves returns? | DP comparison | `ret_to_close` (intraday) vs `ret_1d` (close-to-close) |

#### 1.3.9 Cost / execution realism
| Question | Algorithm | Strategy answer |
|---|---|---|
| Likely slippage? | Direct | `entry_slippage_proxy_bps` distribution per cell |
| Trade without moving market? | Direct | `entry_participation_capacity_*` per cell |
| Costs kill edge for low-price? | DP per `price_bucket` | Cost-adjusted V per bucket |
| Avoid 9:35 due to spreads? | MI + DP | MI on `(offset, slippage)`; DP uses it |

#### 1.3.10 Time underwater / psychological
| Question | Algorithm | Strategy answer |
|---|---|---|
| Fraction of holding profitable? | Direct | `pct_bars_profitable_<H>` distribution |
| Mostly underwater? | Direct | `pct_bars_underwater_<H>` distribution |
| Longest losing streak in bars? | Direct | `max_consecutive_bars_underwater_<H>` |
| If stopped at -2%, would recover? | DP / counterfactual | `P(max_runup > 0 \| min_ret < -0.02)` from path data |

#### 1.3.11 Event-driven effects
| Question | Algorithm | Strategy answer |
|---|---|---|
| Avoid 5d before earnings? | DP + days_to_earnings in state | Policy refuses if true |
| Post-earnings drift? | DP + earnings_calendar join | Conditional V analysis |
| Splits create inefficiencies? | MI + conditional DP | Around `split_event_nearby` flag |
| Dividend events matter? | MI + DP | Around `dividend_event_today` |

#### 1.3.12 Multi-horizon / rolling analysis
| Question | Algorithm | Strategy answer |
|---|---|---|
| 1-day predicts 5-day? | MI | `I(ret_1d; ret_5d)` |
| Rebalance daily or hold? | DP across horizons + costs | V comparison with cost-adjusted reward |
| Optimal stop changes over time? | DP | Policy `π*(s, t)` is already time-varying |

#### 1.3.13 Meta-labeling (ML)
| Question | Algorithm | Strategy answer |
|---|---|---|
| Features predict signal success? | XGB on DP residuals | Phase 4 meta-label classifier |
| VIX × sector interaction? | XGB | Tree models learn interactions natively |
| Filter 80% losers, keep 60% winners? | XGB precision-recall | Phase 4 evaluation metric |

#### 1.3.14 (NEW) Cross-signal independence + portfolio-level
| Question | Algorithm | Strategy answer |
|---|---|---|
| How correlated are same-day signals' outcomes? | Eigendecomp of `forward_outcomes` correlation matrix | Number of "real" independent bets per day |
| If 50 signals fire, how many bets do I have? | PCA on outcome covariance | Effective N via 1/Σ(λᵢ²) over normalized eigenvalues |
| Does signal concentration today predict quality? | MI + BN | `I(signal_concentration_today; expected_ret)` |
| Max concurrent positions before correlation eats edge? | Markowitz-style portfolio opt | Phase 5 layer; solver chooses position count |

#### 1.3.15 (NEW) Regime stability + persistence
| Question | Algorithm | Strategy answer |
|---|---|---|
| How long do regime classifications last? | Survival analysis on regime durations | Median + tail of regime length |
| Does VIX regime flip predict signal failure? | DP across regime transitions | V before/after flip |
| Sector momentum cycles correlated across years? | MI between years per sector | Cross-era stability matrix |
| Half-life of a tradeable regime? | Survival analysis on V(state) | When does V cross zero from positive |

#### 1.3.16 (NEW) Microstructure / order flow proxies
| Question | Algorithm | Strategy answer |
|---|---|---|
| Does opening auction imbalance predict 10:00 strength? | Proxy via first-1m bar range; MI | `I(intraday_first_1m_range; intraday_ret_0930_to_1000)` |
| First-5m volume spikes affect 1-hour outcomes? | MI on `bar_count` features | Conditional MI given offset |
| Premarket volume concentration in first 15m? | MI on share fields | `I(intraday_first_15m_volume_share_of_first_30m; ret)` |
| Wide vs tight first-5m range — different forward distributions? | Quantile regression | 5/50/95 percentile of forward ret by range bucket |

#### 1.3.17 (NEW) Counterfactual / "what if" analysis
| Question | Algorithm | Strategy answer |
|---|---|---|
| What if I'd entered on the OPPOSITE signal (mean reversion)? | Mirror DP with negative signal | Compare V to confirm directionality |
| For stopped-out trades, what if I'd held? | Path data after stop | Distribution of `ret_remaining \| stop_hit` |
| My strategy vs random entry timing? | DP V vs random-baseline V | Sharpe of edge over random |
| Edge vs buy-and-hold SPY on same days? | DP V minus SPY-return-on-same-days | Excess Sharpe vs buy-hold benchmark |

#### 1.3.18 (NEW) Robustness / sensitivity
| Question | Algorithm | Strategy answer |
|---|---|---|
| How does Sharpe change at 5 / 10 / 20 bps costs? | DP with cost-sweep | V curve as function of cost assumption |
| Sensitivity to parameter changes? | DP with state-bin perturbation | V stability across discretization choices |
| If 1 minute late to enter, what happens? | DP shifted entry | V at `entry_offset + 1m` |
| Robustness to classification threshold changes? | HB with threshold perturbation | Posterior shift under threshold edits |

#### 1.3.19 (NEW) Realistic scaling / capacity
| Question | Algorithm | Strategy answer |
|---|---|---|
| At what AUM does slippage eat the edge? | DP with size-dependent cost | V curve vs trade size |
| Max AUM per cell that preserves Sharpe? | Cost-adjusted DP per cell | Capacity by classification |
| Which cells scale best? | Cross-cell DP comparison | V flatness as function of size |
| Where does liquidity cap kick in? | `entry_participation_capacity` × frequency | Practical AUM per cell |

#### 1.3.20 (NEW) Edge decay / non-stationarity
| Question | Algorithm | Strategy answer |
|---|---|---|
| Has the edge decayed over the data window? | DP per era + V comparison | V(era_1) vs V(era_4) for same cells |
| Are there days the signal is "too crowded"? | MI on `signal_concentration_today` | High-concentration days as separate cohort |
| Did the meme era (2020-2021) artificially inflate edges? | Era-conditional DP | V(meme_era) vs V(other_eras) |
| When should we suspect the edge is dying? | Sequential change-point detection | Bayesian online change-point on V time series |

### 1.4 Overall reasoning — why this combination is well-designed

Three principles tie the data, the engine, and the questions together:

**A. Record everything, decide downstream.** The 615 columns × 17 offsets ×
23 checkpoints schema deliberately over-records so that no decision is baked
into the engine. Every entry timing, exit rule, holding period, target/stop
combination is a query-time choice, not an engine-time hardcode. This means
all 20 question categories above can be answered without rerunning the
~5-6 weeks of engine work.

**B. Condition on classification before pooling.** The relaxed universe
deliberately includes ETFs, ADRs, biotech micro-caps, leveraged ETFs, post-
bankruptcy artifacts. Pooling these without `security_classification_daily`
slicing produces fake edges that are actually security-type-mixing artifacts.
The structural defense in the schema (mandatory classification join per RFC
§14.4) is what separates this project from the typical retail momentum
backtest that "discovers" an edge that's actually 90% leveraged-ETF decay
in disguise.

**C. Theoretically grounded algorithms per sub-problem — *under named
assumptions*.** Each question category maps to an algorithm with a clean
theoretical justification (MI for non-linear dependence screening, BN
structure learning for conditional-independence DAG discovery, Bellman DP
for sequential decisions inside a well-specified MDP, hierarchical Bayes
for cell-level shrinkage under partial pooling). No "throw XGBoost at it
and hope." But **none of these is "provably optimal" unconditionally** —
each result depends on assumptions that financial data routinely strains
(stationarity, faithfulness, no hidden confounders, Markov property,
adequate state representation). §7.7 spells out the assumptions and §7.7.1
spells out the required anti-overfitting discipline. The reason to use this
stack is not that it can't fail — it's that it fails *honestly* (with
diagnosable assumption violations) rather than *silently* (like
grid-searching the best cell).

The result: a research engine that, by construction, gives defensible
answers to all 20 question categories above, with statistical confidence
bands, conditional on classification + regime + era. The output is a
*characterization* of where momentum works in 2016-2026 US equities, not
a single backtested strategy. Three valid endpoints (positive cell found,
characterized null, inconclusive) — all three are wins because the dataset
survives the answer.

### 1.5 Headline rule (the single most important constraint)

The search space across the dataset is enormous:

> 615 columns × 17 entry offsets × 23 path checkpoints × 13 horizons ×
> ~10 classification slices × 5 regimes × 4 eras

Brute-force "find the best cell" against that space yields false discoveries
at industrial scale — at α = 0.05, you'd expect thousands of "significant"
cells by pure chance. The non-negotiable rule that gates every reported
finding in Phase 1:

> **Every reported positive finding must survive (a) Benjamini-Yekutieli FDR
> control at α = 0.10 across the explicit discovery cell family,
> (b) era-consistency check on the exploration set, (c) excess-return-over-
> index re-derivation, (d) bootstrap-day-clustered CI lower bound above zero,
> AND (e) untouched-holdout confirmation at α = 0.05 with BY FDR control
> across the pre-registered family. A finding that fails
> any one of these is treated as null.** Concrete procedure in §7.7.1.

This rule subsumes most of §2's reframings and §7.7.1's safeguards. If you
remember nothing else from this document, remember this rule.

### 1.5.1 Power reality — the family size and the Sharpe hurdle collide

Computed before any data is touched: the exploration set is ~1,130 trading
days. Day-clustered (which is correct — cross-sectional breadth doesn't
rescue effective N, because each cell collapses to a daily portfolio
return series), a cell's t-stat ≈ annualized Sharpe × √years. A true
Sharpe-0.5 cell therefore shows t ≈ 1.06 over 4.5 years — it cannot clear
even an *uncorrected* α = 0.05. BY across the full ~154,700-cell cross
(log-penalty H_m ≈ 12) pushes the per-cell threshold to roughly p ≈ 5×10⁻⁶,
i.e. detectable edges of annualized Sharpe ≈ 2. Hunting Sharpe-0.5 edges
with a test that can only see Sharpe-2 edges guarantees a "characterized
null" regardless of truth.

Two structural consequences, specified concretely in §7.7.1:

1. **The primary BY family is coarse** — `(classification × regime)`,
   ~200–1,000 hypotheses, each scored on a *pre-specified* offset/horizon
   aggregation (surface mean over a pre-declared region, never the max).
   Finer slices (era, entry_offset, horizon) are exploratory drill-down
   run only on coarse survivors, under hierarchical FDR (Yekutieli 2008)
   so the guarantee is preserved.
2. **The hierarchical-Bayes posterior (§7.4) and the surface-smoothness
   criterion (§2.4) are the primary discovery instruments** — they share
   strength across neighboring cells instead of testing 154,700 islands.
   BY is the final gate over a small family, not the search mechanism.

A minimum-detectable-effect pass is step −1 of the §7.8 implementation
order; if the detectable floor sits far above the §2.8 hurdle for most of
the family, the family is shrunk or coarsened *before* discovery, not
after.

### 1.6 Constant audit — what's frozen, what's swept, what's learned

Every quantity that affects Phase 1 output falls into exactly one of three
categories. Confusing them is the most common quiet route to overfitting.

**A. Frozen design constants (chosen before discovery, never tuned to
rescue findings):**

| Constant | Value | Locked because |
|---|---|---|
| Schema version (`daily_observation_version`, `forward_path_checkpoints_version`, etc.) | v2 | Stamped in Parquet metadata; readers refuse mismatched joins |
| Entry-offset grid | 17 offsets (v1) | RFC §0.6; bumping requires schema regen |
| Forward horizon set | 13 horizons (v2) | RFC §0.6 |
| Path checkpoint set | 23 checkpoints (v1) | RFC §0.6 |
| Fixed-% threshold set | ±0.5/1/2/3/5/10/20 (v2) | RFC §0.6 |
| ATR-multiple thresholds | ±0.25/0.5/1/1.5/2/3 (v2) | RFC §0.6 |
| Train/validate/holdout split dates | 2016-06→2020-12 / 2021-01→2022-12 / 2023-01→2026-06 | §2.5 |
| BY α (discovery) | 0.10 | §7.7.1 |
| Holdout α | 0.05 | §7.7.1 |
| Target Sharpe (required-edge hurdle) | 0.5 (default) | §2.8 |
| Materialized target-before-stop label set | 9 pairs (v6) | RFC §9 |
| Primary BY discovery family | coarse: (classification × regime) | §1.5.1 power analysis; finer slices = drill-down under hierarchical FDR |
| Signal definition(s) under test | pre-specified before discovery (default: `intraday_ret_0930_to_1000 > 0`) | Each variant tried multiplies the §7.7.1 family; an untracked signal sweep is the one multiplicity door otherwise left open |
| Trade direction | long-only | Borrow costs / locate availability out of scope by decision, not omission |
| Max pre-registered holdout hypotheses | 20 | §7.7.1; fixes the holdout family size before discovery ends |
| Holdout FDR | BY at α = 0.05 across the pre-registered family | §7.7.1 |

**B. Sensitivity-sweep constants (have a default, swept for robustness, sweep
results reported alongside the primary finding):**

| Constant | Default | Sweep |
|---|---|---|
| Cost model (bps) | cost-side derivation per §6.1 (spread proxy + impact + commission) | 2–30 bps in 2-bps steps |
| DP state bin count | 10 per continuous dimension | 5, 10, 15 (§7.7.1) |
| Minimum transitions per (s, a) for trusted DP estimate | 30 | 15, 30, 60 |
| Regime bucket thresholds (VIX, breadth, liquidity) | tertiles of exploration set | ±5 pp percentile shift |
| BN stability-selection threshold | 80% of bootstrap resamples | 70%, 80%, 90% |
| Bootstrap CI level (day-clustered) | 95% | 90%, 95%, 99% |
| Required-edge hurdle (for robustness only) | Sharpe ≥ 0.5 | {0.3, 0.5, 0.7} |
| Edge-trajectory smoothing half-life (§2.9) | 63 trading days | {21, 63, 126} |

Sweep results are **reported alongside** the primary finding, never used to
pick the favorable parameter. If a finding survives only at one sweep
parameter, it's flagged as fragile and not pre-registered.

**C. Learned quantities (outputs of the pipeline; never inputs):**

- MI matrix (`mi_matrix.parquet`)
- BN structure per era + structural-edge set
- DP policies per cell × era
- Hierarchical posterior over cell means
- Cost-adjusted edge per cell
- Pre-registered hypothesis set

**The audit rule:** *no constant in category A or B may be changed after
seeing real-data results unless the schema/config version is bumped and
prior results are invalidated.* Discovering a finding, then "noticing" that
a different cost assumption or bin count would have helped, is post-hoc
selection — bumps version, restarts the discovery clock.

### 1.7 Project-level stop rule

The plan defines three valid endpoints (§1.4), but an endpoint is only
reachable if effort is bounded. Pre-committed budget:

- **One full Phase 1 cycle = the §7.8 sequence, time-boxed at ~6 weeks of
  analytical work** after the engine ships (the §7.8 estimate plus slack;
  the live-shadow tail runs on calendar time and doesn't count).
- **Maximum two cycles.** A second cycle must be justified by specific,
  written lessons from the first (new schema columns, a redefined family —
  with version bumps per §1.6). "Let's look harder" is not a justification.
- At the end of cycle 2, **write the characterization** — positive cells,
  blacklist, ceiling map, whatever it turns out to say — and move on:
  Phase 2 baselining if cells survived, Synthetic Market Hive on top of
  the characterization either way.

Refining methodology is more comfortable than confronting noisy data; this
section exists because that failure mode is real for a solo project. It is
the reason the Status header declares this document feature-frozen.

---

## 2. Ten reframings (ranked by leverage)

### 2.1 Two-stage: regime classifier first, then signal

**Biggest single lever.** Most retail momentum strategies have ~50-55% hit rate
hovering at break-even after costs. The reason isn't bad signals — it's that
they fire every day, including the ~30% of days when momentum is structurally
broken (high-VIX whipsaws, FOMC days, opex pin, low-volume holiday weeks).

**Reframe:**
- **Stage 1:** is *today* tradeable? Output: `tradeable / sit_out`.
- **Stage 2:** within tradeable days, which names?

**Stage 1 design rules — non-negotiable to prevent leakage:**

1. **Pre-decision features ONLY.** Stage 1 must use only information
   knowable by the entry decision time (defaults: pre-10:00 ET for the
   standard 10:00 entry). Approved inputs:
   - Prior-day VIX close (FRED `VIXCLS` is close-only and is the ONLY VIX
     field on disk; premarket VIX / VIX futures open would need a new
     ingest and stay on the deferred list — do not spec features against
     data that doesn't exist)
   - SPY overnight gap (prior close → today's open)
   - First-1m breadth at 09:31 (count of universe stocks up vs down)
   - Premarket dollar volume of universe (04:00–09:30)
   - Prior-day shape (last-30m return, last-30m volume share)
   - Calendar features (day of week, is_fomc, is_opex, holiday week)

   **Forbidden inputs:** VIX close, EOD breadth, full-day SPY return,
   any aggregate that requires data from after the entry decision. If a
   feature could only be known at 16:00, it can't be in Stage 1.

2. **Simple model, not XGBoost.** Use logistic regression with 3-6
   pre-specified features. A complex model on daily features memorizes
   which specific days in the exploration set were bad and doesn't
   generalize. High precision matters more than recall — you'd rather miss
   a few good days than trade many bad ones.

3. **Validate on the validation split (per §2.5)** before trusting:
   "tradeable" days should have meaningfully higher Sharpe-per-trade than
   "sit_out" days on the validation set, and the gap should hold on the
   holdout (only checked once, at pre-registration close).

4. **The "tradeable" label itself must be derivable from pre-decision
   information.** Don't define "tradeable day = day where my Stage 2
   strategy worked well." That's looking ahead. Define it as "day where
   median liquid-name had |intraday_excess_return| > median across exploration
   sample" — a property of the day, computable from features Stage 1 also
   sees.

5. **Output a probability, not just a binary.** The Phase 1 decision rule
   stays binary (`trade / sit_out` at a pre-specified threshold), but the
   predicted probability is recorded for every day. Phase 2 position
   sizing proportional to `P(tradeable)` dominates the threshold rule
   under any reasonable utility, and the calibration curve over time is
   the natural detector for classifier decay.

If stage 1 is even 70% accurate at filtering bad days, the hit rate on
remaining days jumps 5-10 percentage points, which on a 1-to-3 R/R structure
flips the strategy from break-even to clearly positive. All approved inputs
live in `market_context_daily.parquet`.

---

### 2.2 Negative space first

Spend Phase 1's first two weeks finding cells with statistically significant
**negative** expectancy after costs, not positive ones.

Why this beats the positive-edge hunt:
- A "don't trade in regime X" filter is more valuable than a "trade Y" signal
  because the filter compounds across every future strategy variant.
- The asymmetry is decision-theoretic, NOT statistical. Selecting the
  most-negative cells out of the full family is the same
  multiple-comparisons problem with the sign flipped, so the blacklist
  runs through the same FDR machinery (§7.7.1). What differs is the cost
  of being wrong: a false-positive blacklist entry costs a little foregone
  opportunity; a false-positive whitelist entry costs real money.
- "Don't touch leveraged ETFs / China ADRs / first-30-min sub-$5 names" might
  be 90% of the actual realized alpha — pure friction reduction.

A characterized **blacklist** is durable, transferable, and almost certainly
produces better expectancy than any positive signal you'll find unconditionally.

---

### 2.3 Excess return over index as the primary metric

Use `ret_<H>_excess_{spy,qqq,iwm}` as the default lens, not raw `ret_<H>`.

A stock returning +2% when SPY returns +2% has zero idiosyncratic alpha — you
just collected market beta. Most "momentum strategies" that look great in
backtests are actually long-beta wrappers that die in 2008/2020/2022 because
the strategy was a leveraged-long-equity bet in disguise.

Default Phase 1 rule: every conditional expectancy reported both ways (raw + 
excess). If excess return is statistically zero but raw return is positive,
the strategy is dying the moment beta does.

---

### 2.4 Edge surface, not edge point

When testing any (entry × horizon × cell) combination, plot the **entire 2D
surface** of expected return as a function of `(entry_offset, holding_horizon)`
within that cell.

Real edges show up as **smooth gradients** — return rises monotonically as
entry delays from 9:35 → 10:10, peaks, then decays. Fake edges show up as
**isolated spikes** with random neighborhoods.

Reject any "edge" whose surface doesn't have a smooth, internally-consistent
neighborhood. This single heuristic kills 80% of multiple-comparisons false
discoveries — and it costs one heatmap per cell.

---

### 2.5 Era-conditional analysis is mandatory — but era split ≠ train/test split

Treat the 10-year window as structurally heterogeneous. There are **two
separate axes** that often get conflated, and the conflation is dangerous:

**Eras** (descriptive labels for regime conditions, used to *characterize*
findings):
- 2016-06 → 2019-12: late ZIRP bull
- 2020-01 → 2022-06: COVID + stimulus + meme era
- 2022-07 → 2024-12: rate-shock bear + AI bubble
- 2025-01 → 2026-06: most recent

**Train/test partition** (used to *prevent leakage* in any model fit):
- **Exploration set:** 2016-06 → 2020-12 (~4.5 years)
- **Validation set:** 2021-01 → 2022-12 (~2 years)
- **Holdout set:** 2023-01 → 2026-06 (~3.5 years), **untouched until
  pre-registered hypotheses are frozen**

The eras describe *what the market was like*; the partition controls *what
data you're allowed to learn from*. **A single Phase 1 cell may be analyzed
per-era only within the exploration set, then validated on the validation
set, then confirmed on the holdout set.** Analyzing the 2025-01 → 2026-06
"era" during discovery is the same thing as touching the holdout — that's
the failure mode this section exists to prevent.

In practice:
- Era-conditional descriptive stats may **only** be computed inside the
  current open set (exploration during discovery; +validation during
  pre-registration; +holdout only after hypotheses are frozen).
- **Known weakness — the era axis and the split axis are confounded.**
  Exploration ≈ ZIRP bull, validation ≈ meme + rate-shock, holdout ≈ the
  most recent era. So "doesn't generalize across splits" and "doesn't
  generalize across regimes" are indistinguishable: a real-but-
  regime-dependent edge fails validation, and a holdout failure can't
  separate "never real" from "died." Because of this, **anchored
  walk-forward with an embargo gap is the PRIMARY validation design**,
  not an optional variant: train on `[start, t-1]`, skip an embargo of
  ~10 trading days (forward horizons reach 5d; the embargo prevents
  overlap leakage), evaluate on `t`, advance, repeat across the
  exploration + validation window. It runs as §7.8 step 6.5 and is the
  primary instrument for *stability* claims under the §5.0 hierarchy. The
  static three-way partition is retained for exactly one job: the
  2023-01 → 2026-06 holdout as the untouched confirmation set.

A signal that's "positive on average 2016-2026" while being negative in 3
eras and strongly positive in 1 ≠ a strategy — it's "I was lucky to
backtest the meme era." That diagnosis comes from era-conditional analysis
inside the exploration set, not from peeking at the holdout.

---

### 2.6 Cross-sectional and time-series momentum in parallel

The frozen spec is time-series momentum (this stock up vs its own open).
The data also supports cross-sectional momentum (this stock up the most in
its sector / market today).

Different conditional structures:
- TS momentum works in trending regimes, fails in mean-reverting ones.
- CS momentum works in dispersion regimes (variance across names), fails when
  correlations are high.

Inputs you already have:
- TS: `intraday_ret_0930_to_1000`
- CS: `intraday_ret_0930_to_1000_rank_today`, `intraday_ret_0930_to_1000_percentile_today`,
  plus the whole `sector_aggregates_daily.parquet` for sector-relative.

Run Phase 1 on both in parallel. They may have low correlation — if so, a
blended signal materially outperforms either alone.

---

### 2.7 Survival analysis for exits — competing risks, not single-event KM

The standard approach to exit-rule research is "compute mean return at horizon
H." That's the wrong math for asymmetric exits.

Right math: **competing-risks survival analysis.** For each trade, there are
THREE possible events that end the path observation:
1. **Target hit** (e.g., return ≥ +X%)
2. **Stop hit** (e.g., return ≤ −Y%)
3. **Time-based exit** (end of horizon — EOD, 1d, 5d, etc. — without
   either threshold being hit; right-censored at the horizon boundary)

Treating this as a single-event curve mis-states the result — naive
Kaplan-Meier on "time to target" treats stop-hit observations as if they
were merely censored, which inflates the estimated target-hit probability.
The right tool is **competing-risks cumulative incidence functions
(Aalen-Johansen estimator)** — one CIF per event type, with proper joint
handling of the three competing exit events. For semi-parametric modeling
with covariates, **Fine-Gray subdistribution hazards** is the standard
choice.

**Required schema addition:** `forward_path_short` must include a
`censor_type` column (`target_hit / stop_hit / time_exit / still_open`) per
(day, security_id, entry_offset, path_checkpoint) to support competing-risks
fitting. Without this, the trade-ending event is ambiguous and the CIFs
can't be computed correctly. Add this in §3.

**Model choice — discrete-time hazards over Cox PH.** Cox proportional
hazards assumes a proportional baseline hazard, which intraday bar-level
data routinely violates (hazard rates are sharply non-stationary across the
session — opening-range volatility, midday lull, power hour). Use a
**discrete-time competing-risks hazard model** (multinomial logistic at each
bar checkpoint, conditional on survival to that checkpoint, outputting
`P(target_hit | s, t)`, `P(stop_hit | s, t)`, `P(survive | s, t)`).
Aalen-Johansen CIFs remain useful for descriptive plots; the model layer is
where competing risks must be respected.

Two CIFs per cell:
- `CIF_target(t)` = P(target hit by bar t, ignoring stop)
- `CIF_stop(t)` = P(stop hit by bar t, ignoring target)

The crossover point — where the marginal hazard of `target_hit` equals the
marginal hazard of `stop_hit` — is one principled stop-placement criterion.
The other is to maximize cost-adjusted expected return: integrate the CIFs
over the cost-adjusted reward function. The two need not agree; the user's
risk preference picks between them.

**Important caveat:** survival analysis doesn't eliminate threshold
selection bias — you still pick the (+X%, −Y%) thresholds. The fix is to
**pre-specify a small set of
economically motivated thresholds** (e.g., ±0.5 / 1 / 2 ATR, plus
±cost-of-trade) **before** looking at outcomes, and report results across
all of them. The materialized labels in `forward_outcomes.parquet` already
freeze a v6 threshold set; that's the pre-registration, by design.

---

### 2.8 Inverted question: required edge first

Before any strategy testing:

> "Given realistic costs of X bps per trade, slippage on the breakeven stop,
> opportunity cost of capital — what hit rate × average winner ÷ average loser
> do I need for Sharpe ≥ 0.5?"

Then count how many cells in the historical data *clear that hurdle* in-sample.
If the answer is zero, the strategy concept is impossible regardless of
optimization. If the answer is N > 0, those N cells become your pre-registered
hypotheses for the holdout test.

This inverts overfitting: instead of "test 154,700 cells, find the best," you
pre-specify the hurdle and FDR-control the count of cells that clear it.
Statistical power without selection bias.

**The target Sharpe is a frozen design constant (see §1.6).** It is chosen
before discovery and **never adjusted to rescue findings.** Default value:
0.5. For robustness only, a sensitivity sweep across {0.3, 0.5, 0.7} may
be run *after* the main analysis to characterize how cell counts vary with
the hurdle — but only the pre-registered hurdle (0.5) controls hypothesis
testing and the holdout decision. Discovering that "Sharpe ≥ 0.45 would
have made cell X pass" is exactly the post-hoc selection this rule
prevents.

---

### 2.9 Time is a first-class variable — edge as a trajectory, not a constant

A cell's edge is not a number; it's a time series. A strategy that was
viable in 2018 and dead by 2024 has a positive *average* edge over the
window and zero *current* edge — and the average is the wrong deliverable.
Era-conditioning (§2.5) treats this discretely; this reframing treats it
continuously.

**Reframe: every per-cell expectancy is estimated twice.**

1. **Static (pooled) estimate** — what the rest of this document computes.
   This stays the basis for all hypothesis tests, because it has the most
   statistical power (§1.5.1 — power is already the binding constraint).
2. **Dynamic estimate** — the same expectancy modeled as a latent
   time-varying state: a dynamic linear model / state-space model (Kalman
   filter on daily cell returns; `statsmodels` state-space, or a
   random-walk-mean model in `pymc`). Cheap to fit per cell.

The dynamic estimate yields three reportable quantities per cell:

- **`edge_now`** — the filtered estimate at the end of the open data
  window, with credible interval. This is the number you'd actually trade
  on.
- **`edge_trend`** — the local slope: growing, stable, or decaying.
- **`edge_half_life`** — if decaying, the time for the edge to halve.
  Connects directly to the §1.3.20 change-point analysis.

**Rules:**

- The pre-registered holdout gate (§7.7.1) remains the *static* test —
  the dynamic model has too few effective observations per window to gate
  on without destroying power. But every pre-registered cell REPORTS its
  trajectory, and a cell whose `edge_now` credible interval includes zero
  is flagged **"historically real, currently unverifiable"** — eligible
  for live shadow (§5 step 9), not for capital.
- The dynamic model obeys the same leakage rules as everything else:
  exploration set only during discovery.
- Smoothing constants are Category B sweep constants (§1.6): half-life
  default 63 trading days, swept over {21, 63, 126}.

This is also the honest frame for the project's own future: the live
shadow period (§7.8 step 10) is just the next points on the trajectory.
A strategy is never "validated once" — `edge_now` is re-estimated for as
long as it trades, and the §1.3.20 change-point detector is the
production kill switch.

---

### 2.10 Randomness as an explicit variable — the variance-attribution ledger

You cannot measure what the unknown *is*, but you can measure how big it
is. Treat "unexplained" as a first-class quantity every analysis must
account for, rather than an embarrassment to be narrated away.

**Reframe: every cell gets a variance-attribution ledger.** Decompose the
variance of cost-adjusted outcomes within the cell:

| Component | How it's estimated |
|---|---|
| Market / beta | factor regression vs SPY/QQQ/IWM (§4) |
| Sector / industry | `sector_aggregates_daily` join |
| Regime-conditional mean shift | `regime_definitions` join |
| Signal-conditioned mean (the "edge") | the conditional expectancy itself |
| Name-specific persistent effects | security fixed effects / hierarchical name terms (§7.4) |
| **Residual — measured randomness** | what's left; reported as a SHARE of total variance, not hidden in an error bar |

Two uses, one defensive and one offensive:

1. **Defensive — the noise null.** Any narrative explanation of cell
   behavior ("biotechs fade after the open because...") must beat the
   "it's residual noise" null quantitatively. If the component being
   narrated explains a trivial share of ledger variance, the narrative is
   decoration. **Unknown behavior is attributed to the residual by
   default** — that is the honest prior, and it is exactly the discipline
   that kills post-hoc storytelling.

2. **Offensive — the predictability ceiling.** The total mutual
   information between the full ML-safe feature set and the outcome
   (Layer 1, §7.1) upper-bounds what ANY model — present or future — can
   extract from the recorded features (via Fano's inequality: MI caps
   achievable predictive accuracy regardless of modeling cleverness).
   Report per cell:
   - `ceiling` — MI-implied maximum explainable share, corrected against
     the §7.7.1 permutation null (finite-sample MI is biased up);
   - `captured` — what the current best model actually explains;
   - `gap = ceiling − captured` — unexplored structure.
   A cell with a near-zero ceiling is **genuinely random with respect to
   everything we measure** — research effort stops there, no matter how
   tempting its point estimate looks. A large gap says the data contains
   structure the current models miss — that's where the remaining
   research budget goes.

The ledger turns "we don't know why it does that" from a shrug into a
number, and the number is decision-relevant twice over: the residual
share feeds position sizing (more noise → smaller size for the same
edge), and the ceiling map prioritizes the research queue.

**Caveat:** finite-sample MI estimates and variance decompositions are
themselves noisy. Ledger entries get the same bootstrap-day-clustered CIs
as everything else, and the ceiling is reported as a range, not a point.
One thing the ledger can NOT see: structure driven by variables nobody
recorded (news, order flow, positioning). The ceiling is conditional on
the feature set — "random given what we measure," not "random."

---

## 3. Schema additions to consider (no new ingest needed)

These are derivable from existing data. Adding them makes them first-class
conditioning variables / inputs for Phase 1.

### 3.1 Additions to `daily_observation`

| Field | Reason |
|---|---|
| `signal_first_in_N_days_{5,10,20}` | Repeated signals decay; freshness matters |
| `signal_concentration_percentile_today` (rank of count of universe stocks giving same signal / universe size that day) | Universe size varies day-to-day (5k–8k names); raw counts aren't comparable across days. Percentile is. |
| `signal_concentration_hhi_today` (Herfindahl across the signal-strength distribution) | Distinguishes "everyone slightly up" from "few stocks strongly up" — different forward dynamics |
| `pre_market_volume_spike_flag` (premarket > 5× 20d median) | News proxy without a news feed |
| `position_in_52w_range` (close / 52w_high) | Powerful conditioning variable, mostly free |
| `days_since_last_X_pct_move` for X ∈ {5, 10, 20} | Volatility-clustering signature |
| `consecutive_up_days_close_to_close` | Daily-frequency trend persistence |
| `gap_filled_today_flag` (did intraday low touch prior close?) | Tradeable structural pattern |

**Conditional structure note for `signal_concentration_*`:** high
concentration is ambiguous — it can mean either crowding (future reversal)
or high-conviction regime (continuation). The data tells you which, but
**you must condition on market direction** (`spy_ret_0930_to_1000`). Include
`signal_concentration × spy_ret` interaction in the §7.1 MI pass; don't
treat concentration as a one-sided filter.

### 3.2 Additions to `forward_path_short`

| Field | Reason |
|---|---|
| `censor_type` (dict_str: `target_hit / stop_hit / time_exit / still_open`) | Required for competing-risks survival analysis per §2.7. Without it, CIFs are mis-stated. |
| `volatility_within_trade` (running stddev of bar returns since entry) | Captures within-trade vol regime that's not Markov in raw `(ret, max_ret, drawdown)` state. See §7.3. |
| `rate_of_change` (`ret - ret_prev_checkpoint`) / `bars_since_prev_checkpoint` | Distinguishes slow-grind +1% from violent-spike +1% — different continuation dynamics. |
| `current_ret_over_atr_14d` | Vol-normalized current return; needed for ATR-based DP state. |

The last three address the Markov assumption gap flagged in §7.3 — raw
`(ret, max_ret, drawdown)` doesn't fully capture the path shape that drives
continuation probabilities.

**Implementation refinement (2026-06-11, see §3.7):** `censor_type` does
NOT land at path grain. The trade-ending event is a property of a
(trade, threshold-pair), not of a checkpoint, and between checkpoints the
target-vs-stop *ordering* is ambiguous — but the engine sees full
1-minute resolution, so it can resolve ordering exactly. It lands in
`forward_outcomes` as `first_event_<pair>` (dict:
`target_first / stop_first / neither / no_data`) for each of the 9 frozen
target-before-stop label pairs. The §2.7 competing-risks fit reads these;
arbitrary non-frozen pairs fall back to checkpoint-granularity derivation
from path columns (approximate, and reported as such).

### 3.3 Dividend adjustment for multi-day horizons (`forward_outcomes` → v2)

Bars are price-only and dividends are stored as events, not folded in
(RFC: single adjustment baseline). That makes every multi-day forward
return a **price return, not a total return** — a stock going ex-dividend
inside a 1d–5d horizon shows a spurious ~−0.3 to −0.5% return (typical
quarterly large-cap dividend). The distortion is the same order of
magnitude as the edges being hunted, and it is **systematic**, not noise:
concentrated in liquid, high-quality names (the most likely hosts of real
cells) and clustered in calendar time.

Two additions, both joins against `dividends.parquet` (no new ingest):

| Field | Reason |
|---|---|
| `ret_<H>_total` for multi-day H (dividend-added total return) | Primary multi-day return lens; price-only `ret_<H>` stays for continuity |
| `dividend_ex_date_within_<H>` (Bool per multi-day horizon) | Mandatory conditioning/exclusion flag wherever total-return columns are not used |

This is a Phase 0 engine question, and it is settled: **Decision
(2026-06-11) — the engine writes the total-return columns and ex-date
flags from day one.** A retrofit after the ~5-6 week engine build would
mean regenerating a 200-300 GB table; the join against
`dividends.parquet` at engine time is cheap. This lands as a line item in
the engine spec, not as a post-hoc v2 migration. Intraday horizons are
unaffected.

### 3.4 Halt / LULD exitability flags

Every forward path and every DP checkpoint assumes the position can be
exited at that bar. Trading halts and LULD pauses break that assumption —
halted stocks print no bars for 5+ minutes — and they break it exactly
where fake edges concentrate: low-float, meme, and biotech-catalyst names
halt constantly, and a "+40% continuation" observed across a halt was
partly uncapturable in practice.

Both fields are derivable from bar timestamps alone (no halt feed, no new
ingest):

| Field | Table | Reason |
|---|---|---|
| `bar_gap_minutes_max_<H>` (largest intra-RTH gap between consecutive bars inside the horizon) | `forward_outcomes` | Halt detector without a halt feed |
| `halt_gap_crossed` (Bool: any intra-RTH bar gap ≥ 5 minutes between entry and this checkpoint) | `forward_path_short` | Marks path observations whose exitability assumption is broken |

Treatment rule: halt-crossing trades are reported as a **separate cohort,
never silently pooled**. A cell whose edge disappears when halt-crossing
trades are excluded was an exitability artifact, not an edge. (Check
whether the RFC's existing data-quality flags already cover intra-day bar
gaps; if so this is a relabeling, not new columns. Note the caveat: thin
names also print gapped bars from simple no-trade minutes — the ≥5-minute
threshold and RTH-only scope keep ordinary illiquidity from being
mislabeled as halts, but the flag is a proxy, not a halt feed.)

### 3.5 Versioning

**Tables that become v2** when the §3.1–§3.4 + §3.6 additions land:
- `daily_observation` → v2 (adds signal-freshness, concentration percentile +
  HHI, position-in-52w-range, days-since-X%-move, consecutive-up-days,
  gap-filled flag, premarket-volume-spike flag).
- `forward_path_short` → v2 (adds `censor_type`, `volatility_within_trade`,
  `rate_of_change`, `current_ret_over_atr_14d`, `halt_gap_crossed`).
- `forward_outcomes` → v2 (adds dividend-adjusted total-return columns +
  ex-date-within-horizon flags per §3.3, halt/bar-gap flags per §3.4,
  cumulative pre-entry volume per §3.6).
- `market_context_daily` → v2 (adds cross-sectional dispersion + universe
  liquidity per §3.6).

**Tables that stay v1:** `regime_definitions` (gains required metadata
stamps per §3.6 but no new columns), `earnings_calendar`,
`sector_aggregates_daily`, `security_classification_daily`.

**Explicit dependency note for §2.7 survival analysis:** the
competing-risks CIF computation requires the `censor_type` column, which
exists only in `forward_path_short` v2. The survival analysis pipeline
*runs only on v2 data* — readers should refuse to attempt it against v1
files and emit a clear error message.

Stamp the new versions into Parquet file metadata so downstream readers
refuse joins across mismatched schemas. Specifically:
- `daily_observation_version = v2`
- `forward_path_checkpoints_version = v2`
- `forward_outcomes_version = v2`
- `market_context_daily_version = v2`

### 3.6 Recording gaps from the schema audit (2026-06-11; engine-time decisions, same rationale as §3.3)

A "record the measurement, derive the decision at query time" audit of the
RFC + coded schemas found four places where a query-time choice is blocked
because the continuous parent is never recorded. All are cheap at engine
time and expensive (full table regen) after. No new ingest.

| Field | Table | Reason |
|---|---|---|
| `cross_sectional_ret_dispersion_at_1000` / `_eod` (cross-sectional stddev of universe `intraday_ret_0930_to_1000` / EOD returns) + `cross_sectional_ret_iqr_at_1000` / `_eod` | `market_context_daily` | §2.6 says CS momentum is a *dispersion-regime* strategy, but no dispersion metric is recorded anywhere — the one regime axis §2.6 depends on doesn't exist. Also enables a 6th regime taxonomy (`dispersion`) and the §1.3.14 concentration questions. |
| `universe_median_addv_20d` + `universe_total_dollar_volume` | `market_context_daily` | The `liquidity` regime taxonomy is the only one with NO recorded continuous parent — VIX, breadth, and SPY trend are all re-derivable at query time; liquidity is not. Blocks §7.7.1's ±5pp bucket-boundary sensitivity sweep for that taxonomy. |
| `cumulative_volume_to_entry`, `cumulative_dollar_volume_to_entry` | `forward_outcomes` (per entry_offset) | Pre-entry *returns* are derivable at all 17 offsets (entry_price ÷ open), but pre-entry *volume* exists only at the 10:00 snapshot — §1.3.7's "does pre-entry volume predict outcomes" is currently answerable at exactly one offset. |
| Threshold metadata stamps (bucket boundary values + the window they were computed on, per taxonomy) | `regime_definitions` (file metadata, not columns) | Regime bucket thresholds are otherwise baked invisibly at engine time; the stamps make every regime assignment reproducible and auditable. |

**Decisions closing the audit (2026-06-11):**
- **Path checkpoints: full v1 (23 checkpoints), not v1-lite.** The lite
  set's ~70–100 GB saving permanently forecloses sub-5-minute exit/fade
  research (§1.3.6, §1.3.16) — an arbitrary cutoff of exactly the kind
  Phase 0 refuses everywhere else. Revisit only if a *measured* disk
  number during the build forces it, and then by checkpoint-version bump,
  never silently.
- **`terminal_event_type` enum (v1):** `none` (trades through the full
  horizon) / `delisted_merger_acquisition` /
  `delisted_bankruptcy_liquidation` / `delisted_exchange_compliance` /
  `delisted_unknown` / `extended_halt_no_bars`. Merger vs bankruptcy
  follows the merger-vs-delist split already frozen in Phase 0;
  ambiguity is carried by the existing `terminal_event_confidence`
  column, not by extra enum values.

### 3.7 v2 implementation checklist (final, 2026-06-11 — the schema delta to land before any engine writer code)

Consolidates §3.1–§3.4 + §3.6 into the exact delta for
`crates/momentum-core/src/phase0_outputs.rs` (+ the roundtrip test + an
RFC changelog entry). Audit finding that motivates doing this FIRST: the
coded schemas currently contain **0 of these columns** — they are at
pre-amendment v1, and retrofitting after the engine writes means
regenerating 200–300 GB.

**`daily_observation` → v2 (+14 columns):**

| Column | Type | Definition |
|---|---|---|
| `signal_first_in_5d` / `_10d` / `_20d` | Boolean ×3 | default signal fired today and on none of the prior N trading days |
| `signal_concentration_percentile_today` | Float64 | share of universe firing the default signal today, as a trailing point-in-time percentile across prior days |
| `signal_concentration_hhi_today` | Float64 | Herfindahl across today's signal-strength distribution |
| `premarket_volume_vs_20d_median` | Float64 | continuous parent of the spike flag |
| `pre_market_volume_spike_flag` | Boolean | parent > 5 |
| `high_52w` / `low_52w` | Float64 ×2 | trailing 252-trading-day split-adjusted high/low. Replaces §3.1's `position_in_52w_range` ratio — record the parents, derive any ratio at query time |
| `days_since_last_5pct_move` / `_10pct_` / `_20pct_` | Int32 ×3 | trading days since last close-to-close \|move\| ≥ X% |
| `consecutive_up_days_close_to_close` | Int32 | |
| `gap_filled_today_flag` | Boolean | intraday range touched prior close |

The three `signal_*` columns embed the default signal definition —
stamp `signal_definition` (the literal expression) + version into the
table metadata alongside the schema version.

**`forward_outcomes` → v2 (+42 columns, 615 → 657):**

| Column | Type | Definition |
|---|---|---|
| `ret_<H>_total` | Float64 ×9 | dividend-added total return, multi-day H ∈ {1d…252d} (§3.3) |
| `dividend_ex_date_within_<H>` | Boolean ×9 | (§3.3) |
| `bar_gap_minutes_max_<H>` | Int32 ×13 | largest intra-RTH gap between consecutive bars inside horizon (§3.4) |
| `first_event_<pair>` | dict_str ×9 | `target_first / stop_first / neither / no_data` per frozen label pair, exact at 1m resolution (§3.2 refinement) |
| `cumulative_volume_to_entry`, `cumulative_dollar_volume_to_entry` | Float64 ×2 | 04:00 ET session start through entry bar inclusive (§3.6) |

**`forward_path_short` → v2 (+4 columns):** `volatility_within_trade`
(Float64), `rate_of_change` (Float64), `current_ret_over_atr_14d`
(Float64), `halt_gap_crossed` (Boolean). (`censor_type` moved to
`forward_outcomes` per the §3.2 refinement.)

**`market_context_daily` → v2 (+6 columns):**
`cross_sectional_ret_dispersion_at_1000` / `_eod`,
`cross_sectional_ret_iqr_at_1000` / `_eod`, `universe_median_addv_20d`,
`universe_total_dollar_volume` (all Float64).

**`regime_definitions` (stays v1):** add required file-metadata stamps —
per-taxonomy bucket threshold values + the window they were computed on.

**`terminal_event_type`:** enum values per the §3.6 decision.

**Version stamps to bump:** `daily_observation_version = v2`,
`forward_outcomes_version = v2`, `forward_path_checkpoints_version = v2`,
`market_context_daily_version = v2`. The path-checkpoint SET stays the
full 23 (§3.6 decision) — only the schema version bumps.

**Definition of done:** schemas + constants updated in
`phase0_outputs.rs`; roundtrip test asserts new column counts, enum
values, and metadata keys (incl. `signal_definition`); RFC changelog gets
a v7 entry referencing this section. Engine writer work starts only after
this lands.

---

## 4. Should we use linear algebra?

Short answer: **yes in specific places, no as the primary modeling tool.** The
data has many variables but the relationships you're chasing are fundamentally
non-linear (asymmetric tail capture is the whole point — you care about extreme
right-tail behavior, which linear methods squash by design).

### Where linear algebra genuinely helps

1. **Factor decomposition (Fama-French-style).** Decompose every trade's return
   into systematic factors (market beta, sector, size, value, momentum factor)
   + idiosyncratic residual. The **residual is the only thing that's actually
   new information.** Your raw `ret_<H>` is usually dominated by 1-2 factor
   loadings; conditioning on the residual is the structural defense against
   "I found a strategy that's secretly long QQQ."

2. **PCA / SVD on the feature matrix** for ML preprocessing. With ~80 ML-safe
   features × millions of rows in `daily_observation`, PCA gives you a
   tractable feature space without losing information. **Only use as input to
   non-linear models** (XGBoost, neural nets) — interpreting PCs directly is
   usually unproductive.

3. **Eigendecomposition of the return covariance matrix** across same-day
   signals. Reveals the dominant correlation modes — useful when you want to
   know "if I take 20 signals today, do I have 20 independent bets or really 3
   highly-correlated clusters?" Portfolio risk math depends on this.

4. **PCA on the edge surface itself.** Instead of testing every
   (entry × horizon × cell) cell, find the principal components of the
   *edge surface across cells*. The first 2-3 PCs often capture 80%+ of the
   variance and tell you "this is fundamentally a 1d strategy parameterized by
   classification." Compresses 154,700 cells to a few dozen interpretable
   modes.

5. **Linear regression baselines for every Phase 1 finding.** Always do this
   first. If a linear model on `ret_<H>` ~ classifications + regime + signal
   already explains 90% of the variance, you don't need ML — and any
   non-linear model claiming additional alpha is probably overfitting.

### Where linear algebra hurts more than helps

1. **The actual return signal is non-linear and asymmetric.** Tail-capture
   strategies care about the 95th percentile of `max_runup`, not the mean.
   Linear models fit means and dump tails. Tree-based models (XGBoost,
   LightGBM) and quantile regression are the right tools for the actual
   prediction layer.

2. **Heavy-tailed return distributions break OLS assumptions.** US equity
   intraday returns have kurtosis 10-30+; OLS standard errors are wrong by
   factor 2-5x without robust corrections. If you use linear models, you
   *must* use cluster-robust (day-clustered) SEs + heavy-tail-aware
   bootstraps. Otherwise your confidence intervals are fantasy.

3. **Categorical interactions are awkward in linear space.** "Does mega-cap
   tech behave differently in low-VIX vs high-VIX?" is a 2-way categorical
   interaction. Tree models eat that for breakfast; linear models need
   careful encoding and miss the structure.

4. **PCA / linear factor models are unstable with regime change.** A factor
   model fit on 2016-2019 explains 2020-2021 badly. Don't use a single linear
   decomposition across the whole window — fit per-era and check stability.

### Concrete recommendation

A layered approach:

1. **Linear baseline first** — factor decomposition + linear regression of
   excess returns on features. Establishes how much of the variance is
   systematic and how much is idiosyncratic. If idiosyncratic variance is
   small, there's no alpha to find.

2. **Non-linear models on the residual** — XGBoost / LightGBM predicting
   "above-median outcome in this cell" using the residual after factor
   removal. This is where meta-labeling lives (Phase 4 of the master plan).

3. **PCA for feature compression** going into the non-linear models — not for
   interpretation, just for tractability.

4. **Survival analysis (semi-parametric, partial linear algebra)** for
   exit-rule research, as in §2.7.

5. **Bayesian hierarchical models (linear algebra under the hood, but the
   point is shrinkage)** for cross-cell pooling. With 154,700 cells and a few
   thousand trades per cell, partial pooling of cell-specific edges toward a
   global prior is *the* right framework. Stops you from declaring victory on
   noisy outlier cells.

Linear algebra is a load-bearing **tool**, not the modeling philosophy. The
modeling philosophy is "characterize the conditional distribution, use the
right math for the question, never let any single method's assumptions go
unchecked." Linear algebra serves that philosophy in specific layers; it
doesn't replace it.

---

## 5. Phase 1 reporting order (recommended)

### 5.0 Deliverable hierarchy (amendment 2026-06-11)

The goal of Phase 1 is **not** a single deployable edge. Primary
deliverables, in order:

1. **The blacklist (§2.2)** — where not to trade. Mostly provable from the
   cost side (spread proxy, exitability flags, leveraged-ETF decay), so it
   has high statistical power by construction.
2. **The predictability-ceiling map (§2.10)** — where there is anything to
   learn at all. Cells with near-zero ceilings end research effort there.
3. **The stability map (§2.9 trajectories + §2.5 walk-forward
   replication)** — which conditional structures persist across eras and
   splits. Stability claims are tested by replication across windows, not
   by pooled p-values.
4. **Meta-label refinement (Phase 4)** — run only where the ceiling is
   non-trivial; it filters a heterogeneous outcome stream, it does not
   conjure edge from nothing.

The §7.7.1 confirmation gauntlet (BY + holdout) is the final-mile gate for
any candidate someone would actually deploy — it is **not** the success
metric of the phase. Per §1.5.1, mean-expectancy tests here are powered
only for realized Sharpe ≈ 1.5–2; second-moment and dependence-structure
claims (hit-rate structure, dispersion, MI, cross-cell rank stability) are
where this dataset has real power.

All four deliverables are **signal-agnostic**: `forward_outcomes` is keyed
on (day, security_id, entry_offset), not on the momentum signal, so the
same machinery evaluates any pre-specified entry rule — subject to the
§1.6 rule that every signal family tried is added to the multiplicity
ledger.

### 5.1 Reporting order

To minimize overfitting risk and maximize durable findings:

0. **Tracer-bullet vertical slice** (days, as soon as the engine writes
   even one month of output) — run the ENTIRE discipline (cost model,
   excess returns, day-clustered CI, surface plot) on ONE pre-chosen
   question (e.g., 10:00 entry, 1d horizon, large-cap CS, signal > 0)
   using plain conditional sorts. No MI/BN/DP/HB. De-risks the power
   question (§1.5.1), the dividend question (§3.3), and the pipeline
   plumbing before the full stack is built.

1. **Negative space pass** (2 weeks) — find and characterize cells with
   significant negative cost-adjusted expectancy. Output: blacklist of
   conditions to avoid.

1.5. **Variance-attribution + predictability-ceiling map** (§2.10) — per
   coarse cell: ledger shares + MI-implied ceiling. Cells with near-zero
   ceilings are dropped from further research effort regardless of how
   their point estimates look; the ceiling map sets the priority order
   for every step after this one.

2. **Required-edge pass** — solve for the minimum edge needed to beat costs at
   target Sharpe; count cells that clear it; this is your pre-registered
   hypothesis set for the holdout test.

3. **Surface heatmaps** for each candidate cell — kill anything without smooth
   neighborhoods.

4. **Excess-over-index re-derivation** — every surviving finding re-checked
   in excess-return space. If it dies here, it was beta in disguise.

5. **Era-conditional decomposition** — every surviving finding reported per-era.
   Inconsistency across eras is itself a finding.

6. **Two-stage regime classifier overlay** — does the surviving finding
   improve when filtered by a regime classifier? If yes, that's the final
   strategy shape; if no, the regime context wasn't load-bearing.

7. **Cross-sectional + time-series blend test** — final check on whether the
   finding generalizes across momentum types or is specific to one.

8. **Holdout pass** — only at this stage does the holdout split see any of
   these hypotheses, with BY FDR control across the pre-registered family
   (a fixed family of ≤ 20 cells per §7.7.1). Findings that survive here
   are real candidates for Phase 2 baselining.

9. **Live shadow period** (2-3 months calendar time, ~zero effort) —
   paper-trade holdout survivors in real time before any capital. The only
   data untouchable by construction is the future; the daily ingest
   already runs, so this is nearly free, and no amount of researcher
   degrees-of-freedom can contaminate it.

Any cell that survives all stages is a real (narrow, capacity-constrained)
edge candidate. The discipline is the edge.

---

## 6. Open questions to resolve before Phase 1 begins

These need user input before the analytical layer starts. The **cost model**
is the single most load-bearing open question because every required-edge
hurdle, every cell ranking, and every "is this tradeable" determination
depends on it. §6.1 below proposes a data-driven derivation rather than
leaving it as a guess.

- **Cost model** — see §6.1; derive data-driven from existing inputs.
- **What's the target Sharpe?** 0.5 (rough hurdle), 1.0 (publishable), 1.5+
  (institutional). Different targets give very different cell counts.
- **What's the max capital deployable per cell?** Affects whether
  capacity-constrained findings (low-float, micro-cap) are worth pursuing.
- **How much manual review per finding?** Each surviving cell deserves
  ~half a day of human eyeballing for sanity. Sets the upper bound on how
  many hypotheses you can responsibly pre-register (frozen at 20, §1.6).
  Prioritize the review queue by **edge × deployable capacity** (both
  already computed), not edge alone — a 3-Sharpe edge in sub-$5 microcaps
  at 0.1% ADV is worth less review time than a 0.8-Sharpe edge in liquid
  mid-caps.

### 6.1 Cost model — data-driven, not assumed

Don't leave this as a 5-vs-10-vs-20 bps guess. The cost model is
**derivable from existing data with no new ingest**:

**Component 1 — spread proxy (per (entry_offset, classification cell)):**

You don't have NBBO quotes, but the difference between bar high and low for
a marketable order at the entry bar is a defensible proxy for the
effective spread experienced. For each cell:
- Compute the distribution of `(entry_1m_high - entry_1m_low) / entry_price`
  (already derivable from the entry bar's OHLC).
- Use the **median** as the typical spread cost, **75th percentile** as the
  conservative case.
- Liquid mega-cap at 10:00 ET: typically 1-3 bps. Micro-cap at 09:35:
  often 30-50+ bps. This drives a huge per-cell variation that flat-rate
  assumptions miss.

**Liquidity-adjusted cap for the most-tradeable cells.** The raw high-low
proxy *overstates* spread for the highest-liquidity names — a stock that
prints a 4-bps range bar at 10:00 isn't actually charging you 4 bps in
effective spread; the bar range reflects the price walk, not the round-trip
cost. For names in the top 200 by ADV (or `addv_20d_rank_today ≤ 200`),
cap the spread proxy at:

```
spread_proxy_bps_capped = min(
    high_low_bps,
    2.0 × median_1min_range_during_10am_to_3pm_for_this_stock
)
```

Rationale: the within-day median 1-minute range during the quiet midday
period (10:00-15:00 ET, excluding open and close) is a low-bound estimate
of "normal" volatility unrelated to spread cost. 2× that bound is a
defensible upper limit for how wide the effective spread can be for these
names.

**Calibration step (recommended, ~half day):** the current Massive plan
(Stocks Advanced) does NOT entitle NBBO quotes, and IEX's free quotes
cover only IEX's ~2% of volume — IEX spreads are not representative of
NBBO. Two workable anchors: (a) one month of a quotes-entitled tier,
pulled once for ~20 representative symbols across cells; or (b) published
spread studies (e.g., SEC MIDAS aggregates) as an external benchmark.
Compare the cap formula's output to the anchor's median quoted spread; if
the cap is consistently >2× the true spread, lower the multiplier; if it
under-estimates, raise it. Document the calibrated multiplier and its
source in `cost_model.parquet` metadata.

Without this cap, the cost model overestimates spreads for the most
tradeable cells, which biases the required-edge hurdle against the cells
most likely to actually contain a real edge.

**Component 2 — market-impact / slippage (per trade size):**

Use the `entry_participation_capacity_*` columns already in
`forward_outcomes`. Standard square-root impact model:

```
slippage_bps(size) = c × sqrt(trade_size / addv_20d) × bps
```

where `c` ≈ 10-50 (empirical; calibrate to size-bucketed observed price
moves around large prints if you have that, otherwise use 20 as a midpoint).

For Phase 1 sensitivity sweeps, evaluate at three size assumptions:
- 0.1% of ADV (effectively zero impact)
- 1% of ADV (typical retail)
- 5% of ADV (where capacity starts mattering)

**Component 3 — commission:**

- Retail (Robinhood / IBKR Lite): ~0 commission, but spread-cost reality
  applies above.
- Institutional: 0.1-0.5 ¢/share.

**Combined cost per cell:** `spread_proxy + slippage(size) + commission`.

**The frozen cost model is built from cost-side inputs ONLY** — spread
proxy + impact curve + commission, per cell — and frozen before discovery.
It must NOT be derived from the distribution of cell edges. An earlier
draft of this section set the cost assumption at the 80th percentile of
break-even ceilings across cells; that is circular — real-world cost is a
property of execution, not of your edges, and a dataset with better edges
would have been assigned higher costs under that rule.

**Sensitivity sweep — the required-edge hurdle exercise:**

Run the §2.8 inverted question across cost assumptions from 2 bps to 30 bps
in 2-bps steps (Category B, §1.6). For each cell, record the maximum cost
it can sustain while keeping cost-adjusted excess return statistically
positive (lower bound of bootstrap CI > 0) — the cell's **break-even cost
ceiling**. The ceiling is reported as a *descriptive headroom statistic*
alongside each finding; it is never used to set the cost assumption.

---

## 7. The algorithmic stack — why this is the right tool for the job

The reframings in §2 tell you *what questions to ask.* This section tells you
*what algorithms to use* to answer them. The framing question that motivates
this section: **"given high-dimensional data in Parquet files, how do we
measure relationships between variables and find an optimal path through the
decision grid?"**

The answer is a four-layer stack, each layer theoretically grounded for its
specific sub-problem (under stated assumptions — see §7.7):

```
┌────────────────────────────────────────────────────────────────────────┐
│ Layer 4: Bayesian hierarchical models                                  │
│ Purpose: cell-level uncertainty + shrinkage                            │
│ Stops sparse-cell noise from being declared as edge                    │
├────────────────────────────────────────────────────────────────────────┤
│ Layer 3: Bellman dynamic programming (backward induction)              │
│ Purpose: optimal entry/exit policy as a function of state              │
│ Solves the sequential decision problem (the "path" question)           │
├────────────────────────────────────────────────────────────────────────┤
│ Layer 2: Bayesian network structure learning (PC / NOTEARS / GES)      │
│ Purpose: conditional dependency DAG across all variables               │
│ Reveals WHICH variables drive WHICH (causal structure under faith.)    │
├────────────────────────────────────────────────────────────────────────┤
│ Layer 1: Mutual information (pairwise + conditional)                   │
│ Purpose: non-linear dependence between every feature and outcome       │
│ Identifies the relevant variables before any modeling                  │
└────────────────────────────────────────────────────────────────────────┘
```

### 7.1 Why mutual information is the right measure of "relationship"

Standard correlation only captures linear relationships. Financial returns are
fundamentally non-linear (fat tails, regime breaks, threshold effects). MI
captures **any** statistical dependence:

```
I(X; Y) = ∑ p(x,y) log(p(x,y) / (p(x)p(y)))
```

Properties that make this the right tool:
- `I(X; Y) = 0` if and only if X ⊥ Y (statistical independence). Correlation
  can be exactly zero with arbitrarily strong non-linear dependence.
- Works on **mixed types** — continuous features, categorical classifications,
  boolean flags all in the same matrix.
- **No distributional assumptions.** Doesn't care about Gaussian, doesn't care
  about stationarity within the sample.
- **Scale-invariant.** No need to standardize features.
- Conditional MI `I(X; Y | Z)` answers "does X tell us about Y *given* that
  we already know Z?" — exactly the question you ask when adding features.

Concrete output: an 80 × 13 matrix of MI between every ML-safe feature and
every horizon outcome. Sort rows by sum; the top-20 features carry essentially
all the predictable signal. Anything below the noise floor is dropped from
downstream layers.

Tools: `sklearn.feature_selection.mutual_info_regression` for continuous
outcomes, `mutual_info_classif` for binary labels (Phase 4 meta-labeling).

### 7.2 Why Bayesian networks reveal the dependency structure

MI is pairwise. Real systems have **conditional** dependencies — `X` and `Y`
may look strongly related, but only because both are driven by `Z`. Bayesian
network structure learning finds the DAG that compactly represents the joint
distribution.

Output: a directed graph like
```
overnight_gap → intraday_ret_0930_to_1000 → ret_1d
                     ↑                          ↓
            premarket_dollar_volume       max_runup_1d
```

This is the answer to questions like:
- "Does the edge come from overnight gaps or intraday continuation?"
  (§1.3.8 in the question catalog) — read it directly off the DAG: which node
  is upstream of `ret_1d`?
- "Does pre-market volume predict intraday momentum?" (§1.3.7) — check for a
  direct edge between `premarket_dollar_volume` and `intraday_ret`.
- "Is breadth predictive?" (§1.3.5) — check whether `breadth_advance_decline_ratio`
  has any outgoing edges to outcome variables.

Algorithms: **PC algorithm** (constraint-based, fast for ~100 variables),
**NOTEARS** (continuous-optimization, scales to thousands), **GES**
(score-based hill climbing). Library: `pgmpy` or `causal-learn`.

**The conditional-independence test is the whole game and must be named.**
PC with the default Fisher-z test assumes joint Gaussianity — which §4
explicitly rejects for this data (kurtosis 10–30+). Kernel-based tests
(KCI) are assumption-light but don't scale to millions of rows. Practical
choice: discretize to ranks / coarse bins and use a G-test or an
MI-based CI test on subsampled data, and treat the resulting DAG as
low-resolution. This is one more reason BN output is demoted to
supporting evidence (§7.3.1.1) rather than the source of the DP state.

The DAG also tells you the **minimal sufficient state** for the path-finding
problem — the variables that, once conditioned on, render the others
irrelevant for outcome prediction. That minimal state is the DP state space.

### 7.3 Why Bellman dynamic programming is the right path-finder

The exit decision is the canonical **optimal stopping problem**:

> At each checkpoint, given current state, decide stop-now vs continue-and-decide-again.

The Bellman optimality principle says: if at every state you make the locally
optimal decision (comparing exit-value to expected-continuation-value), the
trajectory you end up on is globally optimal. This is provable by backward
induction.

For your data, the algorithm is:

```
State:    s = (current_ret, max_ret_so_far, drawdown_so_far,
               bars_elapsed, regime, classification_cell)
Action:   a ∈ {hold, exit}
Reward:   r(s, exit) = current_ret − costs
          r(s, hold) = 0 (continue accruing)

V(s, T) = exit_reward(s)           # at terminal checkpoint, must exit
V(s, t) = max(
    exit_reward(s),                # stop here
    E[V(s', t+1) | s, a=hold]      # keep holding, recurse
)

π*(s, t) = argmax of the above
```

You backward-induct from EOD to entry, computing the optimal policy `π*(s, t)`
as a function of state and checkpoint. **The output is a rule, not a number.**
A typical learned policy:

> Stop if `current_ret < 0` AND `bars_elapsed > 20`.
> Stop if `current_ret < 0.5 × max_ret_so_far` AND `current_ret > 1 ATR`.
> Otherwise hold.

This is exactly the asymmetric tail-capture rule you wanted — **derived from
data**, not hand-tuned.

Why DP and not RL:
- **You have offline data, no simulator.** RL shines with interactive
  exploration; you don't have that.
- **Interpretability matters.** DP gives you a readable policy. RL gives you
  a black-box neural network.
- **Overfit-resistance.** DP is a fixed-capacity estimator (under tabular
  formulation). RL on financial data overfits notoriously.

Why DP and not "test a fixed rule":
- A fixed rule (e.g. "exit at +2% or -1% or EOD") is a **special case** of
  the DP-derived policy where the policy ignores state. DP finds the
  **state-dependent** rule that strictly dominates any fixed rule.
- Fixed-rule backtesting requires you to specify the rule *before* seeing the
  data; DP **discovers** the rule, with the optimal-conditional property as a
  theoretical guarantee *inside the modeled MDP* (see §7.7 for what that
  qualification means).

**Optimism bias — the max operator is not free.** "Strictly dominates any
fixed rule" is true in-population, false in-sample-estimated. Backward
induction takes `max(exit, E[continue])` over *estimated* values; since
`E[max] ≥ max(E)`, estimation noise propagates upward through every backup
and the exploration-set value of the learned policy is biased high —
systematically, even with the ≥30-transitions safeguard. (This is the
reason Double Q-learning exists.) Consequences, enforced in §7.7.1: the
policy is learned and valued on disjoint data (cross-fitting), and
**exploration-set V of a policy valued on its own training data is never
a test statistic.**

#### 7.3.1 The Markov assumption is the weakest link — and how to harden it

DP's optimality only holds if the chosen state representation captures
enough of the path for the future to be conditionally independent of the
past given the state. For raw intraday paths, this is shakier than it sounds:

- A **slow grind to +1%** and a **violent spike to +1%** have very different
  continuation probabilities, but `(current_ret, max_ret_so_far)` records
  the same state for both.
- **Volatility regime within the trade** (how much has the trade been
  moving?) is not captured by raw return state.
- **Rate of change** (`Δret / Δbars`) is not captured by point-in-time
  state.

**Fix — augment the state vector with:**
- `volatility_within_trade` (running stddev of bar returns since entry)
- `rate_of_change` (last-checkpoint return delta / bars elapsed)
- `current_ret_over_atr_14d` (vol-normalized current return)

These three are added to `forward_path_short` in §3.2 and are derivable
from existing data. The minimum-state-vector for DP becomes:
`(current_ret, max_ret_so_far, drawdown_so_far, bars_elapsed,
volatility_within_trade, rate_of_change, regime, classification_cell)`.

**Empirical Markov-adequacy check (mandatory before trusting DP):**
fit a 2-step lookback model (simple LSTM or a logistic-regression with
2-step lagged features) on the *same* `(state → outcome)` task and compare
out-of-sample value to the DP policy's value. If the lookback model beats
DP by more than a few percent, the state vector is missing something; add
features (or fall back to fitted-value-function methods, §7.3.2) until the
gap closes.

#### 7.3.1.1 State selection: MI + domain candidates, BN cross-check, ablation-gated

The state vector for DP is chosen by a documented procedure rather than
hand-picked — but the BN Markov blanket is **supporting evidence, not the
source of truth**. Structure learning on ~80 mixed-type, heavy-tailed,
non-stationary features is unstable (see §7.2 on the CI-test problem),
and making the DP state inherit from a junk DAG would silently poison
Layer 3.

1. **Candidate set from MI ranking (§7.1) + domain priors.** Top-ranked
   features against the relevant horizon outcomes, plus the §7.3.1
   path-shape variables that domain reasoning says are load-bearing.

2. **BN Markov blanket as a cross-check.** Where the BN's Markov blanket
   of the outcome (parents, children, children's other parents) agrees
   with step 1, that's corroboration; where it disagrees, the disagreement
   is logged — but the BN does not veto or add state variables on its own.

3. **Empirical-lift ablation is the arbiter.** A candidate enters (or
   stays in) the state vector iff including it improves out-of-sample
   policy value on the validation set by >5% via ablation. The 5% gate is
   itself noisy on noisy OOS values — run the ablation on ≥3 bootstrap
   resamples and require the *median* lift to clear the gate.

The final state set is documented as **"MI + domain candidates, BN
cross-check, ablation-gated,"** with all three artifacts recorded for
reproducibility. This is the state vector that gets pre-registered in the
§7.10 template.

#### 7.3.2 Scalability — fitted Q iteration when full tabular DP doesn't fit

Naive tabular DP on the augmented state vector blows up:

- 10 bins × `(current_ret, max_ret, drawdown, vol_within_trade, rate_of_change,
  current_ret_over_atr)` = 10⁶ states per trade-bin
- × 23 checkpoints × 200 cells = ~5 × 10⁹ state-time-cell tuples
- Each needs an empirical transition probability estimate

With ~50M trade-checkpoints in the dataset across all cells, that's
~10 samples per state-time-cell — far below the ≥30-per-(s,a) safeguard
in §7.7.1. Tabular DP is not feasible at this dimensionality without
aggressive state reduction.

**Two fixes, applied in order:**

1. **Reduce the state vector using the §7.3.1.1 selection procedure.**
   MI ranking + ablation typically shows 3-4 dimensions are sufficient
   (e.g., `(current_ret_over_atr, drawdown_over_atr, bars_elapsed, regime)`),
   reducing the state space by 10-100×.

2. **Switch from tabular DP to fitted Q iteration (FQI).** Instead of
   storing `V(s, t)` as a table, fit a function approximator (gradient-
   boosted trees, since you're already using XGBoost in Phase 4):

   ```
   for t in [T, T-1, ..., 0]:
       targets = max(exit_reward, V_model(s', t+1))   # Bellman target
       V_model = fit_xgboost(features=s, target=targets)
   ```

   FQI scales gracefully with state dimension, smooths over sparse regions
   naturally (via tree regularization), and produces a single value model
   per cell × era rather than a giant table. Standard in offline RL
   literature (Ernst, Geurts, Wehenkel 2005).

The choice between tabular DP and FQI depends on the minimal state's
effective dimensionality. If state reduction gives ≤4 dimensions × ≤10 bins
each = ≤10k states per cell, tabular DP is fine. If the minimal state is
≥5 dimensions, switch to FQI before declaring policy quality.

**Honesty note:** FQI with gradient-boosted trees IS function-approximation
offline RL — the thing §7.6 calls "practically dangerous." Switching to it
gives up both advantages claimed for tabular DP (fixed capacity, readable
policy). FQI cells therefore get extra skepticism: the fixed-rule-baseline
comparison in §7.7.1 (default to the 1-line rule unless the policy clearly
dominates) is even more binding there, and the learned value model is
never inspected for "insight" — only its out-of-sample policy value
counts.

### 7.4 Why Bayesian hierarchical models tie it together

You'll run the DP per classification cell × per regime × per era. With 10
classifications × 5 regimes × 4 eras = 200 cells, some cells have 10K trades
and some have 50. The 50-trade cells produce **noisy** policy estimates.

Bayesian hierarchical models do **partial pooling**: each cell's parameter
estimate is shrunk toward the global mean, weighted by `1 / sample_size`.
Cells with lots of data trust themselves; cells with little data trust the
global prior more. Under exchangeability across cells, this shrinkage
estimator is **better calibrated than the per-cell MLE** — James-Stein
shrinkage formalizes that the partial-pooling estimator dominates the
naive per-cell point estimate in mean squared error, at the cost of
introducing a small bias toward the prior. This is the right framing:
*calibration improvement under exchangeability via shrinkage*, not a
universal "best estimator" claim.

Output: each cell's expected return + DP policy comes with a **credible
interval**, not a point estimate. You only declare an edge when the credible
interval excludes zero AFTER multiple-comparisons correction (BY, per §7.7.1).

This is the structural defense against "I tested 200 cells and found one
that's positive at p<0.05" — almost certainly a false discovery without
hierarchical shrinkage.

Library: `pymc`, `numpyro`, or `cmdstanpy`. Standard textbook setup
(Gelman et al., *Bayesian Data Analysis*).

---

### 7.5 Mapping the stack to the question catalog (algorithmic reasoning)

§1.3 contains the full operational catalog of 20 question categories with
brief per-question algorithm pointers. This section is the higher-level
*why each layer is the right tool* for each category — same mapping,
algorithmic-reasoning lens rather than query-by-query.

| Question category (from §1.3) | Layer that answers it | Why this layer |
|---|---|---|
| **§1.3.1 Entry timing optimization** | DP (Layer 3) | Optimal `entry_offset` falls out as part of the state-dependent policy. Compare `V(s, entry_offset=0935)` vs `V(s, entry_offset=1010)` across cells. |
| **§1.3.2 Exit rule design** | DP (Layer 3) | This IS the optimal stopping problem. DP-derived policy *is* the exit rule. No threshold sweeping needed. |
| **§1.3.3 Holding period selection** | DP (Layer 3) | The optimal holding period is "however long the policy says hold." It varies by state — and that's the right answer, not a fixed horizon. |
| **§1.3.4 Security type / classification conditioning** | DP per cell + hierarchical (Layers 3+4) | Separate policy per classification cell; hierarchical shrinkage gives you statistical confidence in cross-cell comparisons. |
| **§1.3.5 Market regime interaction** | DP with regime in state + BN (Layers 2+3) | Regime is part of the DP state vector; the learned policy will branch on it if and only if regime is load-bearing. BN tells you whether regime is even a parent of outcome. |
| **§1.3.6 Intraday path / shape analysis** | DP value function (Layer 3) | DP's value function `V(s, t)` literally is "expected outcome given current path state." Visualize as a function of `(current_ret, bars_elapsed)` for direct insight. |
| **§1.3.7 Pre-entry / signal quality filters** | MI + BN (Layers 1+2) | MI tells you which pre-entry features have any information about outcomes; BN shows which are *direct* parents (not just spurious correlates). |
| **§1.3.8 Gap vs RTH decomposition** | BN structure (Layer 2) | The learned DAG shows whether `gap_return_day_1` or `rth_return_day_1` is the parent of `ret_1d`. Direct answer. |
| **§1.3.9 Cost / execution realism** | DP with cost-adjusted reward (Layer 3) | Slippage and commission go into the reward function. Policy automatically refuses trades that don't clear costs. |
| **§1.3.10 Time underwater / psychological** | DP value function + survival analysis (Layer 3) | DP tracks `bars_underwater` as a state variable; the policy naturally avoids cells where underwater time predicts non-recovery. |
| **§1.3.11 Event-driven effects** | DP with `days_to_earnings` in state (Layer 3) | If "avoid 5 days before earnings" is real, DP policy will refuse to enter in that state. If not, it won't. Self-discovering. |
| **§1.3.12 Multi-horizon / rolling** | MI between horizons + DP across horizons (Layers 1+3) | MI between `ret_1d` and `ret_5d` tells you the predictive structure; DP across horizons gives the optimal multi-horizon policy. |
| **§1.3.13 Meta-labeling (ML)** | XGBoost on DP residuals (Layer 4-equivalent) | Phase 4 layer: ML predicts which signals the DP-derived strategy will succeed on, filters precision-recall. Builds on top of, doesn't replace, the DP baseline. |
| **§1.3.14 Cross-signal independence** | Eigendecomp of forward_outcomes correlation + Markowitz (Phase 5) | Linear-algebra core (eigendecomposition) + classical portfolio optimization for position sizing across correlated signals. |
| **§1.3.15 Regime stability / persistence** | Survival analysis on regime durations | Hazard-rate framing for "how long does regime X last" is mathematically the right tool. |
| **§1.3.16 Microstructure / order flow** | MI + quantile regression | MI catches non-linear relationships between bar-level structure and forward returns; quantile regression for full distribution rather than just mean. |
| **§1.3.17 Counterfactual / what-if** | DP value comparisons | DP gives you the expected return under *any* policy; comparing policies is straightforward (your DP V vs random baseline V). |
| **§1.3.18 Robustness / sensitivity** | DP cost-sweep + HB perturbation | Sweep cost assumption + report V curve; bootstrap to get robustness intervals on policy decisions. |
| **§1.3.19 Realistic scaling / capacity** | DP with size-dependent cost | Inject `cost(s, size)` into reward function; V curve as size grows reveals capacity ceiling per cell. |
| **§1.3.20 Edge decay / non-stationarity** | DP per era + Bayesian change-point | Standard regime-shift detection on V time series; declares decay statistically rather than visually. |

All 20 categories answered by one or two layers of the stack. None requires
a tool outside it.

### 7.6 Why this stack dominates alternatives

What you might consider instead, and why each is inferior:

| Alternative | Why it's worse |
|---|---|
| **Grid search over (entry, exit) pairs** | Tests each combination as a fixed policy, selection-bias on the best, doesn't condition on state. DP finds the *state-conditional* optimal which strictly dominates any fixed grid combination. |
| **XGBoost predicting return directly** | Predicts a single number per trade; can't answer "when to exit" since exit is a sequential decision. Useful as Layer 4 input to DP, not a replacement. |
| **Reinforcement learning** | Theoretically more general but practically dangerous on financial data — low SNR, non-stationarity, overfitting. Black-box policy. DP is the special case of RL when you have a model — for offline data, this is the better tool. |
| **Correlation matrix** | Linear-only. Misses non-linear dependencies that dominate financial data. MI subsumes correlation. |
| **Fixed-threshold backtesting** | Hardcodes the strategy upfront, defeating the "record everything, decide later" philosophy. DP discovers the threshold; you don't have to guess. |
| **Simple regression** | OLS assumes homoscedastic Gaussian errors; intraday returns have kurtosis 10-30+. Standard errors will lie by 2-5×. BN + hierarchical Bayesian models handle this natively. |
| **Naive multi-armed bandit / contextual bandit** | Online algorithms designed for live exploration; you have offline data. DP is the offline-data version of the same problem. |
| **Causal inference (Pearl do-calculus, Rubin)** | Strong assumptions (no unobserved confounders, SUTVA) that are usually violated in financial data. BN structure learning is the safer, lower-assumption neighbor. |

The combination is uniquely powerful for **your specific problem shape**:
offline data, sequential decisions, asymmetric payoffs, many variables, need
for interpretability. No single tool covers all four constraints; the stack
does.

### 7.7 What "optimal" means here — and where the assumptions can break

Each layer has a clean theoretical justification under specific assumptions.
The assumptions matter; calling any of these layers "the right tool" without
naming them would be overclaiming. Spelled out:

- **MI** is excellent for non-linear dependence *screening*. Shannon's
  axiomatic framework gives it strong theoretical standing as an information
  measure. **Caveats in practice:** estimation is noisy in finite samples,
  sensitive to binning / KNN choices, biased in high dimensions, and the
  bias is not always monotonic in sample size. Treat MI output as
  feature-ranking, never as ground truth of a relationship's importance.
- **Bayesian network structure learning** (PC / NOTEARS / GES) recovers the
  true conditional-independence DAG **as sample size grows AND under the
  faithfulness assumption AND assuming no hidden confounders AND assuming
  the system is acyclic.** Financial data routinely violates the last three:
  there are unobserved drivers (news, order flow), feedback loops
  (price → trader behavior → price), and the relationships are
  non-stationary. Treat BN output as a *candidate dependency structure for
  hypothesis generation*, not a causal answer. Cross-validate any edge
  that load-bears a strategy decision.
- **Bellman DP** satisfies the optimality principle: the policy is globally
  optimal because it's locally optimal at every state. **Caveats:** this
  optimality is *inside the modeled MDP* — it depends on the state
  representation being adequate, transition probabilities being well-
  estimated, the Markov property holding, the discretization being
  reasonable, the reward function being correct, and the data-generating
  process being stationary across train and deploy. A "provably optimal"
  policy inside a flawed MDP is just a fancy lookup table over noise. The
  DP framework is correct; the DP estimate of the optimal policy is only as
  good as its model. See §7.7.1 below for required safeguards.
- **Bayesian hierarchical models** give well-calibrated uncertainty estimates
  under partial-pooling and shrink noisy small-cell estimates toward the
  global mean. The James-Stein result formalizes that under exchangeability,
  shrinkage dominates the per-cell MLE in mean squared error. **It is NOT
  literally a "minimum-variance unbiased estimator"** — it's biased toward
  the prior by construction, and the dominance result is about MSE, not
  variance alone. Treat hierarchical Bayes as "better calibrated than naive
  per-cell estimates given a defensible prior," not as a free statistical
  guarantee.

"Optimal" in this document means: under the listed assumptions, no other
algorithm can do strictly better on the corresponding sub-problem. When the
assumptions break, optimality breaks with them. The reason to use these
tools is that they fail honestly (with diagnosable assumption violations)
rather than silently (like grid-searching for the best cell).

### 7.7.1 Required anti-overfitting safeguards (DP / MI / BN / threshold selection)

These are non-optional discipline for the stack. Without them, "provably
optimal" becomes "convincingly overfit."

**For DP:**
- Minimum sample count per `(state, action)` transition before that
  transition is trusted (rule of thumb: ≥30 transitions; bins below this
  use the parent-region prior).
- State-space dimensionality cap: total discretized states × 30 ≤ available
  trade-checkpoints in the cell. Above this, aggregate state bins or drop
  variables.
- Hierarchical shrinkage on transition probabilities themselves
  (`P(s' | s, a)`), not just on final cell-level expected returns.
- Out-of-sample policy evaluation on the validation set BEFORE pre-
  registering for the holdout: does the policy learned on exploration
  produce a non-negative V on validation?
- Comparison against fixed-rule baselines: if a 1-line rule
  ("exit at -1 ATR or +2 ATR or EOD") performs ≥80% as well as the DP
  policy, default to the rule. Sophisticated policies that don't dominate
  simple baselines are suspect.
- Bootstrap CIs around the learned policy's value. If the lower bound
  isn't above zero, the policy is statistically indistinguishable from
  randomness.
- **Bin-count sensitivity sweep.** Run DP at three discretization
  granularities for each continuous state variable: **coarse (5 bins),
  medium (10 bins, default), and fine (15 bins).** Report policy value and
  the learned exit rule under each. If the policy changes *qualitatively*
  across granularities (e.g., "exit early" at 5 bins but "hold for runup"
  at 15 bins), flag the cell as **bin-sensitive** and treat its findings
  as provisional. Bin-sensitive cells are reported but never pre-registered
  for holdout testing.
- **Cross-fitting against max-operator optimism (§7.3):** split the
  exploration set; learn the policy on one half, estimate its value on the
  other (or K-fold cross-fitting). The only V that ever feeds a hypothesis
  test is an out-of-sample V — a policy's value on its own training data
  is never a test statistic.
- **Name-concentration check:** a cell's edge driven by a handful of
  tickers is not a cell property. For every surviving cell, recompute the
  edge with the top-5 contributing names removed and report both numbers;
  a sign flip disqualifies the cell from pre-registration. Use two-way
  clustered (day AND security) errors where feasible.

**For regime bucket definitions (VIX, breadth, liquidity):**
- Default bucketing is **percentile-based on the exploration-set
  distribution**, not absolute thresholds. E.g., `vix_low` = bottom 33% of
  exploration-set VIX, `vix_medium` = middle 34%, `vix_high` = top 33%.
  This avoids the "VIX > 25" cutoff that was arbitrary in 2017 and
  inappropriate in 2022.
- **Sensitivity test:** shift percentile boundaries by ±5 percentage
  points (e.g., low = bottom 28% to 38%) and re-run BN + DP. If a
  regime-conditional edge disappears under the shift, the edge is fragile
  to bucket definition and reported as such — not pre-registered for
  holdout.

**For MI:**
- Use multiple estimators (binning + KNN) and report agreement; disagreement
  signals estimation noise.
- Permutation test against the null `I(X; Y_shuffled)` to set a finite-sample
  baseline — any MI value below the 95th percentile of the shuffled null is
  treated as zero.
- For high-dim conditional MI, expect noise; treat as ordinal feature ranking,
  not cardinal information amount.

**For BN structure learning:**
- Stability selection: bootstrap-resample the data, learn the DAG on each
  resample, retain only edges that appear in ≥80% of resamples.
- Sensitivity to era: learn the DAG separately per era, flag any edge
  whose direction flips. Edges that flip are not load-bearing.
- Cross-validation of any edge that informs a strategy decision: does the
  predictive structure on exploration hold on validation?

**For threshold selection (target / stop levels in survival analysis or
fixed-threshold queries):**
- Pre-specify a small set of economically motivated thresholds **before**
  looking at outcomes: e.g., ±0.5/1/2 ATR, plus ±cost-of-trade.
- Document the threshold rationale before running queries; do NOT add
  thresholds after seeing results.
- For materialized columns (`hit_X_before_minus_Y_<H>`), the threshold set
  is already frozen in v6 — that's a feature, not a constraint.

**Multiple-comparisons protocol (concrete spec, not just "use FDR"):**

The naive Benjamini-Hochberg procedure assumes test independence, which is
**violated here** — cells share underlying data, share days, and share
classifications, so p-values are positively correlated. Use
**Benjamini-Yekutieli (BY)** instead, which is FDR-controlling under
arbitrary positive dependence at the cost of a log-factor of conservatism.

Concrete protocol:

1. **Define the hypothesis family explicitly before testing — and keep it
   COARSE.** Per the §1.5.1 power analysis, BY across the full
   `(classification × regime × era × entry_offset × horizon)` cross
   (~154,700 cells) pushes the detectable edge to Sharpe ≈ 2 and
   guarantees a null result. The primary family is
   `(classification × regime)` (~200–1,000 hypotheses), each coarse cell
   scored on a *pre-specified* offset/horizon aggregation (surface mean
   over a pre-declared region — never the max, which re-imports the
   selection problem). Finer slices (era, entry_offset, horizon) are
   exploratory drill-down run ONLY on coarse survivors, under hierarchical
   FDR (Yekutieli 2008) so the guarantee is preserved. Cells eliminated by
   the surface-smoothness filter (§2.4) are excluded from the family *if
   and only if* the smoothness criterion is pre-specified blind to outcome
   labels. Otherwise they're in the family. The family also multiplies
   across **signal definitions**: each signal variant tried (threshold,
   rank-based, VWAP-relative, ...) adds a full copy of the family to the
   ledger. Signal definitions are Category A frozen constants (§1.6) —
   pre-specify them, and any variant explored later is logged and paid
   for in the correction.
2. **Compute per-cell test statistic** = bootstrap-day-clustered z-score of
   cost-adjusted excess return.
3. **Apply BY at α = 0.10** (looser than 0.05 because Phase 1 is discovery,
   not confirmation; the holdout will be α = 0.05). Sort p-values, find
   largest k such that `p_(k) ≤ k × α / (m × H_m)` where `m` = family
   size and `H_m = Σ 1/i` from 1 to m.
4. **Surviving cells go into the pre-registered hypothesis set — capped
   at 20.** The cap is a frozen constant (§1.6), set by the manual-review
   budget (§6), and fixes the holdout family size before discovery ends.
   If more than 20 cells survive, rank by hierarchical-posterior lower
   bound × deployable capacity and register the top 20. The set is frozen,
   written to disk with a timestamp, and only then does the holdout get
   touched.
5. **The holdout is FDR-controlled too.** Testing 20 pre-registered cells
   each at α = 0.05 expects one false survivor; apply BY at α = 0.05
   *across the pre-registered family* on the holdout. Complementary tools
   worth running alongside (dependence-aware and often more powerful than
   BY): Romano-Wolf stepdown, Hansen's SPA test, and López de Prado's
   Deflated Sharpe Ratio / PBO via CSCV as an overfitting diagnostic on
   the discovery process itself.

**Overlapping-horizon inference (amendment 2026-06-11, defect repair):**

Day-clustering alone is wrong for multi-day horizons. A 5d forward return
sampled daily shares 4/5 of its window with the previous day's
observation; that serial overlap inflates effective N and narrows CIs in a
way day-clustering does not touch (day clusters correct within-day
cross-correlation only). For any statistic on a horizon > 1 day, use a
**circular block bootstrap over days with block length ≥ the horizon in
trading days** (≥ 5 for the 5d horizon), or restrict to non-overlapping
samples. Everywhere this document says "bootstrap-day-clustered" for a
multi-day horizon, read "block bootstrap, block ≥ horizon." Note the §2.5
embargo handles *training* leakage in walk-forward; it does not fix
*inference* on overlapping outcomes — both are needed.

**Headline rule (sits above every Phase 1 finding):**

> Every reported positive finding must survive (a) BY multiple-testing
> control at α = 0.10 across the explicit discovery cell family,
> (b) era-consistency check on the exploration set, (c) excess-return-over-
> index re-derivation, (d) bootstrap-day-clustered CI lower bound above zero,
> AND (e) untouched-holdout confirmation at α = 0.05 with BY FDR control
> across the pre-registered family. A finding that fails
> any one of these is treated as null.

### 7.8 Implementation order (concrete plan)

Once the 0-D engine ships and the 8 Parquet tables are written (the §5
step-0 tracer-bullet vertical slice runs earlier still — as soon as the
engine writes its first month of output, before and independent of the
stack below):

-1. **Power / minimum-detectable-effect pass** (~1 day, BEFORE anything
   else; needs no engine output) — per candidate coarse cell, compute:
   effective N (days, after signal-day filtering), the minimum annualized
   Sharpe detectable at the BY-corrected threshold given the family size,
   and the implied detectable-edge map (§1.5.1). If the detectable floor
   sits far above the §2.8 hurdle for most of the family, shrink or
   coarsen the family BEFORE discovery. This pass also fixes the family
   definition that gets pre-registered.

0. **Synthetic-signal pipeline validation** (~2 days, BEFORE running on real
   data) — see §7.9. Inject signals with known properties through the full
   pipeline and verify (a) the pipeline detects edges that exist,
   (b) it rejects nulls at the expected α-rate, (c) the DP recovers the
   known optimal policy on the synthetic transition probabilities. Skip
   this and you risk publishing pipeline bugs as "findings."

0.5. **Placebo runs on real data** (~1 day) — run the full discovery
   pipeline, including the human steps, on real data with randomized
   entry dates (or signals shifted by a random number of weeks), several
   times. The findings-per-placebo-run count is the pipeline's *empirical*
   false-discovery rate — it catches everything §7.9's synthetic
   injections can't (researcher degrees of freedom, plumbing leaks). If
   placebo runs produce "findings" above the nominal α rate, fix the
   pipeline before touching real signals.

1. **MI matrix pass** (a few hours of Python) — `mutual_info_regression`
   between every ML-safe column and every outcome on the exploration set
   only. Multi-estimator agreement (binning + KNN) and permutation null
   per §7.7.1. Output: `mi_matrix.parquet`.

2. **BN structure learning** (a day) — PC or NOTEARS on the top-30 features
   + outcomes. Stability selection via bootstrap; per-era DAGs with edge-
   consistency check. Output: a DAG + edge weights per era + structural
   edges (consistent across all eras).

3. **Cost-model derivation** (a day) — per §6.1, compute per-(offset, cell)
   spread proxy, slippage curve, and break-even cost ceiling per cell.
   The cost model is built from cost-side inputs only and frozen (never
   derived from the edge distribution — §6.1). Output: `cost_model.parquet`.

4. **DP per cell** (a few days, parallelizable) — **gated: DP runs ONLY
   on cells that survived the cheap screen** (conditional-sort expectancy
   clears the noise floor AND the §2.10 predictability ceiling is
   non-trivial). Optimal stopping on noise is noise; do not build the
   optimizer where there is nothing to optimize. For each surviving
   `(classification, regime, era)` cell, use the minimal sufficient state
   selected per §7.3.1.1 (MI + domain candidates, BN cross-check,
   ablation-gated), estimate `P(s' | s, hold)` empirically from
   `forward_path_short` (with hierarchical shrinkage on transitions per
   §7.7.1), run backward induction OR fitted Q iteration per §7.3.2
   depending on state dimensionality. Output: policy per cell + OOS-eval
   value on validation set.

5. **Markov adequacy check** (half-day) — fit 2-step lookback comparison
   model per §7.3.1; if it beats DP by >5%, augment state and retry.

6. **Hierarchical Bayes pass** (a day) — fit a partial-pooling model across
   cells. Output: cell-level credible intervals for expected return under
   the DP policy.

6.5. **Walk-forward stability pass** (a day) — the §2.5 anchored
   walk-forward with ~10-day embargo over exploration + validation: per
   coarse cell, recompute the headline conditional statistics on each
   anchored window and report (a) sign consistency over time, (b) rank
   correlation of the cross-cell ordering between consecutive windows,
   (c) the §2.9 trajectory fit. This is the primary test for stability
   claims (§5.0); cells whose structure does not replicate across windows
   are characterization-only, regardless of pooled significance.

7. **Multiple-testing pass** (half-day) — Benjamini-Yekutieli at α=0.10
   over the explicit cell family per §7.7.1.

8. **Pre-register surviving cells** (~1 day, manual) — identify cells that
   passed the BY multiple-testing pass with bootstrap CI lower bound > 0
   on the exploration set and validation-set sanity check. Capped at 20
   per §7.7.1; over-subscription is ranked by hierarchical-posterior lower
   bound × deployable capacity.

8.5. **Pre-registration finalisation** (~half day) — for each surviving
   cell, fill out the §7.10 template **completely**: classification filters,
   regime filters, entry/exit specification, cost model, performance metric,
   required thresholds, multiple-testing parameters, risk acknowledgments.
   Commit the filled templates to the repo as `pre_registrations/<cell_id>.yaml`
   with a git-tagged timestamp. **The holdout set (2023-01-01 → 2026-06-05)
   is never read before this timestamp.** Any field left blank or "TBD" at
   timestamp time cannot be filled in later — that's the entire point of
   pre-registration.

9. **Holdout test** (a day) — apply pre-registered DP policies + features
   to the 2023-01 → 2026-06 holdout (per §2.5 train/test partition), with
   BY at α = 0.05 across the pre-registered family. Cells that survive
   the holdout statistical test (one-sided bootstrap day-clustered CI
   lower bound > 0) are real candidates.

10. **Live shadow period** (2-3 months calendar time, ~zero effort) —
   paper-trade the holdout survivors in real time before any capital.
   The only data untouchable by construction is the future; the daily
   ingest already runs. A cell that survives discovery, validation,
   holdout, AND live shadow is the real deliverable of Phase 1.

Total: ~3.5 weeks of analytical work after engine ships, plus a 2-3 month
live-shadow tail (calendar time, not effort). The stack runs on a laptop;
nothing here needs a cluster.

### 7.9 Synthetic-signal validation — backtest the backtester

Before running the pipeline on real data, validate that it does what you
think it does. Cheap insurance against bugs that would silently corrupt
findings.

**Inject three synthetic signals, each into a copy of the exploration data
with known properties:**

1. **Known positive edge.** Inject a signal with 55% hit rate, 1:1 R/R,
   5 bps costs. Pipeline must:
   - Identify the cell containing the injection in MI / BN.
   - DP must derive a policy whose OOS value matches the analytical
     optimum to within ~5%.
   - The cell must survive BY at α = 0.10 and the holdout at α = 0.05.

2. **Pure null.** Inject random returns matching the empirical
   distribution but with no signal. Pipeline must:
   - Reject the null at the expected rate (~10% × cells in family).
   - Not produce a holdout-significant finding.

3. **Look-ahead leakage.** Inject a signal that uses future information
   (e.g., feature includes tomorrow's return). Pipeline must:
   - Show suspiciously high in-sample edge.
   - DP should produce a "too good to be true" policy.
   - **The validation-set check should detect the gap** (in-sample much
     better than validation). This tests that the train/validate split is
     wired correctly.

If any of these three fail, the pipeline has a bug; fix before running on
real data.

### 7.10 Pre-registration template (fill out per candidate cell)

Surviving cells from step 8 of §7.8 must be pre-registered using this
template. The completed registration is committed to the repo with a
timestamp and never edited.

```yaml
cell_id: <hash of classification × regime × era × offset × horizon>
registered_at: <ISO timestamp>
exploration_window: 2016-06-08 → 2020-12-31
validation_window: 2021-01-01 → 2022-12-31
holdout_window:    2023-01-01 → 2026-06-05

classification_filters:
  ticker_type: CS
  market_cap_bucket: [large, mega]    # explicit list
  liquidity_bucket: [normal, liquid, highly_liquid]
  price_bucket: [5_to_20, 20_to_100, above_100]
  behavioral_tags_required: []
  behavioral_tags_excluded: [is_leveraged_etf, is_inverse_etf, is_china_adr,
                             is_recent_ipo]

regime_filters:
  vix_level: [low, medium]
  spy_trend: [uptrend, sideways]

entry_specification:
  entry_offset: "1010"                # single offset, NOT a range
  signal_condition: "intraday_ret_0930_to_1000 > 0"

exit_specification:
  source: "DP policy from cell <cell_id>"
  policy_artifact: "policies/cell_<cell_id>_dp.pkl"
  state_vector: [current_ret_over_atr_14d, drawdown_over_atr_14d,
                 bars_elapsed, regime]
  fallback_rule: "EOD exit if policy errors"

cost_model:
  spread_proxy_bps: <from §6.1 derivation>
  slippage_size_assumption: 1pct_addv
  commission_per_share: 0.0

performance_metric:
  primary: "cost-adjusted Sharpe of excess return over SPY"
  secondary: ["hit rate", "max drawdown", "mean trade duration",
              "edge_now + edge_trend trajectory per §2.9",
              "variance-ledger residual share per §2.10"]

required_thresholds:
  exploration_set_sharpe: >= 0.5             # frozen per §1.6 / §2.8
  validation_set_sharpe: >= 0.4              # descriptive sanity gate
  bootstrap_lower_bound_excess_return: > 0   # exploration set, cross-fit V per §7.7.1
  name_concentration_check: "edge sign unchanged with top-5 contributing names removed"

holdout_statistical_test:
  method: "bootstrap day-clustered one-sided CI on cost-adjusted excess return"
  alpha: 0.05
  condition: "lower_bound_excess_return > 0"
  family_fdr: "BY at alpha = 0.05 across the pre-registered family (<= 20 cells)"
  # Sharpe is reported as a secondary descriptive metric on the holdout,
  # not as a pass/fail gate. The headline rule (§1.5) is the bootstrap CI
  # test, not a Sharpe threshold, because Sharpe-as-a-threshold tempts
  # post-hoc threshold-shopping ("if I'd asked for 0.35 instead of 0.4...").

multiple_testing:
  family_definition: "coarse (classification × regime) per §7.7.1; drill-down under hierarchical FDR"
  family_size: <m>
  method: benjamini_yekutieli
  alpha: 0.10 (discovery), 0.05 (holdout)

post_holdout:
  live_shadow_months: 2-3      # paper-trade in real time before any capital

risk_acknowledgment:
  - "Findings may not survive 2025-2026 era"
  - "Capacity capped at <X> AUM per §6"
  - "DP policy is conditional-optimal only inside specified MDP (§7.7)"
```

Anything not in this template at registration time **cannot** be added
when running the holdout test. If you discover at holdout time that the
performance metric should be "Sortino instead of Sharpe," that's a
post-hoc justification — the original Sharpe result is what stands.

---

## 8. What NOT to do

Bright-line rules. Any of these voids the research.

- Don't optimize on the holdout. Don't even look at the holdout until
  Phase 1 hypotheses are pre-registered and frozen.
- Don't pool across classifications without §14.4-style decomposition.
- Don't trust any "discovered edge" with a non-smooth surface neighborhood.
- Don't use OLS standard errors on intraday returns. Day-clustered or bust.
- Don't believe ML feature importance. Use SHAP; even then, treat as
  diagnostic only.
- Don't sweep thresholds without pre-registration. Either test a specific
  threshold or use survival analysis to derive one.
- Don't backtest unconditionally as the "headline" result. Always slice.
- Don't add columns chasing the latest finding. The schema is frozen for
  reasons; revisit only between full Phase 1 cycles, with version bumps.
