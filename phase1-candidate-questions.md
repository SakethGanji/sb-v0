# Phase 1 — Candidate Research Questions (Backlog)

**Status:** CANDIDATE BACKLOG — not frozen. This is a brainstorming catalog
feeding §7.7.1 family definition and the §5 reporting order. Nothing here is
pre-registered; pre-registration happens later, against a *coarse*
`(classification × regime)` family per §1.5.1.

**Revision 2026-06-14 — analysis-spec enrichment:** as of this revision EVERY
in-scope question carries an `Analysis → output` column (placed right after
`Algo`) giving the concrete estimator / test statistic / state-vector-and-reward
(DP) / variable-pair-and-null (MI) / competing-risks setup (survival) /
feature-set-and-k-selection (clustering) / conditional-sort-and-CI (direct),
the mandatory discipline that applies (§2.3 excess lens, §6.1 cost-adjustment,
§7.7.1 day-clustered/block bootstrap CI, §2.4 surface-smoothness + §2.5
walk-forward for "survives/stable/persists" phrasing, name-concentration where
a cell edge could be a few tickers), and the decision-relevant output artifact.
The separate `Data gap` column is RETAINED (width is acceptable); gap notes are
unchanged. Per-section `Analysis approach:` notes (in the spirit of strategy
§7.5) sit under each header. Out-of-scope rows (cross-cutting table, §21.8)
carry no analysis spec because the required data is absent.

**Hard rules this catalog obeys:**

1. **Query-time answerability.** Every question is answerable at query time
   from Phase 0 v7 columns *unless* explicitly flagged
   `NEEDS DATA NOT ON DISK — out of scope`. Flagged questions name the missing
   data. Out-of-scope datasets per RFC §6.1 / strategy §1.1: borrow fees,
   float / historical shares outstanding, index membership, news feed,
   NBBO quotes / tick data, options / implied vol, catalyst events
   (FDA/M&A/analyst/guidance/offerings).
2. **Tier protects power (§1.5.1).** The primary Benjamini-Yekutieli family is
   COARSE — `(classification × regime)`, ~200–1,000 hypotheses, each scored on
   a *pre-specified* offset/horizon aggregation (surface mean, never max). Each
   question is tagged **primary** (belongs in the coarse BY family) or
   **exploratory** (hypothesis-generation / drill-down on coarse survivors
   under hierarchical FDR — does NOT multiply the primary family). Tagging
   conservatively: a question is `primary` only if it can be reduced to a
   single coarse-cell expectancy test; anything that slices finer (per-offset,
   per-horizon, per-era, per-name, per-checkpoint) or that is structural
   (MI / BN / clustering / change-point) is `exploratory` by construction.
3. **Long-only execution; excess-over-index default lens; cost-adjustment
   mandatory; everything slices by classification (§14.4).** Short-side and
   downside questions are research-only: tagged
   `pattern/avoid only — not an executable edge`. Borrow / locate / SSR /
   squeeze data is absent, so no short question can become an executable edge
   in Phase 0.

**Algorithm legend (§7):** MI = mutual information (L1); BN = Bayesian network
structure (L2); DP = Bellman dynamic programming / FQI (L3); HB = hierarchical
Bayes (L4); survival = competing-risks / discrete-time hazards (§2.7);
clustering = unsupervised on pre-entry features; quantile-reg = quantile
regression; change-point = Bayesian online change-point; eigendecomp = PCA /
SVD; direct = conditional sort + day-clustered CI.

**Executable / pattern / avoid legend:**
`exec` = candidate long-only executable edge;
`avoid` = avoid-filter / blacklist (negative space, §2.2);
`pattern` = short-side or descriptive pattern, not executable in Phase 0.

**Column-name caveat:** column names below are quoted from RFC v7 §7–§11.6 and
strategy §3.7. `<H>` ∈ {10min,30min,60min,EOD,1d,2d,3d,5d,10d,21d,42d,63d,252d}
(13 horizons). Threshold-cross columns use the frozen ±0.5/1/2/3/5/10/20pct and
±0.25/0.5/1/1.5/2/3atr sets.

---

## Section 1 — Is-the-day-tradeable / sit-out (regime gating)

*Analysis approach: this is the §2.1 Stage-1 day-gate — a logistic classifier
on 3-6 strictly pre-10:00 features (no leakage) plus direct conditional sorts
of day-level expectancy, with the "tradeable" label itself defined from
pre-decision day properties. The shared discipline: every "survives / stable /
drifts" claim is settled by walk-forward (§2.5, anchored + embargo) on the
exploration+validation window, NEVER the holdout; outputs are day-level
sit-out filters, so they are avoid-tagged and most are exploratory (the day-gate
is structural, not a single coarse-cell test).*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 1.1 | Under which prior-day-knowable regimes does the cross-section of momentum names have positive cost-adjusted excess return that survives walk-forward? | `vix_close` (prior), `spy_overnight_gap`, `breadth_*_at_1000`, prior-day `prior_day_last_30m_return` | direct + HB | Direct conditional sort of daily cross-section mean `ret_<H>_excess_spy` (§2.3 lens) by pre-10:00 regime tercile; HB shrinks per-regime daily means; day-clustered bootstrap CI, cost-adjusted (§6.1). "Survives" → anchored walk-forward replication (§2.5). **Output:** a sit-out regime list (regimes whose CI upper bound < 0 = avoid). | primary | avoid | — |
| 1.2 | Does a logistic "tradeable-day" classifier on 3-6 pre-10:00 features lift Sharpe-per-trade vs sit-out, stable across walk-forward (§2.1.3)? | `spy_overnight_gap`, `premarket_dollar_volume_rank_today` (universe agg), `breadth_pct_universe_green_at_1000`, `day_of_week`, `is_half_day` | direct (logistic) | Logistic P(tradeable) on the 5 pre-decision features; label = day where median liquid-name `|excess ret|` > exploration median (§2.1 rule 4); compare Sharpe-per-trade trade-days vs sit-out-days, day-clustered CI on the gap; stability via walk-forward calibration curve. **Output:** a binary day-gate + recorded P(tradeable) for Phase-2 sizing. | exploratory | avoid | — |
| 1.3 | Is there a day-type where median liquid-name `|intraday_ret_0930_to_1000_excess_spy|` is structurally low (no dispersion → nothing to trade)? | `cross_sectional_ret_dispersion_at_1000`, `cross_sectional_ret_iqr_at_1000` | direct | Direct sort of daily `cross_sectional_ret_dispersion_at_1000` by day-type (day_of_week, half-day, regime); test whether bottom-dispersion days have near-zero realizable spread; day-clustered CI. **Output:** a "no-dispersion → sit out" day filter. | exploratory | avoid | — |
| 1.4 | Do high-`vix_close` (prior-day, top tercile) days have negative cost-adjusted expectancy regardless of name selection? (cf §1.3.5) | `vix_close`, regime `vix_level`, `ret_<H>_excess_spy` | DP + direct | DP with `vix_level` regime as the only state, action = trade/sit-out, reward = cost-adjusted `ret_<H>_excess_spy`; cross-check with direct tercile sort; day-clustered CI, percentile buckets on exploration set (§7.7.1). **Output:** a VIX-regime branch in the day-gate (refuse top tercile if V<0). | primary | avoid | — |
| 1.5 | Do FOMC / opex / holiday-week calendar days carry negative expectancy? | `day_of_week`, `is_half_day`, `days_since_first_bar` (calendar derivation) | direct | Direct conditional sort of daily cost-adjusted excess expectancy on the calendar flag; day-clustered CI. **Output:** a calendar avoid-list — *contingent on the missing flags below.* ⚠ gap: is_fomc/is_opex not on disk. | exploratory | avoid | is_fomc/is_opex flags NOT on disk — derive from external calendar; out of scope unless added |
| 1.6 | Does opening breadth (`breadth_advance_decline_ratio_at_1000`) below a pre-set percentile predict a sit-out day? | `breadth_advance_decline_ratio_at_1000`, `breadth_pct_universe_green_at_1000` | MI + direct | MI of opening-breadth vs day-level mean `ret_<H>_excess_spy` against the shuffled-day null (§7.7.1); then direct sort below the pre-set percentile, day-clustered CI. **Output:** a breadth-floor sit-out threshold. | exploratory | avoid | — |
| 1.7 | Does universe liquidity collapse (low `universe_total_dollar_volume`) mark untradeable days? | `universe_total_dollar_volume`, `universe_median_addv_20d`, regime `liquidity` | direct | Direct sort of cost-adjusted excess expectancy by `liquidity` regime tercile; costs especially bite here (§6.1), so report net; day-clustered CI. **Output:** a low-liquidity-day avoid filter. | primary | avoid | — |
| 1.8 | Is the dispersion regime (new 6th taxonomy parent) a better day-gate than the VIX regime? | `cross_sectional_ret_dispersion_at_1000` vs `vix_close`; regime `dispersion` vs `vix_level` | MI + direct | Compare `I(dispersion_regime; day-outcome)` vs `I(vix_level; day-outcome)` (both vs shuffled null); direct head-to-head of the two gates' Sharpe-per-trade lift, day-clustered CI. **Output:** which taxonomy to use as the primary day-gate. | exploratory | avoid | — |
| 1.9 | Does SPY overnight gap sign/size gate the day's idiosyncratic momentum? | `spy_overnight_gap`, `qqq_overnight_gap`, `iwm_overnight_gap` | DP (regime in state) | DP with `spy_overnight_gap` sign/size bucket in the state, action = trade/sit-out, cost-adjusted excess reward; bin-sensitivity sweep (5/10/15, §7.7.1). **Output:** a gap-conditioned day-gate branch. | primary | avoid | — |
| 1.10 | Calibration decay: does the tradeable-day classifier's reliability drift across eras (decay detector)? | classifier output time series | change-point | Bayesian online change-point on the classifier's daily reliability (predicted vs realized P(tradeable)) time series; exploration set only. **Output:** a calibration-drift kill-switch / decay date for the day-gate. | exploratory | avoid | — |

---

## Section 2 — Beta / sector neutralization & idiosyncratic alpha

*Analysis approach: factor regression (vs SPY/QQQ/IWM) + the excess-over-index
lens (§2.3) are the right tools because the residual after beta/sector removal
is the only genuinely new information; eigendecomposition supplies the §2.10
variance-attribution ledger. Shared discipline: every edge is reported raw AND
excess (§2.3) — an edge that survives raw but vanishes in excess space is a beta
wrapper and is killed.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 2.1 | For each coarse cell, does excess-over-SPY return stay positive after costs when raw return does — i.e., is the edge idiosyncratic, not beta? (cf §2.3) | `ret_<H>`, `ret_<H>_excess_spy/qqq/iwm` | direct + HB | Per coarse `(classification × regime)` cell, paired direct sort of `ret_<H>` vs `ret_<H>_excess_spy`, both cost-adjusted (§6.1); HB shrinkage on cell means; day-clustered (block ≥ horizon, §7.7.1) CI lower bound > 0. **Output:** an idiosyncratic-edge whitelist (positive in excess space). | primary | exec | — |
| 2.2 | Which index (`spy`/`qqq`/`iwm`) is the right benchmark per classification (`style_bucket`)? | `ret_<H>_excess_{spy,qqq,iwm}`, `style_bucket`, `beta_*_60d` | direct | Per `style_bucket`, compare residual variance of `ret_<H>_excess_{spy,qqq,iwm}` and pick the benchmark giving the tightest idiosyncratic residual; day-clustered CI. **Output:** a per-style benchmark map for the §2.3 lens. | exploratory | exec | — |
| 2.3 | Does conditioning on `beta_spy_60d` bucket remove an apparent edge (edge was beta loading)? | `beta_spy_60d`, `beta_qqq_60d`, `ret_<H>_excess_spy` | factor-reg + MI | Factor regression of `ret_<H>` on index returns to strip beta, then `I(signal; residual | beta_bucket)` vs shuffled null; if conditioning on beta bucket collapses the edge it was beta loading. **Output:** an avoid-flag on beta-loading-disguised-as-edge cells. | exploratory | avoid | — |
| 2.4 | Variance-attribution: what share of cell outcome variance is market/sector vs idiosyncratic (§2.10 ledger)? | `ret_<H>`, index returns in `market_context_daily`, `sector_aggregates_daily` | eigendecomp + factor-reg | Build the §2.10 ledger per cell: factor-reg attributes market/sector shares, eigendecomp of the residual covariance gives the idiosyncratic span; residual reported as a SHARE with bootstrap CI. **Output:** a per-cell variance ledger (sizing input + noise-null). | exploratory | exec | — |
| 2.5 | Does sector-relative momentum (`sector_ret_0930_to_1000_rank_today`) carry alpha beyond market-relative? | `sector_aggregates_daily.sector_ret_*`, `ret_<H>_excess_spy` | MI + direct | Conditional `I(sector_ret_rank; ret_<H>_excess_spy | market-relative signal)` vs shuffled null; direct sort confirms sign; day-clustered CI, cost-adjusted. **Output:** a yes/no on sector-relative as an added conditioner. | exploratory | exec | — |
| 2.6 | Is the residual after SPY+QQQ+IWM factor removal large enough that there is any alpha to find per cell (else stop)? | index returns, `ret_<H>` | factor-reg | Factor regression vs the 3 indices per cell; report residual variance share; a near-zero residual = no idiosyncratic alpha possible → stop researching that cell. **Output:** a per-cell go/stop gate (the §2.10 floor). | primary | exec | — |
| 2.7 | Do high-`beta_iwm_60d` (small-cap-like) names carry different idiosyncratic alpha than `spy_like`? | `beta_iwm_60d`, `style_bucket`, `ret_<H>_excess_iwm` | DP per cell + HB | DP per `(style_bucket × regime)` cell with cost-adjusted `ret_<H>_excess_iwm` reward; HB shrinks the small-cap-vs-large-cap difference; day-clustered CI. **Output:** separate policies for iwm-like vs spy-like, with shrunk difference. | primary | exec | — |
| 2.8 | Style-bucket stability: does `style_bucket` assignment persist enough across a hold to make neutralization meaningful? | `style_bucket` across days | direct | Direct measure of `style_bucket` persistence (transition rate) over the hold horizon; if names churn buckets mid-hold, neutralization is ill-defined. **Output:** a validity check on per-style neutralization (pattern note, not an edge). | exploratory | exec | — |

---

## Section 3 — Swing / multi-day continuation (1d–252d)

*Analysis approach: DP across the horizon grid finds the holding-period
sweet-spot as a value-over-t trajectory; survival handles position-lifetime
questions; MI/quantile-reg screen daily-frequency predictors. Shared
discipline: TOTAL-return columns for multi-day (dividend distortion, §3.3),
excess-over-index lens (§2.3), and — non-negotiable — block bootstrap with
block ≥ horizon in trading days (§7.7.1 overlapping-horizon fix), never plain
day-clustering.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 3.1 | Under which cells does multi-day continuation (`ret_5d_total_excess_spy`) stay positive after costs, smooth across the horizon surface, and survive holdout? (cf §1.3.3) | `ret_<H>_total`, `ret_<H>_excess_spy`, `dividend_ex_date_within_<H>` | DP across horizons + HB | DP over the horizon grid per cell, reward = cost-adjusted `ret_<H>_total_excess_spy`; HB shrinks cell means; require a SMOOTH `(offset × horizon)` surface (§2.4, not an isolated spike) + block bootstrap (block ≥ horizon) CI > 0 + walk-forward replication (§2.5). **Output:** a swing whitelist of continuation cells. | primary | exec | — |
| 3.2 | Does the edge reverse after day 3 (continuation → mean reversion)? (cf §1.3.3) | `ret_2d_total`, `ret_3d_total`, `ret_5d_total`, `close_to_close_return_day_{1..5}` | DP value over t | DP value `V(cell, t)` traced across daily checkpoints; locate where V stops rising / turns down (exhaustion horizon); block bootstrap CI on the day-3→day-5 delta. **Output:** an exhaustion-horizon map / time-stop rule. | exploratory | exec | — |
| 3.3 | 1d vs 5d vs 21d Sharpe per cell — where is the holding-period sweet spot? (cf §1.3.3) | `ret_1d_total`, `ret_5d_total`, `ret_21d_total` | DP across horizons | `argmax_horizon` of cost-adjusted Sharpe per cell via DP across horizons, excess lens (§2.3); block bootstrap (block ≥ horizon) CI on each. **Output:** the per-cell optimal hold horizon. | primary | exec | — |
| 3.4 | Does `ret_1d` carry mutual information about `ret_5d` (is short-term continuation predictive of swing)? (cf §1.3.12) | `ret_1d`, `ret_5d` | MI | `I(ret_1d; ret_5d)` (binning + KNN, report agreement) vs the shuffled null at the 95th pctile (§7.7.1). **Output:** ordinal evidence on whether the 1d signal carries swing-horizon info. | exploratory | exec | — |
| 3.5 | Price-only vs total-return: how much does dividend distortion change the multi-day ranking of cells? (cf §3.3) | `ret_<H>` vs `ret_<H>_total`, `dividend_ex_date_within_<H>` | direct | Direct re-ranking of cells under `ret_<H>` vs `ret_<H>_total`, gated on `dividend_ex_date_within_<H>`; report rank-shift magnitude. **Output:** a measurement-honesty flag (cells whose ranking is a dividend artifact). | exploratory | exec | — |
| 3.6 | Long-horizon (63d/252d) cross-sectional momentum: does it survive in excess-return space at all? | `ret_63d_total`, `ret_252d_total`, `ret_*_excess_spy` | direct + HB | Direct sort of `ret_{63,252}d_total_excess_spy` per cell, cost-adjusted, HB shrinkage; **restrict to NON-overlapping windows** — daily-sampled 252d returns overlap ~99% and the §7.7.1 block bootstrap degenerates to ~4–18 blocks over the exploration window, so it is not a valid test here. **Characterization-only: effective N is too small to clear BY; report the distribution, never pre-register.** ⚠ power-limited (§1.5.1). **Output:** a descriptive read on whether long-horizon CS momentum *appears* in excess space — explicitly not a testable edge. | exploratory | pattern | — |
| 3.7 | Does `consecutive_up_days_close_to_close` predict forward multi-day continuation or exhaustion? | `consecutive_up_days_close_to_close`, `ret_5d_total` | MI + quantile-reg | `I(consec_up_days; ret_5d_total)` vs null, then quantile regression of `ret_5d_total` on the streak length to separate continuation (upper-quantile rises) from exhaustion (turns down); block bootstrap CI. **Output:** a continuation-vs-exhaustion curve over streak length. | exploratory | exec | — |
| 3.8 | Survival of a swing position: distribution of `time_to_recover_after_first_drawdown_5d`. | `time_to_recover_after_first_drawdown_<H>`, `max_consecutive_bars_underwater_<H>` | survival | Discrete-time hazard / KM of `time_to_recover_after_first_drawdown_<H>` per cell (recovery as the event, horizon-end as censoring); day-clustered CI. **Output:** a recovery-time distribution → how long to give a swing before bailing. | exploratory | exec | — |
| 3.9 | Does terminal-event risk (delisting) materially truncate long-horizon returns in low-cap cells? | `terminal_event_type`, `terminal_event_return`, `last_valid_trade_date`, `days_with_missing_forward_bars` | direct | Direct comparison of long-horizon return distributions with vs without `terminal_event_type` rows, by `market_cap_bucket`; quantify truncation/tail loss. **Output:** a low-cap long-horizon avoid-flag (delisting truncation risk). | exploratory | avoid | — |

---

## Section 4 — Overnight-gap vs RTH-drift decomposition

*Analysis approach: BN structure learning answers the central question
directly — read off whether the gap node or the RTH node is the parent of
`ret_1d` in the DAG — while DP value comparison sizes the overnight-hold
decision and conditional MI tests gap-on-gap dependence. Shared discipline:
the gap leg carries uncompensated overnight carry risk, so the overnight-hold
comparison is always cost-adjusted (§6.1) and reported in excess space (§2.3);
BN edges must clear 80%-bootstrap stability and not flip by era (§7.7.1).*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 4.1 | For cells with positive `ret_1d`, does the edge come from the overnight gap or the regular session? (cf §1.3.8) | `gap_return_day_1`, `rth_return_day_1`, `open_to_close_return_day_1` | BN structure | Learn the DAG over {gap_return_day_1, rth_return_day_1, ret_1d}; read which node is the parent of `ret_1d`; stability-select edges at 80% bootstrap, check era-flip (§7.7.1). **Output:** a gap-vs-RTH attribution per cell (where the carry risk lives). | exploratory | exec | — |
| 4.2 | Is overnight-hold expectancy positive after costs (does holding through the close beat exiting at the close)? (cf §1.3.8) | `ret_to_close` vs `ret_1d`, `next_day_gap_return` | DP comparison | DP value comparison: V(exit-at-close = `ret_to_close`) vs V(hold-overnight = `ret_1d`), both cost-adjusted (§6.1) and excess (§2.3); day-clustered CI on the difference. **Output:** an exit-at-close vs hold-overnight policy per cell. | primary | exec | — |
| 4.3 | Does intraday momentum gap UP and continue, or gap up and fade the next morning? | `next_day_gap_return`, `next_day_first_30m_return`, `next_day_fade_from_open`, `next_day_continuation_from_open` | direct + BN | Direct sort of `next_day_continuation_from_open` vs `next_day_fade_from_open` conditioned on `next_day_gap_return` sign; BN cross-check of the next-day structure. **Output:** a gap-and-go vs gap-fade next-morning map. | exploratory | exec | — |
| 4.4 | Do overnight gaps reverse the next day (gap mean-reversion)? (cf §1.3.8) | `next_day_gap_return`, `next_day_close_return`, `overnight_gap` | conditional MI | `I(overnight_gap; next_day_close_return | gap_sign)` vs shuffled null; sign of the conditional dependence tells reversion vs continuation. **Output:** a gap-mean-reversion flag (avoid chasing reversed gaps). | exploratory | exec | — |
| 4.5 | Across days 1-5, does the cumulative edge concentrate in gaps or RTH (gap_return vs rth_return running sum)? | `gap_return_day_{1..5}`, `rth_return_day_{1..5}` | direct | Direct running-sum decomposition of `gap_return_day_{1..5}` vs `rth_return_day_{1..5}` per cell; block bootstrap (block ≥ horizon) CI on each leg's share. **Output:** a 5-day gap-vs-RTH edge-concentration profile. | exploratory | exec | — |
| 4.6 | Does next-day first-5m drift (`next_day_first_5m_return`) carry tradeable continuation, or is it noise/spread? | `next_day_first_5m_return`, `next_day_first_15m_return`, entry-cost proxies | MI + direct | `I(next_day_first_5m_return; forward outcome)` vs null, then direct net-of-cost test using the entry-cost proxies (§6.1) — first-5m drift often loses to the spread. **Output:** a tradeable/noise verdict on opening-drift continuation. | exploratory | exec | — |
| 4.7 | Is the gap component's sign conditional on `overnight_gap` of the entry day (gap-on-gap)? | `overnight_gap`, `gap_return_day_1` | conditional MI | `I(overnight_gap; gap_return_day_1)` and its conditional sign vs shuffled null. **Output:** a gap-on-gap dependence sign (does an entry-day gap predict the next gap). | exploratory | exec | — |
| 4.8 | Does `next_day_close_location_in_range` (where it closed in its range) predict day-2 follow-through? | `next_day_close_location_in_range`, `gap_return_day_2`, `rth_return_day_2` | MI | `I(next_day_close_location_in_range; day-2 gap/RTH return)` vs shuffled null; treat as ordinal feature ranking. **Output:** a close-near-high follow-through signal. | exploratory | exec | — |

---

## Section 5 — Short-side / downside-continuation / gap-fade (pattern + avoid only)

*Analysis approach: mirror DP (the §1.3.17 negative-signal counterfactual) and
survival on drawdown paths characterize the downside; the OUTPUT is never an
executable short (borrow/locate/SSR/squeeze data absent, so price-only short
Sharpe is systematically overstated). Shared discipline: every result lands as
a long-side avoid-filter (§2.2 negative space), a downside pattern, OR the
explicit long-vs-short comparison with a graduation note (§1.3.22) — never a
pre-registered short edge.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 5.1 | Do negative-signal names (signal < 0, control group) show downside continuation symmetric to upside? | `pre_entry_ret_from_open` < 0 cohort, `first_cross_down_*pct_<H>`, `ret_<H>` | mirror DP | Mirror DP on the negative-signal cohort (state/reward as in §7 but signal sign flipped); compare its |V| to the long leg's; day-clustered CI. ⚠ pattern only — borrow/locate/SSR absent. **Output:** a downside-continuation pattern + long-vs-short symmetry comparison (graduation note). | exploratory | pattern | borrow/locate/SSR absent — pattern only |
| 5.2 | Gap-fade: do large up-gaps (`overnight_gap` top pctile) fade intraday (short-the-gap pattern)? | `overnight_gap`, `ret_to_close`, `post_entry_close_vs_high_return` | direct | Direct sort of `ret_to_close` for top-percentile `overnight_gap` names; name-concentration check (a few meme tickers can drive it, §7.7.1). **Output:** a gap-fade pattern + a long-side "don't chase the gap-up open" avoid note. | exploratory | pattern | borrow/SSR absent |
| 5.3 | Which cells show reliable downside continuation that the LONG book must AVOID entering against? | `first_cross_down_*atr_<H>`, `max_drawdown_<H>`, classification | DP + direct | DP / direct sort of cost-adjusted downside expectancy per classification cell using `first_cross_down_*atr_<H>` / `max_drawdown_<H>`; day-clustered CI, name-concentration check. **Output:** a long-book avoid-list (cells with reliable downside continuation). | primary | avoid | — |
| 5.4 | Does `gap_filled_today_flag` (low touched prior close) mark a fade-prone setup to avoid going long into? | `gap_filled_today_flag`, `post_entry_morning_low_return` | MI + direct | `I(gap_filled_today_flag; post_entry_morning_low_return)` vs null + direct sort of forward downside conditioned on the flag. **Output:** a fade-prone-setup long-avoid filter. | exploratory | avoid | — |
| 5.5 | Downside path asymmetry: is `max_drawdown_<H>` deeper/faster than `max_runup_<H>` per cell (tail-loss profile)? | `max_drawdown_<H>`, `bars_to_max_drawdown_<H>`, `max_runup_<H>` | survival + direct | Competing-risks-style timing comparison: hazard/timing of `bars_to_max_drawdown_<H>` vs runup, plus direct depth ratio `max_drawdown/max_runup`; day-clustered CI. **Output:** a per-cell tail-loss asymmetry profile (sizing/avoid input). | exploratory | avoid | — |
| 5.6 | Do meme/low-float candidates show squeeze-then-collapse (right-tail then deep drawdown)? | `is_meme_candidate`, `is_low_float_candidate`, `max_runup_<H>`, `max_drawdown_<H>` | clustering + direct | Cluster meme/low-float names on `(max_runup_<H>, max_drawdown_<H>)` shape (k by bootstrap stability); identify a squeeze-then-collapse cluster; direct profile. ⚠ float/short-interest absent (proxy). **Output:** a squeeze-collapse pattern + long-avoid flag. | exploratory | pattern | float + short-interest absent (proxy only) |
| 5.7 | After a stop-out, what's the distribution of remaining downside (would holding have been worse)? | `forward_path_short` after stop checkpoint, `low_ret_so_far` | survival / counterfactual | Counterfactual: condition on the stop checkpoint, then survival of `low_ret_so_far` on the post-stop path; P(further drop | stopped). **Output:** a "stop was right / death spiral" validation of the stop rule (avoid note). | exploratory | avoid | — |

---

## Section 6 — Setup-archetype discovery (unsupervised, blind to outcome)

*Analysis approach: unsupervised clustering (§7.4.1) on PRE-ENTRY / day-0 shape
features ONLY, blind to outcome by construction — clustering on outcomes is the
selection-on-outcome trap §2/§7.7.1 exist to prevent. k is chosen by bootstrap
stability (not silhouette-shopping). Shared discipline: a cluster is only ever a
hypothesis generator — any tradable archetype is promoted to a concrete
`(classification × regime)` cell and run through the full §7.7.1 gauntlet +
holdout; nothing here enters the primary BY family on its own (all exploratory).*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 6.1 | What natural setup archetypes emerge from clustering pre-entry shape features (blind to outcome)? | `pre_entry_ret_from_open`, `pre_entry_ret_from_high/low`, `pre_entry_minutes_since_high/low`, `intraday_first_30m_*`, `entry_bar_*_wick_pct` | clustering (k-means/GMM/HDBSCAN) | Standardize → optional PCA → k-means/GMM/HDBSCAN on the listed pre-entry features (outcome columns excluded); pick k by bootstrap-resample stability (§7.4.1). **Output:** an archetype taxonomy (gap-and-go, opening-drive, breakout, drift, spike…). | exploratory | exec | — |
| 6.2 | Do the discovered archetypes have distinguishable forward-return distributions (is the clustering outcome-relevant)? | cluster id × `ret_<H>`, `max_runup_<H>` | clustering + quantile-reg | Per cluster, quantile regression of `ret_<H>` / `max_runup_<H>` on cluster id (5/50/95 pctiles), cost-adjusted; day-clustered CI. **Output:** a verdict on whether archetypes are outcome-relevant (else the clustering is decorative). | exploratory | exec | — |
| 6.3 | Does an archetype map cleanly onto a known classification (or is it cross-cutting)? | cluster id × `security_classification_daily.*` | clustering + MI | `I(cluster_id; classification cols)` vs shuffled null; high MI = redundant with classification, low MI = genuinely cross-cutting. **Output:** a map of which archetypes are net-new vs classification-redundant. | exploratory | exec | — |
| 6.4 | Are "grind-up" vs "spike-up" archetypes separable from `rate_of_change` / `volatility_within_trade` and do they continue differently? | `rate_of_change`, `volatility_within_trade`, `current_ret_over_atr_14d` (path), `ret_<H>` | clustering + DP | Cluster on `(rate_of_change, volatility_within_trade)` to separate grind vs spike; DP per cluster with cost-adjusted reward to test differential continuation; day-clustered CI. **Output:** a grind-vs-spike continuation split. | exploratory | exec | — |
| 6.5 | Does cluster membership add MI about outcome beyond raw classification (does archetype carry extra info)? | cluster id, classification cols, `ret_<H>` | conditional MI | `I(cluster_id; ret_<H> | classification)` vs shuffled null — positive conditional MI means the archetype carries info beyond classification. **Output:** a yes/no on whether clustering earns its place as a conditioner. | exploratory | exec | — |
| 6.6 | Eigendecompose the pre-entry feature matrix: how many independent setup dimensions exist? | all `pre_entry_*`, `intraday_first_*` ML features | eigendecomp | PCA/SVD on the standardized pre-entry feature matrix; count components to a variance threshold (effective dimensionality). **Output:** the number of independent setup dimensions (informs k and feature reduction). | exploratory | exec | — |

---

## Section 7 — Entry STATE (not just entry time) & entry-quality

*Analysis approach: DP is the right tool because optimal entry is a state, not
a clock — `π*(state) → entry_offset` falls out of the policy; MI screens which
entry-quality proxies carry any info before they earn a place in the state.
Shared discipline: entry-quality proxies (`entry_*`) are co-decision (usable iff
signal_time < entry_offset, no leakage); reward is always cost-adjusted (§6.1,
the spread proxy lives here) and excess (§2.3); DP results get the bin-count
sweep + cross-fitting against max-operator optimism (§7.7.1).*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 7.1 | What entry STATE (not time) maximizes cost-adjusted V — does `π*(state)→entry_offset` beat "10:00 always"? (cf §1.3.1) | DP state = `(pre_entry_ret_from_open, pre_entry_ret_from_high, regime, cell)`; action = `entry_offset` | DP | DP over the stated state vector, action = entry_offset, reward = cost-adjusted `ret_<H>_excess_spy`; compare V(π*) vs V(offset=1000 always); cross-fit OOS V (§7.3 optimism), bin sweep. **Output:** a state-conditional entry policy + its lift over the fixed-10:00 baseline. | primary | exec | — |
| 7.2 | Is there a "don't-enter" entry window where V < cost for all states? (cf §1.3.1) | `entry_offset`, `entry_slippage_proxy_bps`, `ret_<H>` | DP | DP scan across offsets; identify offsets where V < cost (§6.1) for ALL states; day-clustered CI. **Output:** a "don't-enter" offset window (avoid-filter). | primary | avoid | — |
| 7.3 | Does pre-entry volume (`cumulative_dollar_volume_to_entry`, now at all 17 offsets) predict a better entry? (cf §1.3.1, §3.6) | `cumulative_volume_to_entry`, `cumulative_dollar_volume_to_entry`, `ret_<H>` | MI + BN | `I(cumulative_dollar_volume_to_entry; ret_<H> | entry_offset)` vs null; BN check for a direct edge to outcome (not via a confounder). **Output:** a verdict on pre-entry volume as an entry-state variable. | exploratory | exec | — |
| 7.4 | Does entry-bar location-in-range (`entry_price_location_in_1m_bar`) predict fill quality / forward return? | `entry_price_location_in_1m_bar`, `entry_open_to_close_1m_return`, `ret_<H>` | MI | `I(entry_price_location_in_1m_bar; ret_<H>)` vs shuffled null; ordinal ranking. **Output:** a fill-quality / entry-bar-location signal. | exploratory | exec | — |
| 7.5 | Do wide entry bars (`entry_1m_range`, `entry_range_vs_atr_14d`) predict worse net outcomes (spread/slippage drag)? (cf §1.3.7, §1.3.9) | `entry_1m_range`, `entry_range_vs_atr_14d`, `entry_slippage_proxy_bps`, `ret_<H>` | MI + DP | `I(entry_range_vs_atr_14d; net ret)` vs null, then DP with the wide-bar flag in state and cost-adjusted reward (the §6.1 spread proxy IS the entry range). **Output:** a wide-bar avoid-filter (slippage drag). | exploratory | avoid | — |
| 7.6 | Does upper/lower wick at entry (`entry_bar_upper_wick_pct`) predict slippage or reversal? (cf §1.3.7) | `entry_bar_upper_wick_pct`, `entry_bar_lower_wick_pct`, `entry_slippage_proxy_bps` | MI | `I(entry_bar_wick_pct; entry_slippage_proxy_bps)` and vs forward reversal, both vs shuffled null. **Output:** a wick-based slippage/reversal avoid signal. | exploratory | avoid | — |
| 7.7 | Does entering at a recent intraday high (`pre_entry_ret_from_high` near 0) hurt vs entering on a pullback? (cf §1.3.7) | `pre_entry_ret_from_high`, `pre_entry_minutes_since_high`, `ret_<H>` | MI + DP | MI screen, then DP with `pre_entry_ret_from_high` in state (cost-adjusted reward) to compare entering at-high vs on-pullback. **Output:** an at-high vs pullback entry rule. | exploratory | exec | — |
| 7.8 | Is the entry feasible at size — what's the `entry_participation_capacity_*` distribution per cell? (cf §1.3.7, §1.3.9) | `entry_participation_capacity_1pct_adv`, `entry_participation_capacity_5pct_1m_volume` | direct | Direct distribution (median/tail) of `entry_participation_capacity_*` per cell. **Output:** a per-cell feasible-size profile (capacity gate, also feeds §15). | exploratory | exec | — |
| 7.9 | Does `is_halted_at_entry` flag a non-fillable cohort that inflates apparent edge? | `is_halted_at_entry`, `ret_<H>` | direct | Direct comparison of cell edge with vs without `is_halted_at_entry` rows; if dropping them kills the edge it was a non-fillable artifact. **Output:** a halt-at-entry exclusion filter (artifact rejection). | exploratory | avoid | — |
| 7.10 | If 1 minute late to enter (offset+1), how much does V degrade (entry-timing robustness)? (cf §1.3.18) | adjacent `entry_offset` rows | DP | Recompute DP V at `entry_offset+1` and report the degradation (§2.4 surface-smoothness — a robust edge degrades gradually, a fragile one collapses). **Output:** an entry-timing robustness number per cell. | exploratory | exec | — |

---

## Section 8 — Exit / trade-management, survival / competing-risks, DP stops

*Analysis approach: exit is the canonical optimal-stopping problem → DP/FQI on
the path-state vector; competing-risks survival (Aalen-Johansen CIFs +
discrete-time multinomial hazard, NOT single-event KM, NOT Cox-PH given
non-stationary intraday hazards) is the right math for asymmetric target/stop
exits (§2.7). Shared discipline: cost-adjusted reward (§6.1); the DP policy must
beat the 1-line ATR rule (else default to the rule, §7.7.1), survive the bin
sweep, and be valued cross-fit OOS (max-operator optimism, §7.3) — training-set
V is never a test statistic.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 8.1 | What state-dependent exit policy maximizes cost-adjusted V per cell, and does it clearly dominate the 1-line ATR rule (§7.7.1 gate)? (cf §1.3.2) | path state `(current_ret, max_ret_so_far, drawdown, bars_elapsed, volatility_within_trade, rate_of_change)` | DP / FQI | DP/FQI on the stated path state, reward = cost-adjusted exit return; baseline = "exit at -1ATR/+2ATR/EOD"; apply the §7.7.1 **dominance gate** — default to the 1-liner unless the rule is <80%-as-good as DP (equivalently DP lifts V by ≳20%); then cross-fit OOS V > 0, bin sweep, day-clustered CI. **Output:** a per-cell state-dependent exit policy (or "use the 1-liner"). | primary | exec | — |
| 8.2 | For each frozen target/stop pair, what's the competing-risks CIF of target-first vs stop-first per cell? (cf §1.3.2) | `first_event_<pair>` (9 pairs) | survival (Aalen-Johansen / discrete hazard) | Aalen-Johansen CIFs for target-hit vs stop-hit vs time-exit from `first_event_<pair>` per cell (proper competing-risks, stop-hits not treated as censored); day-clustered CI bands. **Output:** per-cell CIF curves per frozen pair. | exploratory | exec | — |
| 8.3 | Where is the crossover point where the hazard of stop-hit exceeds target-hit (principled stop placement)? (§2.7) | `first_cross_up_*`/`first_cross_down_*` `<H>`, `first_event_<pair>` | survival | Discrete-time competing-risks hazards; find the bar where marginal `P(stop_hit|s,t)` exceeds `P(target_hit|s,t)`. **Output:** a principled stop-placement crossover point per cell. | exploratory | exec | — |
| 8.4 | Does a trailing stop (`max_ret_so_far` as state) improve V over a fixed stop? (cf §1.3.2) | `close_max_ret_so_far` (path), `high_ret_so_far` | DP | DP with `max_ret_so_far` in state (trailing rule falls out) vs fixed-stop DP; compare cost-adjusted V, cross-fit OOS. **Output:** a trailing-vs-fixed stop verdict per cell. | primary | exec | — |
| 8.5 | Does moving the stop to breakeven after +1% help or hurt? (cf §1.3.2) | `first_cross_up_1pct_<H>`, subsequent path `low_ret_so_far` | DP (special-case policy) | DP evaluation of the breakeven-after-+1% special-case policy vs no-move, conditioned on `first_cross_up_1pct_<H>`; cost-adjusted, day-clustered CI. **Output:** a help/hurt verdict on the breakeven-stop tactic. | exploratory | exec | — |
| 8.6 | P(+2% before -1% within 5d) per cell (materialized target-before-stop)? (cf §1.3.2) | `hit_2pct_before_minus_2pct_1d`, `hit_3pct_before_minus_3pct_5d`, `first_event_<pair>` | direct / survival | Direct rate of the materialized `hit_X_before_minus_Y` label per cell (pre-frozen thresholds, §7.7.1); block bootstrap (block ≥ horizon) CI. **Output:** a per-cell target-before-stop probability. | primary | exec | — |
| 8.7 | After a -2% drawdown, what's P(recover to +X) — is the dip buyable or a death spiral? (cf §1.3.6) | `forward_path_short` `low_ret_so_far`, `close_max_ret_so_far`, `ret` | DP transition probs | DP transition probabilities: `P(reach +X | low_ret_so_far < -2%)` from the path data; day-clustered CI. **Output:** a dip-buyable vs death-spiral map per cell. | exploratory | exec | — |
| 8.8 | Discrete-time hazard non-stationarity: do exit hazards spike at open / midday / power hour (Cox-PH violated)? (§2.7) | `forward_path_short` checkpoints, `bars_elapsed` | survival (discrete hazard) | Discrete-time hazard estimated per checkpoint vs `bars_elapsed`; test for intraday hazard spikes (justifies dropping Cox-PH per §2.7). **Output:** a time-of-day hazard profile (confirms discrete-hazard choice). | exploratory | exec | — |
| 8.9 | Does fixed-% stop or ATR-normalized stop give better V per volatility bucket? (cf §1.3.2) | `first_cross_down_*pct` vs `first_cross_down_*atr`, `volatility_bucket` | DP | DP per `volatility_bucket` comparing fixed-% vs ATR-normalized stop reward (cost-adjusted); day-clustered CI. **Output:** a per-vol-bucket stop-type rule. | primary | exec | — |
| 8.10 | Halt-crossing trades: does excluding `halt_gap_crossed` paths kill the cell's edge (exitability artifact)? (§3.4) | `halt_gap_crossed` (path), `bar_gap_minutes_max_<H>` | direct | Direct edge recompute with vs without `halt_gap_crossed` paths; a sign/magnitude collapse means the edge is an exitability artifact. **Output:** a halt-gap exclusion filter (artifact falsification). | primary | avoid | — |
| 8.11 | Is the DP policy bin-sensitive (qualitatively different exit rule at 5 vs 15 bins)? (§7.7.1) | DP state bins | DP sensitivity sweep | Re-run DP at 5/10/15 bins per continuous dim; flag cells whose exit rule changes qualitatively as bin-sensitive (reported, never pre-registered). **Output:** a bin-sensitivity flag per cell. | exploratory | exec | — |
| 8.12 | Max-operator optimism: does cross-fitted OOS policy V stay positive (not just training V)? (§7.3) | DP policy value, cross-fit folds | DP cross-fit | K-fold cross-fitting: learn policy on one fold, value it on held-out folds; only OOS V counts (§7.3 optimism). **Output:** the honest (deflated) policy value per cell. | exploratory | exec | — |

---

## Section 9 — Signal freshness & repeated / chased signals

*Analysis approach: direct conditional sort (with HB shrinkage) for the
fresh-vs-chased expectancy contrast, conditional MI to test whether freshness is
distinct from the 52w/extension axis or just collinear. Shared discipline:
excess-over-index lens (§2.3), cost-adjustment (§6.1), day-clustered CI; the
signal-definition sweep (9.4) is explicitly logged into the §1.6 multiplicity
ledger — each variant multiplies the BY family.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 9.1 | Do FIRST-occurrence signals (`signal_first_in_20d`) carry higher cost-adjusted excess return than repeated/chased ones? | `signal_first_in_5d/10d/20d`, `ret_<H>_excess_spy` | direct + HB | Direct sort of cost-adjusted `ret_<H>_excess_spy` for first-occurrence vs repeated cohorts per cell; HB shrinkage; day-clustered CI on the difference. **Output:** a first-signal-vs-chased expectancy contrast (freshness whitelist). | primary | exec | — |
| 9.2 | Does signal staleness (many prior firings) predict decay / exhaustion? | `signal_first_in_*`, `days_since_last_5pct_move`, `ret_<H>` | MI + direct | `I(staleness; ret_<H>)` vs null + direct sort of forward return by prior-firing count; day-clustered CI. **Output:** a staleness/exhaustion avoid-threshold. | exploratory | avoid | — |
| 9.3 | Is the freshness effect distinct from the 52w-range / extension effect, or collinear? | `signal_first_in_20d`, `high_52w`, `consecutive_up_days_close_to_close` | conditional MI | `I(signal_first_in_20d; ret_<H> | high_52w_ratio, consec_up_days)` vs shuffled null — residual MI means freshness is distinct, not collinear. **Output:** a verdict on whether freshness is a standalone conditioner. | exploratory | exec | — |
| 9.4 | Does the signal-definition itself matter for freshness (sweep adds to the multiplicity ledger, §1.6)? | `signal_definition` metadata, `signal_first_in_*` | direct | Direct re-run of 9.1 across signal-definition variants; report sensitivity AND log each variant as a full BY-family copy (§1.6/§7.7.1). **Output:** a freshness-robustness-across-definitions report + multiplicity-ledger entry. | exploratory | exec | — |

---

## Section 10 — 52-week-range position / breakout vs exhaustion

*Analysis approach: DP (with the 52w-position ratio in state) settles breakout
vs exhaustion as a policy; conditional MI tests whether 52w-position beats
intraday extension or is redundant; quantile-reg captures the full
coiling→breakout-magnitude distribution, not just the mean. Shared discipline:
the 52w ratio (close / `high_52w`) is derived at query time; excess lens (§2.3),
cost-adjustment (§6.1), day-clustered CI throughout.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 10.1 | Do names near 52w-high (close/`high_52w` ≈ 1) continue (breakout) or exhaust per cell? | `high_52w`, `prior_day_eod_close`, `ret_<H>` | DP + direct | DP with the 52w-position ratio bucket in state, cost-adjusted excess reward; direct sort cross-check; day-clustered CI. **Output:** a breakout-continuation vs exhaustion rule near the 52w high. | primary | exec | — |
| 10.2 | Do names near 52w-low show momentum or falling-knife behavior (avoid)? | `low_52w`, `prior_day_eod_close`, `max_drawdown_<H>` | direct | Direct sort of forward `max_drawdown_<H>` / return for near-52w-low names; name-concentration check. **Output:** a falling-knife long-avoid filter. | exploratory | avoid | — |
| 10.3 | Is 52w-range position a better conditioning variable than intraday extension for continuation? | `high_52w` ratio vs `pre_entry_ret_from_open`, `ret_<H>` | conditional MI | `I(52w_ratio; ret_<H> | pre_entry_ret_from_open)` and the reverse, vs shuffled null — which conditioner carries residual info. **Output:** which of the two is the stronger continuation conditioner. | exploratory | exec | — |
| 10.4 | Does a fresh breakout (52w-high AND `signal_first_in_20d`) beat either alone? | `high_52w`, `signal_first_in_20d` interaction | MI + direct | MI/interaction test + direct sort of the joint (52w-high ∧ fresh) cohort vs each marginal; day-clustered CI. **Output:** a fresh-breakout interaction verdict. | exploratory | exec | — |
| 10.5 | Does `days_since_last_{5,10,20}pct_move` (coiling/quiet) predict breakout magnitude? | `days_since_last_5pct_move/_10pct_/_20pct_`, `max_runup_<H>` | quantile-reg | Quantile regression of `max_runup_<H>` on coiling duration (5/50/95 pctiles) — coiling should fatten the upper tail; day-clustered CI. **Output:** a coiling→breakout-magnitude curve. | exploratory | exec | — |

---

## Section 11 — Crowding / signal-concentration (conditioned on market direction)

*Analysis approach: conditional MI + BN-interaction because concentration is
AMBIGUOUS in isolation — it must be conditioned on market direction
(`spy_ret_0930_to_1000`) to separate crowding (reversal) from conviction
(continuation); eigendecomp quantifies how many independent bets a concentrated
day actually offers. Shared discipline: always condition on SPY direction;
day-clustered CI; cost-adjusted excess lens.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 11.1 | Does today's signal concentration predict outcome quality, conditioned on SPY direction? (cf §1.3.14, §3.1) | `signal_concentration_percentile_today`, `signal_concentration_hhi_today`, `spy_ret_0930_to_1000`, `ret_<H>` | MI + BN (interaction) | `I(signal_concentration; ret_<H> | spy_ret_0930_to_1000 sign)` vs shuffled null; BN check for a concentration→outcome edge with SPY-direction interaction. **Output:** a concentration-conditioned-on-direction outcome signal. | exploratory | exec | — |
| 11.2 | Is high concentration crowding (future reversal) or conviction (continuation) — which, when? | `signal_concentration_hhi_today` × `spy_ret_0930_to_1000`, `ret_<H>`, `next_day_*` | conditional MI + DP | Conditional MI of HHI × SPY-direction on `ret_<H>` and `next_day_*`; DP with concentration×direction in state to learn when it reverses vs continues. **Output:** a crowding-vs-conviction regime map. | exploratory | exec | — |
| 11.3 | On high-concentration days, is the effective number of independent bets lower (correlated outcomes)? | same-day `ret_<H>` covariance, `signal_concentration_*` | eigendecomp | Eigendecompose same-day outcome covariance on high- vs low-concentration days; effective N = `1/Σλ²` over normalized eigenvalues. **Output:** effective-independent-bets as a function of concentration (sizing input). | exploratory | exec | — |
| 11.4 | Does HHI (few strong names) vs broad (everyone slightly up) distinguish forward dynamics? (cf §3.1) | `signal_concentration_hhi_today` vs `signal_concentration_percentile_today` | MI | `I(HHI; ret_<H>)` vs `I(percentile; ret_<H>)` against the null to see which concentration measure carries more forward info. **Output:** which concentration metric to use. | exploratory | exec | — |

---

## Section 12 — Cross-sectional vs time-series momentum

*Analysis approach: direct conditional sort + HB for the two coarse-cell
expectancy tests (TS = stock vs own open, CS = stock vs peers today); DP with
the regime in state because the two have OPPOSITE regime dependence (TS in
trending, CS in dispersion); MI + eigendecomp to test whether they are
low-correlated enough that a blend dominates. Shared discipline: excess lens
(§2.3), cost-adjustment (§6.1), day-clustered CI; run both signals in parallel.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 12.1 | TS momentum: under which cells does `intraday_ret_0930_to_1000 > 0` → positive cost-adjusted excess `ret_<H>`? (cf §2.6) | `intraday_ret_0930_to_1000`, `ret_<H>_excess_spy` | direct + HB | Direct sort of cost-adjusted `ret_<H>_excess_spy` for TS-positive cohort per coarse cell; HB shrinkage; day-clustered (block ≥ horizon) CI > 0. **Output:** the TS-momentum whitelist cells. | primary | exec | — |
| 12.2 | CS momentum: does top-rank `intraday_ret_0930_to_1000_rank_today` carry excess return beyond TS? (cf §2.6) | `intraday_ret_0930_to_1000_percentile_today`, `ret_<H>_excess_spy` | direct + HB | Direct sort by `intraday_ret_0930_to_1000_percentile_today` conditioned on TS sign; HB; day-clustered CI. **Output:** the CS-momentum whitelist + its increment over TS. | primary | exec | — |
| 12.3 | Are TS and CS signals low-correlated (does a blend dominate)? (cf §2.6) | TS vs CS signal, `ret_<H>` | MI + eigendecomp | `I(TS_signal; CS_signal)` + eigendecomp of the (TS, CS, outcome) covariance; low overlap ⇒ a blend dominates either alone. **Output:** a blend-vs-standalone recommendation. | exploratory | exec | — |
| 12.4 | Does CS momentum work in dispersion regimes and fail when correlations are high? (cf §2.6) | `cross_sectional_ret_dispersion_at_1000`, CS signal, `ret_<H>` | DP (regime in state) | DP with `dispersion` regime in state, cost-adjusted excess reward; percentile buckets (§7.7.1) + ±5pp boundary sweep. **Output:** a dispersion-conditioned CS-momentum policy branch. | primary | exec | — |
| 12.5 | Does TS momentum work in trending (SPY-trend) regimes and fail in mean-reverting ones? (cf §2.6) | regime `spy_trend`, TS signal, `ret_<H>` | DP | DP with `spy_trend` regime in state, cost-adjusted excess reward; boundary sweep. **Output:** a trend-conditioned TS-momentum policy branch. | primary | exec | — |
| 12.6 | Sector-relative CS: does `sector_ret_0930_to_1000_rank_today` add over market-relative CS? | `sector_aggregates_daily.*`, `ret_<H>_excess_spy` | conditional MI | `I(sector_rank; ret_<H>_excess_spy | market-relative CS rank)` vs shuffled null. **Output:** a yes/no on sector-relative as an added CS conditioner. | exploratory | exec | — |

---

## Section 13 — Security-type / classification conditioning & blacklist (negative space)

*Analysis approach: this is the highest-power deliverable (§5.0) — a blacklist
is provable from the cost side, where the long leg only foregoes opportunity.
Direct + HB + BY for the negative-expectancy family (§2.2 negative space first),
DP per cell for cost-driven sign flips, conditional MI/BN for behavioral
differences. Shared discipline: cost-adjustment is the whole point (§6.1),
excess lens (§2.3), the blacklist runs through the SAME BY FDR machinery as the
whitelist (§2.2 — multiple-comparisons with the sign flipped); classification
join is mandatory (§14.4).*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 13.1 | Which classification cells have statistically significant NEGATIVE cost-adjusted expectancy (blacklist)? (cf §2.2) | full `security_classification_daily.*`, `ret_<H>_excess_spy` | direct + HB + BY | Direct per-cell mean of cost-adjusted `ret_<H>_excess_spy`, HB shrinkage, BY FDR at α=0.10 on the SIGN-FLIPPED family; day-clustered CI upper bound < 0. **Output:** the cost-side blacklist (the highest-power deliverable). | primary | avoid | — |
| 13.2 | Are leveraged/inverse ETFs net-negative after costs (decay artifact)? (cf §1.3.4) | `is_leveraged_etf`, `is_inverse_etf`, `is_etn`, `ret_<H>` | DP (cost-adjusted) | DP with cost-adjusted reward on the leveraged/inverse cohort; policy refuses if V<0 (decay artifact); day-clustered CI. ⚠ leverage flags partial. **Output:** a leveraged/inverse-ETF blacklist entry. | primary | avoid | leverage-factor flags partial in tickers_enriched — gaps likely |
| 13.3 | Do China ADRs gap/behave differently (avoid-filter)? (cf §1.3.4) | `is_china_adr`, `is_adr`, `next_day_gap_return`, `ret_<H>` | conditional MI + BN | `I(is_china_adr; next_day_gap_return | regime)` vs null + BN structure for an ADR→gap edge; cost-adjusted expectancy check. ⚠ country classification partial. **Output:** a China-ADR avoid-filter. | primary | avoid | strict country classification partial (locale) |
| 13.4 | Momentum in mega-cap tech vs biotech — separate policies, shrunk differences? (cf §1.3.4) | `is_mega_cap_tech`, `is_biotech`, `ret_<H>_excess_qqq` | DP per cell + HB | DP per cohort with cost-adjusted `ret_<H>_excess_qqq` reward; HB shrinks the mega-tech-vs-biotech difference; day-clustered CI. **Output:** separate policies with a shrunk cross-cohort difference. | primary | exec | — |
| 13.5 | Do low-float candidates show bigger moves but worse net-of-cost outcomes? (cf §1.3.4) | `is_low_float_candidate`, `liquidity_bucket`, `entry_slippage_proxy_bps`, `ret_<H>` | DP per liquidity bucket | DP per `liquidity_bucket` with the §6.1 slippage proxy in the reward; test whether gross move is positive but net is negative; day-clustered CI. ⚠ float proxy only. **Output:** a low-float net-negative avoid map. | primary | avoid | float data absent — proxy only |
| 13.6 | Are recent IPOs (`is_recent_ipo`, `days_since_ipo_or_first_bar`<60) too risky (avoid)? (cf §1.3.4) | `is_recent_ipo`, `days_since_ipo_or_first_bar`, `realized_vol_21d`, `ret_<H>` | DP per cell | DP on the recent-IPO cohort with cost-adjusted reward; policy refuses if vol-adjusted V<0; day-clustered CI. ⚠ IPO date is a first-bar proxy. **Output:** a recent-IPO avoid-window. | exploratory | avoid | accurate IPO date partial (first-bar proxy) |
| 13.7 | Does sub-$5 / `price_bucket` interact with cost to flip the edge negative? (cf §1.3.9) | `price_bucket`, `entry_slippage_proxy_bps`, `ret_<H>` | DP per price bucket | DP per `price_bucket` with the §6.1 spread proxy in reward; locate the price where gross-positive flips net-negative; day-clustered CI. **Output:** a sub-$X price-bucket cost-driven blacklist. | primary | avoid | — |
| 13.8 | Proxy validation: does `is_meme_candidate` actually capture GameStop-like behavior in the 2021 era? (RFC §13.8) | `is_meme_candidate`, era, `realized_vol_21d_rank`, `max_runup_<H>` | direct + clustering | Direct profile of `is_meme_candidate` names in the 2021 era (vol rank, runup) + cluster check that they form a distinct high-vol cluster. ⚠ proxy validation only. **Output:** a proxy-validity verdict (pattern, not an edge). | exploratory | pattern | float/short-interest absent — proxy validation only |
| 13.9 | Does market-cap bucket affect momentum persistence (micro→mega)? (cf §1.3.4) | `market_cap_bucket`, `ret_5d_total_excess_spy` | DP per bucket + HB | DP per `market_cap_bucket` on cost-adjusted `ret_5d_total_excess_spy`; HB shrinkage; block bootstrap (block ≥ 5d) CI. ⚠ market_cap NULL where shares-out missing. **Output:** a persistence-by-cap-bucket curve. | primary | exec | market_cap NULL where shares-outstanding missing |
| 13.10 | Is preferred/warrant/unit/SPAC behavior so different it must be excluded outright? | `is_preferred`, `is_warrant`, `is_unit`, `is_spac`, `ret_<H>` | direct | Direct comparison of these cohorts' return distributions vs common-stock baseline; day-clustered CI. **Output:** a hard-exclude list for non-common-stock types. | exploratory | avoid | — |
| 13.11 | Classification-mix audit: do apparent cross-cell edges collapse once mix is disclosed (§14.4 / §15)? | classification mix per cell | direct | Direct disclosure of the classification mix inside each apparent-edge cell; re-test the edge holding mix fixed (§14.4). **Output:** a mix-artifact flag (edges that were really security-type-mixing). | exploratory | avoid | — |

---

## Section 14 — Market-regime interaction (VIX, breadth, SPY trend, liquidity, dispersion)

*Analysis approach: regime enters as a DP state variable and the policy branches
on it iff it is genuinely load-bearing; BN/MI test whether a regime is even a
parent of outcome before it earns a branch; survival handles regime-duration and
transition-risk questions. Shared discipline: percentile-based bucketing on the
exploration set (§7.7.1), the ±5pp boundary sensitivity sweep is mandatory for
every regime-conditional edge (a fragile edge that vanishes under the shift is
not pre-registered), excess lens + cost-adjustment throughout.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 14.1 | Does momentum survive only in low-`vix_close` regimes (policy branches on VIX)? (cf §1.3.5) | regime `vix_level`, `vix_close`, `ret_<H>` | DP (VIX in state) | DP with `vix_level` in state, cost-adjusted excess reward; ±5pp boundary sweep + percentile buckets (§7.7.1). **Output:** a VIX-regime policy branch (trade low-VIX / sit-out high-VIX). | primary | exec | — |
| 14.2 | Does SPY-trend regime gate the edge? (cf §1.3.5) | regime `spy_trend`, `spy_realized_vol_21d`, `ret_<H>` | DP | DP with `spy_trend` in state, cost-adjusted excess reward; boundary sweep. **Output:** a SPY-trend policy branch. | primary | exec | — |
| 14.3 | Is breadth (`breadth_advance_decline_ratio_at_1000`) a parent of outcome in the DAG? (cf §1.3.5) | `breadth_*_at_1000`, `ret_<H>` | BN + MI | BN structure: does `breadth_*_at_1000` have an outgoing edge to outcome (80% stability, no era-flip)? cross-checked by `I(breadth; ret_<H>)` vs null. **Output:** a verdict on breadth as a load-bearing parent. | exploratory | exec | — |
| 14.4 | Does momentum fade in high-volatility regimes per cell? (cf §1.3.5) | `volatility_bucket`, `realized_vol_21d`, `ret_<H>` | DP per vol bucket | DP per `volatility_bucket`, cost-adjusted excess reward; compare V across buckets; boundary sweep. **Output:** a momentum-fades-in-high-vol verdict per cell. | primary | exec | — |
| 14.5 | Do small caps (`iwm`-like) work only in certain `(cap × regime)` states? (cf §1.3.5) | `style_bucket`, regime, `ret_<H>_excess_iwm` | DP (cap×regime state) | DP with `(style_bucket × regime)` joint state, cost-adjusted `ret_<H>_excess_iwm`; boundary sweep + HB on sparse cells. **Output:** a `(cap × regime)` conditional policy for small caps. | primary | exec | — |
| 14.6 | Is the new dispersion regime taxonomy a load-bearing parent for CS momentum? (§3.6) | regime `dispersion`, `cross_sectional_ret_iqr_at_1000` | BN + DP | BN test for a `dispersion`→CS-outcome edge, then DP with dispersion in state; boundary sweep. **Output:** a verdict + policy branch for the dispersion taxonomy. | primary | exec | — |
| 14.7 | Regime bucket-boundary sensitivity: does any regime-edge vanish under ±5pp shift? (§7.7.1) | regime threshold metadata, `regime_thresholds_<taxonomy>` | DP sensitivity sweep | Re-run BN+DP under ±5pp percentile-boundary shifts; flag any regime-edge that disappears as fragile (reported, not pre-registered). **Output:** a fragility flag per regime-conditional edge. | exploratory | exec | — |
| 14.8 | Does a VIX-regime FLIP predict signal failure (transition risk)? (cf §1.3.15) | regime `vix_level` transitions, `ret_<H>` | DP across transitions + survival | DP V before vs after a `vix_level` transition + survival of edge across the flip; day-clustered CI. **Output:** a transition-risk avoid-window around regime flips. | exploratory | avoid | — |
| 14.9 | How long do regime classifications last (median + tail of regime duration)? (cf §1.3.15) | `regime_definitions` durations | survival | KM / discrete-hazard on regime durations from `regime_definitions`; report median + tail. **Output:** a regime-duration distribution (informs how long a branch stays valid). | exploratory | exec | — |

---

## Section 15 — Cost / capacity / scaling

*Analysis approach: the §6.1 cost model (spread proxy + sqrt-impact + commission)
is built from cost-side inputs ONLY and frozen before discovery; this section
makes cost the swept variable (DP cost-sweep 2-30 bps). Shared discipline: the
break-even ceiling is a DESCRIPTIVE headroom statistic, NEVER used to set the
cost assumption (that would be circular, §6.1); excess lens, day-clustered CI;
the liquidity cap (15.4) for top-ADV names needs an external NBBO anchor.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 15.1 | What's the break-even cost ceiling per cell (max bps it sustains with CI lower-bound > 0)? (§6.1) | `entry_slippage_proxy_bps`, `ret_<H>_excess_spy`, cost sweep 2-30 bps | DP cost-sweep | DP cost-sweep 2-30 bps (2-bps steps); record the max cost where day-clustered CI lower bound of cost-adjusted `ret_<H>_excess_spy` stays > 0. **Output:** a per-cell break-even cost ceiling (descriptive headroom). | primary | exec | — |
| 15.2 | At what AUM (% of ADV) does slippage eat the edge per cell? (cf §1.3.19) | `entry_participation_capacity_*`, `addv_20d`, sqrt-impact model | DP size-dependent cost | DP with the §6.1 sqrt-impact `slippage(size)` in the reward, swept at 0.1%/1%/5% of ADV; locate the size where V crosses zero. **Output:** a per-cell AUM capacity ceiling. | exploratory | exec | — |
| 15.3 | Which cells scale best (V flattest as trade size grows)? (cf §1.3.19) | `entry_participation_capacity_*`, `ret_<H>` | cross-cell DP | Cross-cell DP comparison of the V-vs-size slope; flattest slope = best scaling. **Output:** a ranking of cells by capacity-robustness. | exploratory | exec | — |
| 15.4 | Does the spread proxy (`entry_1m_range`/2 in bps) need the §6.1 liquidity cap for top-ADV names? | `entry_slippage_proxy_bps`, `addv_20d_rank_today`, midday `entry_1m_range` | direct | Direct comparison of raw high-low proxy vs the §6.1 capped proxy (`min(high_low, 2× midday median range)`) for `addv_20d_rank_today ≤ 200`; validate against an external anchor. ⚠ NBBO absent. **Output:** a calibrated cap multiplier for top-ADV cells. | exploratory | exec | NBBO quotes absent — proxy calibration needs external anchor |
| 15.5 | Do costs kill the edge specifically for low-`price_bucket` names? (cf §1.3.9) | `price_bucket`, `entry_slippage_proxy_bps`, `ret_<H>` | DP per price bucket | DP per `price_bucket` with §6.1 spread proxy in reward; isolate low-price buckets where net flips negative. **Output:** a low-price cost-kill avoid-filter (overlaps §13.7). | primary | avoid | — |
| 15.6 | Should we avoid 9:35 entry due to wide opening spreads? (cf §1.3.9) | `entry_offset`=0935, `entry_slippage_proxy_bps`, `entry_1m_range` | MI + DP | `I(entry_offset=0935; slippage_proxy)` vs null + DP comparing net V at 0935 vs later offsets. **Output:** a 9:35-entry avoid recommendation. | exploratory | avoid | — |
| 15.7 | Prioritize review by edge × deployable capacity — which surviving cells are worth manual review (§6)? | hierarchical posterior LB × `entry_participation_capacity_*` | HB + direct | Rank surviving cells by HB posterior lower bound × deployable capacity (the §7.7.1 step-4 tie-breaker for the 20-cell holdout cap). **Output:** a prioritized manual-review queue. | exploratory | exec | — |

---

## Section 16 — Microstructure proxies (first-1m range, volume share, wicks)

*Analysis approach: MI screens these 1m-bar-derived proxies for any information
about forward outcome (true NBBO/auction-imbalance is absent, so everything here
is a proxy), and quantile-reg captures distributional (not just mean) effects of
range. Shared discipline: MI vs the shuffled-day null at the 95th pctile, treat
as ordinal feature ranking (§7.7.1); proxies are flagged as proxies; the
path-shape variables (16.7) are vetted for DP-state inclusion via §7.3.1.1
ablation, not asserted.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 16.1 | Does opening-auction imbalance proxy (first-1m bar range) predict 10:00 strength? (cf §1.3.16) | first-1m bar from `intraday_1m_first_hour`, `intraday_ret_0930_to_1000` | MI | `I(first-1m range; intraday_ret_0930_to_1000)` vs shuffled null. ⚠ true auction imbalance absent — range is the proxy. **Output:** an opening-imbalance-proxy predictive verdict. | exploratory | exec | true auction imbalance absent — range is proxy |
| 16.2 | Does first-15m volume concentration (`intraday_first_15m_volume_share_of_first_30m`) predict forward outcome? (cf §1.3.16) | `intraday_first_15m_volume_share_of_first_30m`, `ret_<H>` | conditional MI | `I(first_15m_volume_share; ret_<H> | entry_offset)` vs shuffled null. **Output:** a volume-concentration forward signal. | exploratory | exec | — |
| 16.3 | Wide vs tight first-5m range — different forward return DISTRIBUTIONS (not just means)? (cf §1.3.16) | first-5m range (path/list), `ret_<H>` quantiles | quantile-reg | Quantile regression of `ret_<H>` (5/50/95 pctiles) on first-5m range bucket — wide ranges may fatten tails without moving the mean; day-clustered CI. **Output:** a range→forward-distribution map. | exploratory | exec | — |
| 16.4 | Do premarket-volume spikes (`pre_market_volume_spike_flag`, news proxy) predict intraday continuation? (§3.1) | `pre_market_volume_spike_flag`, `premarket_volume_vs_20d_median`, `ret_<H>` | MI + direct | `I(pre_market_volume_spike_flag; ret_<H>)` vs null + direct sort of net continuation conditioned on the flag. ⚠ no news feed — spike is the news proxy. **Output:** a premarket-spike continuation signal. | exploratory | exec | no news feed — volume spike is news proxy |
| 16.5 | Does `prior_day_last_30m_volume_share` (late-day accumulation) predict next-day gap/drift? | `prior_day_last_30m_volume_share`, `prior_day_last_30m_return`, `overnight_gap` | MI | `I(prior_day_last_30m_volume_share; overnight_gap / next-day drift)` vs shuffled null. **Output:** a late-day-accumulation → next-day signal. | exploratory | exec | — |
| 16.6 | Do zero-volume / missing first-30m bars (`zero_volume_1m_bars_first_30m`) mark untradeable thin names? | `zero_volume_1m_bars_first_30m`, `missing_1m_bars_first_30m`, `bar_count_first_30m` | direct | Direct sort: cohorts with high zero-volume/missing-bar counts vs realizable net edge (thin names are uninvestable). **Output:** a thin-name untradeable avoid-filter. | exploratory | avoid | — |
| 16.7 | Does `volatility_within_trade` / `rate_of_change` (slow-grind vs spike) carry continuation info the raw-return state misses? (§7.3.1) | `volatility_within_trade`, `rate_of_change` (path), `ret_<H>` | MI + DP | `I(volatility_within_trade, rate_of_change; ret_<H> | current_ret)` vs null, then DP ablation (§7.3.1.1) — does adding them lift OOS policy value >5%? **Output:** a yes/no on these as DP-state additions (Markov-adequacy fix). | exploratory | exec | — |

---

## Section 17 — Edge decay / non-stationarity / currentness

*Analysis approach: edge is a trajectory, not a constant (§2.9) — state-space
(Kalman) filtering gives `edge_now`/`edge_trend`/`edge_half_life`, Bayesian
online change-point gives the production kill-switch, and anchored walk-forward
with embargo is the PRIMARY stability instrument (§2.5). Shared discipline:
exploration set only during discovery (holdout untouched); the static pooled
estimate still gates hypothesis tests (dynamic models lack power, §2.9), but
every cell REPORTS its trajectory and a cell whose `edge_now` CI includes zero
is flagged "historically real, currently unverifiable."*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 17.1 | Has the cell's edge decayed over the window — `edge_now` CI vs pooled estimate? (cf §1.3.20, §2.9) | per-cell daily return series, era | change-point + state-space (Kalman) | State-space (Kalman random-walk-mean) on the per-cell daily cost-adjusted excess return; compare filtered `edge_now` CI vs the pooled estimate; smoothing half-life swept {21,63,126} (§1.6). **Output:** `edge_now` + decay flag per cell. | exploratory | exec | — |
| 17.2 | What's the `edge_half_life` of decaying cells? (§2.9) | per-cell return series | state-space | From the §17.1 state-space slope, compute `edge_half_life` for cells with negative `edge_trend`. **Output:** a half-life number per decaying cell. | exploratory | exec | — |
| 17.3 | Did the 2020-2021 meme era artificially inflate edges (era-conditional V)? (cf §1.3.20) | era axis, `ret_<H>`, `is_meme_candidate` | era-conditional DP | Era-conditional DP V (meme era vs others), exploration set only; quantify the meme-era inflation share. **Output:** an era-inflation flag (edges that are a meme-era artifact). | exploratory | avoid | — |
| 17.4 | When should we suspect a cell's edge is dying (production kill-switch)? (cf §1.3.20) | per-cell V time series | Bayesian online change-point | Bayesian online change-point on the per-cell V time series; flag the change date. **Output:** a production kill-switch trigger per cell. | exploratory | exec | — |
| 17.5 | Is a cell positive on average but negative in 3 of 4 eras ("lucky backtest" diagnosis)? (§2.5) | era-conditional `ret_<H>_excess_spy` | direct (era decomposition) | Direct per-era decomposition of cost-adjusted `ret_<H>_excess_spy` inside the exploration set; flag pooled-positive-but-era-mostly-negative cells. **Output:** a "lucky backtest" disqualifier. | exploratory | avoid | — |
| 17.6 | Anchored walk-forward with embargo: does the cell's edge replicate window-to-window (PRIMARY stability test, §2.5)? | rolling windows over exploration+validation | walk-forward replication | Anchored walk-forward: train on `[start,t-1]`, embargo ~10 trading days, evaluate on `t`, advance across exploration+validation; this IS the §5.0 stability instrument. **Output:** a window-to-window replication record (the primary stability verdict). | exploratory | exec | — |

---

## Section 18 — Robustness / falsification / placebo

*Analysis approach: this section IS the §7.7.1 anti-overfitting gauntlet
expressed as questions — placebo/mirror DP, random-baseline, name-concentration,
surface-smoothness, cost/bin sweeps, Markov-adequacy, MI permutation null, BN
stability, overlapping-horizon block bootstrap. Shared discipline: these are not
optional add-ons but the gates every reported finding must pass; a finding that
fails any one is treated as null (the §1.5 headline rule). Most are exploratory
diagnostics; name-concentration (18.3) is primary because a sign-flip there
disqualifies a cell from pre-registration.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 18.1 | Placebo: does the OPPOSITE (mean-reversion) signal have V ≈ −(momentum V), confirming directionality? (cf §1.3.17) | mirror signal cohort, `ret_<H>` | mirror DP | Mirror DP on the inverted (mean-reversion) signal; check V ≈ −(momentum V). **Output:** a directionality confirmation (or a red flag if the placebo also "works"). | exploratory | exec | — |
| 18.2 | Does the edge beat a random-entry-timing baseline (Sharpe of edge over random)? (cf §1.3.17) | random `entry_offset` baseline V | DP vs random | DP V vs the distribution of random-entry-offset baseline V; report the edge's percentile / Sharpe over random. **Output:** an edge-over-random number. | exploratory | exec | — |
| 18.3 | Name-concentration: does removing the top-5 contributing tickers flip the cell's sign (§7.7.1)? | per-name contribution, `ret_<H>` | direct (two-way clustered) | Recompute cell edge with top-5 contributing names removed, two-way (day AND security) clustered errors; a sign flip disqualifies the cell. **Output:** a name-concentration pass/fail (pre-registration gate). | primary | avoid | — |
| 18.4 | Surface-smoothness: is the edge a smooth gradient over `(entry_offset × horizon)` or an isolated spike (§2.4)? | `ret_<H>` over all offsets/horizons in cell | direct (heatmap) | Plot the full `(entry_offset × horizon)` expectancy surface; require a smooth internally-consistent neighborhood (§2.4 — pre-specified blind to outcome labels). **Output:** a smooth/spike verdict (the §2.4 false-discovery killer). | exploratory | exec | — |
| 18.5 | Cost-sensitivity: how does Sharpe move at 5/10/20/30 bps (§1.3.18)? | `ret_<H>`, cost sweep | DP cost-sweep | DP cost-sweep; report the Sharpe-vs-cost curve (Category B sweep, §1.6). **Output:** a cost-sensitivity curve per cell. | exploratory | exec | — |
| 18.6 | Discretization robustness: does V change qualitatively across 5/10/15 state bins (§7.7.1)? | DP state bins | DP sweep | Re-run DP at 5/10/15 bins; flag qualitative policy changes as bin-sensitive. **Output:** a bin-robustness flag (bin-sensitive cells not pre-registered). | exploratory | exec | — |
| 18.7 | Markov-adequacy: does a 2-step-lookback model beat the DP policy (state under-specified)? (§7.3.1) | path features, lagged | DP vs lookback model | Fit a 2-step-lookback model on the same `(state→outcome)` task; if it beats DP OOS by >few %, the state is under-specified (§7.3.1). **Output:** a Markov-adequacy verdict (add state or trust DP). | exploratory | exec | — |
| 18.8 | Permutation null for MI: is the feature's MI above the 95th-pctile shuffled null (§7.7.1)? | feature, `ret_<H>` shuffled | MI permutation | Shuffle the outcome, recompute MI many times, take the 95th pctile as the floor; any MI below it is treated as zero. **Output:** a per-feature MI significance floor. | exploratory | exec | — |
| 18.9 | BN edge stability: do load-bearing DAG edges survive 80% bootstrap resamples and not flip by era (§7.7.1)? | BN structure, bootstrap | BN stability selection | Bootstrap-resample, relearn the DAG each time, retain edges in ≥80% of resamples; flag any era-direction-flip. **Output:** the set of stable load-bearing DAG edges. | exploratory | exec | — |
| 18.10 | Overlapping-horizon inference: does the multi-day CI hold under block bootstrap (block ≥ horizon) vs day-clustered (§7.7.1)? | multi-day `ret_<H>` series | block bootstrap | Circular block bootstrap over days, block ≥ horizon, vs naive day-clustering; the gap shows the overlap-inflation correction (§7.7.1 amendment). **Output:** corrected multi-day CIs. | exploratory | exec | — |

---

## Section 19 — Portfolio-level / cross-signal independence (Phase 5)

*Analysis approach: eigendecomposition / PCA of the same-day outcome covariance
is the right tool for "how many real independent bets" (effective N via
`1/Σλ²`); Markowitz portfolio optimization sizes positions but is explicitly
Phase 5. Shared discipline: `forward_outcomes` is keyed independent of signal so
the same machinery applies; all exploratory (structural, not coarse-cell tests);
day-clustered CI on covariance estimates.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 19.1 | How correlated are same-day signals' outcomes (real independent bets per day)? (cf §1.3.14) | same-day `ret_<H>` matrix | eigendecomp | Eigendecompose the same-day `ret_<H>` correlation matrix; report the spectrum. **Output:** the realized correlation structure of concurrent signals. | exploratory | exec | — |
| 19.2 | If N signals fire, what's the effective N via 1/Σλ² over normalized eigenvalues? (cf §1.3.14) | same-day outcome covariance | PCA | Effective N = `1/Σλ²` over normalized eigenvalues of the same-day covariance. **Output:** effective-independent-bets per day (sizing input). | exploratory | exec | — |
| 19.3 | Max concurrent positions before correlation eats the edge (Phase 5 solver)? (cf §1.3.14) | outcome covariance, capacity | Markowitz (Phase 5) | Markowitz-style solver over the outcome covariance + capacity constraint — **Phase 5**, flagged accordingly. **Output:** a max-concurrent-positions number (deferred to Phase 5). | exploratory | exec | — |
| 19.4 | Do cells cluster into a few correlation modes (compress 200 cells to a handful)? (§4) | edge surface across cells | eigendecomp (PCA on edge surface) | PCA on the cross-cell edge surface; count dominant modes. **Output:** a compression of ~200 cells into a handful of correlation modes. | exploratory | exec | — |
| 19.5 | Does combining TS + CS + swing reduce portfolio variance (independence across signal types)? | TS/CS/swing outcome series | eigendecomp | Eigendecompose the joint TS/CS/swing outcome series; measure variance reduction from combining low-correlated signal types. **Output:** a diversification benefit estimate across signal families. | exploratory | exec | — |

---

## Section 20 — Predictability-ceiling / variance-attribution

*Analysis approach: MI gives the predictability ceiling (Fano upper bound on any
model, §2.10) and factor-reg builds the variance-attribution ledger; together
they answer "is there anything to learn here at all" — primary deliverable #2
(§5.0). Shared discipline: the ceiling is corrected against the permutation null
(finite-sample MI is biased up, §7.7.1) and reported as a RANGE not a point;
unknown behavior is attributed to the residual by default (the noise-null);
all exploratory (structural).*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 20.1 | What's the MI-implied predictability ceiling per cell (Fano upper bound on any model)? (§2.10) | all ML-safe features, `ret_<H>` | MI (ceiling) | Total MI between the full ML-safe feature set and `ret_<H>` per cell, corrected against the shuffled null (Fano bound on achievable accuracy); reported as a CI range. **Output:** a per-cell predictability ceiling. | exploratory | exec | — |
| 20.2 | Which cells have near-zero ceiling (genuinely random w.r.t. measured features — stop research)? (§2.10) | `ceiling` map | MI | Threshold the §20.1 ceiling map at the permutation-null floor. **Output:** a stop-research list (cells random w.r.t. everything measured). | exploratory | avoid | — |
| 20.3 | Gap = ceiling − captured: where does the data contain structure current models miss (research queue)? (§2.10) | `ceiling`, best-model captured | MI + model | `gap = ceiling − captured` (current best model's explained share). **Output:** a research queue ranked by unexploited structure. | exploratory | exec | — |
| 20.4 | Variance-attribution ledger per cell: market / sector / regime / signal / name / residual shares (§2.10)? | factor-reg, sector/regime joins, name FE | factor-reg + HB | Factor-reg + hierarchical name fixed-effects decompose variance into market/sector/regime/signal/name/residual shares; bootstrap CI on each share. **Output:** the full §2.10 ledger per cell. | exploratory | exec | — |
| 20.5 | Noise-null: does any narrated effect beat "it's residual" quantitatively (§2.10)? | ledger residual share | factor-reg | Compare any narrated component's variance share against the residual share; if it explains a trivial share the narrative is decoration. **Output:** a noise-null pass/fail per narrated effect. | exploratory | exec | — |
| 20.6 | Linear-baseline check: does OLS on classification+regime+signal already explain ~90% (no ML needed)? (§4) | features, `ret_<H>_excess_spy` | factor-reg | OLS of cost-adjusted `ret_<H>_excess_spy` on classification+regime+signal; report R²; high R² means ML adds little. **Output:** a no-ML-needed verdict (linear baseline). | exploratory | exec | — |

---

## Section 21 — Event-driven (earnings proximity, splits, dividends)

*Analysis approach: DP with `days_to_earnings` in state self-discovers the
avoid-before / drift-after rules (the policy refuses or holds iff the effect is
real); conditional MI handles the BMO/AMC gap-timing and split/dividend
questions. Shared discipline: earnings via point-in-time `earnings_calendar` /
`days_to_next_known_earnings` (no look-ahead); TOTAL-return for the
dividend-distortion questions (§3.3); cost-adjustment + day-clustered CI. The
last row is OUT OF SCOPE (catalyst data absent) and carries no analysis spec.*

| # | Question | Columns / data | Algo | Analysis → output | Tier | exec/avoid/pattern | Data gap |
|---|---|---|---|---|---|---|---|
| 21.1 | Should the long book AVOID entering 5d before earnings (DP refuses in that state)? (cf §1.3.11) | `days_to_next_known_earnings`, `ret_<H>` | DP (days_to_earnings in state) | DP with `days_to_next_known_earnings` bucket in state, cost-adjusted reward; the policy refuses the pre-earnings state iff V<0; day-clustered CI. **Output:** a pre-earnings entry avoid-window. | primary | avoid | — |
| 21.2 | Post-earnings drift: positive cost-adjusted continuation after the report? (cf §1.3.11) | `days_since_last_earnings`, `is_earnings_day`, `earnings_report_timing`, `ret_<H>` | DP + earnings join | DP conditional on `days_since_last_earnings`, cost-adjusted excess reward; test positive post-report continuation; day-clustered CI. **Output:** a post-earnings-drift whitelist + entry window. | primary | exec | — |
| 21.3 | Does `earnings_report_timing` (BMO/AMC) change the next-day gap behavior? | `earnings_report_timing`, `next_day_gap_return` | conditional MI | `I(earnings_report_timing; next_day_gap_return)` vs shuffled null. **Output:** a BMO-vs-AMC next-day gap-behavior map. | exploratory | exec | — |
| 21.4 | Do splits (`split_event_nearby`) create exploitable inefficiencies? (cf §1.3.11) | `split_event_nearby`, `adjustment_factor_on_day`, `ret_<H>` | MI + DP | `I(split_event_nearby; ret_<H>)` vs null + DP around the split flag, cost-adjusted. **Output:** a split-inefficiency verdict. | exploratory | exec | — |
| 21.5 | Do dividend events (`dividend_event_today`, ex-date within horizon) distort or signal? (cf §1.3.11) | `dividend_event_today`, `dividend_ex_date_within_<H>`, `ret_<H>_total` vs `ret_<H>` | MI + direct | Direct `ret_<H>_total` vs `ret_<H>` comparison gated on `dividend_ex_date_within_<H>` + `I(dividend_event_today; outcome)` vs null. **Output:** a distortion-vs-signal verdict on dividend events. | exploratory | exec | — |
| 21.6 | Is post-earnings drift's optimal HOLD horizon different from the generic momentum hold? (cf §1.3.3) | `days_since_last_earnings`, `ret_<H>_total` | DP across horizons | DP across horizons conditioned on `days_since_last_earnings` (TOTAL return); compare optimal hold to the generic momentum hold; block bootstrap (block ≥ horizon) CI. **Output:** a post-earnings-specific hold horizon. | exploratory | exec | — |
| 21.7 | Earnings-day entries (`is_earnings_day`): gamble cohort to exclude, or edge? | `is_earnings_day`, `max_drawdown_<H>`, `max_runup_<H>` | direct | Direct comparison of `is_earnings_day` entries' tail profile (drawdown/runup) vs baseline; name-concentration check. **Output:** an exclude-or-edge verdict on earnings-day entries. | exploratory | avoid | — |
| 21.8 | Catalyst-driven moves (FDA/M&A/analyst) — can't be isolated without event data. | — | — | OUT OF SCOPE — no analysis spec; the required catalyst data is absent. | exploratory | pattern | NEEDS DATA NOT ON DISK — catalyst_events (FDA/M&A/analyst/guidance/offerings) out of scope |

---

## Cross-cutting: questions that NEED DATA NOT ON DISK (out of scope, listed for completeness)

These recur across themes; collecting them so the backlog is honest about the
boundary. None are answerable from Phase 0 v7. **They carry no `Analysis →
output` spec because the required data is absent** — there is no estimator to
design until the missing table lands; the "Where it would slot" column names the
in-scope section whose analysis approach they would inherit on arrival.

| # | Question | Missing data | Where it would slot |
|---|---|---|---|
| X.1 | Does borrow fee / days-to-cover predict squeeze vs collapse for short patterns? | `float_short_interest.parquet` (borrow fee, DTC, short interest) | §5 short-side |
| X.2 | Does true float (not proxy) sharpen the low-float edge/avoid map? | float / historical shares outstanding | §13 |
| X.3 | Does S&P/Russell index-membership change (add/delete) drive momentum? | `index_membership_daily.parquet` | §14 / §21 |
| X.4 | Do analyst revisions / guidance / offerings explain residual structure (the ceiling gap)? | `catalyst_events.parquet`, news feed | §20 / §21 |
| X.5 | Does true NBBO spread change the cost model / break-even ceilings materially? | NBBO quotes / tick data | §15 |
| X.6 | Does implied-vol / options positioning predict realized continuation? | options chain / IV | §14 / §16 |
| X.7 | Is intraday/premarket VIX a better day-gate than prior-day VIX close? | intraday VIX / VIX futures | §1 |
| X.8 | Are is_fomc / is_opex calendar days distinct (need an events calendar)? | macro/options-expiry calendar | §1 |

---

## Genuinely new vs already-covered

**Themes that are direct re-expressions of strategy §1.3 (overlap, not new) —
the catalog sharpens these into the strong "under which conditions ... smooth,
stable, survives holdout" format and adds explicit column/tier/exec tags:**

- §7 Entry state & quality ↔ §1.3.1 + §1.3.7 + §1.3.9
- §8 Exit / survival / DP stops ↔ §1.3.2 + §1.3.6 + §1.3.10 + reframing §2.7
- §3 Swing / multi-horizon ↔ §1.3.3 + §1.3.12
- §13 Classification & blacklist ↔ §1.3.4 + reframing §2.2
- §14 Regime interaction ↔ §1.3.5 + reframing §2.5 + §1.3.15
- §4 Gap-vs-RTH ↔ §1.3.8
- §15 Cost / capacity ↔ §1.3.9 + §1.3.19
- §16 Microstructure ↔ §1.3.16
- §11/§19 Crowding / cross-signal ↔ §1.3.14
- §17 Edge decay ↔ §1.3.20 + reframing §2.9
- §18 Robustness / placebo ↔ §1.3.17 + §1.3.18 + §7.7.1
- §20 Predictability ceiling ↔ reframing §2.10
- §21 Event-driven ↔ §1.3.11
- §12 CS vs TS ↔ reframing §2.6
- §2 Beta neutralization ↔ reframing §2.3 + §4

**Genuinely NET-NEW beyond §1.3 (no dedicated §1.3.x entry; these are the
highest-value additions this backlog contributes):**

1. **§6 Setup-archetype discovery (unsupervised clustering, blind to outcome).**
   §1.3 has no unsupervised category at all. Clustering pre-entry features
   independent of outcome, then profiling outcomes per cluster, is a distinct
   discovery mode (and a guard against outcome-driven cell definition). The
   richest net-new theme.
2. **§9 Signal freshness / repeated-chased signals.** Enabled purely by the v7
   `signal_first_in_{5,10,20}d` columns (strategy §3.7); §1.3 predates them and
   has no freshness question.
3. **§10 52-week-range position / breakout-vs-exhaustion.** Enabled by v7
   `high_52w` / `low_52w` and `days_since_last_X%_move`; §1.3 has no 52w or
   coiling/extension question.
4. **§1 Is-the-day-tradeable as its own family.** §1.3.5 covers regime
   interaction *for name selection*; the §2.1 two-stage *day-gate* (Stage-1
   sit-out classifier on pre-decision features) is operationally distinct and
   was a reframing, not a §1.3 catalog entry. Pulled out here as a first-class
   section.
5. **Dispersion-regime questions (§1.8, §12.4, §14.6).** Enabled by the v7
   `cross_sectional_ret_dispersion_*` columns (§3.6); §1.3 had no dispersion
   axis because the parent wasn't recorded.
6. **Halt/exitability-artifact filters (§8.10).** Enabled by v7
   `halt_gap_crossed` / `bar_gap_minutes_max_<H>` (§3.4); a distinct
   falsification angle (is the edge an exitability artifact?) absent from §1.3.
7. **Total-return vs price-return ranking shift (§3.5, §21.5).** Enabled by v7
   `ret_<H>_total` / `dividend_ex_date_within_<H>` (§3.3); a measurement-honesty
   question §1.3 couldn't pose pre-amendment.

**Tier discipline summary:** of ~135 questions, the `primary` tag is reserved
for the ~40 that reduce to a single coarse `(classification × regime)`
expectancy test (mostly in §13 blacklist, §14 regime, §3 swing, §8 stops,
§12 CS/TS, §1 day-gate, §15 cost ceilings). Everything structural (MI/BN/
clustering/eigendecomp/change-point/survival) and everything finer than the
coarse cell is `exploratory` so it does NOT inflate the BY family that §1.5.1
says is the binding power constraint.
