# Phase 9 — the Feature Dynamics Atlas (characterization, not a test)

**Date:** 2026-07-06 · **Status:** charter, declared before the build ran.
**Question:** the variables we track — are they just numbers, or do they mean something?
What happens when each one rises, falls, reverses; what happens in pre-declared
combinations; what does a stock reversal look like in every dimension we track?

**What this is NOT (frozen up front):** not a pre-registered test, not an alpha search.
There is no PASS/FAIL. The atlas prints descriptive distributions with day-clustered
error bars, *including* the direction columns that Phases 0–6B already measured to be
≈0 — printing the null is part of the map. **Graduation rule:** any cell of this atlas
that looks like an edge is NOT one — it is a candidate that must go through the
standard pipeline (power gate → pre-registration → BY-FDR → permutation placebo) as a
new phase before any belief attaches. This is the Phase 4 lesson applied to a
descriptive sweep: thousands of cells get looked at here, so atlas error bars are
descriptive only.

**Window:** train era only, 2016-06-08 → 2020-12-31. The 2021-22 validation and 2023+
holdout stay sealed even for descriptive work. Universe: CS, cap ∈ {mega, large, mid},
liquidity ∈ {highly_liquid, liquid, normal} (the Phase 1 train universe).

## Curated variable set (declared; full-sweep extension is a later decision)

Dynamic panel variables (cross-sectional rank computed per day for each):

- **Tape:** `intraday_ret_0930_to_1000`, `overnight_gap`, `prior_day_last_30m_return`,
  `premarket_volume_vs_20d_median`, `volume_ratio` = eod_day_volume / adv_20d
- **Volatility:** `vol_frac` = atr_14d / prior_day_eod_close, `realized_vol_21d`,
  `vol_trend` = atr_5d / atr_42d
- **Liquidity/activity:** `addv_20d`, `volm_trend` = adv_5d / adv_60d,
  `signal_concentration_hhi_today`
- **History/position:** `dist_52w_high` = eod_day_close / high_52w,
  `days_since_last_5pct_move`, `consecutive_up_days_close_to_close`, `ret_cc` (daily
  close-close return), 5d and 21d trailing returns
- **Linkage:** `beta_spy_60d`
- **Market layer:** vix_close, breadth_pct_universe_green_at_1000,
  cross_sectional_ret_dispersion_at_1000, spy_realized_vol_21d

Outcomes attached at the 10:00 clock (`forward_outcomes`, offset 1000):
`ret_1d/5d/21d_excess_spy`, `fwd_range_21d` = max_runup_21d − max_drawdown_21d; plus
each variable's own future rank (lead 21 trading days) for mean-reversion measurement.

**Clock rule (added after the first build caught its own lookahead):** several
conditioning variables (daily green/red, volume_ratio, dist-from-52w-high, trailing
returns) are only known at day t's close, while outcomes are anchored at 10:00
entries — attaching day t's 10:00 outcome to a close-of-t condition leaks the event
day's own afternoon into its "forward" return (first build showed reversal-up
"+77bp next-day", which was the event day measuring itself). Uniform rule: every
outcome is taken from the NEXT day's 10:00 entry — all atlas numbers read "what
happens from the first tradeable clock after the condition is knowable."

## Layers

1. **Univariate dynamics** — per variable: cross-sectional rank autocorrelation at
   lags 1/5/21/63 trading days (per-day Spearman, averaged), implied half-life, and
   1d jump rate P(|Δrank| > 0.25). Answers "is this variable an identity, a state, or
   noise."
2. **Level × trend outcome maps** — per variable: level quintile (of today's rank) ×
   21d rank trend (rising / flat / falling = rank change > +0.10 / within ±0.10 /
   < −0.10) → mean fwd_range_21d (magnitude),
   mean ret_21d_excess (direction, expected ≈0), own-rank 21d-ahead change (mean
   reversion), with day-clustered SEs and cell counts.
3. **Event anatomy** — pre-declared events, before/after profile (trailing 21d return,
   vol level at −21/0/+21, volume ratio at −5/0/+5, forward 1/5/21d excess and
   21d range):
   - reversal-up: ≥3 consecutive red closes then a green close (mirror: reversal-down)
   - vol expansion: vol_trend crosses above 1.5 from below (mirror: collapse below 0.75)
   - volume surge: volm_trend crosses above 2.0 from below
   - market breadth flip: breadth < 35% after its trailing 5d mean > 50% (mirror ≥ 65%
     after < 50%)
4. **Pre-declared combination grid (27 cells, never mined):** vol_trend tercile ×
   volm_trend tercile × cap bucket → fwd_range_21d, ret_5d/21d_excess,
   P(next-day excess > 0).
5. **Market layer** — persistence/half-life of VIX, breadth, dispersion; and the Layer-2
   vol_trend map re-cut inside VIX terciles (does stock-level vol dynamics mean
   something different in calm vs stressed tape).

**Deliverables:** `scripts/phase9_dynamics_atlas.py` (single build, ~one run) →
tables in `data/phase1_analysis/atlas_l{1..5}_*.parquet` + printed summary →
`phase9-atlas-findings.md` narrating what the numbers say.
