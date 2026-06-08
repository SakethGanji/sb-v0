# Phase 0 — Observation-Centric Recording (RFC v6)

**Audience:** an LLM or human reviewer with no prior context.
Self-contained.

**Status:** Proposal. v6 sharpens the recording for day-trading
and short-swing horizons (denser sub-day checkpoints, gap-vs-RTH
decomposition, target-before-stop materializations, entry-quality
proxies), and adds point-in-time security classification so the
research never blindly pools mega-cap tech with low-float biotech
gappers, leveraged ETFs, ADRs, SPACs, or warrants. Changelog at end.

---

## 0. The deliverable, framed honestly

Momentum is a documented phenomenon — three decades of academic
literature (Jegadeesh & Titman 1993 onward) and live-trading practice
say it exists in some form. The question Phase 0 exists to answer is
**not** "does momentum exist." It is:

> Under what conditions, in this universe and time window, is a
> momentum-hold strategy profitable *after realistic execution
> costs*? At what statistical confidence?

Three valid endpoints:

1. **Profitable in regime X** — characterize, hand off.
2. **Not profitable** — characterize tightly enough that "no edge"
   is conclusive.
3. **Inconclusive** — power too low.

All three are acceptable iff the distributions are tightly
characterized. The distribution itself is the deliverable.

Design principle: **honest research must be the path of least
resistance.** Schema that makes leakage easy to write is worse than
schema that catches it at the column-name layer.

Optimization target: **robustness and statistical validity, NOT
maximum backtest performance, NOT minimum storage.**

## 0.5 The research arc: edge first, ML second

This project is NOT primarily an ML project. ML enters only after a
statistical edge is independently established.

- **Phase 0** — Observational recording (this RFC). Eight Parquet
  tables; no engine-time cutoffs.
- **Phase 1** — Pure statistical research. Conditional distributions,
  day-clustered CIs. Determine whether an edge exists before any
  model.
- **Phase 2** — Rule-based baseline strategy with positive
  expectancy. Failure here ends the project with a characterized
  null.
- **Phase 3** — Build `signal_dataset.parquet` from the baseline
  rule. Versioned label recipes generate binary targets.
- **Phase 4** — Meta-labeling (Lopez de Prado). ML decides which
  signals to take, not which signals exist.

The schema must serve all four phases without redesign. The
biggest design implication remains the **research dataset vs ML
dataset separation** (§6.5).

## 0.6 Versioned defaults

Several parameter sets are versioned defaults written into
file-level Parquet metadata. Changing any of them means
regenerating the file with a bumped version; downstream readers
check versions and refuse to join across mismatches.

| Parameter set | Default in v6 | Metadata key |
|---|---|---|
| Entry-offset grid | 17 offsets (§9) | `entry_offset_grid_version = v1` |
| Forward-horizon set (wide outcomes) | 13 horizons (§9) | `forward_horizons_version = v2` |
| Forward-path checkpoint set | 23 checkpoints (§9.5) | `forward_path_checkpoints_version = v1` |
| Fixed-% threshold set | 7 thresholds ±0.5/1/2/3/5/10/20 | `pct_threshold_version = v2` |
| ATR-multiple threshold set | 6 thresholds ±0.25/0.5/1/1.5/2/3 | `atr_threshold_version = v2` |
| Regime taxonomy | 5 taxonomies (§10) | `regime_taxonomy_version = v1` |
| Security-classification rule set | 1.x rules (§11.6) | `security_classification_version = v1` |
| Label recipes | TBD per Phase 3 | `label_recipe_version = TBD` |

These grids are NOT externalized at runtime because they shape the
schema. The version stamps make parameter dependence explicit.

## 1. What the project is

Rust implementation of a momentum-hold observational backtest over
US equities, ~2016 to present. Data: Massive (Polygon clone) flat
files. ~2,500 trading days already ingested at
`data/bars_1m_raw/YYYY-MM-DD.parquet`.

## 2. What's already built

- `momentum-core`, `momentum-store`, `momentum-calendar` — schemas,
  readers, calendar, FigiMap.
- 51 unit tests.
- Engine, signal computation, recorder, classification: not yet
  built.

## 3. Original frozen spec (departing from)

Signal: `(close_at_1000 / open_at_0930) - 1`, positive only.
Entry: 10:00–10:10 bar open. Exit: breakeven stop from 10:10.
Recording: 252-day path. Universe: CS only on NYSE/NASDAQ/AMEX.

## 4. Amendments already in force

- **Universe relaxation.** No listing-type or exchange filter at
  engine time.
- **No arbitrary cutoffs.** Record everything; downstream filters.

## 5. The structural reframe

Trade-centric output answers "did this strategy work." Observation-
centric output answers "where does momentum work, under what
conditions, after what costs, with what confidence." The project's
stated goal (§0) demands the second.

## 6. Schema discipline (binding rules)

### 6.1 Output tables

**Phase 0 outputs (eight tables in v6):**

| Table | Grain | v6 size estimate |
|---|---|---|
| `daily_observation.parquet` | (day, security_id) with bars | ~50-85 GB |
| `market_context_daily.parquet` | (day) | <200 MB |
| `forward_outcomes.parquet` | (day, security_id, entry_offset) | ~200-300 GB |
| `forward_path_short.parquet` | (day, security_id, entry_offset, path_checkpoint) — long | ~150-220 GB |
| `regime_definitions.parquet` | (day, taxonomy, regime_id) | <50 MB |
| `earnings_calendar.parquet` | (security_id, earnings_date) | <100 MB |
| `sector_aggregates_daily.parquet` | (day, sector_id) | <100 MB |
| `security_classification_daily.parquet` | (day, security_id) | ~3-5 GB |

**Phase 3 derivation:**

| Table | Grain | Notes |
|---|---|---|
| `signal_dataset.parquet` | (day, sid, entry_offset) where baseline fires | ~5-15 GB |

**Contemplated future tables (Phase 1+ ingest, schema reserves joins):**

- `catalyst_events.parquet` — FDA / M&A / analyst / SEC / guidance
  / offering / split.
- `float_short_interest.parquet` — point-in-time short interest,
  borrow fee, days-to-cover, float, shares outstanding.
- `index_membership_daily.parquet` — S&P 500, Russell 1000/2000/3000,
  Nasdaq 100, sector ETF holdings, leveraged-ETF flag.
- `execution_cost_estimates.parquet` — Phase 3 derivation, multiple
  cost assumptions per signal.

**Total v6 storage budget: ~450-650 GB.** Substantially larger than
v5 (~120-180 GB). Driver: denser short-term outcomes, new
forward_path_short, expanded threshold sets, security
classification. The bump is deliberate per §0 — short-swing edge
characterization requires sub-day path resolution.

### 6.2 Column naming convention — leakage prevention

Every column in `daily_observation` falls into one of three buckets:

- **Bare name** — point-in-time over `[D-N, D-1]` or pre-day-open.
  Always safe.
- **`intraday_*`** — derived from 9:30 ET bars onward on day D.
  Per-window-end safety.
- **`eod_*`** — full-session derivative. NEVER an ML feature.

### 6.3 List columns are RAW SOURCE

Nested intraday lists contain both pre- and post-decision data
depending on entry_offset. Slicing happens at signal_dataset
derivation time.

### 6.4 Point-in-time hygiene

Every rolling statistic computed over `[D-N, D-1]`, strictly
excluding day D. Hard unit test enforced.

### 6.5 Research dataset vs ML dataset separation

| Aspect | Research dataset | ML dataset |
|---|---|---|
| Contents | Phase 0 facts + sidecars | `signal_dataset.parquet` |
| Purpose | Phase 1 descriptive stats | Phase 4 meta-labeling |
| Information policy | Information-rich; includes `eod_*` + outcomes + classifications | Strictly signal-time available |
| Leakage discipline | Convention; reviewer-checked | Structurally enforced at derivation |

### 6.6 Universe classification mandatory at two layers

**Reference layer (unchanged):** `tickers_enriched.parquet` carries
point-in-time `ticker_type`, `is_etf`, `is_etn`, `is_adr`,
`is_spac`, `is_warrant`, `is_preferred`, `is_unit`,
`primary_exchange`, `listing_status`.

**Per-day classification layer (NEW in v6):**
`security_classification_daily.parquet` (§11.6) carries
point-in-time behavioral tags computed nightly. Downstream MUST
condition on classification before interpreting any momentum edge.
Failing to condition produces apparent edges that are
security-type mixing artifacts (the single biggest false-positive
risk in the relaxed universe).

## 7. Schema — `daily_observation.parquet`

Unchanged from v5 in structure. Annotations: `[ML]` signal-time
safe; `[RES]` research only; `[SRC]` raw source requiring slicing.

```
# ===== Identity =====
day, security_id, display_symbol_on_day                              [ML × 3]

# ===== Adjusted intraday paths =====
intraday_1m_first_hour   List<Struct>   [SRC]    # 9:30-10:30 ET, 60 bars
intraday_10m_rest        List<Struct>   [SRC]    # 10:30-close

# ===== Materialized signal snapshots (per-window-end safety) =====
intraday_ret_0930_to_{0940,0950,1000,1010,1030}     f64 × 5  [ML if entry >= T]
intraday_volume_0930_to_1000                        f64       [ML if entry >= 10:00]
intraday_dollar_volume_0930_to_1000                 f64       [ML if entry >= 10:00]
intraday_vwap_0930_to_1000                          f64       [ML if entry >= 10:00]

# ===== First-30m shape (safe for entry >= 10:00) =====
intraday_first_30m_high_return / low_return         f64 × 2   [ML if entry >= 10:00]
intraday_time_of_first_30m_high / low               Utf8 × 2  [ML if entry >= 10:00]
intraday_ret_from_first_30m_high_to_1000            f64       [ML if entry >= 10:00]
intraday_minutes_since_first_30m_high_at_1000       Int32     [ML if entry >= 10:00]
intraday_first_15m_volume_share_of_first_30m        f64       [ML if entry >= 10:00]

# ===== First-hour shape (safe for entry >= 10:30) =====
intraday_first_hour_high_return / low_return        f64 × 2   [ML if entry >= 10:30]
intraday_time_of_first_hour_high                    Utf8      [ML if entry >= 10:30]
intraday_ret_from_first_hour_high_to_1030           f64       [ML if entry >= 10:30]
intraday_first_30m_volume_share_of_first_hour       f64       [ML if entry >= 10:30]

# ===== Cross-sectional ranks at snapshot times =====
intraday_ret_0930_to_1000_rank_today                Int32     [ML if entry >= 10:00]
intraday_ret_0930_to_1000_percentile_today          f64       [ML if entry >= 10:00]
intraday_dollar_volume_0930_to_1000_rank_today      Int32     [ML if entry >= 10:00]
premarket_volume_rank_today                         Int32     [ML]
premarket_dollar_volume_rank_today                  Int32     [ML]
overnight_gap_rank_today                            Int32     [ML]
addv_20d_rank_today                                 Int32     [ML]
realized_vol_21d_rank_today                         Int32     [ML]

# ===== EOD aggregates — RESEARCH ONLY =====
eod_day_{open,high,low,close,volume,dollar_volume,vwap}            f64 × 7  [RES]
eod_unadjusted_day_{open,high,low,close}                            f64 × 4  [RES]

# ===== Pre-market (04:00-09:30 ET) =====
premarket_{volume,high,low,vwap,dollar_volume}                      f64 × 5  [ML]

# ===== Overnight context =====
prior_day_eod_close, prior_day_unadjusted_eod_close                 f64 × 2  [ML]
overnight_gap                                                       f64      [ML]
prior_day_last_30m_return                                           f64      [ML]
prior_day_last_30m_volume_share                                     f64      [ML]

# ===== Rolling volatility (point-in-time) =====
atr_5d / atr_14d / atr_42d                                          f64 × 3  [ML]
realized_vol_21d                                                    f64      [ML]
yang_zhang_vol_5d / 14d / 21d / 42d                                 f64 × 4  [ML]

# ===== Rolling liquidity (point-in-time) =====
adv_5d / 20d / 60d                                                  f64 × 3  [ML]
addv_5d / 20d / 60d                                                 f64 × 3  [ML]

# ===== Rolling betas =====
beta_spy_60d / qqq_60d / iwm_60d                                    f64 × 3  [ML]

# ===== Earnings proximity (point-in-time) =====
days_to_next_known_earnings, days_since_last_earnings               Int32 × 2 [ML]
is_earnings_day                                                     Bool     [ML]
earnings_report_timing                                              Utf8     [ML]

# ===== Data quality =====
bar_count_rth                                                       Int32    [RES]
bar_count_first_hour / first_30m / premarket                        Int32 × 3 [ML/SRC]
missing_1m_bars_first_30m / first_hour                              Int32 × 2 [ML/SRC]
zero_volume_1m_bars_first_30m                                       Int32    [ML]
first_rth_bar_time                                                  Utf8     [ML]
minutes_after_open_first_bar                                        Int32    [ML]
has_bad_ohlc                                                        Bool     [RES]
split_event_nearby                                                  Bool     [ML]
dividend_event_today                                                Bool     [RES if today]
ticker_event_today                                                  Bool     [ML]
adjustment_factor_on_day                                            f64      [RES]

# ===== Calendar / static =====
day_of_week, days_since_first_bar                                   [ML × 2]
is_half_day                                                         Bool     [ML]
```

Size: ~50-85 GB across ~15M rows.

## 8. Schema — `market_context_daily.parquet`

```
day                                                          Date32

# Index intraday paths (10-min) + EOD aggregates
{spy,qqq,iwm}_intraday_10m                List<Struct>     [SRC × 3]
{spy,qqq,iwm}_eod_{open,high,low,close,volume}             f64 × 15  [RES]

# Overnight / signal context
{spy,qqq,iwm}_overnight_gap                                f64 × 3   [ML]
{spy,qqq,iwm}_ret_0930_to_1000                             f64 × 3   [ML if entry >= 10:00]
{spy,qqq,iwm}_realized_vol_21d                             f64 × 3   [ML]

# VIX
vix_open / vix_close                                       f64 × 2   [ML / RES]

# Cross-sectional market breadth at snapshot times
breadth_pct_universe_green_at_1000 / 1030                  f64 × 2   [ML if entry >= T]
breadth_pct_universe_above_premarket_vwap_at_1000          f64       [ML if entry >= 10:00]
breadth_advance_decline_ratio_at_1000                      f64       [ML if entry >= 10:00]
breadth_count_movers_above_5pct_at_1000                    Int32     [ML if entry >= 10:00]
breadth_count_movers_above_1atr_at_1000                    Int32     [ML if entry >= 10:00]
breadth_count_movers_above_5pct_eod                        Int32     [RES]
breadth_advance_decline_ratio_eod                          f64       [RES]
breadth_total_universe_with_bars                           Int32     [ML]
```

Size: <200 MB.

## 9. Schema — `forward_outcomes.parquet`

One row per `(day, security_id, entry_offset)`. Records every entry
timing for every observation. Negative / neutral signals included
as control group. **Every field is `[RES]` or `[LABEL]`** — outcomes
become labels in the ML dataset, never features.

**Entry-offset grid v1** (17 offsets, dense early sparse late):
```
09:35 09:40 09:45 09:50 09:55 10:00 10:05 10:10 10:15 10:20
10:30 10:45 11:00 11:30 12:00 13:00 15:30
```

**Forward-horizon set v2** (13 horizons, wide per-horizon stats):
```
intraday milestones: 10min, 30min, 60min, EOD
day+:                1d, 2d, 3d, 5d, 10d, 21d, 42d, 63d, 252d
```

The sub-hour horizons (1m, 2m, 3m, 5m, 15m, 20m, 45m, 90m, 120m,
180m) are NOT in this wide table — they live in
`forward_path_short.parquet` (§9.5) which is long-format and built
exactly for the sub-day path resolution day trading needs.

```
# ===== Identity =====
day, security_id, entry_offset, entry_price, entry_unadjusted_price

# ===== Pre-entry parametric features (per entry_offset) =====
pre_entry_ret_from_open                                  f64    [ML]
pre_entry_volume_from_open / dollar_volume_from_open     f64 × 2 [ML]
pre_entry_vwap_from_open                                 f64    [ML]
pre_entry_high_return_so_far / low_return_so_far         f64 × 2 [ML]
pre_entry_minutes_since_high / since_low                 Int32 × 2 [ML]
pre_entry_ret_from_high / from_low                       f64 × 2 [ML]
pre_entry_ret_rank_today                                 Int32  [ML]
pre_entry_ret_percentile_today                           f64    [ML]
pre_entry_dollar_volume_rank_today                       Int32  [ML]

# ===== Entry-quality / fill-realism proxies (NEW v6) =====
# These describe the SPECIFIC 1-min bar at entry_offset. They are
# co-decision relative to a "decide at entry" model — allowed in
# signal_dataset IFF signal_time < entry_offset.
entry_price_location_in_1m_bar     f64   # (close - low) / (high - low) of entry 1m bar
entry_open_to_close_1m_return      f64   # close / open - 1 of entry 1m bar
entry_bar_upper_wick_pct           f64   # (high - max(open,close)) / open
entry_bar_lower_wick_pct           f64   # (min(open,close) - low) / open
entry_slippage_proxy_bps           f64   # entry_1m_range / 2 expressed in bps (proxy for spread)
entry_participation_capacity_1pct_adv     f64   # 0.01 * adv_20d * entry_price
entry_participation_capacity_5pct_1m_volume f64 # 0.05 * entry_1m_dollar_volume
entry_1m_volume                    f64
entry_1m_range                     f64
entry_1m_dollar_volume             f64
entry_range_vs_atr_14d             f64
entry_range_vs_yz_vol_14d          f64
entry_dollar_volume_vs_addv_20d    f64
is_halted_at_entry                 Bool

# ===== Per-horizon outcomes (13 horizons) =====
# All fields [LABEL] for ML purposes.

ret_<H>                                                  f64 × 13
max_drawdown_<H> / max_runup_<H>                         f64 × 26
close_max_ret_<H> / close_min_ret_<H>                    f64 × 26
bars_to_max_drawdown_<H> / bars_to_max_runup_<H>         UInt32 × 26
bars_to_close_min_<H> / bars_to_close_max_<H>            UInt32 × 26

# ===== Fixed-% threshold crossings (v2: 7 thresholds) =====
# ±0.5%, ±1%, ±2%, ±3%, ±5%, ±10%, ±20%
# 0 = never crossed within horizon; otherwise the bar index of first cross.
first_cross_up_{0_5,1,2,3,5,10,20}pct_<H>                UInt32 × 91
first_cross_down_{0_5,1,2,3,5,10,20}pct_<H>              UInt32 × 91

# ===== ATR-normalized threshold crossings (v2: 6 thresholds) =====
# ±0.25, ±0.5, ±1, ±1.5, ±2, ±3 multiples of atr_14d
first_cross_up_{0_25,0_5,1,1_5,2,3}atr_<H>               UInt32 × 78
first_cross_down_{0_25,0_5,1,1_5,2,3}atr_<H>             UInt32 × 78

# ===== Market-relative outcomes =====
ret_<H>_excess_{spy,qqq,iwm}                             f64 × 39

# ===== Day-0 post-entry session-segment returns (NEW v6) =====
# NULL when entry_offset > the segment-end time (segment is in the past).
ret_to_1030 / ret_to_1100 / ret_to_1130 / ret_to_1200
ret_to_1300 / ret_to_1400 / ret_to_1500 / ret_to_1530 / ret_to_close   f64 × 9 [LABEL]

# ===== Day-0 post-entry session-shape (NEW v6) =====
# All measured strictly AFTER entry_offset, within day 0.
post_entry_morning_high_return / morning_low_return       f64 × 2  [LABEL]
post_entry_midday_high_return / midday_low_return         f64 × 2  [LABEL]
post_entry_afternoon_high_return / afternoon_low_return   f64 × 2  [LABEL]
post_entry_power_hour_return                              f64      [LABEL]   # last hour
post_entry_close_vs_high_return                           f64      [LABEL]
post_entry_close_vs_low_return                            f64      [LABEL]

# ===== Time-underwater / time-profitable (NEW v6) =====
# For H ∈ {EOD, 1d, 2d, 3d, 5d}
pct_bars_profitable_<H>                                   f64 × 5  [LABEL]
pct_bars_underwater_<H>                                   f64 × 5  [LABEL]
max_consecutive_bars_profitable_<H>                       UInt32 × 5 [LABEL]
max_consecutive_bars_underwater_<H>                       UInt32 × 5 [LABEL]
time_to_recover_after_first_drawdown_<H>                  UInt32 × 5 [LABEL]
                                                          # 0 if never recovered

# ===== Next-day outcomes (NEW v6) =====
# Critical for distinguishing intraday continuation from overnight gap effects.
next_day_open_return                                      f64      [LABEL]
next_day_gap_return                                       f64      [LABEL]   # open / prev_close - 1
next_day_first_5m_return / first_15m_return / first_30m_return  f64 × 3 [LABEL]
next_day_high_return / low_return / close_return          f64 × 3  [LABEL]
next_day_close_location_in_range                          f64      [LABEL]
next_day_fade_from_open                                   f64      [LABEL]   # close - high (if open was high)
next_day_continuation_from_open                           f64      [LABEL]

# ===== Gap-vs-RTH decomposition for days 1-5 (NEW v6) =====
# Lets downstream isolate: does the edge come from overnight gaps or
# regular-session continuation?
gap_return_day_{1,2,3,4,5}                                f64 × 5  [LABEL]
rth_return_day_{1,2,3,4,5}                                f64 × 5  [LABEL]
close_to_close_return_day_{1,2,3,4,5}                     f64 × 5  [LABEL]
open_to_close_return_day_{1,2,3,4,5}                      f64 × 5  [LABEL]

# ===== Materialized target-before-stop labels (NEW v6) =====
# Derivable from threshold crossings; materialized because they're
# constant Phase 1 queries.
hit_0_5pct_before_minus_0_5pct_30min                      Bool, nullable [LABEL]
hit_1pct_before_minus_1pct_EOD                            Bool, nullable [LABEL]
hit_2pct_before_minus_1pct_EOD                            Bool, nullable [LABEL]
hit_3pct_before_minus_1_5pct_EOD                          Bool, nullable [LABEL]
hit_2pct_before_minus_2pct_1d                             Bool, nullable [LABEL]
hit_3pct_before_minus_3pct_5d                             Bool, nullable [LABEL]
hit_1atr_before_minus_0_5atr_EOD                          Bool, nullable [LABEL]
hit_2atr_before_minus_1atr_5d                             Bool, nullable [LABEL]
hit_3atr_before_minus_1_5atr_21d                          Bool, nullable [LABEL]

# ===== Terminal events =====
terminal_event_type                                       Utf8     # enum
terminal_event_date                                       Date32, nullable
terminal_event_return                                     f64, nullable
terminal_event_confidence                                 Utf8     # high/medium/low
last_valid_trade_date                                     Date32, nullable
days_with_missing_forward_bars                            Int32
```

Approximate size: ~200-300 GB compressed.

Yes, large. The driver is per-horizon × per-threshold combinatorics
(13 horizons × 26 fixed-% + 12 ATR threshold-cross fields = ~500
UInt32 per row) plus the new day-0 segment / next-day / gap-vs-RTH
fields. Long-format would reduce this to ~50 GB but would force a
horizon-filter on every join. Wide format kept for the same reason
as v5: the join from signal_dataset is one-row-per-signal-grabs-
all-horizons. The expanded threshold set is the bigger cost driver
than horizons, but it's load-bearing for vol-bucketed cutoff
research.

## 9.5 Schema — `forward_path_short.parquet` (NEW v6)

Long-format companion to `forward_outcomes`. Captures the sub-day
and gap-day path resolution that day-trading and short-swing
research need but that the wide outcomes table can't carry without
exploding.

Grain: one row per `(day, security_id, entry_offset, path_checkpoint)`.

**Path checkpoint set v1** (23 checkpoints):
```
intraday:  1m, 2m, 3m, 5m, 10m, 15m, 20m, 30m, 45m, 60m, 90m,
           120m, 180m, EOD
day+:      1d_open, 1d_30m, 1d_close,
           2d_open, 2d_close,
           3d_open, 3d_close,
           5d_open, 5d_close
```

Sub-minute granularity stops at 1m because the underlying bars are
1-min. Day-N split into open and close lets gap-vs-intraday questions
be answered checkpoint-by-checkpoint. Beyond 5d, the wide
forward_outcomes carries the daily+ stats.

```
day                              Date32
security_id                      Utf8
entry_offset                     Utf8
path_checkpoint                  Utf8       # see set above

# Position state at this checkpoint
ret                              f64        # cumulative return since entry
high_ret_so_far                  f64
low_ret_so_far                   f64
close_max_ret_so_far             f64
close_min_ret_so_far             f64

# Trade book at this checkpoint
volume_since_entry               f64
dollar_volume_since_entry        f64
vwap_since_entry                 f64
bars_elapsed                     Int32

# Time-quality at this checkpoint
pct_bars_profitable_so_far       f64
pct_bars_underwater_so_far       f64
```

Storage: 23 checkpoints × 17 entry_offsets × ~15M (day, sid) =
~6B rows × ~30 bytes compressed = **~150-220 GB**.

**Storage-constrained fallback v1-lite** (15 checkpoints):
drops the very fine 1m/2m/3m and the redundant 10m/20m/45m/90m/180m,
drops 1d_30m. Keeps:
```
5m, 15m, 30m, 60m, 120m, EOD,
1d_open, 1d_close, 2d_open, 2d_close,
3d_open, 3d_close, 5d_open, 5d_close
```
Cuts forward_path_short to ~100 GB. The fine 1m/2m/3m checkpoints
are usually only needed for HFT/scalping research; the 1d_30m is
for fading the next morning's early session, useful but not
critical.

If storage matters, the fallback is the safe pick. If we want to
ask "where exactly did the move start to fade in the first 5
minutes," only the full v1 captures it.

## 10. Schema — `regime_definitions.parquet`

Long format. One row per `(day, taxonomy, regime_id)`. Same day
participates in multiple taxonomies.

```
day, taxonomy, taxonomy_version, regime_id, regime_name
```

Default taxonomies v1: `era` (calendar), `spy_trend` (data-driven
from rolling SPY returns), `vix_level`, `breadth` (data-driven from
A/D ratio), `liquidity` (data-driven from median ADDV).
Bucket boundaries documented in v5; unchanged.

## 11. Schema — `earnings_calendar.parquet`

```
security_id, earnings_date, report_timing, announced_at_date,
earnings_window, revision_count, source
```

Point-in-time via `announced_at_date`. Requires
`bin/build-earnings` ingest (1-2 days).

## 11.5 Schema — `sector_aggregates_daily.parquet`

```
day, sector_id, sector_constituent_count_with_bars
sector_ret_0930_to_{0950,1000,1030}
sector_eod_ret    [RES]
sector_pct_green_at_{1000,1030}
sector_ret_0930_to_1000_rank_today
```

Size: <100 MB.

## 11.6 Schema — `security_classification_daily.parquet` (NEW v6)

One row per `(day, security_id)` where bars exist. Point-in-time
behavioral tagging so downstream never blindly pools mega-cap tech
with low-float biotech gappers, leveraged ETFs, ADRs, SPACs, or
warrants.

```
# ===== Identity =====
day, security_id, display_symbol_on_day

# ===== Security structure (from tickers_enriched, joined point-in-time) =====
ticker_type                                                Utf8
is_common_stock                                            Bool
is_etf, is_leveraged_etf, is_inverse_etf, is_etn           Bool × 4
is_adr                                                     Bool
is_spac                                                    Bool
is_warrant, is_preferred, is_unit                          Bool × 3

# ===== Listing =====
primary_exchange                                           Utf8
listing_status                                             Utf8
listing_status_date                                        Date32
days_since_ipo_or_first_bar                                Int32

# ===== Sector / industry (from tickers_enriched) =====
sector                                                     Utf8, nullable
industry                                                   Utf8, nullable
sub_industry                                               Utf8, nullable

# ===== Size (requires reference data) =====
market_cap                                                 f64, nullable
market_cap_bucket                                          Utf8   # micro/small/mid/large/mega
market_cap_rank_today                                      Int32, nullable
market_cap_percentile_today                                f64, nullable

# ===== Liquidity (derivable from bars) =====
adv_20d / adv_60d                                          f64 × 2
addv_20d / addv_60d                                        f64 × 2
liquidity_bucket                                           Utf8   # illiquid/thin/normal/liquid/highly_liquid
addv_rank_today                                            Int32
dollar_volume_rank_today                                   Int32

# ===== Price (derivable from bars) =====
prior_close_price                                          f64
price_bucket                                               Utf8   # sub_1 / 1_to_5 / 5_to_20 / 20_to_100 / above_100

# ===== Volatility (derivable from bars) =====
atr_14d                                                    f64
realized_vol_21d                                           f64
yang_zhang_vol_21d                                         f64
volatility_bucket                                          Utf8   # low_vol/normal_vol/high_vol/extreme_vol
volatility_percentile_today                                f64

# ===== Beta / style (derivable from bars + index data) =====
beta_spy_60d / qqq_60d / iwm_60d                           f64 × 3
style_bucket                                               Utf8   # qqq_like/spy_like/smallcap_like/idiosyncratic

# ===== Behavioral / theme tags (rule-based, point-in-time) =====
# Rules in §11.6.3. All bool, point-in-time as of day D.
is_mega_cap_tech                                           Bool
is_large_cap_tech                                          Bool
is_semiconductor                                           Bool
is_biotech                                                 Bool
is_regional_bank                                           Bool
is_energy                                                  Bool
is_china_adr                                               Bool
is_low_float_candidate                                     Bool
is_meme_candidate                                          Bool
is_recent_ipo                                              Bool
```

All fields are `[ML]` safe IFF rolling components honor the
[D-N, D-1] window. Reference-data fields (sector, market_cap)
must use the as-of-D snapshot.

### 11.6.1 Classifications derivable from existing OHLCV bars

These can be computed entirely from `bars_1m_raw/`:

- `prior_close_price`, `price_bucket`
- `adv_*`, `addv_*`, `liquidity_bucket`, `addv_rank_today`,
  `dollar_volume_rank_today`
- `atr_*`, `realized_vol_*`, `yang_zhang_vol_*`,
  `volatility_bucket`, `volatility_percentile_today`
- overnight gap behavior (already in daily_observation)
- `beta_*` (requires SPY/QQQ/IWM bars — already available)
- `style_bucket` (derived from betas)
- cross-sectional ranks within today's universe

### 11.6.2 Classifications requiring external/reference data

These cannot be computed from bars alone:

- `sector`, `industry`, `sub_industry` — partially in
  `tickers_enriched`; sub_industry coverage gaps possible
- `market_cap` — requires shares outstanding × close; shares
  outstanding requires fundamental data ingest (not in Phase 0)
- `float`, `shares_outstanding` — fundamental data ingest
- `short_interest`, `borrow_fee`, `days_to_cover` — bi-monthly
  exchange feeds; not in Phase 0
- `is_leveraged_etf`, `is_inverse_etf` — ETF fact-sheet flags
  (mapping from ticker → leverage factor); partially in
  `tickers_enriched`, gaps likely
- `index_membership` (S&P 500, Russell 2000, etc.) — historical
  index constituents file; not in Phase 0
- IPO date (for `days_since_ipo_or_first_bar` to be accurate vs
  first-bar proxy); fundamental data
- Country / strict ADR classification — `tickers_enriched.locale`
  partially covers

**Rule of the schema:** every reference-data column above is
populated when available, NULL otherwise. NULL is informational —
it tells downstream "we don't know on this date" rather than
producing a fake answer. Phase 0 ships with whatever
`tickers_enriched` carries today; future reference-data ingest
(`float_short_interest.parquet`, `index_membership_daily.parquet`)
fills the gaps without schema change.

### 11.6.3 Behavioral tag rules (v1)

Tags are rule-based, point-in-time. NOT a hardcoded ticker list.
The same security can flip in and out of tags across years — NVDA
in 2016 is not the same as NVDA in 2024.

```
is_mega_cap_tech =
    market_cap_bucket == "mega"
    AND sector IN ("Technology", "Communication Services",
                   "Consumer Discretionary")
    AND addv_20d_rank_today <= 50              # top-50 by dollar volume
    AND beta_qqq_60d > 0.9

is_large_cap_tech =
    market_cap_bucket IN ("large", "mega")
    AND sector IN ("Technology", "Communication Services")
    AND addv_20d_rank_today <= 500

is_semiconductor =
    industry == "Semiconductors" OR sub_industry == "Semiconductors"

is_biotech =
    industry IN ("Biotechnology", "Pharmaceuticals")
    OR sub_industry IN ("Biotechnology", "Pharmaceuticals")

is_regional_bank =
    industry == "Regional Banks"
    OR sub_industry == "Regional Banks"

is_energy =
    sector == "Energy"

is_china_adr =
    is_adr == TRUE
    AND tickers_enriched.country_of_origin IN ("CN", "HK")

is_low_float_candidate =
    EITHER
      (float < 50_000_000 if float available)
      OR (proxy: addv_20d_rank_today > 5000 AND atr_14d / prior_close > 0.05
          AND prior_close > 1.0)
    # When float data lands, the proxy becomes the fallback for
    # missing data only.

is_meme_candidate =
    realized_vol_21d_rank_today >= percentile_95
    AND (intraday_dollar_volume_0930_to_1000_rank_today <= 50
         OR addv_20d_rank_today >= percentile_90 percentile-jump-vs-prior-30d)
    # "Excess volume + excess vol + cross-sectional attention spike."
    # Heuristic — Phase 1 should explicitly test whether this proxy
    # captures the population it claims to.

is_recent_ipo =
    days_since_ipo_or_first_bar < 60
```

Each rule version-tagged in `security_classification_version =
v1`. Bumping a rule = new file version.

**Warning to downstream:** these tags are *heuristic proxies*, not
ground truth. Use them as conditioning variables in Phase 1
descriptive stats and as features in Phase 4 ML. Do not treat them
as eternal correct labels. Specifically, `is_meme_candidate` and
`is_low_float_candidate` will produce false positives — that's why
they're proxies, not definitions.

## 12. Schema — `signal_dataset.parquet` (Phase 3 derivation)

Grain: one row per `(day, security_id, entry_offset)` where the
baseline rule fires.

```
# Identity
signal_id, day, security_id, entry_offset, entry_time_utc
baseline_rule_version

# Signal-time features (allowlist-enforced).
# Pulled from [ML] fields in daily_observation, market_context_daily,
# sector_aggregates_daily, security_classification_daily,
# earnings_calendar, tickers_enriched, regime_definitions,
# AND pre_entry_* fields from forward_outcomes. List columns
# sliced per entry_offset.
# NEW in v6: security_classification_daily columns are first-class
# ML-safe features — sector, industry, market_cap_bucket,
# liquidity_bucket, price_bucket, volatility_bucket, style_bucket,
# all behavioral tags from §11.6.3.

# Labels (generated by versioned recipes — see below)
label_<recipe>_<H>                Bool
labels_recipe_version             Utf8     # in Parquet metadata

# Time-split membership
research_period                   Utf8     # exploration/validation/holdout
```

Versioned label recipes — examples:

```
positive_ret_<H>             ret_<H> > 0
above_p<N>_<H>               ret_<H> > p_N(cohort)
target_before_stop_<X>pct_<H>   first_cross_up_<X>pct_<H> <
                                first_cross_down_<X>pct_<H>
target_before_stop_<X>atr_<H>   ATR variant
top_decile_<H>               ret_<H> top 10% of day-cohort
hit_2atr_within_<H>          first_cross_up_2atr_<H> > 0
hit_3pct_before_minus_1_5pct_EOD   materialized in forward_outcomes
intraday_only_target_<X>pct  hit ±X% within EOD; matched against
                              materialized target_before_stop_<X>pct_EOD
```

Each recipe lives in code with a version stamp. Allowlist-enforced
derivation helper rejects any column not on the allowlist.

## 13. Design rationale — non-obvious choices

### 13.1 Why the forward_outcomes / forward_path_short split

Day-trading research wants 1m-granularity path information.
Day-or-longer momentum research wants daily-or-longer per-horizon
summary stats. The wide format that's ergonomic for the second is
impossible to scale to the first (1B+ columns). The split gives
each query class the data layout it actually wants:

- `forward_outcomes` wide: 13 horizons of per-horizon stats. Join
  pattern: one signal grabs all horizons in one row. Used for
  conditional-distribution analysis and target/stop combinatorics.
- `forward_path_short` long: 23 checkpoints of position-state
  snapshots. Join pattern: one signal grabs a path (filter by
  signal_id, ORDER BY checkpoint). Used for path-shape analysis
  and exit-rule simulation at sub-day resolution.

### 13.2 Why ±0.5% and ±1% thresholds matter

For intraday or 1-day horizons, the v5 minimum of ±5% is way too
coarse — a stock that moves 1.5% intraday is meaningful but invisible
in v5 stats. The added ±0.5%, ±1%, ±2%, ±3% thresholds cover the
short-swing range. Cost: +8 UInt32 per horizon. Worth it.

### 13.3 Why ATR ±0.25 and ±1.5 in addition to v5 set

±0.25 ATR is the natural sub-noise threshold for "did this move at
all relative to its own vol." ±1.5 ATR fills the gap between 1
and 2 ATR where many real-world stops sit. Adds 4 UInt32 per
horizon.

### 13.4 Why next-day-specific outcomes

Overnight gap behavior is structurally different from intraday
continuation. Without `next_day_gap_return`, `next_day_first_5m_return`,
and the gap-vs-RTH decomposition for days 1-5, you cannot
distinguish "intraday momentum that fades overnight and bleeds out
the next morning" from "intraday momentum that gaps up overnight
and continues." Both look identical in `ret_1d`. The decomposition
is one of the highest-information additions in v6.

### 13.5 Why time-underwater / time-profitable

Two trades with identical `ret_5d = +3%` may have spent very
different times underwater. One was flat for 4 days then popped 3%
at the end; the other shot up 3% in the first hour and held. They
have different psychological and execution profiles — a real
broker might exit one and not the other. `pct_bars_underwater` +
`max_consecutive_bars_underwater` + `time_to_recover` characterize
this directly.

### 13.6 Why materialized target-before-stop labels

`hit_X_before_stop_Y` is derivable from `first_cross_up_X` <
`first_cross_down_Y`, but Phase 1 will query this constantly across
many (X, Y) combinations. Materializing the common cases
(`hit_1pct_before_minus_1pct_EOD`, etc.) makes those queries one
column-read instead of a comparison. Cost is small (~9 bool
nullable per row); ergonomics win is large.

### 13.7 Why security_classification_daily

The universe-relaxation amendment means the relaxed universe pools
mega-cap tech, leveraged ETFs, biotech runners, ADRs, SPACs,
warrants, post-bankruptcy artifacts. Without per-day classification,
Phase 1 can "discover" momentum that is actually "leveraged ETFs
behave differently from common stocks" or "low-float biotechs
mean-revert harder than industrials." The classification table is
the structural defense.

The behavioral tags (is_mega_cap_tech, is_biotech, is_meme_candidate)
are point-in-time *rules*, not hardcoded ticker lists. NVDA in
2016 is not the same security profile as NVDA in 2024. Hardcoding
would silently encode the analyst's 2026 priors into 2017 data.

### 13.8 Why behavioral tags ship as "heuristic proxies"

Some tags (is_meme_candidate, is_low_float_candidate without float
data) are proxies that will produce false positives. Documenting
them as proxies, not ground truth, means Phase 1 will explicitly
validate the proxies (e.g. "does is_meme_candidate actually capture
GameStop-like behavior in 2021?") before using them as ML features.

### 13.9 Why this is a 450-650 GB output

Storage is the price of the project's stated goal: "characterize
the conditional return distribution under every regime the data
supports" (§0). Per-horizon per-threshold combinatorics, dense
short-term path resolution, day-0 segment outcomes,
classification — all are load-bearing for "where does the edge
survive costs, in which regime, at what confidence." None can be
recomputed cheaply downstream.

The output is not meant to be redistributed or live in cloud
storage. It lives on the research machine. ~500 GB of NVMe is
$50 and the cheapest line item in the whole project.

## 14. Statistical methodology rules

### 14.1 Time-based splits only

Default:
- 2016-06-08 → 2019-12-31 — exploration
- 2020-01-01 → 2022-06-30 — validation
- 2022-07-01 → 2026-06-05 — holdout

Random splits leak future regimes.

### 14.2 Cluster-robust SE by day

Rows are NOT independent. Day-clustered SEs or bootstrap-by-day
required for all CIs.

### 14.3 Multiple-comparison discipline

Pre-register before holdout. FDR control on bucket sweeps. OOS
confirmation required for any discovered edge.

### 14.4 Mandatory conditioning on security classification (v6)

Phase 1 reports MUST slice by at least:
- `ticker_type`
- `sector` (where available)
- `industry` (where available)
- `market_cap_bucket`
- `price_bucket`
- `liquidity_bucket`
- `volatility_bucket`
- `style_bucket`
- `earnings_status` (is_earnings_day / days_to_next_known_earnings
  bucketed)
- `premarket_activity_bucket` (e.g., percentile bins of
  premarket_dollar_volume_rank_today)

**Warning, top of every Phase 1 report:**

> Do not let low-float biotech gappers, leveraged ETFs, ADRs,
> SPACs, warrants, and mega-cap technology stocks sit in the same
> undifferentiated bucket. Apparent momentum edges may be artifacts
> of mixing different security types.

This warning is binding. A Phase 1 report that pools across
classification without disclosing the mix is rejected, full stop.

### 14.5 Phase 4 meta-labeling discipline

- ML is a filter on the Phase 2 baseline, NOT a price predictor.
- Labels are binary classification.
- CV is walk-forward, time-based, day-clustered folds.
- Evaluation: precision/recall, cost-adjusted return, drawdown of
  meta-filtered vs unfiltered baseline. NOT accuracy.
- Feature importance is diagnostic, not a finding.
- Classification fields are first-class features; sector / industry
  / market_cap_bucket / behavioral_tag interactions are expected
  to carry signal.

## 15. Reporting contract per analysis cell

```
n, n_clusters_by_day
mean, median, std
day-clustered SE
95% CI (cluster-robust)
win rate, tail loss (5th %ile)
cost-adjusted return at conservative slippage
turnover / capacity
classification mix (top-3 ticker_types, top-3 sectors, top-3
                    market_cap_buckets in the cell)
```

The last line is new in v6 and serves the §14.4 warning at the
report level: every cell must disclose its security-type mix.

## 16. Spec contract preservation

`per_trade_summary` + `per_trade_bar_path` fall out as ~30-line
Polars/DuckDB derivation. Audit ("where did stop fire on day +47?")
answered by re-querying `bars_1m_raw/` for the specific (security,
range).

## 17. Implementation order

**Phase 0-A — Prototype (4-6 days).** 3-month slice, full
eight-table schema. Benchmark nested-list queries AND
long-format checkpoint queries. Confirm compression assumptions
for the bigger tables. Synthetic leakage tests pass. The prototype
must include `forward_path_short` and `security_classification_daily`
so their ergonomics are validated before the full build.

**Phase 0-B — Earnings ingest (1-2 days, parallel).** Same as v5.

**Phase 0-C — Classification ingest (1-2 days, parallel).** Pull
`sub_industry`, `country_of_origin`, leveraged/inverse ETF flags
from Massive ticker details endpoint. Stub the columns we can't
fill (market_cap, float) as NULL until fundamental data lands.

**Phase 0-D — Full build (3-4 weeks).** All eight tables across
~2,500 days. Sliding-window memory pattern. Sort/partition by
`(day, security_id)` (or `(day, security_id, entry_offset,
path_checkpoint)` for forward_path_short). Bumped from v5's
2-3 weeks to account for the larger output volume and the
classification computation.

**Phase 0-E — Golden tests (a few days).** Frozen-decisions tests
+ point-in-time leakage + earnings revision visibility +
research/ML separation enforcement + classification stability
(same security on same date returns the same tags across runs).

**Phase 0-F — Trade-table derivation (a day).** Spec compatibility.

**Phase 1 — Statistical research (weeks).** §14.4 conditioning is
mandatory. Reports include classification mix per cell.

**Phase 2 — Baseline strategy (days).** Rule-based, positive
expectancy. Conditioned on classification.

**Phase 3 — Signal dataset build (days).** Classification fields
are first-class ML features; allowlist updated accordingly.

**Phase 4 — Meta-labeling (weeks).** Logistic → XGBoost/LightGBM →
ensembles.

## 18. Implementation notes

### 18.1 Sliding-window memory

253 days resident. Two writers (observation lag 0, outcomes lag
252). Batched `scan_day(day)`.

### 18.2 Cross-sectional batching

Rank features, breadth, sector aggregates, market_cap_rank,
volatility_percentile require seeing the whole day's universe
before emitting any row.

### 18.3 Classification computation

`security_classification_daily.parquet` is written in the same
pass as `daily_observation`. Behavioral-tag rules (§11.6.3) are a
single shared function with version stamping. Rule logic lives in
`crates/momentum-classify`; a CI test snapshots the v1 rules and
fails on accidental edits without version bump.

### 18.4 forward_path_short writer

Long-format means many more rows per (day, sid, entry_offset).
Memory pattern: complete all checkpoints for an outcome row before
emitting; never hold partial paths across day boundaries.
Row-group size tuned for the long-format shape (~1M rows per
group, smaller than the wide-format groups).

### 18.5 Point-in-time enforcement

Shared rolling-window helper. Hard unit test with sentinel
injection. Classification tags subject to the same rule — no
2026 sector mapping used for a 2017 row.

### 18.6 Signal_dataset derivation helper (Phase 3)

`extract_ml_features(...)` allowlist now includes
security_classification fields. CI test fails the build on
unallowed columns.

### 18.7 Versioned-metadata writes

Every Phase 0 fact table carries file-level Parquet metadata for
every applicable version key from §0.6. Downstream readers refuse
joins across mismatched versions.

## 19. Open scope decisions (v6 defaults; user may override)

1. **Entry-offset grid v1:** 17 offsets.
2. **Forward-horizons v2 (wide outcomes):** 13 horizons (added 2d,
   3d, 10d, 42d vs v5; sub-hour resolution moved to
   forward_path_short).
3. **Forward-path checkpoints v1:** 23 checkpoints (full). v1-lite
   fallback (15 checkpoints) available if storage matters.
4. **Fixed-% thresholds v2:** ±0.5/1/2/3/5/10/20 (added the
   sub-5% range).
5. **ATR-normalized thresholds v2:** ±0.25/0.5/1/1.5/2/3 (added
   ±0.25 and ±1.5 vs v5).
6. **Regime taxonomies v1:** era + spy_trend + vix_level + breadth
   + liquidity. Unchanged.
7. **Security-classification v1 rules:** §11.6.3.
8. **Materialized target-before-stop labels:** the ~9 listed in
   §9. Extend if Phase 1 asks for specific (X, Y) pairs we missed.
9. **Baseline rule for Phase 2:** TBD per Phase 1 results.
10. **Label recipes v1 for Phase 3:** TBD; minimum
    `positive_ret_<H>` + `target_before_stop_<X>atr_<H>` for at
    least H ∈ {EOD, 1d, 5d, 21d, 63d}.
11. **Future tables:** `catalyst_events`, `float_short_interest`,
    `index_membership_daily`, `execution_cost_estimates`. Joins
    reserved; ingest deferred.

## 20. What the reviewer should evaluate

1. **Is the forward_outcomes/forward_path_short split the right
   architectural cut**, or should the path data also live wide
   alongside the outcomes? Counter-arguments welcome.

2. **Are the v6 horizon and threshold sets dense enough** for
   day-trading research while still tractable? Specifically the
   13-horizon forward_outcomes set excludes 5m/15m/45m/90m/etc. —
   they live only in forward_path_short. Is that the right cut?

3. **forward_path_short v1 vs v1-lite checkpoint set.** 23 vs 15.
   Difference is ~70-100 GB. v1 captures sub-5-minute resolution
   day-trading shape; v1-lite drops it. Which is correct as the
   default?

4. **Day-0 post-entry session-segment outcomes.** Useful? Or are
   they fully recoverable from the path-short table at lower
   storage cost?

5. **Time-underwater / time-profitable fields.** Recorded only for
   H ∈ {EOD, 1d, 2d, 3d, 5d}. Should we extend to longer horizons
   (21d, 63d, 252d)?

6. **Gap-vs-RTH decomposition for days 1-5.** Why not 1-10? 1-21?
   Cost is +~80 bytes per row per added day.

7. **Security-classification rules (§11.6.3).** Are the rules
   right? Specifically:
   - `is_mega_cap_tech` requires sector AND mega-cap AND
     top-50 dollar volume AND beta_qqq > 0.9. Is the AND chain
     too restrictive? Too lax?
   - `is_meme_candidate` is a proxy. Will it produce too many
     false positives to be useful?
   - `is_low_float_candidate` proxy (no float data) — is the
     fallback proxy sane?

8. **Which classification fields should be denormalized into
   daily_observation** instead of (or in addition to) living in
   security_classification_daily? The current design keeps them
   separate to avoid bloat; the cost is one join per query. Is
   the join cost worth the separation?

9. **Classification fields as Phase 4 ML features.** First-class.
   Are there interaction-encoding considerations
   (one-hot, target encoding, etc.) the Phase 0 schema should
   anticipate?

10. **Storage budget (450-650 GB) acceptable**, or do we need to
    drop something? Most-droppable: forward_path_short v1 → v1-lite
    (saves ~100 GB); drop a few horizons from forward_outcomes;
    drop next_day_first_5m / first_15m / first_30m subfields.

11. **The §14.4 mandatory-conditioning warning** is operationally
    binding. Is the warning enforceable, or just a documentation
    artifact?

12. **Anything else** that the day-trading / short-swing /
    meta-labeling research will wish we had recorded?

---

## Appendix A — Frozen constraints still in force

- Identity = `security_id` (composite FIGI).
- Split adjustment at read time; pinned to `splits.parquet`.
- Dividends NOT folded into bars; stored separately.
- Per-day storage shape (`bars_1m_raw/YYYY-MM-DD.parquet`).
- Half-day sessions calendar-driven.
- Bankruptcy vs acquisition distinct in `terminal_event_type`.
- `data/_staging/flat_files/` read-only.

## Appendix B — Build state

- ~2,513 days ingested (2016-06-08 → 2026-06-05).
- Reference data present and snapshot-stamped.
- 51 unit tests passing.
- Schema in this RFC = next thing to build.

## Appendix C — Changelog

### v1 → v2 (reviewers 1+2)
Initial reviewer feedback — `bars_to_max_*`, pre-market,
unadjusted prices, ATR/ADV lookbacks, 1-min first hour, entry
liquidity proxies, QQQ+IWM, §0 deliverable framing.

### v2 → v3 (reviewer 3)
Naming discipline (`intraday_*` vs `eod_*`), market context split
out, regime in own table, classification binding, materialized
signal snapshots + shape descriptors, close-based stats + threshold
crossings + excess returns + betas, terminal event enum, data
quality, methodology rules, earnings ingest, prototype-first
order.

### v3 → v4 (user: meta-labeling arc)
§0.5 Phase 0→4 arc, §6.5 research/ML separation, list columns
tagged [SRC], signal_dataset.parquet, first-30m shape, per-column
[ML]/[RES]/[SRC] annotations, meta-labeling discipline,
allowlist-enforced derivation.

### v4 → v5 (reviewer 4: hardcoded strategy choices)
§0.6 versioned defaults, Yang-Zhang vol, vol-normalized threshold
crossings, cross-sectional ranks, sector_aggregates table, market
breadth, multi-taxonomy regimes, parametric pre-entry features,
extra horizons (10d, 42d), versioned label recipes, future-table
placeholders. Pushed back: long-format outcomes, fully replacing
fixed-% thresholds, full externalization of grids, 2d/126d horizons.

### v5 → v6 (user: day-trading + classification)

**Forward-outcomes denser short-term + new dimensions:**
- 2d, 3d horizons added (v5's 2d-skip reversed; 3d new).
- Sub-5% threshold range added: ±0.5%, ±1%, ±2%, ±3%.
- ATR ±0.25 and ±1.5 added.
- Day-0 post-entry session-segment returns (`ret_to_1030`...
  `ret_to_close`).
- Post-entry session-shape (morning/midday/afternoon/power hour
  high-low returns).
- Time-underwater / time-profitable per horizon for short windows.
- Next-day-specific outcomes (gap return, first-5m/15m/30m,
  fade/continuation).
- Gap-vs-RTH decomposition for days 1-5 (overnight vs intraday
  driver of multi-day returns).
- Materialized target-before-stop labels for common (X, Y) pairs.
- Entry-quality / fill-realism proxies
  (price_location_in_1m_bar, wicks, slippage_proxy_bps, capacity).

**New table — `forward_path_short.parquet`** (long format):
- 23 path checkpoints from 1m through 5d_close.
- Per-checkpoint position state (ret, high/low_ret_so_far,
  close_max/min_ret_so_far, volume, vwap, bars_elapsed,
  pct_bars_profitable_so_far).
- Storage-constrained v1-lite alternative (15 checkpoints).

**New table — `security_classification_daily.parquet`:**
- Per-day, per-security tags. Point-in-time.
- Security structure (is_etf, is_leveraged_etf, is_adr, ...).
- Size / liquidity / price / volatility / beta buckets, all
  computed from data we have.
- Behavioral tags (is_mega_cap_tech, is_biotech,
  is_low_float_candidate, is_meme_candidate, ...) as
  point-in-time *rules*, not hardcoded ticker lists.
- Subsections distinguishing bars-derivable from
  reference-data-dependent fields.

**Methodology updates:**
- §14.4 mandatory conditioning on security_classification before
  interpreting any momentum edge.
- §15 reporting contract requires classification mix per cell.
- Top-of-Phase-1-report warning against pooling biotech gappers,
  leveraged ETFs, ADRs, SPACs, mega-cap tech in the same bucket.

**Versioning:**
- `forward_horizons_version = v2` (denser short-term).
- `pct_threshold_version = v2` (added sub-5%).
- `atr_threshold_version = v2` (added 0.25, 1.5).
- `forward_path_checkpoints_version = v1`.
- `security_classification_version = v1`.

**Storage:**
- Output budget jumps from ~120-180 GB (v5) to ~450-650 GB (v6).
  Driver: forward_outcomes denser threshold combinatorics,
  forward_path_short long-format size, classification cheap.
- Documented as deliberate per §0 — short-swing edge
  characterization requires this resolution. ~500 GB of NVMe is
  cheap; under-characterized research is expensive.

**Future-table additions:** `index_membership_daily.parquet`
contemplated (joins reserved).
