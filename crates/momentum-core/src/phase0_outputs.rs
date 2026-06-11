//! Phase 0 observation-pivot output schemas (RFC v6 + the v2 table
//! amendments from `phase1-research-strategy.md` §3.7, 2026-06-11).
//!
//! Eight Parquet tables; one `SchemaRef` per table behind a `OnceLock`. The
//! RFC §0.6 versioned-metadata keys are exported as `pub const` so writers
//! stamp the same string into every file's file-level Parquet metadata.
//!
//! v2 delta (§3.7): `daily_observation` +14, `forward_outcomes` +42
//! (615 → 657), `forward_path_short` +4 (15 → 19), `market_context_daily`
//! +6; per-table version stamps + `signal_definition` stamp; terminal-event
//! and first-event value enums; regime-threshold metadata stamps.
//!
//! ## What's here
//!
//! | Schema fn                          | Grain                                              | RFC §  |
//! |------------------------------------|----------------------------------------------------|--------|
//! | `daily_observation_schema`         | (day, security_id)                                 | §7     |
//! | `market_context_daily_schema`      | (day)                                              | §8     |
//! | `forward_outcomes_schema`          | (day, security_id, entry_offset)                   | §9     |
//! | `forward_path_short_schema`        | (day, security_id, entry_offset, path_checkpoint)  | §9.5   |
//! | `regime_definitions_schema`        | (day, taxonomy, regime_id)                         | §10    |
//! | `earnings_calendar_schema`         | (security_id, earnings_date)                       | §11    |
//! | `sector_aggregates_daily_schema`   | (day, sector_id)                                   | §11.5  |
//! | `security_classification_daily_schema` | (day, security_id)                             | §11.6  |
//!
//! ## Parameter grids (mirror RFC §0.6 / §9 / §9.5)
//!
//! - **Entry offsets v1** (17): see `ENTRY_OFFSETS_V1`.
//! - **Forward horizons v2 wide-outcomes** (13): see `FORWARD_HORIZONS_V2`.
//! - **Path checkpoints v1** (23): see `PATH_CHECKPOINTS_V1`.
//! - **Fixed-% thresholds v2** (7): see `PCT_THRESHOLDS_V2`.
//! - **ATR-multiple thresholds v2** (6): see `ATR_THRESHOLDS_V2`.
//!
//! These grids are NOT externalized at runtime — they shape the column set.
//! Bumping any of them = bumping the matching `*_VERSION` constant +
//! regenerating the output file.
//!
//! ## Annotations (RFC §6.2, §7)
//!
//! Each column in `daily_observation` is one of:
//! - **bare** — point-in-time over `[D-N, D-1]` (always ML-safe)
//! - **`intraday_*`** — derived from day-D bars; per-window-end safety
//! - **`eod_*`** — full-session derivative; NEVER an ML feature
//!
//! `forward_outcomes` fields are all `[RES]` or `[LABEL]` — they become
//! labels in the ML dataset, never features.

// The schemas below build their field vectors via repeated `.push()` calls
// inside loops. Clippy's `vec_init_then_push` lint would prefer a `vec![...]`
// literal, but with ~600 conditionally-generated fields per schema that
// pattern is strictly worse — it forces a giant literal that hides the
// per-section structure that makes the RFC mapping reviewable.
#![allow(clippy::vec_init_then_push)]

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, OnceLock};

use crate::schema::{dict_str, utc_ns};

// =============================================================================
// Versioned-metadata keys + version values (RFC §0.6).
//
// Writers must stamp `<key> = <version>` into each table's file-level Parquet
// metadata. Readers refuse joins across mismatched versions.
// =============================================================================

pub const ENTRY_OFFSET_GRID_VERSION_KEY: &str = "entry_offset_grid_version";
pub const ENTRY_OFFSET_GRID_VERSION: &str = "v1";

pub const FORWARD_HORIZONS_VERSION_KEY: &str = "forward_horizons_version";
pub const FORWARD_HORIZONS_VERSION: &str = "v2";

/// v2 per strategy-doc §3.7: the checkpoint SET stays the full 23 (§3.6
/// decision); only the schema version bumps (+4 path-state columns).
pub const FORWARD_PATH_CHECKPOINTS_VERSION_KEY: &str = "forward_path_checkpoints_version";
pub const FORWARD_PATH_CHECKPOINTS_VERSION: &str = "v2";

pub const PCT_THRESHOLD_VERSION_KEY: &str = "pct_threshold_version";
pub const PCT_THRESHOLD_VERSION: &str = "v2";

pub const ATR_THRESHOLD_VERSION_KEY: &str = "atr_threshold_version";
pub const ATR_THRESHOLD_VERSION: &str = "v2";

pub const REGIME_TAXONOMY_VERSION_KEY: &str = "regime_taxonomy_version";
pub const REGIME_TAXONOMY_VERSION: &str = "v1";

pub const SECURITY_CLASSIFICATION_VERSION_KEY: &str = "security_classification_version";
pub const SECURITY_CLASSIFICATION_VERSION: &str = "v1";

// ---------------------------------------------------------------------------
// Per-table schema versions (strategy doc §3.7, amendment 2026-06-11).
// v2 = the §3.1–§3.4 + §3.6 column additions. Readers refuse joins across
// mismatched table versions, same contract as the grid versions above.
// ---------------------------------------------------------------------------

pub const DAILY_OBSERVATION_VERSION_KEY: &str = "daily_observation_version";
pub const DAILY_OBSERVATION_VERSION: &str = "v2";

pub const FORWARD_OUTCOMES_VERSION_KEY: &str = "forward_outcomes_version";
pub const FORWARD_OUTCOMES_VERSION: &str = "v2";

pub const MARKET_CONTEXT_DAILY_VERSION_KEY: &str = "market_context_daily_version";
pub const MARKET_CONTEXT_DAILY_VERSION: &str = "v2";

// ---------------------------------------------------------------------------
// Default-signal definition stamp (strategy doc §3.7).
//
// The `signal_first_in_*` / `signal_concentration_*` columns in
// daily_observation embed this definition; writers stamp the literal
// expression + its version into the table's file-level metadata so the
// columns are interpretable without this source file.
// ---------------------------------------------------------------------------

pub const SIGNAL_DEFINITION_KEY: &str = "signal_definition";
pub const SIGNAL_DEFINITION_V1: &str = "intraday_ret_0930_to_1000 > 0";
pub const SIGNAL_DEFINITION_VERSION_KEY: &str = "signal_definition_version";
pub const SIGNAL_DEFINITION_VERSION: &str = "v1";

// ---------------------------------------------------------------------------
// Regime-threshold metadata stamps (strategy doc §3.6/§3.7).
//
// regime_definitions stays v1 (no new columns), but writers MUST stamp, per
// taxonomy, the bucket boundary values and the window they were computed on,
// so every regime assignment is reproducible. Key shape:
//   regime_thresholds_<taxonomy>        = "<boundary values>"
//   regime_thresholds_window            = "<start>..<end>"
// ---------------------------------------------------------------------------

pub const REGIME_THRESHOLDS_KEY_PREFIX: &str = "regime_thresholds_";
pub const REGIME_THRESHOLDS_WINDOW_KEY: &str = "regime_thresholds_window";

/// File-metadata key carrying the bucket boundary values for one taxonomy,
/// e.g. `regime_thresholds_vix_level`.
pub fn regime_thresholds_key(taxonomy: &str) -> String {
    format!("{REGIME_THRESHOLDS_KEY_PREFIX}{taxonomy}")
}

// =============================================================================
// Parameter grids (RFC §0.6, §9, §9.5).
// =============================================================================

/// Entry-offset grid v1 (RFC §9). 17 offsets; dense early, sparse late.
/// Encoded as `HHMM` strings (e.g. `"0935"`, `"1530"`) so the dictionary
/// column compresses tightly and joins stay string-typed.
pub const ENTRY_OFFSETS_V1: &[&str] = &[
    "0935", "0940", "0945", "0950", "0955",
    "1000", "1005", "1010", "1015", "1020",
    "1030", "1045", "1100", "1130", "1200", "1300", "1530",
];

/// Forward-horizons v2 (RFC §9). 13 horizons.
/// String labels used in column names: `ret_<H>`, `max_drawdown_<H>`, etc.
pub const FORWARD_HORIZONS_V2: &[&str] = &[
    "10min", "30min", "60min", "EOD",
    "1d", "2d", "3d", "5d", "10d", "21d", "42d", "63d", "252d",
];

/// Path-checkpoints v1 (RFC §9.5). 23 checkpoints; long-format companion
/// to `forward_outcomes`. Drops to 15 under the v1-lite fallback (storage
/// constrained); v1 captures sub-5-minute resolution day-trading shape.
pub const PATH_CHECKPOINTS_V1: &[&str] = &[
    "1m", "2m", "3m", "5m", "10m", "15m", "20m", "30m", "45m", "60m",
    "90m", "120m", "180m", "EOD",
    "1d_open", "1d_30m", "1d_close",
    "2d_open", "2d_close",
    "3d_open", "3d_close",
    "5d_open", "5d_close",
];

/// Fixed-% thresholds v2 (RFC §9, §13.2). 7 thresholds; covers short-swing
/// range that v1's ±5% minimum couldn't reach.
/// Column-name suffix style: `0_5`, `1`, `2`, `3`, `5`, `10`, `20`.
pub const PCT_THRESHOLDS_V2: &[&str] = &["0_5", "1", "2", "3", "5", "10", "20"];

/// ATR-multiple thresholds v2 (RFC §9, §13.3). 6 thresholds.
/// Column-name suffix style: `0_25`, `0_5`, `1`, `1_5`, `2`, `3`.
pub const ATR_THRESHOLDS_V2: &[&str] = &["0_25", "0_5", "1", "1_5", "2", "3"];

/// Multi-day suffix of `FORWARD_HORIZONS_V2` (9 horizons). Drives the
/// dividend-adjusted `ret_<H>_total` + `dividend_ex_date_within_<H>` columns
/// (strategy doc §3.3/§3.7) — intraday horizons are unaffected by dividends.
/// Invariant (tested): this is exactly the tail of `FORWARD_HORIZONS_V2`.
pub const MULTIDAY_HORIZONS_V2: &[&str] =
    &["1d", "2d", "3d", "5d", "10d", "21d", "42d", "63d", "252d"];

/// The 9 frozen target-before-stop label pairs (RFC §9 v6). Single source
/// for BOTH column families so they can never drift apart:
///   `hit_<pair>`          — Boolean materialized label (v6)
///   `first_event_<pair>`  — dict_str first-event resolution (v2, §3.7;
///                           exact at 1m resolution, §3.2 refinement)
pub const TARGET_STOP_PAIRS_V6: &[&str] = &[
    "0_5pct_before_minus_0_5pct_30min",
    "1pct_before_minus_1pct_EOD",
    "2pct_before_minus_1pct_EOD",
    "3pct_before_minus_1_5pct_EOD",
    "2pct_before_minus_2pct_1d",
    "3pct_before_minus_3pct_5d",
    "1atr_before_minus_0_5atr_EOD",
    "2atr_before_minus_1atr_5d",
    "3atr_before_minus_1_5atr_21d",
];

/// Values of `first_event_<pair>` (strategy doc §3.2 refinement / §3.7).
/// The trade-ending event is a property of a (trade, threshold-pair) — the
/// engine resolves target-vs-stop ordering exactly from 1-minute bars.
pub const FIRST_EVENT_VALUES_V1: &[&str] =
    &["target_first", "stop_first", "neither", "no_data"];

/// Values of `terminal_event_type` in `forward_outcomes` (strategy doc §3.6
/// decision, 2026-06-11). Merger vs bankruptcy follows the merger-vs-delist
/// split frozen in Phase 0; ambiguity is carried by the existing
/// `terminal_event_confidence` column, never by extra enum values.
///
/// NOT the `enums.rs` types — those describe the derived-layer
/// per-trade tables (see the warning at the top of `enums.rs`).
pub const TERMINAL_EVENT_TYPES_V1: &[&str] = &[
    "none",
    "delisted_merger_acquisition",
    "delisted_bankruptcy_liquidation",
    "delisted_exchange_compliance",
    "delisted_unknown",
    "extended_halt_no_bars",
];

// -----------------------------------------------------------------------------
// Local helpers
// -----------------------------------------------------------------------------

fn f(name: &str, dt: DataType, nullable: bool) -> Field {
    Field::new(name, dt, nullable)
}

/// Struct<t: ts[ns,UTC], open: f64, high: f64, low: f64, close: f64, volume: f64>
/// — the canonical intraday-bar struct used inside list columns.
fn bar_struct(extra_volume_dtype: DataType) -> DataType {
    DataType::Struct(
        vec![
            f("t", utc_ns(), false),
            f("open", DataType::Float64, false),
            f("high", DataType::Float64, false),
            f("low", DataType::Float64, false),
            f("close", DataType::Float64, false),
            f("volume", extra_volume_dtype, false),
        ]
        .into(),
    )
}

fn list_of_bars() -> DataType {
    DataType::List(Arc::new(Field::new("item", bar_struct(DataType::Float64), false)))
}

// =============================================================================
// 1. daily_observation.parquet (RFC §7)
// =============================================================================

/// `daily_observation.parquet` — one row per `(day, security_id)`.
/// ~50–85 GB across ~15M rows. RFC §7.
///
/// Bare names = point-in-time over `[D-N, D-1]`. `intraday_*` = day-D bars
/// (per-window-end safe). `eod_*` = full-session; NEVER an ML feature.
pub fn daily_observation_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let mut fields: Vec<Field> = Vec::new();

            // Identity
            fields.push(f("day", DataType::Date32, false));
            fields.push(f("security_id", DataType::Utf8, false));
            fields.push(f("display_symbol_on_day", DataType::Utf8, false));

            // Adjusted intraday paths (raw source — slice at signal_dataset time)
            fields.push(f("intraday_1m_first_hour", list_of_bars(), false));
            fields.push(f("intraday_10m_rest", list_of_bars(), false));

            // Materialized signal snapshots
            for end in ["0940", "0950", "1000", "1010", "1030"] {
                fields.push(f(&format!("intraday_ret_0930_to_{end}"), DataType::Float64, true));
            }
            fields.push(f("intraday_volume_0930_to_1000", DataType::Float64, true));
            fields.push(f("intraday_dollar_volume_0930_to_1000", DataType::Float64, true));
            fields.push(f("intraday_vwap_0930_to_1000", DataType::Float64, true));

            // First-30m shape
            fields.push(f("intraday_first_30m_high_return", DataType::Float64, true));
            fields.push(f("intraday_first_30m_low_return", DataType::Float64, true));
            fields.push(f("intraday_time_of_first_30m_high", DataType::Utf8, true));
            fields.push(f("intraday_time_of_first_30m_low", DataType::Utf8, true));
            fields.push(f("intraday_ret_from_first_30m_high_to_1000", DataType::Float64, true));
            fields.push(f("intraday_minutes_since_first_30m_high_at_1000", DataType::Int32, true));
            fields.push(f("intraday_first_15m_volume_share_of_first_30m", DataType::Float64, true));

            // First-hour shape
            fields.push(f("intraday_first_hour_high_return", DataType::Float64, true));
            fields.push(f("intraday_first_hour_low_return", DataType::Float64, true));
            fields.push(f("intraday_time_of_first_hour_high", DataType::Utf8, true));
            fields.push(f("intraday_ret_from_first_hour_high_to_1030", DataType::Float64, true));
            fields.push(f("intraday_first_30m_volume_share_of_first_hour", DataType::Float64, true));

            // Cross-sectional ranks at snapshot times
            fields.push(f("intraday_ret_0930_to_1000_rank_today", DataType::Int32, true));
            fields.push(f("intraday_ret_0930_to_1000_percentile_today", DataType::Float64, true));
            fields.push(f("intraday_dollar_volume_0930_to_1000_rank_today", DataType::Int32, true));
            fields.push(f("premarket_volume_rank_today", DataType::Int32, true));
            fields.push(f("premarket_dollar_volume_rank_today", DataType::Int32, true));
            fields.push(f("overnight_gap_rank_today", DataType::Int32, true));
            fields.push(f("addv_20d_rank_today", DataType::Int32, true));
            fields.push(f("realized_vol_21d_rank_today", DataType::Int32, true));

            // EOD aggregates (RES only)
            for name in ["open", "high", "low", "close", "volume", "dollar_volume", "vwap"] {
                fields.push(f(&format!("eod_day_{name}"), DataType::Float64, true));
            }
            for name in ["open", "high", "low", "close"] {
                fields.push(f(&format!("eod_unadjusted_day_{name}"), DataType::Float64, true));
            }

            // Pre-market (04:00–09:30 ET)
            for name in ["volume", "high", "low", "vwap", "dollar_volume"] {
                fields.push(f(&format!("premarket_{name}"), DataType::Float64, true));
            }

            // Overnight context
            fields.push(f("prior_day_eod_close", DataType::Float64, true));
            fields.push(f("prior_day_unadjusted_eod_close", DataType::Float64, true));
            fields.push(f("overnight_gap", DataType::Float64, true));
            fields.push(f("prior_day_last_30m_return", DataType::Float64, true));
            fields.push(f("prior_day_last_30m_volume_share", DataType::Float64, true));

            // Rolling volatility
            for win in ["5d", "14d", "42d"] {
                fields.push(f(&format!("atr_{win}"), DataType::Float64, true));
            }
            fields.push(f("realized_vol_21d", DataType::Float64, true));
            for win in ["5d", "14d", "21d", "42d"] {
                fields.push(f(&format!("yang_zhang_vol_{win}"), DataType::Float64, true));
            }

            // Rolling liquidity
            for win in ["5d", "20d", "60d"] {
                fields.push(f(&format!("adv_{win}"), DataType::Float64, true));
            }
            for win in ["5d", "20d", "60d"] {
                fields.push(f(&format!("addv_{win}"), DataType::Float64, true));
            }

            // Rolling betas
            for idx in ["spy", "qqq", "iwm"] {
                fields.push(f(&format!("beta_{idx}_60d"), DataType::Float64, true));
            }

            // Earnings proximity
            fields.push(f("days_to_next_known_earnings", DataType::Int32, true));
            fields.push(f("days_since_last_earnings", DataType::Int32, true));
            fields.push(f("is_earnings_day", DataType::Boolean, true));
            fields.push(f("earnings_report_timing", dict_str(), true));

            // Data quality
            fields.push(f("bar_count_rth", DataType::Int32, true));
            fields.push(f("bar_count_first_hour", DataType::Int32, true));
            fields.push(f("bar_count_first_30m", DataType::Int32, true));
            fields.push(f("bar_count_premarket", DataType::Int32, true));
            fields.push(f("missing_1m_bars_first_30m", DataType::Int32, true));
            fields.push(f("missing_1m_bars_first_hour", DataType::Int32, true));
            fields.push(f("zero_volume_1m_bars_first_30m", DataType::Int32, true));
            fields.push(f("first_rth_bar_time", DataType::Utf8, true));
            fields.push(f("minutes_after_open_first_bar", DataType::Int32, true));
            fields.push(f("has_bad_ohlc", DataType::Boolean, true));
            fields.push(f("split_event_nearby", DataType::Boolean, true));
            fields.push(f("dividend_event_today", DataType::Boolean, true));
            fields.push(f("ticker_event_today", DataType::Boolean, true));
            fields.push(f("adjustment_factor_on_day", DataType::Float64, true));

            // v2 additions (strategy doc §3.7, +14) ------------------------
            // Signal freshness / concentration. These embed the default
            // signal definition — writers stamp SIGNAL_DEFINITION_KEY into
            // file metadata alongside the schema version.
            for n in ["5d", "10d", "20d"] {
                fields.push(f(&format!("signal_first_in_{n}"), DataType::Boolean, true));
            }
            fields.push(f("signal_concentration_percentile_today", DataType::Float64, true));
            fields.push(f("signal_concentration_hhi_today", DataType::Float64, true));
            // Premarket volume spike: record the continuous parent, derive
            // the flag (> 5) at query time too if a different cut is wanted.
            fields.push(f("premarket_volume_vs_20d_median", DataType::Float64, true));
            fields.push(f("pre_market_volume_spike_flag", DataType::Boolean, true));
            // 52w range: record the parents; any ratio is query-time.
            fields.push(f("high_52w", DataType::Float64, true));
            fields.push(f("low_52w", DataType::Float64, true));
            for x in ["5", "10", "20"] {
                fields.push(f(&format!("days_since_last_{x}pct_move"), DataType::Int32, true));
            }
            fields.push(f("consecutive_up_days_close_to_close", DataType::Int32, true));
            fields.push(f("gap_filled_today_flag", DataType::Boolean, true));

            // Calendar / static
            fields.push(f("day_of_week", DataType::Int32, false));
            fields.push(f("days_since_first_bar", DataType::Int32, false));
            fields.push(f("is_half_day", DataType::Boolean, false));

            Arc::new(Schema::new(fields))
        })
        .clone()
}

// =============================================================================
// 2. market_context_daily.parquet (RFC §8)
// =============================================================================

pub fn market_context_daily_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let mut fields: Vec<Field> = Vec::new();
            fields.push(f("day", DataType::Date32, false));

            // Index intraday paths (10-min)
            for idx in ["spy", "qqq", "iwm"] {
                fields.push(f(&format!("{idx}_intraday_10m"), list_of_bars(), false));
            }
            // Index EOD aggregates (RES)
            for idx in ["spy", "qqq", "iwm"] {
                for name in ["open", "high", "low", "close", "volume"] {
                    fields.push(f(&format!("{idx}_eod_{name}"), DataType::Float64, true));
                }
            }
            // Overnight / signal context
            for idx in ["spy", "qqq", "iwm"] {
                fields.push(f(&format!("{idx}_overnight_gap"), DataType::Float64, true));
                fields.push(f(&format!("{idx}_ret_0930_to_1000"), DataType::Float64, true));
                fields.push(f(&format!("{idx}_realized_vol_21d"), DataType::Float64, true));
            }
            // VIX
            fields.push(f("vix_open", DataType::Float64, true));
            fields.push(f("vix_close", DataType::Float64, true));

            // Cross-sectional market breadth
            fields.push(f("breadth_pct_universe_green_at_1000", DataType::Float64, true));
            fields.push(f("breadth_pct_universe_green_at_1030", DataType::Float64, true));
            fields.push(f("breadth_pct_universe_above_premarket_vwap_at_1000", DataType::Float64, true));
            fields.push(f("breadth_advance_decline_ratio_at_1000", DataType::Float64, true));
            fields.push(f("breadth_count_movers_above_5pct_at_1000", DataType::Int32, true));
            fields.push(f("breadth_count_movers_above_1atr_at_1000", DataType::Int32, true));
            fields.push(f("breadth_count_movers_above_5pct_eod", DataType::Int32, true));
            fields.push(f("breadth_advance_decline_ratio_eod", DataType::Float64, true));
            fields.push(f("breadth_total_universe_with_bars", DataType::Int32, true));

            // v2 additions (strategy doc §3.7, +6) -------------------------
            // Cross-sectional dispersion — the regime axis §2.6's CS-momentum
            // analysis depends on; enables a 6th regime taxonomy.
            fields.push(f("cross_sectional_ret_dispersion_at_1000", DataType::Float64, true));
            fields.push(f("cross_sectional_ret_dispersion_eod", DataType::Float64, true));
            fields.push(f("cross_sectional_ret_iqr_at_1000", DataType::Float64, true));
            fields.push(f("cross_sectional_ret_iqr_eod", DataType::Float64, true));
            // Continuous parents of the `liquidity` regime taxonomy.
            fields.push(f("universe_median_addv_20d", DataType::Float64, true));
            fields.push(f("universe_total_dollar_volume", DataType::Float64, true));

            Arc::new(Schema::new(fields))
        })
        .clone()
}

// =============================================================================
// 3. forward_outcomes.parquet (RFC §9)
// =============================================================================

/// `forward_outcomes.parquet` — one row per `(day, security_id, entry_offset)`.
/// ~200-300 GB. Every field is `[RES]` or `[LABEL]` per RFC §9.
///
/// Schema is dynamically constructed from `FORWARD_HORIZONS_V2`,
/// `PCT_THRESHOLDS_V2`, and `ATR_THRESHOLDS_V2`. Bumping any of those
/// requires bumping the matching version constant.
pub fn forward_outcomes_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let mut fields: Vec<Field> = Vec::new();

            // Identity
            fields.push(f("day", DataType::Date32, false));
            fields.push(f("security_id", DataType::Utf8, false));
            fields.push(f("entry_offset", dict_str(), false));
            fields.push(f("entry_price", DataType::Float64, true));
            fields.push(f("entry_unadjusted_price", DataType::Float64, true));

            // Pre-entry parametric features
            fields.push(f("pre_entry_ret_from_open", DataType::Float64, true));
            fields.push(f("pre_entry_volume_from_open", DataType::Float64, true));
            fields.push(f("pre_entry_dollar_volume_from_open", DataType::Float64, true));
            fields.push(f("pre_entry_vwap_from_open", DataType::Float64, true));
            fields.push(f("pre_entry_high_return_so_far", DataType::Float64, true));
            fields.push(f("pre_entry_low_return_so_far", DataType::Float64, true));
            fields.push(f("pre_entry_minutes_since_high", DataType::Int32, true));
            fields.push(f("pre_entry_minutes_since_low", DataType::Int32, true));
            fields.push(f("pre_entry_ret_from_high", DataType::Float64, true));
            fields.push(f("pre_entry_ret_from_low", DataType::Float64, true));
            fields.push(f("pre_entry_ret_rank_today", DataType::Int32, true));
            fields.push(f("pre_entry_ret_percentile_today", DataType::Float64, true));
            fields.push(f("pre_entry_dollar_volume_rank_today", DataType::Int32, true));

            // Entry-quality / fill-realism proxies (v6)
            fields.push(f("entry_price_location_in_1m_bar", DataType::Float64, true));
            fields.push(f("entry_open_to_close_1m_return", DataType::Float64, true));
            fields.push(f("entry_bar_upper_wick_pct", DataType::Float64, true));
            fields.push(f("entry_bar_lower_wick_pct", DataType::Float64, true));
            fields.push(f("entry_slippage_proxy_bps", DataType::Float64, true));
            fields.push(f("entry_participation_capacity_1pct_adv", DataType::Float64, true));
            fields.push(f("entry_participation_capacity_5pct_1m_volume", DataType::Float64, true));
            fields.push(f("entry_1m_volume", DataType::Float64, true));
            fields.push(f("entry_1m_range", DataType::Float64, true));
            fields.push(f("entry_1m_dollar_volume", DataType::Float64, true));
            fields.push(f("entry_range_vs_atr_14d", DataType::Float64, true));
            fields.push(f("entry_range_vs_yz_vol_14d", DataType::Float64, true));
            fields.push(f("entry_dollar_volume_vs_addv_20d", DataType::Float64, true));
            fields.push(f("is_halted_at_entry", DataType::Boolean, true));

            // Per-horizon outcomes (13 horizons)
            for h in FORWARD_HORIZONS_V2 {
                fields.push(f(&format!("ret_{h}"), DataType::Float64, true));
                fields.push(f(&format!("max_drawdown_{h}"), DataType::Float64, true));
                fields.push(f(&format!("max_runup_{h}"), DataType::Float64, true));
                fields.push(f(&format!("close_max_ret_{h}"), DataType::Float64, true));
                fields.push(f(&format!("close_min_ret_{h}"), DataType::Float64, true));
                fields.push(f(&format!("bars_to_max_drawdown_{h}"), DataType::UInt32, true));
                fields.push(f(&format!("bars_to_max_runup_{h}"), DataType::UInt32, true));
                fields.push(f(&format!("bars_to_close_min_{h}"), DataType::UInt32, true));
                fields.push(f(&format!("bars_to_close_max_{h}"), DataType::UInt32, true));
            }

            // Fixed-% threshold crossings (7 × 2 × 13)
            for h in FORWARD_HORIZONS_V2 {
                for t in PCT_THRESHOLDS_V2 {
                    fields.push(f(&format!("first_cross_up_{t}pct_{h}"), DataType::UInt32, true));
                    fields.push(f(&format!("first_cross_down_{t}pct_{h}"), DataType::UInt32, true));
                }
            }

            // ATR-normalized threshold crossings (6 × 2 × 13)
            for h in FORWARD_HORIZONS_V2 {
                for t in ATR_THRESHOLDS_V2 {
                    fields.push(f(&format!("first_cross_up_{t}atr_{h}"), DataType::UInt32, true));
                    fields.push(f(&format!("first_cross_down_{t}atr_{h}"), DataType::UInt32, true));
                }
            }

            // Market-relative outcomes (13 × 3)
            for h in FORWARD_HORIZONS_V2 {
                for idx in ["spy", "qqq", "iwm"] {
                    fields.push(f(&format!("ret_{h}_excess_{idx}"), DataType::Float64, true));
                }
            }

            // Day-0 post-entry session-segment returns (v6)
            for seg in ["1030", "1100", "1130", "1200", "1300", "1400", "1500", "1530", "close"] {
                fields.push(f(&format!("ret_to_{seg}"), DataType::Float64, true));
            }

            // Day-0 post-entry session-shape (v6)
            fields.push(f("post_entry_morning_high_return", DataType::Float64, true));
            fields.push(f("post_entry_morning_low_return", DataType::Float64, true));
            fields.push(f("post_entry_midday_high_return", DataType::Float64, true));
            fields.push(f("post_entry_midday_low_return", DataType::Float64, true));
            fields.push(f("post_entry_afternoon_high_return", DataType::Float64, true));
            fields.push(f("post_entry_afternoon_low_return", DataType::Float64, true));
            fields.push(f("post_entry_power_hour_return", DataType::Float64, true));
            fields.push(f("post_entry_close_vs_high_return", DataType::Float64, true));
            fields.push(f("post_entry_close_vs_low_return", DataType::Float64, true));

            // Time-underwater / time-profitable (v6) — 5 short horizons
            for h in ["EOD", "1d", "2d", "3d", "5d"] {
                fields.push(f(&format!("pct_bars_profitable_{h}"), DataType::Float64, true));
                fields.push(f(&format!("pct_bars_underwater_{h}"), DataType::Float64, true));
                fields.push(f(&format!("max_consecutive_bars_profitable_{h}"), DataType::UInt32, true));
                fields.push(f(&format!("max_consecutive_bars_underwater_{h}"), DataType::UInt32, true));
                fields.push(f(&format!("time_to_recover_after_first_drawdown_{h}"), DataType::UInt32, true));
            }

            // Next-day outcomes (v6)
            fields.push(f("next_day_open_return", DataType::Float64, true));
            fields.push(f("next_day_gap_return", DataType::Float64, true));
            fields.push(f("next_day_first_5m_return", DataType::Float64, true));
            fields.push(f("next_day_first_15m_return", DataType::Float64, true));
            fields.push(f("next_day_first_30m_return", DataType::Float64, true));
            fields.push(f("next_day_high_return", DataType::Float64, true));
            fields.push(f("next_day_low_return", DataType::Float64, true));
            fields.push(f("next_day_close_return", DataType::Float64, true));
            fields.push(f("next_day_close_location_in_range", DataType::Float64, true));
            fields.push(f("next_day_fade_from_open", DataType::Float64, true));
            fields.push(f("next_day_continuation_from_open", DataType::Float64, true));

            // Gap-vs-RTH decomposition for days 1-5 (v6)
            for d in 1..=5 {
                fields.push(f(&format!("gap_return_day_{d}"), DataType::Float64, true));
                fields.push(f(&format!("rth_return_day_{d}"), DataType::Float64, true));
                fields.push(f(&format!("close_to_close_return_day_{d}"), DataType::Float64, true));
                fields.push(f(&format!("open_to_close_return_day_{d}"), DataType::Float64, true));
            }

            // Materialized target-before-stop labels (v6); pairs frozen in
            // TARGET_STOP_PAIRS_V6 so first_event_<pair> below can't drift.
            for pair in TARGET_STOP_PAIRS_V6 {
                fields.push(f(&format!("hit_{pair}"), DataType::Boolean, true));
            }

            // v2 additions (strategy doc §3.7, +42; 615 → 657) -------------
            // First-event resolution per frozen pair (§3.2 refinement) —
            // exact at 1m resolution; values: FIRST_EVENT_VALUES_V1.
            for pair in TARGET_STOP_PAIRS_V6 {
                fields.push(f(&format!("first_event_{pair}"), dict_str(), true));
            }
            // Dividend-adjusted total returns + ex-date flags (§3.3) —
            // multi-day horizons only; price-only ret_<H> stays for continuity.
            for h in MULTIDAY_HORIZONS_V2 {
                fields.push(f(&format!("ret_{h}_total"), DataType::Float64, true));
            }
            for h in MULTIDAY_HORIZONS_V2 {
                fields.push(f(&format!("dividend_ex_date_within_{h}"), DataType::Boolean, true));
            }
            // Halt/LULD exitability proxy (§3.4): largest intra-RTH gap
            // between consecutive bars inside the horizon, in minutes.
            for h in FORWARD_HORIZONS_V2 {
                fields.push(f(&format!("bar_gap_minutes_max_{h}"), DataType::Int32, true));
            }
            // Cumulative pre-entry volume from 04:00 ET session start through
            // the entry bar inclusive (§3.6) — pre-entry volume at all 17
            // offsets, not just the 10:00 snapshot.
            fields.push(f("cumulative_volume_to_entry", DataType::Float64, true));
            fields.push(f("cumulative_dollar_volume_to_entry", DataType::Float64, true));

            // Terminal events (values: TERMINAL_EVENT_TYPES_V1)
            fields.push(f("terminal_event_type", dict_str(), true));
            fields.push(f("terminal_event_date", DataType::Date32, true));
            fields.push(f("terminal_event_return", DataType::Float64, true));
            fields.push(f("terminal_event_confidence", dict_str(), true));
            fields.push(f("last_valid_trade_date", DataType::Date32, true));
            fields.push(f("days_with_missing_forward_bars", DataType::Int32, true));

            Arc::new(Schema::new(fields))
        })
        .clone()
}

// =============================================================================
// 4. forward_path_short.parquet (RFC §9.5)
// =============================================================================

pub fn forward_path_short_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                f("day", DataType::Date32, false),
                f("security_id", DataType::Utf8, false),
                f("entry_offset", dict_str(), false),
                f("path_checkpoint", dict_str(), false),
                // Position state at this checkpoint
                f("ret", DataType::Float64, true),
                f("high_ret_so_far", DataType::Float64, true),
                f("low_ret_so_far", DataType::Float64, true),
                f("close_max_ret_so_far", DataType::Float64, true),
                f("close_min_ret_so_far", DataType::Float64, true),
                // Trade book
                f("volume_since_entry", DataType::Float64, true),
                f("dollar_volume_since_entry", DataType::Float64, true),
                f("vwap_since_entry", DataType::Float64, true),
                f("bars_elapsed", DataType::Int32, true),
                // Time-quality
                f("pct_bars_profitable_so_far", DataType::Float64, true),
                f("pct_bars_underwater_so_far", DataType::Float64, true),
                // v2 additions (strategy doc §3.7, +4) ----------------------
                // Markov-state augmentation (§3.2/§7.3.1): path shape that
                // raw (ret, max_ret, drawdown) state can't carry.
                f("volatility_within_trade", DataType::Float64, true),
                f("rate_of_change", DataType::Float64, true),
                f("current_ret_over_atr_14d", DataType::Float64, true),
                // Exitability flag (§3.4): any intra-RTH bar gap ≥ 5 minutes
                // between entry and this checkpoint.
                f("halt_gap_crossed", DataType::Boolean, true),
            ]))
        })
        .clone()
}

// =============================================================================
// 5. regime_definitions.parquet (RFC §10)
// =============================================================================

pub fn regime_definitions_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                f("day", DataType::Date32, false),
                f("taxonomy", dict_str(), false),
                f("taxonomy_version", DataType::Utf8, false),
                f("regime_id", DataType::Int32, false),
                f("regime_name", DataType::Utf8, false),
            ]))
        })
        .clone()
}

// =============================================================================
// 6. earnings_calendar.parquet (RFC §11)
// =============================================================================

pub fn earnings_calendar_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                f("security_id", DataType::Utf8, false),
                f("earnings_date", DataType::Date32, false),
                f("report_timing", dict_str(), true),
                f("announced_at_date", DataType::Date32, true),
                f("earnings_window", dict_str(), true),
                f("revision_count", DataType::Int32, true),
                f("source", dict_str(), false),
            ]))
        })
        .clone()
}

// =============================================================================
// 7. sector_aggregates_daily.parquet (RFC §11.5)
// =============================================================================

pub fn sector_aggregates_daily_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                f("day", DataType::Date32, false),
                f("sector_id", dict_str(), false),
                f("sector_constituent_count_with_bars", DataType::Int32, false),
                f("sector_ret_0930_to_0950", DataType::Float64, true),
                f("sector_ret_0930_to_1000", DataType::Float64, true),
                f("sector_ret_0930_to_1030", DataType::Float64, true),
                f("sector_eod_ret", DataType::Float64, true),
                f("sector_pct_green_at_1000", DataType::Float64, true),
                f("sector_pct_green_at_1030", DataType::Float64, true),
                f("sector_ret_0930_to_1000_rank_today", DataType::Int32, true),
            ]))
        })
        .clone()
}

// =============================================================================
// 8. security_classification_daily.parquet (RFC §11.6) — NEW in v6
// =============================================================================

/// `security_classification_daily.parquet` — one row per `(day, security_id)`.
/// Point-in-time behavioral tagging so downstream never blindly pools
/// mega-cap tech with low-float biotech gappers, leveraged ETFs, etc.
/// All `[ML]` IFF rolling components honor the `[D-N, D-1]` window.
///
/// The behavioral-tag columns are produced by the rules in
/// `momentum-classify` (§11.6.3); their rule logic is pinned to
/// `SECURITY_CLASSIFICATION_VERSION` and tested by a snapshot.
pub fn security_classification_daily_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let mut fields: Vec<Field> = Vec::new();

            // Identity
            fields.push(f("day", DataType::Date32, false));
            fields.push(f("security_id", DataType::Utf8, false));
            fields.push(f("display_symbol_on_day", DataType::Utf8, false));

            // Security structure
            fields.push(f("ticker_type", dict_str(), true));
            fields.push(f("is_common_stock", DataType::Boolean, true));
            for name in ["is_etf", "is_leveraged_etf", "is_inverse_etf", "is_etn"] {
                fields.push(f(name, DataType::Boolean, true));
            }
            fields.push(f("is_adr", DataType::Boolean, true));
            fields.push(f("is_spac", DataType::Boolean, true));
            for name in ["is_warrant", "is_preferred", "is_unit"] {
                fields.push(f(name, DataType::Boolean, true));
            }

            // Listing
            fields.push(f("primary_exchange", dict_str(), true));
            fields.push(f("listing_status", dict_str(), true));
            fields.push(f("listing_status_date", DataType::Date32, true));
            fields.push(f("days_since_ipo_or_first_bar", DataType::Int32, true));

            // Sector / industry
            fields.push(f("sector", dict_str(), true));
            fields.push(f("industry", dict_str(), true));
            fields.push(f("sub_industry", dict_str(), true));

            // Size
            fields.push(f("market_cap", DataType::Float64, true));
            fields.push(f("market_cap_bucket", dict_str(), true));
            fields.push(f("market_cap_rank_today", DataType::Int32, true));
            fields.push(f("market_cap_percentile_today", DataType::Float64, true));

            // Liquidity
            for win in ["20d", "60d"] {
                fields.push(f(&format!("adv_{win}"), DataType::Float64, true));
            }
            for win in ["20d", "60d"] {
                fields.push(f(&format!("addv_{win}"), DataType::Float64, true));
            }
            fields.push(f("liquidity_bucket", dict_str(), true));
            fields.push(f("addv_rank_today", DataType::Int32, true));
            fields.push(f("dollar_volume_rank_today", DataType::Int32, true));

            // Price
            fields.push(f("prior_close_price", DataType::Float64, true));
            fields.push(f("price_bucket", dict_str(), true));

            // Volatility
            fields.push(f("atr_14d", DataType::Float64, true));
            fields.push(f("realized_vol_21d", DataType::Float64, true));
            fields.push(f("yang_zhang_vol_21d", DataType::Float64, true));
            fields.push(f("volatility_bucket", dict_str(), true));
            fields.push(f("volatility_percentile_today", DataType::Float64, true));

            // Beta / style
            for idx in ["spy", "qqq", "iwm"] {
                fields.push(f(&format!("beta_{idx}_60d"), DataType::Float64, true));
            }
            fields.push(f("style_bucket", dict_str(), true));

            // Behavioral / theme tags (RFC §11.6.3 v1)
            for name in [
                "is_mega_cap_tech",
                "is_large_cap_tech",
                "is_semiconductor",
                "is_biotech",
                "is_regional_bank",
                "is_energy",
                "is_china_adr",
                "is_low_float_candidate",
                "is_meme_candidate",
                "is_recent_ipo",
            ] {
                fields.push(f(name, DataType::Boolean, true));
            }

            Arc::new(Schema::new(fields))
        })
        .clone()
}

// =============================================================================
// Tests — stability + parameter-grid invariants.
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_stable(get: impl Fn() -> SchemaRef) {
        let a = get();
        let b = get();
        assert!(
            Arc::ptr_eq(&a, &b),
            "schema must be cached behind OnceLock; new Arc on each call"
        );
    }

    fn assert_has_field(s: &SchemaRef, name: &str) {
        assert!(
            s.field_with_name(name).is_ok(),
            "schema missing field `{name}`"
        );
    }

    #[test]
    fn parameter_grids_match_rfc_v6() {
        // RFC §0.6: 17 entry offsets, 13 horizons, 23 path checkpoints,
        // 7 pct thresholds, 6 atr thresholds. Strategy doc §3.7: 9 multi-day
        // horizons, 9 frozen target/stop pairs.
        assert_eq!(ENTRY_OFFSETS_V1.len(), 17);
        assert_eq!(FORWARD_HORIZONS_V2.len(), 13);
        assert_eq!(PATH_CHECKPOINTS_V1.len(), 23);
        assert_eq!(PCT_THRESHOLDS_V2.len(), 7);
        assert_eq!(ATR_THRESHOLDS_V2.len(), 6);
        assert_eq!(MULTIDAY_HORIZONS_V2.len(), 9);
        assert_eq!(TARGET_STOP_PAIRS_V6.len(), 9);

        // MULTIDAY_HORIZONS_V2 must be exactly the tail of FORWARD_HORIZONS_V2
        // (everything from "1d" on) — dividend columns key off it.
        assert_eq!(
            &FORWARD_HORIZONS_V2[FORWARD_HORIZONS_V2.len() - MULTIDAY_HORIZONS_V2.len()..],
            MULTIDAY_HORIZONS_V2,
        );
    }

    #[test]
    fn version_constants_stamp_v1_or_v2() {
        // Mirror the RFC §0.6 default-versions table (+ §3.7 table versions).
        assert_eq!(ENTRY_OFFSET_GRID_VERSION, "v1");
        assert_eq!(FORWARD_HORIZONS_VERSION, "v2");
        // v2 per §3.7 — schema bump only; the checkpoint SET stays 23.
        assert_eq!(FORWARD_PATH_CHECKPOINTS_VERSION, "v2");
        assert_eq!(PCT_THRESHOLD_VERSION, "v2");
        assert_eq!(ATR_THRESHOLD_VERSION, "v2");
        assert_eq!(REGIME_TAXONOMY_VERSION, "v1");
        assert_eq!(SECURITY_CLASSIFICATION_VERSION, "v1");
        assert_eq!(DAILY_OBSERVATION_VERSION, "v2");
        assert_eq!(FORWARD_OUTCOMES_VERSION, "v2");
        assert_eq!(MARKET_CONTEXT_DAILY_VERSION, "v2");
        assert_eq!(SIGNAL_DEFINITION_VERSION, "v1");
    }

    #[test]
    fn enum_value_sets_match_strategy_doc() {
        // §3.6 decision: 6 terminal-event values; ambiguity lives in
        // terminal_event_confidence, never in extra enum values.
        assert_eq!(TERMINAL_EVENT_TYPES_V1.len(), 6);
        assert_eq!(TERMINAL_EVENT_TYPES_V1[0], "none");
        assert!(TERMINAL_EVENT_TYPES_V1.contains(&"delisted_merger_acquisition"));
        assert!(TERMINAL_EVENT_TYPES_V1.contains(&"delisted_bankruptcy_liquidation"));
        assert!(TERMINAL_EVENT_TYPES_V1.contains(&"extended_halt_no_bars"));

        // §3.2 refinement: 4 first-event values.
        assert_eq!(
            FIRST_EVENT_VALUES_V1,
            &["target_first", "stop_first", "neither", "no_data"]
        );

        // Regime-threshold stamp key shape.
        assert_eq!(regime_thresholds_key("vix_level"), "regime_thresholds_vix_level");
    }

    #[test]
    fn daily_observation_is_stable_and_has_key_columns() {
        assert_stable(daily_observation_schema);
        let s = daily_observation_schema();
        for name in [
            "day",
            "security_id",
            "intraday_1m_first_hour",
            "intraday_10m_rest",
            "intraday_ret_0930_to_1000",
            "eod_day_close",
            "premarket_volume",
            "overnight_gap",
            "atr_14d",
            "realized_vol_21d",
            "beta_spy_60d",
            "is_earnings_day",
            "is_half_day",
            // v2 (§3.7)
            "signal_first_in_5d",
            "signal_first_in_20d",
            "signal_concentration_percentile_today",
            "signal_concentration_hhi_today",
            "premarket_volume_vs_20d_median",
            "pre_market_volume_spike_flag",
            "high_52w",
            "low_52w",
            "days_since_last_5pct_move",
            "days_since_last_20pct_move",
            "consecutive_up_days_close_to_close",
            "gap_filled_today_flag",
        ] {
            assert_has_field(&s, name);
        }
    }

    #[test]
    fn market_context_daily_is_stable_and_shaped() {
        assert_stable(market_context_daily_schema);
        let s = market_context_daily_schema();
        for name in [
            "day",
            "spy_intraday_10m",
            "qqq_intraday_10m",
            "iwm_intraday_10m",
            "vix_close",
            "breadth_pct_universe_green_at_1000",
            // v2 (§3.7)
            "cross_sectional_ret_dispersion_at_1000",
            "cross_sectional_ret_dispersion_eod",
            "cross_sectional_ret_iqr_at_1000",
            "cross_sectional_ret_iqr_eod",
            "universe_median_addv_20d",
            "universe_total_dollar_volume",
        ] {
            assert_has_field(&s, name);
        }
    }

    #[test]
    fn forward_outcomes_is_stable_and_carries_every_horizon() {
        assert_stable(forward_outcomes_schema);
        let s = forward_outcomes_schema();

        // Per-horizon outcome columns.
        for h in FORWARD_HORIZONS_V2 {
            assert_has_field(&s, &format!("ret_{h}"));
            assert_has_field(&s, &format!("max_drawdown_{h}"));
            assert_has_field(&s, &format!("close_max_ret_{h}"));
            assert_has_field(&s, &format!("bars_to_max_runup_{h}"));
        }
        // Per-horizon × per-threshold combinatorics.
        for h in FORWARD_HORIZONS_V2 {
            for t in PCT_THRESHOLDS_V2 {
                assert_has_field(&s, &format!("first_cross_up_{t}pct_{h}"));
                assert_has_field(&s, &format!("first_cross_down_{t}pct_{h}"));
            }
            for t in ATR_THRESHOLDS_V2 {
                assert_has_field(&s, &format!("first_cross_up_{t}atr_{h}"));
                assert_has_field(&s, &format!("first_cross_down_{t}atr_{h}"));
            }
            for idx in ["spy", "qqq", "iwm"] {
                assert_has_field(&s, &format!("ret_{h}_excess_{idx}"));
            }
        }
        // A sampling of v6 additions.
        for name in [
            "entry_slippage_proxy_bps",
            "ret_to_1030",
            "ret_to_close",
            "post_entry_power_hour_return",
            "pct_bars_profitable_5d",
            "next_day_gap_return",
            "gap_return_day_5",
            "hit_2atr_before_minus_1atr_5d",
            "terminal_event_type",
        ] {
            assert_has_field(&s, name);
        }

        // v2 (§3.7): both label families generate from TARGET_STOP_PAIRS_V6;
        // dividend columns from MULTIDAY_HORIZONS_V2; bar-gap from the full
        // horizon grid.
        for pair in TARGET_STOP_PAIRS_V6 {
            assert_has_field(&s, &format!("hit_{pair}"));
            assert_has_field(&s, &format!("first_event_{pair}"));
        }
        for h in MULTIDAY_HORIZONS_V2 {
            assert_has_field(&s, &format!("ret_{h}_total"));
            assert_has_field(&s, &format!("dividend_ex_date_within_{h}"));
        }
        for h in FORWARD_HORIZONS_V2 {
            assert_has_field(&s, &format!("bar_gap_minutes_max_{h}"));
        }
        assert_has_field(&s, "cumulative_volume_to_entry");
        assert_has_field(&s, "cumulative_dollar_volume_to_entry");
        // Intraday horizons must NOT get dividend columns.
        assert!(s.field_with_name("ret_EOD_total").is_err());
        assert!(s.field_with_name("dividend_ex_date_within_30min").is_err());
    }

    #[test]
    fn forward_outcomes_column_count_matches_expected() {
        // Identity (5) + pre-entry (13) + entry-quality (14) +
        // per-horizon outcomes (9 × 13 = 117) +
        // fixed-% crossings (2 × 7 × 13 = 182) +
        // ATR crossings (2 × 6 × 13 = 156) +
        // market-relative (3 × 13 = 39) +
        // day-0 segments (9) + day-0 shape (9) +
        // time-underwater (5 × 5 = 25) +
        // next-day (11) +
        // gap-vs-RTH days 1-5 (4 × 5 = 20) +
        // materialized labels (9) +
        // terminal events (6).
        // RFC v6 subtotal = 5 + 13 + 14 + 117 + 182 + 156 + 39 + 9 + 9 + 25
        //                 + 11 + 20 + 9 + 6 = 615.
        // v2 additions (strategy doc §3.7): first_event (9) +
        // ret_<H>_total (9) + dividend_ex_date_within (9) +
        // bar_gap_minutes_max (13) + cumulative pre-entry volume (2) = 42.
        // Total = 615 + 42 = 657.
        let s = forward_outcomes_schema();
        assert_eq!(
            s.fields().len(),
            657,
            "forward_outcomes column count drifted from v2 (§3.7) expectation"
        );
    }

    #[test]
    fn forward_path_short_is_stable_and_long_format() {
        assert_stable(forward_path_short_schema);
        let s = forward_path_short_schema();
        // 15 (v1) + 4 v2 path-state columns (§3.7).
        assert_eq!(s.fields().len(), 19);
        for name in [
            "day",
            "security_id",
            "entry_offset",
            "path_checkpoint",
            "ret",
            "vwap_since_entry",
            // v2 (§3.7)
            "volatility_within_trade",
            "rate_of_change",
            "current_ret_over_atr_14d",
            "halt_gap_crossed",
        ] {
            assert_has_field(&s, name);
        }
        // censor_type lands in forward_outcomes as first_event_<pair>
        // (§3.2 refinement), NOT at path grain.
        assert!(s.field_with_name("censor_type").is_err());
    }

    #[test]
    fn regime_definitions_is_stable() {
        assert_stable(regime_definitions_schema);
        let s = regime_definitions_schema();
        assert_eq!(s.fields().len(), 5);
    }

    #[test]
    fn earnings_calendar_is_stable() {
        assert_stable(earnings_calendar_schema);
        let s = earnings_calendar_schema();
        assert_eq!(s.fields().len(), 7);
    }

    #[test]
    fn sector_aggregates_daily_is_stable() {
        assert_stable(sector_aggregates_daily_schema);
        let s = sector_aggregates_daily_schema();
        assert_eq!(s.fields().len(), 10);
    }

    #[test]
    fn security_classification_daily_is_stable_and_carries_v1_tags() {
        assert_stable(security_classification_daily_schema);
        let s = security_classification_daily_schema();
        for name in [
            "is_etf",
            "is_leveraged_etf",
            "is_adr",
            "is_mega_cap_tech",
            "is_biotech",
            "is_low_float_candidate",
            "is_meme_candidate",
            "is_recent_ipo",
            "market_cap_bucket",
            "liquidity_bucket",
            "price_bucket",
            "volatility_bucket",
            "style_bucket",
            "beta_qqq_60d",
        ] {
            assert_has_field(&s, name);
        }
    }
}
