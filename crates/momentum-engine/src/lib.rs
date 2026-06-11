//! Phase 0 writer engine (Workstream B of `implementation-plan.md`).
//!
//! Day-major single chronological sweep over `bars_1m_raw/`, producing the
//! eight observation-pivot output tables (schemas in
//! `momentum_core::phase0_outputs`, RFC v7). Output mirrors the input
//! layout: `data/outputs/<table>/YYYY-MM-DD.parquet`, every file stamped
//! with the table's version constants via [`stamps`].
//!
//! ## B0 state
//!
//! Skeleton milestone: bulk day read (`MaterializedBarReader::day_sessions`),
//! session window slicing, the SQLite resume cursor, metadata stamping, and
//! a partial `daily_observation` builder (identity + [SRC] bar-list columns
//! + bar-count data-quality + calendar; everything else null). Files written
//! at this milestone carry `engine_milestone = B0-skeleton` so nothing
//! downstream mistakes them for finished output — B1 regenerates them.

pub mod cursor;
pub mod daily_observation;
pub mod slices;
pub mod stamps;
