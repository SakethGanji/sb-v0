//! Canonical file-level Parquet metadata stamps per output table.
//!
//! Single source of truth for what the engine writes into every file's
//! key-value metadata (RFC §0.6 contract: readers refuse joins across
//! mismatched versions). The version values themselves live in
//! `momentum_core::phase0_outputs`; this module just assembles the
//! per-table stamp lists and the `WriterProperties` carrying them.

use momentum_core::phase0_outputs::*;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

/// Marks which engine milestone wrote the file. Pre-B6 files are partial
/// by construction; downstream must treat anything not stamped `complete`
/// as scaffolding output.
pub const ENGINE_MILESTONE_KEY: &str = "engine_milestone";

pub fn daily_observation_stamps() -> Vec<(String, String)> {
    own(&[
        (DAILY_OBSERVATION_VERSION_KEY, DAILY_OBSERVATION_VERSION),
        (SIGNAL_DEFINITION_KEY, SIGNAL_DEFINITION_V1),
        (SIGNAL_DEFINITION_VERSION_KEY, SIGNAL_DEFINITION_VERSION),
    ])
}

pub fn market_context_daily_stamps() -> Vec<(String, String)> {
    own(&[(MARKET_CONTEXT_DAILY_VERSION_KEY, MARKET_CONTEXT_DAILY_VERSION)])
}

pub fn forward_outcomes_stamps() -> Vec<(String, String)> {
    own(&[
        (FORWARD_OUTCOMES_VERSION_KEY, FORWARD_OUTCOMES_VERSION),
        (ENTRY_OFFSET_GRID_VERSION_KEY, ENTRY_OFFSET_GRID_VERSION),
        (FORWARD_HORIZONS_VERSION_KEY, FORWARD_HORIZONS_VERSION),
        (PCT_THRESHOLD_VERSION_KEY, PCT_THRESHOLD_VERSION),
        (ATR_THRESHOLD_VERSION_KEY, ATR_THRESHOLD_VERSION),
    ])
}

pub fn forward_path_short_stamps() -> Vec<(String, String)> {
    own(&[
        (
            FORWARD_PATH_CHECKPOINTS_VERSION_KEY,
            FORWARD_PATH_CHECKPOINTS_VERSION,
        ),
        (ENTRY_OFFSET_GRID_VERSION_KEY, ENTRY_OFFSET_GRID_VERSION),
    ])
}

pub fn security_classification_daily_stamps() -> Vec<(String, String)> {
    own(&[(
        SECURITY_CLASSIFICATION_VERSION_KEY,
        SECURITY_CLASSIFICATION_VERSION,
    )])
}

/// `regime_definitions` additionally requires per-taxonomy threshold
/// stamps (`regime_thresholds_key(taxonomy)` + window) — those carry
/// computed values, so the regime writer appends them at write time.
pub fn regime_definitions_stamps() -> Vec<(String, String)> {
    own(&[(REGIME_TAXONOMY_VERSION_KEY, REGIME_TAXONOMY_VERSION)])
}

/// Writer properties for an engine output file: zstd + the table's stamps
/// + the engine-milestone marker.
pub fn writer_props(stamps: &[(String, String)], milestone: &str) -> WriterProperties {
    let mut kv: Vec<KeyValue> = stamps
        .iter()
        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
        .collect();
    kv.push(KeyValue::new(
        ENGINE_MILESTONE_KEY.to_string(),
        milestone.to_string(),
    ));
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).expect("valid zstd level")))
        .set_key_value_metadata(Some(kv))
        .build()
}

fn own(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_table_stamp_list_carries_its_version_key() {
        assert!(daily_observation_stamps().iter().any(|(k, v)| k == "daily_observation_version" && v == "v2"));
        assert!(daily_observation_stamps().iter().any(|(k, _)| k == "signal_definition"));
        assert!(forward_outcomes_stamps().iter().any(|(k, v)| k == "forward_outcomes_version" && v == "v2"));
        assert!(forward_path_short_stamps().iter().any(|(k, v)| k == "forward_path_checkpoints_version" && v == "v2"));
        assert!(market_context_daily_stamps().iter().any(|(k, v)| k == "market_context_daily_version" && v == "v2"));
    }
}
