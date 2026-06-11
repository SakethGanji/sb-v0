//! End-to-end smoke: every RFC v6 (+ §3.7 v2 amendment) output schema must
//! build an empty `RecordBatch`, write to Parquet **with its versioned
//! metadata stamps**, and read back with identical field count and intact
//! stamps. Catches collisions / unsupported types in the wide schemas
//! (`forward_outcomes` has 657 columns) at CI time, and pins the
//! file-level metadata contract that readers use to refuse mismatched
//! joins (incl. the `signal_definition` stamp required by §3.7).

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use momentum_core::phase0_outputs::*;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use parquet::file::metadata::KeyValue;
use std::fs::File;
use tempfile::tempdir;

fn roundtrip(name: &str, schema: SchemaRef, stamps: &[(&str, &str)]) {
    let dir = tempdir().unwrap();
    let path = dir.path().join(format!("{name}.parquet"));

    let batch = RecordBatch::new_empty(schema.clone());

    {
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(
                stamps
                    .iter()
                    .map(|(k, v)| KeyValue::new(k.to_string(), v.to_string()))
                    .collect(),
            ))
            .build();
        let file = File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    let file = File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    // Every stamp must survive the round-trip byte-identically.
    let kv = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned()
        .unwrap_or_default();
    for (k, v) in stamps {
        let found = kv.iter().find(|e| e.key == *k);
        assert_eq!(
            found.and_then(|e| e.value.as_deref()),
            Some(*v),
            "{name}: metadata stamp `{k}` missing or wrong after round-trip"
        );
    }

    let reader = builder.build().unwrap();

    let read_schema = reader.schema().clone();
    assert_eq!(
        read_schema.fields().len(),
        schema.fields().len(),
        "{name}: field count drift across Parquet round-trip"
    );

    // Spot-check the first and last field name survived the round-trip
    // (Parquet rejects empty names, duplicate names, etc.).
    assert_eq!(
        read_schema.field(0).name(),
        schema.field(0).name(),
        "{name}: first field name drift"
    );
    let last = schema.fields().len() - 1;
    assert_eq!(
        read_schema.field(last).name(),
        schema.field(last).name(),
        "{name}: last field name drift"
    );
}

#[test]
fn daily_observation_roundtrip() {
    // §3.7: the signal_* columns embed the default signal definition, so the
    // literal expression + version are stamped alongside the schema version.
    roundtrip(
        "daily_observation",
        daily_observation_schema(),
        &[
            (DAILY_OBSERVATION_VERSION_KEY, DAILY_OBSERVATION_VERSION),
            (SIGNAL_DEFINITION_KEY, SIGNAL_DEFINITION_V1),
            (SIGNAL_DEFINITION_VERSION_KEY, SIGNAL_DEFINITION_VERSION),
        ],
    );
}

#[test]
fn market_context_daily_roundtrip() {
    roundtrip(
        "market_context_daily",
        market_context_daily_schema(),
        &[(MARKET_CONTEXT_DAILY_VERSION_KEY, MARKET_CONTEXT_DAILY_VERSION)],
    );
}

#[test]
fn forward_outcomes_roundtrip() {
    roundtrip(
        "forward_outcomes",
        forward_outcomes_schema(),
        &[
            (FORWARD_OUTCOMES_VERSION_KEY, FORWARD_OUTCOMES_VERSION),
            (ENTRY_OFFSET_GRID_VERSION_KEY, ENTRY_OFFSET_GRID_VERSION),
            (FORWARD_HORIZONS_VERSION_KEY, FORWARD_HORIZONS_VERSION),
            (PCT_THRESHOLD_VERSION_KEY, PCT_THRESHOLD_VERSION),
            (ATR_THRESHOLD_VERSION_KEY, ATR_THRESHOLD_VERSION),
        ],
    );
}

#[test]
fn forward_path_short_roundtrip() {
    roundtrip(
        "forward_path_short",
        forward_path_short_schema(),
        &[
            (
                FORWARD_PATH_CHECKPOINTS_VERSION_KEY,
                FORWARD_PATH_CHECKPOINTS_VERSION,
            ),
            (ENTRY_OFFSET_GRID_VERSION_KEY, ENTRY_OFFSET_GRID_VERSION),
        ],
    );
}

#[test]
fn regime_definitions_roundtrip() {
    // §3.6/§3.7: regime_definitions stays v1 but writers must stamp, per
    // taxonomy, the bucket boundary values + the window they were computed
    // on. Exercise the key-shape helper with a representative stamp.
    let vix_key = regime_thresholds_key("vix_level");
    roundtrip(
        "regime_definitions",
        regime_definitions_schema(),
        &[
            (REGIME_TAXONOMY_VERSION_KEY, REGIME_TAXONOMY_VERSION),
            (vix_key.as_str(), "tertiles:15.1,22.4"),
            (REGIME_THRESHOLDS_WINDOW_KEY, "2016-06-08..2020-12-31"),
        ],
    );
}

#[test]
fn earnings_calendar_roundtrip() {
    roundtrip("earnings_calendar", earnings_calendar_schema(), &[]);
}

#[test]
fn sector_aggregates_daily_roundtrip() {
    roundtrip("sector_aggregates_daily", sector_aggregates_daily_schema(), &[]);
}

#[test]
fn security_classification_daily_roundtrip() {
    roundtrip(
        "security_classification_daily",
        security_classification_daily_schema(),
        &[(
            SECURITY_CLASSIFICATION_VERSION_KEY,
            SECURITY_CLASSIFICATION_VERSION,
        )],
    );
}
