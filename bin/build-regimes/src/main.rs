//! Post-pass: `market_context_daily/` → `regime_definitions.parquet`
//! (RFC §10). Runs AFTER the sweep because tertile thresholds need the
//! exploration-window distribution of each metric; thresholds + window
//! are stamped into file metadata (strategy doc §3.6/§3.7).
//!
//! ```text
//! cargo run --release --bin build-regimes -- [--out data/outputs]
//! ```

use anyhow::{Context, Result, bail};
use arrow::array::{Array, AsArray};
use arrow::datatypes::Float64Type;
use chrono::NaiveDate;
use momentum_core::phase0_outputs::regime_definitions_schema;
use momentum_engine::regimes::{self, DayMetrics};
use momentum_engine::stamps;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::PathBuf;

fn main() -> Result<()> {
    let mut out = PathBuf::from("data/outputs");
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--out" => out = PathBuf::from(it.next().context("--out needs a value")?),
            other => bail!("unknown arg: {other}"),
        }
    }

    let ctx_dir = out.join("market_context_daily");
    let mut paths: Vec<(NaiveDate, PathBuf)> = std::fs::read_dir(&ctx_dir)
        .with_context(|| format!("read {}", ctx_dir.display()))?
        .filter_map(|e| {
            let p = e.ok()?.path();
            let stem = p.file_stem()?.to_str()?;
            Some((NaiveDate::parse_from_str(stem, "%Y-%m-%d").ok()?, p))
        })
        .collect();
    paths.sort();
    if paths.is_empty() {
        bail!("no market_context_daily files — run write-phase0 first");
    }

    let mut metrics = Vec::with_capacity(paths.len());
    for (day, path) in &paths {
        let file = File::open(path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        for batch in reader {
            let batch = batch?;
            let g = |name: &str| -> Option<f64> {
                let col = batch.column_by_name(name)?;
                let arr = col.as_primitive::<Float64Type>();
                (!arr.is_null(0)).then(|| arr.value(0))
            };
            metrics.push(DayMetrics {
                day: *day,
                spy_close: g("spy_eod_close"),
                vix_close: g("vix_close"),
                breadth_ad_eod: g("breadth_advance_decline_ratio_eod"),
                median_addv: g("universe_median_addv_20d"),
            });
        }
    }
    println!("days: {} ({} → {})", metrics.len(), paths[0].0, paths[paths.len() - 1].0);

    let (rows, threshold_stamps) = regimes::derive(&metrics);
    let mut by_tax: std::collections::BTreeMap<&str, usize> = Default::default();
    for r in &rows {
        *by_tax.entry(r.taxonomy).or_default() += 1;
    }
    println!("assignments: {} {:?}", rows.len(), by_tax);
    for (k, v) in &threshold_stamps {
        println!("  stamp {k} = {v}");
    }

    let mut all_stamps = stamps::regime_definitions_stamps();
    all_stamps.extend(threshold_stamps);
    let batch = regimes::to_batch(&rows)?;
    let path = out.join("regime_definitions.parquet");
    let props = stamps::writer_props(&all_stamps, "B2");
    let file = File::create(&path)?;
    let mut w = ArrowWriter::try_new(file, regime_definitions_schema(), Some(props))?;
    w.write(&batch)?;
    w.close()?;
    println!("wrote {}", path.display());
    Ok(())
}
