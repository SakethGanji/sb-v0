//! One-shot derivation: `financials.parquet` + EDGAR acceptance backfill
//! → `data/outputs/earnings_calendar.parquet` (RFC §11).
//!
//! ```text
//! cargo run --release --bin build-earnings-calendar
//!   [--reference data/reference] [--out data/outputs]
//! ```

use anyhow::{Context, Result, bail};
use momentum_core::phase0_outputs::earnings_calendar_schema;
use momentum_engine::{earnings, stamps};
use momentum_store::acceptance_backfill::read_acceptance_by_accession;
use momentum_store::financials::read_filings_lite;
use parquet::arrow::ArrowWriter;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<()> {
    let mut reference = PathBuf::from("data/reference");
    let mut out = PathBuf::from("data/outputs");
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        let mut val = |n: &str| it.next().with_context(|| format!("{n} needs a value"));
        match a.as_str() {
            "--reference" => reference = PathBuf::from(val("--reference")?),
            "--out" => out = PathBuf::from(val("--out")?),
            other => bail!("unknown arg: {other}"),
        }
    }

    let t0 = Instant::now();
    let filings = read_filings_lite(&reference.join("financials.parquet"))
        .context("read financials.parquet")?;
    let backfill = read_acceptance_by_accession(
        &reference.join("acceptance_datetime_backfill.parquet"),
    )
    .context("read acceptance_datetime_backfill.parquet")?;
    println!(
        "filings: {}   backfill accessions: {}   read in {:.2?}",
        filings.len(),
        backfill.len(),
        t0.elapsed()
    );

    let events = earnings::derive(&filings, &backfill);
    let mut by_timing: HashMap<&str, usize> = HashMap::new();
    let mut by_source: HashMap<&str, usize> = HashMap::new();
    for e in &events {
        *by_timing.entry(e.report_timing.as_str()).or_default() += 1;
        *by_source.entry(e.source.as_str()).or_default() += 1;
    }
    println!("events: {}", events.len());
    println!("  timing: {by_timing:?}");
    println!("  source: {by_source:?}");

    std::fs::create_dir_all(&out)?;
    let path = out.join("earnings_calendar.parquet");
    let batch = earnings::to_batch(&events)?;
    let props = stamps::writer_props(&[], "B2");
    let file = File::create(&path)?;
    let mut w = ArrowWriter::try_new(file, earnings_calendar_schema(), Some(props))?;
    w.write(&batch)?;
    w.close()?;
    println!(
        "wrote {} ({:.1} MiB) in {:.2?}",
        path.display(),
        std::fs::metadata(&path)?.len() as f64 / (1024.0 * 1024.0),
        t0.elapsed()
    );
    Ok(())
}
