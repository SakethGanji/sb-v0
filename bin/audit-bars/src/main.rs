//! Whole-corpus audit of `bars_1m_raw/`:
//! * file count
//! * date coverage continuity (gaps in trading-day calendar)
//! * total row count (sum across all per-day files)
//! * schema uniformity (all files match the canonical Arrow schema)
//! * file metadata sanity
//!
//! Cheap — reads only Parquet footer metadata, no data scan.

use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use chrono::{Datelike, NaiveDate};
use momentum_core::schema::bars_1m_raw_schema;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::collections::BTreeSet;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

fn main() -> Result<()> {
    let bars_dir: PathBuf = std::env::var("BARS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/bars_1m_raw"));

    if !bars_dir.is_dir() {
        anyhow::bail!("not a dir: {}", bars_dir.display());
    }

    println!("auditing {} ...\n", bars_dir.display());

    let expected_schema = bars_1m_raw_schema();

    let mut paths: Vec<(NaiveDate, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(&bars_dir)? {
        let entry = entry?;
        let p = entry.path();
        if !p.is_file() {
            continue;
        }
        let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let Some(stem) = name.strip_suffix(".parquet") else { continue };
        let date = NaiveDate::parse_from_str(stem, "%Y-%m-%d")
            .with_context(|| format!("bad date in filename: {name}"))?;
        paths.push((date, p));
    }
    paths.sort();

    println!("file count: {}", paths.len());
    if let (Some(first), Some(last)) = (paths.first(), paths.last()) {
        println!("date range: {} → {}", first.0, last.0);
    }

    // Coverage: weekdays between min and max should each have a file
    // (modulo US market holidays, which we can't easily enumerate here —
    // so we report gaps and let the user eyeball them against the holiday list).
    let dates: BTreeSet<NaiveDate> = paths.iter().map(|(d, _)| *d).collect();
    let (min, max) = (*dates.iter().next().unwrap(), *dates.iter().next_back().unwrap());
    let mut missing_weekdays: Vec<NaiveDate> = Vec::new();
    let mut d = min;
    while d <= max {
        let wd = d.weekday().number_from_monday();
        if wd <= 5 && !dates.contains(&d) {
            missing_weekdays.push(d);
        }
        d = d.succ_opt().unwrap();
    }
    println!(
        "weekday gaps inside range (likely US holidays, sanity-check this list): {}",
        missing_weekdays.len()
    );
    if !missing_weekdays.is_empty() {
        let show = missing_weekdays.len().min(10);
        println!("  first {show}: {:?}", &missing_weekdays[..show]);
    }

    // Walk files: schema check + row count tally + min/max row count.
    let mut total_rows: u64 = 0;
    let mut schema_mismatches: Vec<NaiveDate> = Vec::new();
    let mut empty_files: Vec<NaiveDate> = Vec::new();
    let mut min_rows: (NaiveDate, u64) = (paths[0].0, u64::MAX);
    let mut max_rows: (NaiveDate, u64) = (paths[0].0, 0);

    for (date, path) in &paths {
        let file = File::open(path)
            .with_context(|| format!("open {}", path.display()))?;
        let reader = SerializedFileReader::new(file)?;
        let meta = reader.metadata();

        let n: u64 = meta
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows() as u64)
            .sum();
        total_rows += n;
        if n == 0 {
            empty_files.push(*date);
        }
        if n < min_rows.1 {
            min_rows = (*date, n);
        }
        if n > max_rows.1 {
            max_rows = (*date, n);
        }

        // Schema check
        let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
            meta.file_metadata().schema_descr(),
            meta.file_metadata().key_value_metadata(),
        )?;
        if !schemas_field_equal(&Arc::new(arrow_schema), &expected_schema) {
            schema_mismatches.push(*date);
        }
    }

    println!("total rows: {total_rows}");
    println!(
        "rows per day: min {} ({}), max {} ({})",
        min_rows.1, min_rows.0, max_rows.1, max_rows.0
    );
    println!("empty files: {}", empty_files.len());
    println!("schema mismatches: {}", schema_mismatches.len());
    if !schema_mismatches.is_empty() {
        println!("  first 5: {:?}", &schema_mismatches[..5.min(schema_mismatches.len())]);
    }

    if schema_mismatches.is_empty() && empty_files.is_empty() {
        println!("\nAUDIT OK");
    } else {
        anyhow::bail!("audit failures — see above");
    }

    Ok(())
}

/// Compare two Arrow schemas by field name + data type, ignoring metadata
/// (Parquet round-trip can add `PARQUET:field_id` keys we don't care about).
fn schemas_field_equal(a: &Arc<Schema>, b: &Arc<Schema>) -> bool {
    if a.fields().len() != b.fields().len() {
        return false;
    }
    for (fa, fb) in a.fields().iter().zip(b.fields().iter()) {
        if fa.name() != fb.name() || fa.data_type() != fb.data_type() {
            return false;
        }
    }
    true
}
