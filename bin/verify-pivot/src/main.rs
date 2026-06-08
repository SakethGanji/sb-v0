//! End-to-end converter sanity check: for each requested date, open
//! `bars_1m_raw/YYYY-MM-DD.parquet`, filter to TICKER, and print
//! the same 5 numbers we computed from the raw `.csv.gz` so the two
//! can be diffed line-by-line:
//!
//!   rows, vol_sum, earliest_t + OHLCV, latest_t + OHLCV
//!
//! Usage:
//!   verify-pivot TICKER YYYY-MM-DD [YYYY-MM-DD ...]
//!
//! Example:
//!   verify-pivot SPY 2016-06-08 2020-08-31 2026-06-05

use anyhow::{Context, Result};
use arrow::array::{Array, AsArray, Float64Array, TimestampNanosecondArray};
use chrono::NaiveDate;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::PathBuf;

struct Row {
    t_ns: i64,
    o: f64,
    h: f64,
    l: f64,
    c: f64,
    v: f64,
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        eprintln!("usage: verify-pivot TICKER YYYY-MM-DD [YYYY-MM-DD ...]");
        std::process::exit(2);
    }
    let ticker = &args[0];
    let dates: Vec<NaiveDate> = args[1..]
        .iter()
        .map(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").with_context(|| format!("bad date {s}")))
        .collect::<Result<_>>()?;

    let bars_dir: PathBuf = std::env::var("BARS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/bars_1m_raw"));

    for date in &dates {
        let path = bars_dir.join(format!("{date}.parquet"));
        println!("\n=== {ticker} on {date} :: {} ===", path.display());

        if !path.exists() {
            println!("  *** file not found ***");
            continue;
        }

        let file = File::open(&path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

        let mut rows: Vec<Row> = Vec::new();
        let mut total_in_file: usize = 0;
        let mut sort_ok = true;
        let mut prev_key: Option<(String, i64)> = None;

        for batch_res in reader {
            let batch = batch_res?;
            total_in_file += batch.num_rows();
            let symbols = batch
                .column_by_name("display_symbol")
                .context("display_symbol col")?
                .as_string::<i32>();
            let t = batch
                .column_by_name("t")
                .context("t col")?
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .context("t timestamp(ns)")?;
            let o = batch.column_by_name("open").context("open")?.as_any().downcast_ref::<Float64Array>().context("open f64")?;
            let h = batch.column_by_name("high").context("high")?.as_any().downcast_ref::<Float64Array>().context("high f64")?;
            let l = batch.column_by_name("low").context("low")?.as_any().downcast_ref::<Float64Array>().context("low f64")?;
            let c = batch.column_by_name("close").context("close")?.as_any().downcast_ref::<Float64Array>().context("close f64")?;
            let v = batch.column_by_name("volume").context("volume")?.as_any().downcast_ref::<Float64Array>().context("volume f64")?;

            for i in 0..batch.num_rows() {
                let sym = symbols.value(i).to_string();
                let ts = t.value(i);
                if let Some((ps, pt)) = &prev_key {
                    if (sym.as_str(), ts) < (ps.as_str(), *pt) {
                        sort_ok = false;
                    }
                }
                prev_key = Some((sym.clone(), ts));

                if sym == *ticker {
                    rows.push(Row {
                        t_ns: ts,
                        o: o.value(i),
                        h: h.value(i),
                        l: l.value(i),
                        c: c.value(i),
                        v: v.value(i),
                    });
                }
            }
        }

        println!("  file total rows: {total_in_file}");
        println!("  sort (display_symbol, t): {}", if sort_ok { "OK" } else { "*** VIOLATION ***" });
        println!("  {} rows: {}", ticker, rows.len());
        if rows.is_empty() {
            continue;
        }
        let vol_sum: f64 = rows.iter().map(|r| r.v).sum();
        println!("  vol_sum: {vol_sum}");
        let mut by_t = rows.iter().collect::<Vec<_>>();
        by_t.sort_by_key(|r| r.t_ns);
        let first = by_t.first().unwrap();
        let last = by_t.last().unwrap();
        println!(
            "  earliest_t: {} ohlcv: {},{},{},{},{}",
            first.t_ns, first.o, first.h, first.l, first.c, first.v
        );
        println!(
            "  latest_t: {} ohlcv: {},{},{},{},{}",
            last.t_ns, last.o, last.h, last.l, last.c, last.v
        );
    }

    Ok(())
}
