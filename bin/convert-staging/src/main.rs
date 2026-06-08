//! Drive `momentum_store::daily_convert::convert_one_day` across every
//! staged `_staging/flat_files/YYYY/MM/YYYY-MM-DD.csv.gz` into
//! `bars_1m_raw/YYYY-MM-DD.parquet`. Bounded concurrency, resumable
//! (skips outputs that already exist).
//!
//! Optional env with defaults:
//!   STAGING_DIR=data/_staging/flat_files
//!   OUT_DIR=data/bars_1m_raw
//!   TICKERS_PATH=data/_smoke/reference/tickers_enriched.parquet
//!   CONCURRENCY=16

use anyhow::{Context, Result};
use momentum_store::daily_convert::convert_one_day;
use momentum_store::tickers::read_security_id_map;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

fn walk_csv_gz(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    walk(root, &mut out)?;
    out.sort();
    Ok(out)
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.is_dir() {
            walk(&p, out)?;
        } else if p.extension().and_then(|s| s.to_str()) == Some("gz") {
            out.push(p);
        }
    }
    Ok(())
}

fn out_path_for(in_path: &Path, out_dir: &Path) -> Option<PathBuf> {
    // `2024-01-03.csv.gz` → `2024-01-03.parquet`
    let stem = in_path.file_name()?.to_str()?;
    let date = stem.strip_suffix(".csv.gz")?;
    Some(out_dir.join(format!("{date}.parquet")))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let staging: PathBuf = std::env::var("STAGING_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/_staging/flat_files"));
    let out_dir: PathBuf = std::env::var("OUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/bars_1m_raw"));
    let tickers_path: PathBuf = std::env::var("TICKERS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/_smoke/reference/tickers_enriched.parquet"));
    let concurrency: usize = std::env::var("CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);

    if !staging.exists() {
        anyhow::bail!("STAGING_DIR does not exist: {}", staging.display());
    }
    if !tickers_path.exists() {
        anyhow::bail!(
            "TICKERS_PATH does not exist: {} — run build-universe first",
            tickers_path.display()
        );
    }
    std::fs::create_dir_all(&out_dir)?;

    println!("loading FIGI map from {} ...", tickers_path.display());
    let figi: Arc<HashMap<String, String>> = Arc::new(
        read_security_id_map(&tickers_path)
            .with_context(|| format!("read_security_id_map({})", tickers_path.display()))?,
    );
    println!("  FIGI map size: {}", figi.len());

    println!("scanning {} ...", staging.display());
    let inputs = walk_csv_gz(&staging)?;
    println!("  found {} csv.gz files", inputs.len());
    println!("out_dir:     {}", out_dir.display());
    println!("concurrency: {concurrency}");
    println!();

    let total = inputs.len();
    let done = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let rows = Arc::new(AtomicUsize::new(0));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

    let started = Instant::now();
    let mut handles = Vec::with_capacity(total);
    for in_path in inputs {
        let Some(out_path) = out_path_for(&in_path, &out_dir) else {
            tracing::warn!(path = %in_path.display(), "skipping: unparseable filename");
            continue;
        };
        let permit = semaphore.clone().acquire_owned().await?;
        let figi = figi.clone();
        let done = done.clone();
        let skipped = skipped.clone();
        let rows = rows.clone();

        handles.push(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            // Resumable: skip if output already exists and is non-empty.
            if let Ok(meta) = std::fs::metadata(&out_path) {
                if meta.len() > 0 {
                    skipped.fetch_add(1, Ordering::Relaxed);
                    let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                    if d % 100 == 0 {
                        println!("  [{d}/{total}] skipped + done");
                    }
                    return Ok::<(), anyhow::Error>(());
                }
            }
            let file = std::fs::File::open(&in_path)
                .with_context(|| format!("open {}", in_path.display()))?;
            let n = convert_one_day(file, &out_path, &figi)
                .with_context(|| format!("convert {}", in_path.display()))?;
            rows.fetch_add(n, Ordering::Relaxed);
            let d = done.fetch_add(1, Ordering::Relaxed) + 1;
            if d % 50 == 0 {
                println!("  [{d}/{total}] {} rows written so far", rows.load(Ordering::Relaxed));
            }
            Ok(())
        }));
    }

    let mut failed = 0usize;
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!(error = %e, "convert failed");
                failed += 1;
            }
            Err(e) => {
                tracing::error!(error = %e, "task join failed");
                failed += 1;
            }
        }
    }

    let elapsed = started.elapsed();
    let mut total_size: u64 = 0;
    if let Ok(read) = std::fs::read_dir(&out_dir) {
        for entry in read.flatten() {
            if let Ok(meta) = entry.metadata() {
                total_size += meta.len();
            }
        }
    }

    println!();
    println!(
        "DONE: {} done ({} skipped resumed), {} failed, {} rows, {:.2} GB out, in {:.1} min",
        done.load(Ordering::Relaxed),
        skipped.load(Ordering::Relaxed),
        failed,
        rows.load(Ordering::Relaxed),
        total_size as f64 / 1e9,
        elapsed.as_secs_f64() / 60.0,
    );
    if failed > 0 {
        anyhow::bail!("{failed} day(s) failed — see logs above");
    }
    Ok(())
}
