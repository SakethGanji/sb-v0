//! Phase 0 writer engine driver (B0: one day at a time, with timing).
//!
//! ```text
//! cargo run --release --bin write-phase0 -- --day 2021-03-15
//!   [--bars data/bars_1m_raw] [--reference data/reference]
//!   [--out data/outputs] [--cursor data/_engine_state.sqlite] [--force]
//! ```
//!
//! B0 writes `daily_observation/YYYY-MM-DD.parquet` only (partial columns,
//! stamped `engine_milestone = B0-skeleton`) and prints the per-stage
//! timing + bytes that drive the full-run runtime/disk forecast
//! (implementation-plan.md, milestone B0).

use anyhow::{Context, Result, bail};
use chrono::NaiveDate;
use momentum_calendar::Calendar;
use momentum_core::phase0_outputs::daily_observation_schema;
use momentum_engine::cursor::EngineCursor;
use momentum_engine::{daily_observation, stamps};
use momentum_store::bar_reader::MaterializedBarReader;
use momentum_store::figi_map::FigiMap;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;

const TABLE: &str = "daily_observation";
const MILESTONE: &str = "B0-skeleton";

struct Args {
    day: NaiveDate,
    bars: PathBuf,
    reference: PathBuf,
    out: PathBuf,
    cursor: PathBuf,
    force: bool,
}

fn parse_args() -> Result<Args> {
    let mut day = None;
    let mut bars = PathBuf::from("data/bars_1m_raw");
    let mut reference = PathBuf::from("data/reference");
    let mut out = PathBuf::from("data/outputs");
    let mut cursor = PathBuf::from("data/_engine_state.sqlite");
    let mut force = false;

    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        let mut val = |name: &str| {
            it.next().with_context(|| format!("{name} needs a value"))
        };
        match a.as_str() {
            "--day" => day = Some(NaiveDate::parse_from_str(&val("--day")?, "%Y-%m-%d")?),
            "--bars" => bars = PathBuf::from(val("--bars")?),
            "--reference" => reference = PathBuf::from(val("--reference")?),
            "--out" => out = PathBuf::from(val("--out")?),
            "--cursor" => cursor = PathBuf::from(val("--cursor")?),
            "--force" => force = true,
            other => bail!("unknown arg: {other}"),
        }
    }
    Ok(Args {
        day: day.context("--day YYYY-MM-DD is required")?,
        bars,
        reference,
        out,
        cursor,
        force,
    })
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = parse_args()?;
    let day = args.day;

    let cursor = EngineCursor::open(&args.cursor)?;
    if cursor.is_done(TABLE, day)? && !args.force {
        println!("{TABLE} {day}: already done (use --force to rewrite)");
        return Ok(());
    }

    // --- Stage 1: open reference data ---------------------------------
    let t0 = Instant::now();
    let figi = FigiMap::open(&args.reference.join("figi_map.parquet"))
        .context("open figi_map.parquet")?;
    let reader = MaterializedBarReader::open(
        &args.bars,
        &args.reference.join("splits.parquet"),
        figi,
    )
    .context("open bar reader")?;
    let calendar = Calendar::open(&args.bars).context("open calendar")?;
    if !calendar.has_day(day) {
        bail!("{day} is not a trading day in {}", args.bars.display());
    }
    let t_open = t0.elapsed();

    // --- Stage 2: bulk-read the day ------------------------------------
    let t1 = Instant::now();
    let sessions = reader.day_sessions(day)?;
    let total_bars: usize = sessions.iter().map(|s| s.session.len()).sum();
    let t_read = t1.elapsed();

    // --- Stage 3: build the batch ---------------------------------------
    let t2 = Instant::now();
    let session_close = calendar.session_close(day)?;
    let batch = daily_observation::build_b0(day, &sessions, session_close)?;
    let t_build = t2.elapsed();

    // --- Stage 4: write + stamp ------------------------------------------
    let t3 = Instant::now();
    let dir = args.out.join(TABLE);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{day}.parquet"));
    {
        let props = stamps::writer_props(&stamps::daily_observation_stamps(), MILESTONE);
        let file = File::create(&path)?;
        let mut w = ArrowWriter::try_new(file, daily_observation_schema(), Some(props))?;
        w.write(&batch)?;
        w.close()?;
    }
    let bytes = std::fs::metadata(&path)?.len();
    let t_write = t3.elapsed();

    cursor.mark_done(TABLE, day)?;

    // --- Report -----------------------------------------------------------
    let n_days = calendar.trading_days().len();
    let per_day = t_read + t_build + t_write;
    println!("{TABLE} {day} [{MILESTONE}] → {}", path.display());
    println!("  securities: {}   bars: {}", sessions.len(), total_bars);
    println!(
        "  open refs: {:.2?}   read: {:.2?}   build: {:.2?}   write: {:.2?}",
        t_open, t_read, t_build, t_write
    );
    println!("  file size: {:.1} MiB", bytes as f64 / (1024.0 * 1024.0));
    println!(
        "  forecast over {n_days} trading days: {:.1} h compute, {:.0} GiB for this table",
        per_day.as_secs_f64() * n_days as f64 / 3600.0,
        bytes as f64 * n_days as f64 / (1024.0 * 1024.0 * 1024.0),
    );
    Ok(())
}
