//! Phase 0 writer engine driver — chronological sweep (B1).
//!
//! ```text
//! cargo run --release --bin write-phase0 -- [--from 2016-06-08] [--to 2026-06-05]
//!   [--bars data/bars_1m_raw] [--reference data/reference]
//!   [--out data/outputs] [--cursor data/_engine_state.sqlite] [--force]
//! ```
//!
//! Days run in calendar order because trailing state (ATR, ADV, 52w
//! range, first-bar map) accumulates during the sweep — the RFC §6.2
//! `[D-N, D-1]` contract is enforced by reading features BEFORE pushing
//! the day into [`momentum_engine::rolling::RollingState`].
//!
//! Resume semantics: a day already marked done in the cursor is still
//! READ (its aggregates feed later days' trailing windows) but not
//! rebuilt or rewritten. `--from` later than the dataset start trades
//! trailing-window completeness for speed — fine for smoke runs, never
//! for the real run (trailing columns near `--from` are null/short
//! exactly like the true dataset start).

use anyhow::{Context, Result, bail};
use chrono::NaiveDate;
use momentum_calendar::Calendar;
use momentum_core::phase0_outputs::daily_observation_schema;
use momentum_engine::cursor::EngineCursor;
use momentum_engine::daily_observation::{self, RowInput};
use momentum_engine::rolling::RollingState;
use momentum_engine::{aggregates, stamps};
use momentum_store::bar_reader::MaterializedBarReader;
use momentum_store::figi_map::FigiMap;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;

const TABLE: &str = "daily_observation";
const MILESTONE: &str = "B1-partial";

struct Args {
    from: Option<NaiveDate>,
    to: Option<NaiveDate>,
    bars: PathBuf,
    reference: PathBuf,
    out: PathBuf,
    cursor: PathBuf,
    force: bool,
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        from: None,
        to: None,
        bars: PathBuf::from("data/bars_1m_raw"),
        reference: PathBuf::from("data/reference"),
        out: PathBuf::from("data/outputs"),
        cursor: PathBuf::from("data/_engine_state.sqlite"),
        force: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        let mut val = |name: &str| it.next().with_context(|| format!("{name} needs a value"));
        match a.as_str() {
            "--from" => args.from = Some(NaiveDate::parse_from_str(&val("--from")?, "%Y-%m-%d")?),
            "--to" => args.to = Some(NaiveDate::parse_from_str(&val("--to")?, "%Y-%m-%d")?),
            "--day" => {
                let d = NaiveDate::parse_from_str(&val("--day")?, "%Y-%m-%d")?;
                args.from = Some(d);
                args.to = Some(d);
            }
            "--bars" => args.bars = PathBuf::from(val("--bars")?),
            "--reference" => args.reference = PathBuf::from(val("--reference")?),
            "--out" => args.out = PathBuf::from(val("--out")?),
            "--cursor" => args.cursor = PathBuf::from(val("--cursor")?),
            "--force" => args.force = true,
            other => bail!("unknown arg: {other}"),
        }
    }
    Ok(args)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = parse_args()?;

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
    let cursor = EngineCursor::open(&args.cursor)?;

    let days: Vec<NaiveDate> = calendar
        .trading_days()
        .iter()
        .copied()
        .filter(|d| args.from.is_none_or(|f| *d >= f) && args.to.is_none_or(|t| *d <= t))
        .collect();
    if days.is_empty() {
        bail!("no trading days in range");
    }
    if args.from.is_some() && days[0] != calendar.trading_days()[0] {
        tracing::warn!(
            from = %days[0],
            "sweep does not start at dataset start — trailing windows near --from will be null/short"
        );
    }
    println!(
        "sweep: {} → {} ({} trading days), opened refs in {:.2?}",
        days[0],
        days[days.len() - 1],
        days.len(),
        t0.elapsed()
    );

    let dir = args.out.join(TABLE);
    std::fs::create_dir_all(&dir)?;
    let mut rolling = RollingState::new();
    let mut written = 0usize;
    let mut skipped = 0usize;
    let sweep_t = Instant::now();

    for (i, day) in days.iter().copied().enumerate() {
        let sessions = reader.day_sessions(day)?;
        let session_close = calendar.session_close(day)?;
        let aggs: Vec<_> = sessions
            .iter()
            .map(|s| aggregates::compute(&s.session, day, session_close))
            .collect();

        let done = cursor.is_done(TABLE, day)? && !args.force;
        if done {
            skipped += 1;
        } else {
            let rows: Vec<RowInput<'_>> = sessions
                .iter()
                .zip(&aggs)
                .map(|(s, agg)| RowInput {
                    session: s,
                    agg: *agg,
                    adjustment_factor: reader.adjustment_factor(
                        &s.security_id,
                        &s.display_symbol,
                        day,
                    ),
                })
                .collect();
            let batch = daily_observation::build(day, &rows, session_close, &rolling)?;

            let path = dir.join(format!("{day}.parquet"));
            let props = stamps::writer_props(&stamps::daily_observation_stamps(), MILESTONE);
            let file = File::create(&path)?;
            let mut w = ArrowWriter::try_new(file, daily_observation_schema(), Some(props))?;
            w.write(&batch)?;
            w.close()?;
            cursor.mark_done(TABLE, day)?;
            written += 1;
        }

        // Push AFTER building: trailing reads stay [D-N, D-1].
        for (s, agg) in sessions.iter().zip(&aggs) {
            if let Some(agg) = agg {
                rolling.update(s.security_id.as_str(), day, *agg);
            }
        }

        if (i + 1) % 50 == 0 || i + 1 == days.len() {
            let el = sweep_t.elapsed().as_secs_f64();
            println!(
                "  [{}/{}] {}  written={} skipped={}  {:.2} s/day  ETA {:.1} min",
                i + 1,
                days.len(),
                day,
                written,
                skipped,
                el / (i + 1) as f64,
                el / (i + 1) as f64 * (days.len() - i - 1) as f64 / 60.0,
            );
        }
    }

    println!(
        "done: {written} written, {skipped} skipped, {} securities tracked, total {:.1} min",
        rolling.len(),
        sweep_t.elapsed().as_secs_f64() / 60.0
    );
    Ok(())
}
