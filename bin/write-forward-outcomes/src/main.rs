//! `forward_outcomes` writer — B3 forward pass.
//!
//! ```text
//! cargo run --release --bin write-forward-outcomes -- [--from 2016-06-08] [--to 2016-12-30]
//!   [--bars data/bars_1m_raw] [--reference data/reference]
//!   [--out data/outputs] [--cursor data/_engine_state.sqlite] [--force]
//! ```
//!
//! Separate pass from `write-phase0` (the chronological trailing sweep):
//! `forward_outcomes` for entry day D looks FORWARD into D+1…D+5, the
//! opposite direction from the trailing windows. A ~6-day ring buffer of
//! full 1-minute sessions holds D plus its five following trading days;
//! when day D has five buffered successors it is emitted and dropped.
//!
//! Entry-day trailing context (atr_14d / yang_zhang_vol_14d / adv_20d /
//! addv_20d, used only by the entry-quality normalizers) is read back
//! from the already-validated `daily_observation[D]` (B1) output rather
//! than recomputed — so this pass needs no `RollingState`.
//!
//! Forward days are rescaled to D's split basis before any return is
//! taken (`ForwardDay::from_session`); the 705 splits inside the 2016
//! smoke window exercise that path.
//!
//! B3 milestone is `B3-partial`: this writes identity + entry pricing +
//! pre-entry + entry-quality + per-horizon core (ret / drawdown / runup /
//! close-extremes / bars-to-extreme) + excess-over-index for the eight
//! short horizons. Threshold crossings, day-0 segments, time-underwater,
//! next-day, gap-vs-RTH, and the hit_/first_event labels are typed nulls
//! until later B3 increments; 10d–252d and terminal events are B5.

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate, Utc};
use momentum_calendar::Calendar;
use momentum_core::phase0_outputs::{forward_outcomes_schema, forward_path_short_schema};
use momentum_engine::cursor::EngineCursor;
use momentum_engine::forward_outcomes::{self, EntryCtx, ForwardDay, ForwardInput};
use momentum_engine::forward_path;
use momentum_engine::stamps;
use momentum_store::bar_reader::{DaySession, MaterializedBarReader};
use momentum_store::figi_map::FigiMap;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

const FWD_TABLE: &str = "forward_outcomes";
const FWD_PATH_TABLE: &str = "forward_path_short";
/// All B3 short-horizon families filled (horizons, crossings, labels, day-0,
/// next-day, gap-vs-RTH, time-underwater, pre-entry ranks, cumulative volume).
/// B4 adds `forward_path_short` (long-format checkpoints) in the same pass.
/// B5 completes multi-day horizons + dividend totals + bar_gap + terminal events.
const FWD_MILESTONE: &str = "B3";
const FP_MILESTONE: &str = "B4";
/// Forward trading days needed to finalize a day's short horizons.
const FORWARD_DAYS: usize = 5;

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

/// One buffered trading day: every security's session plus the per-sid
/// `factor_at(day)` and sid→index lookup for the forward join.
struct BufferedDay {
    day: NaiveDate,
    sessions: Vec<DaySession>,
    session_close: DateTime<Utc>,
    factor: HashMap<String, f64>,
    sid_index: HashMap<String, usize>,
}

fn read_buffered(
    reader: &MaterializedBarReader,
    calendar: &Calendar,
    day: NaiveDate,
) -> Result<BufferedDay> {
    let sessions = reader.day_sessions(day)?;
    let session_close = calendar.session_close(day)?;
    let mut factor = HashMap::with_capacity(sessions.len());
    let mut sid_index = HashMap::with_capacity(sessions.len());
    for (i, s) in sessions.iter().enumerate() {
        let sid = s.security_id.as_str().to_string();
        factor.insert(sid.clone(), reader.adjustment_factor(&s.security_id, &s.display_symbol, day));
        sid_index.insert(sid, i);
    }
    Ok(BufferedDay { day, sessions, session_close, factor, sid_index })
}

/// Read the four entry-context columns from `daily_observation[D]`.
fn read_entry_ctx(path: &Path) -> Result<HashMap<String, EntryCtx>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut map: HashMap<String, EntryCtx> = HashMap::new();
    use arrow::array::{Array, Float64Array, StringArray};
    let optf = |a: &arrow::array::ArrayRef, i: usize| -> Option<f64> {
        let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
        (!a.is_null(i)).then(|| a.value(i))
    };
    for batch in reader {
        let b = batch?;
        let sid = b
            .column_by_name("security_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let atr = b.column_by_name("atr_14d").cloned();
        let yz = b.column_by_name("yang_zhang_vol_14d").cloned();
        let adv = b.column_by_name("adv_20d").cloned();
        let addv = b.column_by_name("addv_20d").cloned();
        let pmv = b.column_by_name("premarket_volume").cloned();
        let pmdv = b.column_by_name("premarket_dollar_volume").cloned();
        for i in 0..b.num_rows() {
            map.insert(
                sid.value(i).to_string(),
                EntryCtx {
                    atr_14d: atr.as_ref().and_then(|a| optf(a, i)),
                    yang_zhang_vol_14d: yz.as_ref().and_then(|a| optf(a, i)),
                    adv_20d: adv.as_ref().and_then(|a| optf(a, i)),
                    addv_20d: addv.as_ref().and_then(|a| optf(a, i)),
                    premarket_volume: pmv.as_ref().and_then(|a| optf(a, i)),
                    premarket_dollar_volume: pmdv.as_ref().and_then(|a| optf(a, i)),
                },
            );
        }
    }
    Ok(map)
}

/// Build one security's forward-day list from the ring buffer. All days
/// share the pin basis (see `ForwardDay::from_session`), so no rescaling.
/// Missing forward days are pushed as empty `ForwardDay`s so day-index
/// alignment (and thus the 1d…5d horizon mapping) is preserved.
fn build_fdays(sid: &str, buf: &VecDeque<BufferedDay>) -> Vec<ForwardDay> {
    let d0 = &buf[0];
    let idx0 = d0.sid_index[sid];
    let mut fdays = Vec::with_capacity(1 + FORWARD_DAYS);
    fdays.push(ForwardDay::from_session(
        d0.day,
        &d0.sessions[idx0].session.bars,
        d0.session_close,
    ));
    for k in 1..buf.len().min(1 + FORWARD_DAYS) {
        let bd = &buf[k];
        match bd.sid_index.get(sid) {
            Some(&idx) => fdays.push(ForwardDay::from_session(
                bd.day,
                &bd.sessions[idx].session.bars,
                bd.session_close,
            )),
            None => fdays.push(ForwardDay {
                day: bd.day,
                rth_bars: Vec::new(),
                session_close: bd.session_close,
            }),
        }
    }
    fdays
}

fn write_table(
    dir: &Path,
    day: NaiveDate,
    schema: arrow::datatypes::SchemaRef,
    batch: &arrow::array::RecordBatch,
    stamps_list: &[(String, String)],
    milestone: &str,
) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{day}.parquet"));
    let props = stamps::writer_props(stamps_list, milestone);
    let file = File::create(&path)?;
    let mut w = ArrowWriter::try_new(file, schema, Some(props))?;
    w.write(batch)?;
    w.close()?;
    Ok(())
}

/// Writes both forward tables for the front day; returns (fo_rows, fp_rows).
fn emit_day(buf: &VecDeque<BufferedDay>, out: &Path, fwd_dir: &Path, path_dir: &Path) -> Result<(usize, usize)> {
    let d0 = &buf[0];
    let day = d0.day;
    let ctx_path = out.join("daily_observation").join(format!("{day}.parquet"));
    let ctx_map = read_entry_ctx(&ctx_path).unwrap_or_default();

    let owned: Vec<Vec<ForwardDay>> = d0
        .sessions
        .iter()
        .map(|s| build_fdays(s.security_id.as_str(), buf))
        .collect();
    let inputs: Vec<ForwardInput> = d0
        .sessions
        .iter()
        .zip(&owned)
        .map(|(s, days)| {
            let sid = s.security_id.as_str();
            ForwardInput {
                security_id: sid,
                display_symbol: &s.display_symbol,
                days,
                entry_ctx: ctx_map.get(sid).copied().unwrap_or_default(),
                entry_day_factor: d0.factor.get(sid).copied().unwrap_or(1.0),
            }
        })
        .collect();

    let fo = forward_outcomes::build(day, &inputs)?;
    let fo_rows = fo.num_rows();
    write_table(fwd_dir, day, forward_outcomes_schema(), &fo, &stamps::forward_outcomes_stamps(), FWD_MILESTONE)?;

    let fp = forward_path::build(day, &inputs)?;
    let fp_rows = fp.num_rows();
    write_table(path_dir, day, forward_path_short_schema(), &fp, &stamps::forward_path_short_stamps(), FP_MILESTONE)?;

    Ok((fo_rows, fp_rows))
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = parse_args()?;
    let t0 = Instant::now();

    let figi = FigiMap::open(&args.reference.join("figi_map.parquet")).context("open figi_map")?;
    let reader =
        MaterializedBarReader::open(&args.bars, &args.reference.join("splits.parquet"), figi)
            .context("open bar reader")?;
    let calendar = Calendar::open(&args.bars).context("open calendar")?;
    let cursor = EngineCursor::open(&args.cursor)?;

    let all_days = calendar.trading_days().to_vec();
    let write_days: Vec<NaiveDate> = all_days
        .iter()
        .copied()
        .filter(|d| args.from.is_none_or(|f| *d >= f) && args.to.is_none_or(|t| *d <= t))
        .collect();
    if write_days.is_empty() {
        bail!("no trading days in range");
    }
    let write_set: HashSet<NaiveDate> = write_days.iter().copied().collect();
    let first = write_days[0];
    let last = *write_days.last().unwrap();

    // Read FORWARD_DAYS extra trading days past `to` so the last write
    // days get complete horizons (bars exist corpus-wide). At the true
    // corpus end the buffer simply runs out and tail days get nulls.
    // next_trading_day returns the day ON OR AFTER its arg, so advance
    // from read_end+1 to get the strictly-following trading day.
    let mut read_end = last;
    for _ in 0..FORWARD_DAYS {
        match read_end.succ_opt().and_then(|d| calendar.next_trading_day(d)) {
            Some(d) => read_end = d,
            None => break,
        }
    }
    let read_days: Vec<NaiveDate> = all_days
        .iter()
        .copied()
        .filter(|d| *d >= first && *d <= read_end)
        .collect();

    println!(
        "forward pass: write {first} → {last} ({} days), read through {read_end}, opened refs in {:.2?}",
        write_days.len(),
        t0.elapsed()
    );

    let fwd_dir = args.out.join(FWD_TABLE);
    let path_dir = args.out.join(FWD_PATH_TABLE);
    let mut buf: VecDeque<BufferedDay> = VecDeque::with_capacity(2 + FORWARD_DAYS);
    let mut written = 0usize;
    let mut skipped = 0usize;
    let mut fo_rows = 0usize;
    let mut fp_rows = 0usize;
    let sweep_t = Instant::now();

    let mut maybe_emit = |buf: &VecDeque<BufferedDay>| -> Result<()> {
        let day = buf[0].day;
        if !write_set.contains(&day) {
            return Ok(());
        }
        let done = cursor.is_done(FWD_TABLE, day)? && cursor.is_done(FWD_PATH_TABLE, day)?;
        if done && !args.force {
            skipped += 1;
            return Ok(());
        }
        let (fo, fp) = emit_day(buf, &args.out, &fwd_dir, &path_dir)?;
        cursor.mark_done(FWD_TABLE, day)?;
        cursor.mark_done(FWD_PATH_TABLE, day)?;
        written += 1;
        fo_rows += fo;
        fp_rows += fp;
        if written % 20 == 0 || written == 1 {
            println!("  {day}: fo={fo} fp={fp} rows ({written} written, {:.1?})", sweep_t.elapsed());
        }
        Ok(())
    };

    for rd in read_days {
        buf.push_back(read_buffered(&reader, &calendar, rd)?);
        while buf.len() >= 1 + FORWARD_DAYS {
            maybe_emit(&buf)?;
            buf.pop_front();
        }
    }
    // Flush the tail (partial forward windows near the corpus end).
    while !buf.is_empty() {
        maybe_emit(&buf)?;
        buf.pop_front();
    }

    println!(
        "done: {written} written, {skipped} skipped, forward_outcomes={fo_rows} rows, forward_path_short={fp_rows} rows, {:.1?}",
        sweep_t.elapsed()
    );
    Ok(())
}
