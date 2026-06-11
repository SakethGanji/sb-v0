//! Phase 0 writer engine driver — chronological sweep (B1).
//!
//! ```text
//! cargo run --release --bin write-phase0 -- [--from 2016-06-08] [--to 2026-06-05]
//!   [--bars data/bars_1m_raw] [--reference data/reference]
//!   [--out data/outputs] [--cursor data/_engine_state.sqlite] [--force]
//! ```
//!
//! Writes per day: `daily_observation/` (stamped `B1-partial` — earnings
//! proximity lands with B2's calendar join) and `market_context_daily/`
//! (stamped `B1`). Days run in calendar order because trailing state
//! (rolling windows, index cum-log returns for betas, the signal-share
//! history) accumulates during the sweep — the RFC §6.2 `[D-N, D-1]`
//! contract is enforced by reading features BEFORE pushing the day.
//!
//! Resume semantics: a day already marked done is still READ (its
//! aggregates feed later days' trailing state) but not rebuilt or
//! rewritten. `--from` later than the dataset start trades
//! trailing-window completeness for speed — fine for smoke runs, never
//! for the real run.

use anyhow::{Context, Result, bail};
use chrono::NaiveDate;
use momentum_calendar::Calendar;
use momentum_core::phase0_outputs::{daily_observation_schema, market_context_daily_schema};
use momentum_engine::cursor::EngineCursor;
use momentum_engine::daily_observation::{self, DayContext, RowInput};
use momentum_engine::earnings::EarningsLookup;
use momentum_engine::market_context::{self, IndexDay, UniverseSnapshot};
use momentum_engine::rolling::RollingState;
use momentum_engine::slices::{et, slice};
use momentum_engine::{aggregates, stamps};
use momentum_store::bar_reader::{DaySession, MaterializedBarReader};
use momentum_store::dividends::read_ex_dividend_dates;
use momentum_store::figi_map::FigiMap;
use momentum_store::vix::read_vix_closes;
use parquet::arrow::ArrowWriter;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;

const OBS_TABLE: &str = "daily_observation";
// "B2" once the derived earnings calendar is present; only
// days_to_next_known_earnings remains null (no scheduled-earnings feed —
// permanent for Phase 0, see momentum_engine::earnings module docs).
const OBS_MILESTONE_WITH_EARNINGS: &str = "B2";
const OBS_MILESTONE_NO_EARNINGS: &str = "B1-partial";
const CTX_TABLE: &str = "market_context_daily";
const CTX_MILESTONE: &str = "B1";
/// "Split nearby" = execution date within ±3 calendar days of D
/// (documented definition; the RFC leaves the window unspecified).
const SPLIT_NEARBY_CAL_DAYS: i64 = 3;

const INDEX_SYMBOLS: [&str; 3] = ["SPY", "QQQ", "IWM"];

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

fn write_table(
    dir: &PathBuf,
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
    // Second instance for rename probes — the first moves into the reader.
    let figi_probe = FigiMap::open(&args.reference.join("figi_map.parquet"))?;
    let reader = MaterializedBarReader::open(
        &args.bars,
        &args.reference.join("splits.parquet"),
        figi,
    )
    .context("open bar reader")?;
    let calendar = Calendar::open(&args.bars).context("open calendar")?;
    let cursor = EngineCursor::open(&args.cursor)?;
    let dividends = match read_ex_dividend_dates(&args.reference.join("dividends.parquet")) {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::warn!(%e, "dividends.parquet unavailable — dividend_event_today will be null");
            None
        }
    };
    let vix = match read_vix_closes(&args.reference.join("vix_daily.parquet")) {
        Ok(m) => Some(m),
        Err(e) => {
            tracing::warn!(%e, "vix_daily.parquet unavailable — vix_close will be null");
            None
        }
    };
    let earnings_path = args.out.join("earnings_calendar.parquet");
    let earnings = match EarningsLookup::open(&earnings_path) {
        Ok(lk) => {
            println!("earnings calendar: {} securities", lk.securities());
            Some(lk)
        }
        Err(e) => {
            tracing::warn!(%e, path = %earnings_path.display(),
                "earnings_calendar.parquet unavailable — run build-earnings-calendar first; earnings columns will be null");
            None
        }
    };
    let obs_milestone = if earnings.is_some() {
        OBS_MILESTONE_WITH_EARNINGS
    } else {
        OBS_MILESTONE_NO_EARNINGS
    };

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

    let obs_dir = args.out.join(OBS_TABLE);
    let ctx_dir = args.out.join(CTX_TABLE);
    let mut rolling = RollingState::new();
    // Index cum-log close returns, one map per [SPY, QQQ, IWM].
    let mut index_cumlog: [HashMap<NaiveDate, f64>; 3] = Default::default();
    let mut index_cum: [f64; 3] = [0.0; 3];
    let mut signal_share_history: Vec<f64> = Vec::with_capacity(days.len());
    let mut written = 0usize;
    let mut skipped = 0usize;
    let sweep_t = Instant::now();

    for (i, day) in days.iter().copied().enumerate() {
        let sessions = reader.day_sessions(day)?;
        let session_close = calendar.session_close(day)?;
        let factors: Vec<f64> = sessions
            .iter()
            .map(|s| reader.adjustment_factor(&s.security_id, &s.display_symbol, day))
            .collect();
        let aggs: Vec<_> = sessions
            .iter()
            .zip(&factors)
            .map(|(s, f)| aggregates::compute(&s.session, day, session_close, *f))
            .collect();

        let obs_done = cursor.is_done(OBS_TABLE, day)? && !args.force;
        let ctx_done = cursor.is_done(CTX_TABLE, day)? && !args.force;

        if !obs_done {
            let rows: Vec<RowInput<'_>> = sessions
                .iter()
                .zip(&aggs)
                .zip(&factors)
                .map(|((s, agg), f)| RowInput {
                    session: s,
                    agg: *agg,
                    adjustment_factor: *f,
                    dividend_event_today: dividends.as_ref().map(|set| {
                        set.contains(&(s.security_id.as_str().to_string(), day))
                            || set.contains(&(s.display_symbol.clone(), day))
                    }),
                    ticker_event_today: Some(figi_probe.renamed_on(&s.security_id, day)),
                    split_event_nearby: Some(reader.split_event_within(
                        &s.security_id,
                        &s.display_symbol,
                        day,
                        SPLIT_NEARBY_CAL_DAYS,
                    )),
                    days_since_last_earnings: earnings
                        .as_ref()
                        .and_then(|lk| lk.days_since_last(s.security_id.as_str(), day)),
                    is_earnings_day: earnings
                        .as_ref()
                        .map(|lk| lk.on_day(s.security_id.as_str(), day).0),
                    earnings_report_timing: earnings
                        .as_ref()
                        .and_then(|lk| lk.on_day(s.security_id.as_str(), day).1.map(String::from)),
                })
                .collect();
            let day_ctx = DayContext {
                index_cumlog: [&index_cumlog[0], &index_cumlog[1], &index_cumlog[2]],
                signal_share_history: &signal_share_history,
            };
            let batch = daily_observation::build(day, &rows, session_close, &rolling, &day_ctx)?;
            write_table(
                &obs_dir,
                day,
                daily_observation_schema(),
                &batch,
                &stamps::daily_observation_stamps(),
                obs_milestone,
            )?;
            cursor.mark_done(OBS_TABLE, day)?;
            written += 1;
        } else {
            skipped += 1;
        }

        if !ctx_done {
            let universe: Vec<UniverseSnapshot> = sessions
                .iter()
                .zip(&aggs)
                .map(|(s, agg)| {
                    let hist = rolling.get(s.security_id.as_str());
                    let bars = &s.session.bars;
                    let rth_open_t = et(day, 9, 30);
                    let ret_1030 = slice(bars, rth_open_t, et(day, 10, 30))
                        .last()
                        .and_then(|l| {
                            agg.map(|a| a.rth_open)
                                .filter(|o| *o > 0.0)
                                .map(|o| l.close / o - 1.0)
                        });
                    let ret_1000 = agg.and_then(|a| a.snapshot_ret_1000);
                    let px_1000 = agg
                        .map(|a| a.rth_open)
                        .zip(ret_1000)
                        .map(|(o, r)| o * (1.0 + r));
                    UniverseSnapshot {
                        ret_0930_to_1000: ret_1000,
                        ret_0930_to_1030: ret_1030,
                        eod_intraday_return: agg
                            .filter(|a| a.rth_open > 0.0)
                            .map(|a| a.rth_close / a.rth_open - 1.0),
                        above_premarket_vwap_at_1000: px_1000
                            .zip(agg.and_then(|a| a.premarket_vwap))
                            .map(|(p, v)| p > v),
                        move_vs_atr14_at_1000: px_1000
                            .zip(agg.map(|a| a.rth_open))
                            .zip(hist.and_then(|h| h.atr(14)).filter(|a| *a > 0.0))
                            .map(|((p, o), atr)| (p - o).abs() / atr),
                        rth_dollar_volume: agg.map(|a| a.rth_dollar_volume),
                        addv_20d: hist.and_then(|h| h.addv(20)),
                    }
                })
                .collect();

            let index_day = |sym: &str| -> Option<IndexDay<'_>> {
                let pos = sessions.iter().position(|s| s.display_symbol == sym)?;
                let s: &DaySession = &sessions[pos];
                let agg = aggs[pos]?;
                let hist = rolling.get(s.security_id.as_str());
                let prior_close = hist.and_then(|h| h.prior()).map(|p| p.rth_close);
                Some(IndexDay {
                    bars: &s.session.bars,
                    eod_open: Some(agg.rth_open),
                    eod_high: Some(agg.rth_high),
                    eod_low: Some(agg.rth_low),
                    eod_close: Some(agg.rth_close),
                    eod_volume: Some(agg.rth_volume),
                    overnight_gap: prior_close
                        .filter(|pc| *pc > 0.0)
                        .map(|pc| agg.rth_open / pc - 1.0),
                    ret_0930_to_1000: agg.snapshot_ret_1000,
                    realized_vol_21d: hist.and_then(|h| h.realized_vol(21)),
                })
            };
            let indices = [
                index_day(INDEX_SYMBOLS[0]),
                index_day(INDEX_SYMBOLS[1]),
                index_day(INDEX_SYMBOLS[2]),
            ];
            let batch = market_context::build(
                day,
                session_close,
                indices,
                &universe,
                vix.as_ref().and_then(|m| m.get(&day)).copied(),
            )?;
            write_table(
                &ctx_dir,
                day,
                market_context_daily_schema(),
                &batch,
                &stamps::market_context_daily_stamps(),
                CTX_MILESTONE,
            )?;
            cursor.mark_done(CTX_TABLE, day)?;
        }

        // ---- Push AFTER building: trailing reads stay [D-N, D-1]. ----
        // Index cum-log returns (uses rolling's prior close, pre-update).
        for (k, sym) in INDEX_SYMBOLS.iter().enumerate() {
            if let Some(pos) = sessions.iter().position(|s| s.display_symbol == *sym)
                && let Some(agg) = &aggs[pos]
            {
                if let Some(pc) = rolling
                    .get(sessions[pos].security_id.as_str())
                    .and_then(|h| h.prior())
                    .map(|p| p.rth_close)
                    .filter(|pc| *pc > 0.0)
                {
                    index_cum[k] += (agg.rth_close / pc).ln();
                }
                index_cumlog[k].insert(day, index_cum[k]);
            }
        }
        // Universe signal share (same definition as the builder's).
        let valid = aggs.iter().flatten().filter(|a| a.snapshot_ret_1000.is_some()).count();
        let fired = aggs
            .iter()
            .flatten()
            .filter(|a| a.snapshot_ret_1000.is_some_and(|r| r > 0.0))
            .count();
        if valid > 0 {
            signal_share_history.push(fired as f64 / valid as f64);
        }
        // Per-security rolling state.
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
        "done: {written} obs written, {skipped} skipped, {} securities tracked, total {:.1} min",
        rolling.len(),
        sweep_t.elapsed().as_secs_f64() / 60.0
    );
    Ok(())
}
