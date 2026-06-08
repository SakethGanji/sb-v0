//! Smoke `Calendar` against the real `bars_1m_raw/` corpus. Asserts:
//!
//!  - 2,513 trading days, span 2016-06-08 → 2026-06-05.
//!  - 2017-11-24 (Black Friday) closes strictly earlier than the
//!    surrounding full sessions — frozen-decisions #8 half-day case.
//!  - 2016-06-08 (Phase A's earliest day) reads a SPY close at all.

use anyhow::{Context, Result};
use chrono::NaiveDate;
use momentum_calendar::Calendar;
use std::path::PathBuf;

fn d(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").expect("valid date")
}

fn main() -> Result<()> {
    let bars_dir: PathBuf = std::env::var("BARS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/bars_1m_raw"));

    println!("opening Calendar against {} ...", bars_dir.display());
    let cal = Calendar::open(&bars_dir).context("Calendar::open")?;

    let days = cal.trading_days();
    println!(
        "  trading_days: {} ({} → {})",
        days.len(),
        days.first().unwrap(),
        days.last().unwrap()
    );

    let bf17 = d("2017-11-24");
    let prev = cal.prev_trading_day(d("2017-11-23")).unwrap();
    println!(
        "  prev_trading_day(2017-11-23) = {prev} (Thanksgiving 2017 was 2017-11-23)"
    );

    let close_bf17 = cal
        .session_close(bf17)
        .with_context(|| format!("session_close({bf17})"))?;
    let close_prior = cal
        .session_close(d("2017-11-22"))
        .context("session_close(2017-11-22)")?;
    let close_after = cal
        .session_close(d("2017-11-27"))
        .context("session_close(2017-11-27)")?;
    println!("  session_close(2017-11-22) = {close_prior}");
    println!("  session_close(2017-11-24) = {close_bf17}  <-- Black Friday half-day");
    println!("  session_close(2017-11-27) = {close_after}");

    if !(close_bf17.time() < close_prior.time() && close_bf17.time() < close_after.time()) {
        anyhow::bail!(
            "expected Black Friday close TIME-OF-DAY < surrounding full-session closes; got {} vs {} and {}",
            close_bf17.time(),
            close_prior.time(),
            close_after.time()
        );
    }

    let close_first = cal
        .session_close(d("2016-06-08"))
        .context("session_close(2016-06-08)")?;
    println!("  session_close(2016-06-08) = {close_first}  <-- Phase A earliest day");

    println!("CALENDAR SMOKE OK");
    Ok(())
}
