#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow python3
"""Independent cross-validation of Phase 0 engine outputs (v1 — the
task-#8 validation battery).

A second implementation in polars, written from the column definitions,
recomputing engine outputs from the raw tape and diffing. Catches
correlated author errors the Rust unit tests can't.

Sections (per stress day unless noted):
  A  daily_observation day-local columns, full cross-section
  B  overnight context vs prior day's raw tape
  C  cross-table identities (market_context / sector vs obs)
  D  trailing windows, sampled symbols, FULL-history exact recompute
     (ATR, ADV/ADDV, realized vol, Yang-Zhang, beta, 52w, days-since-
     move, consecutive-up, premarket median + spike, signal-first)
  E  earnings proximity recount (AAPL)
  F  remaining B1 day-local: snapshots 0940/1010/1030, shape
     descriptors, ranks/percentiles, gap-filled, signal concentration
  G  market_context: index columns + SPY 10m list contents + breadth
  H  sector_aggregates via an independent SIC port
  I  security_classification: caps, buckets, ranks, behavioral-tag port
  J  regime_definitions: tertile + era recompute, stamps (once)
  K  property sweep over ALL days (bounds, bijectivity, stamps; once)

Adjudicated semantics encoded here (do not "fix" without re-adjudicating):
RTH includes the 16:00 auction print; session membership is by ET date;
sid-first split factors with symbol fallback; volumes uniformly
pin-adjusted; vendor sid-collision rows carry no trailing state;
.TEST / premarket-only securities belong in the universe.

Usage: scripts/validate_phase0_outputs.py [DAY ...]
       (default battery: 2016-12-30 2016-11-25 2016-11-07 2016-06-09)
"""

import math
import sys
from datetime import date, time
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "outputs"
BARS = ROOT / "data" / "bars_1m_raw"
REF = ROOT / "data" / "reference"

G, R, Y, X = "\033[32m", "\033[31m", "\033[33m", "\033[0m"
REL = 1e-9


class Reporter:
    def __init__(self):
        self.passes = self.fails = 0
        self.fail_msgs = []

    def check(self, name, ok, detail=""):
        if ok:
            self.passes += 1
            print(f"  {G}PASS{X} {name} {detail}")
        else:
            self.fails += 1
            self.fail_msgs.append(f"{name}: {detail}")
            print(f"  {R}FAIL{X} {name} {detail}")


r = Reporter()


def close_enough(a, b, tol=REL):
    """Null-aware compare: both-null passes, one-null fails."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) <= tol * max(abs(a), abs(b), 1.0)


def mismatch_count(df, a, b, tol=REL):
    lhs = (pl.col(a) - pl.col(b)).abs() > tol * pl.max_horizontal(
        pl.col(a).abs(), pl.col(b).abs(), pl.lit(1.0)
    )
    one_null = pl.col(a).is_null() != pl.col(b).is_null()
    return df.filter(lhs.fill_null(False) | one_null).height


# ---------------------------------------------------------------------------
# Raw-tape recomputation primitives
# ---------------------------------------------------------------------------

def load_day_bars(d: date) -> pl.DataFrame:
    df = pl.read_parquet(BARS / f"{d}.parquet")
    return (
        df.with_columns(
            pl.col("t").dt.convert_time_zone("America/New_York").dt.time().alias("et_time"),
            pl.col("t").dt.convert_time_zone("America/New_York").dt.date().alias("et_date"),
        )
        .filter(pl.col("et_date") == d)  # session membership = ET date (adjudicated)
        .with_columns(
            pl.coalesce(pl.col("security_id"), pl.col("display_symbol")).alias("sid_key")
        )
    )


def spy_session_close(bars: pl.DataFrame) -> time:
    """Data-derived session close = max SPY bar time ≤ 16:00 ET (the
    engine's calendar contract; 13:00 on half days)."""
    spy = bars.filter(
        (pl.col("display_symbol") == "SPY") & (pl.col("et_time") <= time(16, 0))
    )
    return spy["et_time"].max()


def session_windows(bars: pl.DataFrame, close_t: time) -> pl.DataFrame:
    """Per-(sid, symbol) UNADJUSTED aggregates. Universe = any session
    bar; RTH = [09:30, session close] inclusive of the auction print."""
    rth = bars.filter((pl.col("et_time") >= time(9, 30)) & (pl.col("et_time") <= close_t))
    pm = bars.filter((pl.col("et_time") >= time(4, 0)) & (pl.col("et_time") < time(9, 30)))
    pre1000 = rth.filter(pl.col("et_time") < time(10, 0))

    keys = ["sid_key", "display_symbol"]
    universe = bars.group_by(keys).agg(pl.len().alias("n_bars"))
    agg = rth.group_by(keys).agg(
        pl.col("open").first().alias("rth_open"),
        pl.col("high").max().alias("rth_high"),
        pl.col("low").min().alias("rth_low"),
        pl.col("close").last().alias("rth_close"),
        pl.col("volume").sum().alias("rth_volume"),
        pl.len().alias("bar_count_rth"),
        pl.col("et_time").first().alias("first_rth_time"),
    )
    agg = universe.join(agg, on=keys, how="left")
    agg = agg.join(
        pre1000.group_by(keys).agg(pl.col("close").last().alias("close_1000")),
        on=keys, how="left",
    )
    agg = agg.join(
        pm.group_by(keys).agg(pl.col("volume").sum().alias("pm_volume")),
        on=keys, how="left",
    )
    return agg.with_columns(
        (pl.col("close_1000") / pl.col("rth_open") - 1.0).alias("ret_0930_1000"),
        pl.col("pm_volume").fill_null(0.0),
    )


def pin_factor_maps(d: date):
    """sid-first / symbol-fallback split factors (adjudicated)."""
    pin = date.fromisoformat(
        pq.ParquetFile(REF / "splits.parquet").metadata.metadata[b"splits_snapshot_date"].decode()
    )
    sp = pl.read_parquet(REF / "splits.parquet").filter(
        (pl.col("execution_date") <= pin) & (pl.col("execution_date") > d)
    )
    bysym = sp.group_by("display_symbol").agg(
        (pl.col("split_from") / pl.col("split_to")).product().alias("sym_factor")
    )
    bysid = sp.filter(pl.col("security_id").is_not_null()).group_by("security_id").agg(
        (pl.col("split_from") / pl.col("split_to")).product().alias("sid_factor")
    )
    return bysym, bysid


def adjusted_raw(d: date):
    """session_windows + factor + adjusted columns for one day."""
    bars = load_day_bars(d)
    close_t = spy_session_close(bars)
    raw = session_windows(bars, close_t)
    bysym, bysid = pin_factor_maps(d)
    raw = (
        raw.join(bysym, on="display_symbol", how="left")
        .join(bysid, left_on="sid_key", right_on="security_id", how="left")
        .with_columns(
            pl.coalesce(pl.col("sid_factor"), pl.col("sym_factor"), pl.lit(1.0)).alias("factor")
        )
        .with_columns(
            (pl.col("rth_open") * pl.col("factor")).alias("adj_open"),
            (pl.col("rth_high") * pl.col("factor")).alias("adj_high"),
            (pl.col("rth_low") * pl.col("factor")).alias("adj_low"),
            (pl.col("rth_close") * pl.col("factor")).alias("adj_close"),
            (pl.col("rth_volume") / pl.col("factor")).alias("adj_volume"),
            (pl.col("pm_volume") / pl.col("factor")).alias("adj_pm_volume"),
        )
    )
    return raw, close_t, bars


def all_trading_days():
    return sorted(date.fromisoformat(p.stem) for p in (OUT / "daily_observation").glob("*.parquet"))


def collision_sids(raw: pl.DataFrame) -> set:
    return set(raw.group_by("sid_key").len().filter(pl.col("len") > 1)["sid_key"].to_list())


# SIC → sector/industry: independent python port of the documented v1
# ranges (crates/momentum-engine/src/sic.rs). Specific codes are listed
# before their containing ranges; first match wins.
SIC_RULES = [
    ((2834, 2835), "Healthcare", "Pharmaceuticals"),
    ((2836, 2836), "Healthcare", "Biotechnology"),
    ((3570, 3579), "Technology", "Computer Hardware"),
    ((3674, 3674), "Technology", "Semiconductors"),
    ((3711, 3716), "Consumer Discretionary", "Automobiles"),
    ((3841, 3851), "Healthcare", "Medical Devices"),
    ((5400, 5499), "Consumer Staples", "Food Retail"),
    ((6022, 6022), "Financials", "Regional Banks"),
    ((6798, 6798), "Real Estate", "REITs"),
    ((7370, 7379), "Technology", "Software & IT Services"),
    ((8731, 8731), "Healthcare", "Biotechnology"),
    ((100, 999), "Consumer Staples", "Agriculture"),
    ((1000, 1299), "Materials", "Metals & Mining"),
    ((1300, 1399), "Energy", "Oil & Gas"),
    ((1400, 1499), "Materials", "Mining & Quarrying"),
    ((1500, 1799), "Industrials", "Construction"),
    ((2000, 2199), "Consumer Staples", "Food, Beverage & Tobacco"),
    ((2200, 2399), "Consumer Discretionary", "Textiles & Apparel"),
    ((2400, 2499), "Materials", "Wood Products"),
    ((2500, 2599), "Consumer Discretionary", "Furniture"),
    ((2600, 2699), "Materials", "Paper"),
    ((2700, 2799), "Communication Services", "Publishing"),
    ((2800, 2899), "Materials", "Chemicals"),
    ((2900, 2999), "Energy", "Petroleum Refining"),
    ((3000, 3199), "Consumer Discretionary", "Rubber, Plastics & Leather"),
    ((3200, 3299), "Materials", "Stone, Clay & Glass"),
    ((3300, 3499), "Materials", "Metals"),
    ((3500, 3599), "Industrials", "Machinery"),
    ((3600, 3699), "Technology", "Electronics"),
    ((3700, 3799), "Industrials", "Transportation Equipment"),
    ((3800, 3839), "Industrials", "Instruments"),
    ((3860, 3999), "Consumer Discretionary", "Misc Manufacturing"),
    ((4000, 4799), "Industrials", "Transportation"),
    ((4800, 4899), "Communication Services", "Telecommunications"),
    ((4900, 4999), "Utilities", "Utilities"),
    ((5000, 5199), "Consumer Discretionary", "Wholesale"),
    ((5200, 5999), "Consumer Discretionary", "Retail"),
    ((6000, 6099), "Financials", "Banks"),
    ((6100, 6199), "Financials", "Credit Services"),
    ((6200, 6299), "Financials", "Capital Markets"),
    ((6300, 6499), "Financials", "Insurance"),
    ((6500, 6599), "Real Estate", "Real Estate"),
    ((6600, 6799), "Financials", "Investment Offices"),
    ((7000, 7299), "Consumer Discretionary", "Consumer Services"),
    ((7300, 7399), "Industrials", "Business Services"),
    ((7400, 7799), "Consumer Discretionary", "Consumer Services"),
    ((7800, 7899), "Communication Services", "Media & Entertainment"),
    ((7900, 7999), "Consumer Discretionary", "Entertainment & Recreation"),
    ((8000, 8099), "Healthcare", "Healthcare Services"),
    ((8100, 8999), "Industrials", "Professional Services"),
]


def sic_sector_industry(code):
    try:
        c = int(str(code).strip())
    except (ValueError, TypeError):
        return None, None
    for (lo, hi), sector, industry in SIC_RULES:
        if lo <= c <= hi:
            return sector, industry
    return None, None


def reference_lookup() -> dict:
    """sid AND symbol → reference row dict (sid wins), mirroring the
    engine's classified_idx."""
    tc = pl.read_parquet(REF / "tickers_classified.parquet").with_columns(
        pl.col("ticker_type").cast(pl.Utf8)
    )
    rows = tc.select(
        "security_id", "display_symbol", "ticker_type", "sic_code", "list_date",
        "weighted_shares_outstanding_snapshot",
    ).to_dicts()
    idx = {}
    for row in rows:
        idx.setdefault(row["display_symbol"], row)
    for row in rows:
        if row["security_id"] is not None:
            idx[row["security_id"]] = row
    return idx


def filed_shares_lookup() -> dict:
    """sid/ticker → sorted [(filing_date, basic_average_shares)] —
    independent port of SharesLookup."""
    fin = pl.read_parquet(REF / "financials.parquet").select(
        "security_id", "ticker", "filing_date", "basic_average_shares"
    ).filter(pl.col("basic_average_shares") > 0)
    m = {}
    for row in fin.to_dicts():
        key = row["security_id"] or row["ticker"]
        m.setdefault(key, []).append((row["filing_date"], row["basic_average_shares"]))
    for v in m.values():
        v.sort()
    return m


def shares_as_of(m: dict, key: str, d: date):
    v = m.get(key)
    if not v:
        return None
    out = None
    for fd, sh in v:
        if fd <= d:
            out = sh
        else:
            break
    return out


def ranks_desc(values):
    """rank 1 = largest, percentile = (m-1-r0)/m; None stays None."""
    valid = [(i, v) for i, v in enumerate(values) if v is not None and math.isfinite(v)]
    valid.sort(key=lambda t: -t[1])
    m = len(valid)
    rank = [None] * len(values)
    pct = [None] * len(values)
    for r0, (i, _) in enumerate(valid):
        rank[i] = r0 + 1
        pct[i] = (m - 1 - r0) / m
    return rank, pct


# ---------------------------------------------------------------------------
# Per-day validation
# ---------------------------------------------------------------------------

OBS_COLS = [
    "security_id", "display_symbol_on_day",
    "eod_day_open", "eod_day_high", "eod_day_low", "eod_day_close",
    "eod_day_volume", "eod_day_dollar_volume",
    "eod_unadjusted_day_close", "adjustment_factor_on_day",
    "premarket_volume", "premarket_dollar_volume", "premarket_vwap",
    "intraday_ret_0930_to_0940", "intraday_ret_0930_to_0950",
    "intraday_ret_0930_to_1000", "intraday_ret_0930_to_1010",
    "intraday_ret_0930_to_1030",
    "intraday_dollar_volume_0930_to_1000",
    "bar_count_rth", "first_rth_bar_time", "is_half_day",
    "overnight_gap", "prior_day_eod_close", "prior_day_unadjusted_eod_close",
    "gap_filled_today_flag",
    "intraday_first_30m_high_return", "intraday_first_30m_low_return",
    "intraday_time_of_first_30m_high", "intraday_time_of_first_30m_low",
    "intraday_ret_from_first_30m_high_to_1000",
    "intraday_minutes_since_first_30m_high_at_1000",
    "intraday_first_15m_volume_share_of_first_30m",
    "intraday_first_hour_high_return", "intraday_first_hour_low_return",
    "intraday_time_of_first_hour_high",
    "intraday_ret_from_first_hour_high_to_1030",
    "intraday_first_30m_volume_share_of_first_hour",
    "intraday_ret_0930_to_1000_rank_today",
    "intraday_ret_0930_to_1000_percentile_today",
    "intraday_dollar_volume_0930_to_1000_rank_today",
    "premarket_volume_rank_today", "premarket_dollar_volume_rank_today",
    "overnight_gap_rank_today", "addv_20d_rank_today",
    "realized_vol_21d_rank_today",
    "addv_20d", "realized_vol_21d", "atr_14d",
    "signal_concentration_percentile_today", "signal_concentration_hhi_today",
    "is_earnings_day", "days_since_last_earnings",
]


def snapshot_recompute(bars, close_t, cutoffs):
    """last close strictly before each ET cutoff ÷ rth open − 1, per sid_key."""
    rth = bars.filter((pl.col("et_time") >= time(9, 30)) & (pl.col("et_time") <= close_t))
    base = rth.group_by("sid_key", "display_symbol").agg(pl.col("open").first().alias("o"))
    out = base
    for hh, mm, label in cutoffs:
        w = rth.filter(pl.col("et_time") < time(hh, mm)).group_by(
            "sid_key", "display_symbol"
        ).agg(pl.col("close").last().alias(f"c_{label}"))
        out = out.join(w, on=["sid_key", "display_symbol"], how="left")
    exprs = [
        (pl.col(f"c_{label}") / pl.col("o") - 1.0).alias(f"snap_{label}")
        for _, _, label in cutoffs
    ]
    return out.with_columns(exprs)


def shape_recompute(bars, close_t):
    """First-30m / first-hour shape descriptors per sid_key (ratios —
    split factors cancel)."""
    rth = bars.filter((pl.col("et_time") >= time(9, 30)) & (pl.col("et_time") <= close_t))
    base = rth.group_by("sid_key", "display_symbol").agg(pl.col("open").first().alias("o"))

    def window_stats(end_hh, end_mm, prefix):
        w = rth.filter(pl.col("et_time") < time(end_hh, end_mm))
        return w.group_by("sid_key", "display_symbol").agg(
            pl.col("high").max().alias(f"{prefix}_hi"),
            pl.col("low").min().alias(f"{prefix}_lo"),
            pl.col("et_time").gather(pl.col("high").arg_max()).first().alias(f"{prefix}_t_hi"),
            pl.col("et_time").gather(pl.col("low").arg_min()).first().alias(f"{prefix}_t_lo"),
            pl.col("close").last().alias(f"{prefix}_close"),
            pl.col("volume").sum().alias(f"{prefix}_vol"),
        )

    out = base.join(window_stats(10, 0, "f30"), on=["sid_key", "display_symbol"], how="left")
    out = out.join(window_stats(10, 30, "fh"), on=["sid_key", "display_symbol"], how="left")
    f15 = rth.filter(pl.col("et_time") < time(9, 45)).group_by(
        "sid_key", "display_symbol"
    ).agg(pl.col("volume").sum().alias("f15_vol"))
    out = out.join(f15, on=["sid_key", "display_symbol"], how="left")
    return out.with_columns(
        (pl.col("f30_hi") / pl.col("o") - 1.0).alias("e_f30_high_ret"),
        (pl.col("f30_lo") / pl.col("o") - 1.0).alias("e_f30_low_ret"),
        (pl.col("f30_close") / pl.col("f30_hi") - 1.0).alias("e_f30_ret_from_high"),
        pl.col("f30_t_hi").cast(pl.Utf8).str.slice(0, 5).alias("e_f30_time_hi"),
        pl.col("f30_t_lo").cast(pl.Utf8).str.slice(0, 5).alias("e_f30_time_lo"),
        pl.when(pl.col("f30_vol") > 0)
        .then(pl.col("f15_vol").fill_null(0.0) / pl.col("f30_vol"))
        .otherwise(None)
        .alias("e_f15_share"),
        (pl.col("fh_hi") / pl.col("o") - 1.0).alias("e_fh_high_ret"),
        (pl.col("fh_lo") / pl.col("o") - 1.0).alias("e_fh_low_ret"),
        (pl.col("fh_close") / pl.col("fh_hi") - 1.0).alias("e_fh_ret_from_high"),
        pl.col("fh_t_hi").cast(pl.Utf8).str.slice(0, 5).alias("e_fh_time_hi"),
        pl.when(pl.col("fh_vol") > 0)
        .then(pl.col("f30_vol").fill_null(0.0) / pl.col("fh_vol"))
        .otherwise(None)
        .alias("e_f30_share_of_hour"),
    )


def validate_day(d, days_all, ref_idx, shares_map, share_history, amb=frozenset()):
    print(f"\n{'='*64}\n=== {d} ===")
    raw, close_t, bars = adjusted_raw(d)
    obs = pl.read_parquet(OUT / "daily_observation" / f"{d}.parquet").select(OBS_COLS)
    prior_days = [x for x in days_all if x < d]
    prior_day = prior_days[-1] if prior_days else None
    colls = collision_sids(raw) | set(amb)  # engine ambiguity is permanent

    # ---- A. day-local vs raw tape ----
    print("A. daily_observation day-local vs raw tape")
    j = obs.join(
        raw, left_on=["security_id", "display_symbol_on_day"],
        right_on=["sid_key", "display_symbol"], how="inner",
    )
    r.check(f"{d} universe matches", obs.height == raw.height,
            f"obs={obs.height} raw={raw.height}")
    r.check(f"{d} join exact", j.height == obs.height, f"joined={j.height}")
    jr = j.filter(pl.col("bar_count_rth_right").is_not_null())
    no_rth = j.filter(pl.col("bar_count_rth_right").is_null())
    r.check(f"{d} no-RTH rows null both sides",
            no_rth.filter(pl.col("eod_day_close").is_not_null()).height == 0,
            f"rows={no_rth.height}")
    for a, b in [
        ("eod_day_open", "adj_open"), ("eod_day_high", "adj_high"),
        ("eod_day_low", "adj_low"), ("eod_day_close", "adj_close"),
        ("eod_day_volume", "adj_volume"),
        ("eod_unadjusted_day_close", "rth_close"),
        ("premarket_volume", "adj_pm_volume"),
        ("intraday_ret_0930_to_1000", "ret_0930_1000"),
        ("adjustment_factor_on_day", "factor"),
    ]:
        bad = mismatch_count(jr, a, b)
        r.check(f"{d} {a}", bad == 0, f"mismatches={bad}/{jr.height}")
    bad = jr.filter(pl.col("bar_count_rth") != pl.col("bar_count_rth_right")).height
    r.check(f"{d} bar_count_rth", bad == 0, f"mismatches={bad}")
    bad = jr.filter(
        pl.col("first_rth_bar_time")
        != pl.col("first_rth_time").cast(pl.Utf8).str.slice(0, 5)
    ).height
    r.check(f"{d} first_rth_bar_time", bad == 0, f"mismatches={bad}")
    want_half = close_t < time(14, 0)
    r.check(f"{d} is_half_day == {want_half}",
            obs.filter(pl.col("is_half_day") != want_half).height == 0,
            f"close={close_t}")

    # ---- B. overnight vs prior raw ----
    print("B. overnight context")
    if prior_day is None:
        for c in ["overnight_gap", "prior_day_eod_close", "gap_filled_today_flag"]:
            r.check(f"{d} {c} all null (first day)",
                    obs.filter(pl.col(c).is_not_null()).height == 0, "")
        praw = None
    else:
        praw, _, _ = adjusted_raw(prior_day)
        colls |= collision_sids(praw)
        pr = praw.filter(pl.col("rth_close").is_not_null()).select(
            "sid_key", "display_symbol",
            pl.col("adj_close").alias("prior_adj_close"),
            pl.col("rth_close").alias("prior_raw_close"),
            pl.col("factor").alias("prior_factor"),
        )
        jg = jr.join(
            pr, left_on=["security_id", "display_symbol_on_day"],
            right_on=["sid_key", "display_symbol"], how="inner",
        ).filter(~pl.col("security_id").is_in(list(colls)))
        bad = mismatch_count(jg, "prior_day_eod_close", "prior_adj_close")
        r.check(f"{d} prior_day_eod_close", bad == 0, f"mismatches={bad}/{jg.height}")
        bad = mismatch_count(jg, "prior_day_unadjusted_eod_close", "prior_raw_close")
        r.check(f"{d} prior_day_unadjusted_eod_close", bad == 0, f"mismatches={bad}")
        jg2 = jg.with_columns(
            (pl.col("adj_open") / pl.col("prior_adj_close") - 1.0).alias("e_gap"),
            ((pl.col("adj_low") <= pl.col("prior_adj_close"))
             & (pl.col("prior_adj_close") <= pl.col("adj_high"))).alias("e_gap_filled"),
        )
        bad = mismatch_count(jg2, "overnight_gap", "e_gap")
        r.check(f"{d} overnight_gap", bad == 0, f"mismatches={bad}")
        bad = jg2.filter(
            pl.col("gap_filled_today_flag") != pl.col("e_gap_filled")
        ).height
        r.check(f"{d} gap_filled_today_flag", bad == 0, f"mismatches={bad}")

    # ---- F. remaining B1 day-local ----
    print("F. snapshots, shape, ranks, concentration")
    snaps = snapshot_recompute(
        bars, close_t,
        [(9, 40, "0940"), (9, 50, "0950"), (10, 10, "1010"), (10, 30, "1030")],
    )
    js = obs.join(
        snaps, left_on=["security_id", "display_symbol_on_day"],
        right_on=["sid_key", "display_symbol"], how="inner",
    )
    for label in ["0940", "0950", "1010", "1030"]:
        bad = mismatch_count(js, f"intraday_ret_0930_to_{label}", f"snap_{label}")
        r.check(f"{d} intraday_ret_0930_to_{label}", bad == 0, f"mismatches={bad}")

    shp = shape_recompute(bars, close_t)
    jp = obs.join(
        shp, left_on=["security_id", "display_symbol_on_day"],
        right_on=["sid_key", "display_symbol"], how="inner",
    )
    for a, b in [
        ("intraday_first_30m_high_return", "e_f30_high_ret"),
        ("intraday_first_30m_low_return", "e_f30_low_ret"),
        ("intraday_ret_from_first_30m_high_to_1000", "e_f30_ret_from_high"),
        ("intraday_first_15m_volume_share_of_first_30m", "e_f15_share"),
        ("intraday_first_hour_high_return", "e_fh_high_ret"),
        ("intraday_first_hour_low_return", "e_fh_low_ret"),
        ("intraday_ret_from_first_hour_high_to_1030", "e_fh_ret_from_high"),
        ("intraday_first_30m_volume_share_of_first_hour", "e_f30_share_of_hour"),
    ]:
        bad = mismatch_count(jp, a, b)
        r.check(f"{d} {a}", bad == 0, f"mismatches={bad}")
    for a, b in [
        ("intraday_time_of_first_30m_high", "e_f30_time_hi"),
        ("intraday_time_of_first_30m_low", "e_f30_time_lo"),
        ("intraday_time_of_first_hour_high", "e_fh_time_hi"),
    ]:
        bad = jp.filter(
            (pl.col(a) != pl.col(b))
            | (pl.col(a).is_null() != pl.col(b).is_null())
        ).height
        r.check(f"{d} {a}", bad == 0, f"mismatches={bad}")
    # minutes-since-30m-high: 10:00 − t_hi
    jm = jp.with_columns(
        (
            (10 * 60)
            - (pl.col("f30_t_hi").cast(pl.Utf8).str.slice(0, 2).cast(pl.Int32) * 60
               + pl.col("f30_t_hi").cast(pl.Utf8).str.slice(3, 2).cast(pl.Int32))
        ).alias("e_min_since")
    )
    bad = jm.filter(
        (pl.col("intraday_minutes_since_first_30m_high_at_1000") != pl.col("e_min_since"))
        | (pl.col("intraday_minutes_since_first_30m_high_at_1000").is_null()
           != pl.col("e_min_since").is_null())
    ).height
    r.check(f"{d} intraday_minutes_since_first_30m_high_at_1000", bad == 0, f"mismatches={bad}")

    # Ranks: recompute from the (already-L2-verified) parent columns.
    for parent, rank_col, pct_col in [
        ("intraday_ret_0930_to_1000", "intraday_ret_0930_to_1000_rank_today",
         "intraday_ret_0930_to_1000_percentile_today"),
        ("intraday_dollar_volume_0930_to_1000",
         "intraday_dollar_volume_0930_to_1000_rank_today", None),
        ("premarket_volume", "premarket_volume_rank_today", None),
        ("premarket_dollar_volume", "premarket_dollar_volume_rank_today", None),
        ("overnight_gap", "overnight_gap_rank_today", None),
        ("addv_20d", "addv_20d_rank_today", None),
        ("realized_vol_21d", "realized_vol_21d_rank_today", None),
    ]:
        erank, epct = ranks_desc(obs[parent].to_list())
        got = obs[rank_col].to_list()
        bad = sum(1 for a, b in zip(got, erank) if a != b)
        r.check(f"{d} {rank_col}", bad == 0, f"mismatches={bad}")
        if pct_col:
            gotp = obs[pct_col].to_list()
            badp = sum(1 for a, b in zip(gotp, epct) if not close_enough(a, b))
            r.check(f"{d} {pct_col}", badp == 0, f"mismatches={badp}")

    # Signal concentration (day-level).
    rets = [v for v in obs["intraday_ret_0930_to_1000"].to_list() if v is not None]
    share = (sum(1 for v in rets if v > 0) / len(rets)) if rets else None
    hist = [share_history[x] for x in prior_days if x in share_history]
    e_pct = (sum(1 for h in hist if h < share) / len(hist)) if (share is not None and hist) else None
    strengths = [max(v, 0.0) for v in rets]
    tot = sum(strengths)
    e_hhi = sum((s / tot) ** 2 for s in strengths) if tot > 0 else None
    got_pct = obs["signal_concentration_percentile_today"][0]
    got_hhi = obs["signal_concentration_hhi_today"][0]
    r.check(f"{d} signal_concentration_percentile_today",
            close_enough(got_pct, e_pct), f"engine={got_pct} indep={e_pct}")
    r.check(f"{d} signal_concentration_hhi_today",
            close_enough(got_hhi, e_hhi), f"engine={got_hhi} indep={e_hhi}")

    return obs, raw, jr, colls, close_t, praw, prior_day


# ---------------------------------------------------------------------------
# D. trailing windows — exact recompute over the FULL prior history
# ---------------------------------------------------------------------------

SAMPLE = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "XOM", "JPM", "GE"]
MOVE_THRESHOLDS = [0.05, 0.10, 0.20]


def sample_history(days, symbols):
    """Adjusted per-day aggregates for sample symbols over `days`
    (raw recompute + per-day factors; symbols chosen with no renames)."""
    frames = []
    pin = date.fromisoformat(
        pq.ParquetFile(REF / "splits.parquet").metadata.metadata[b"splits_snapshot_date"].decode()
    )
    sp_all = pl.read_parquet(REF / "splits.parquet").filter(
        pl.col("display_symbol").is_in(symbols) & (pl.col("execution_date") <= pin)
    )

    def factor_for(sym, d):
        rows = sp_all.filter(
            (pl.col("display_symbol") == sym) & (pl.col("execution_date") > d)
        )
        f = 1.0
        for fr, to in rows.select("split_from", "split_to").iter_rows():
            f *= fr / to
        return f

    for d in days:
        bars = load_day_bars(d).filter(pl.col("display_symbol").is_in(symbols))
        close_t = spy_session_close(bars)
        w = session_windows(bars, close_t).filter(pl.col("rth_close").is_not_null())
        rows = w.to_dicts()
        for row in rows:
            f = factor_for(row["display_symbol"], d)
            frames.append({
                "symbol": row["display_symbol"], "day": d,
                "open": row["rth_open"] * f, "high": row["rth_high"] * f,
                "low": row["rth_low"] * f, "close": row["rth_close"] * f,
                "volume": row["rth_volume"] / f,
                "dollar_volume": None,  # filled from bars below if needed
                "pm_volume": row["pm_volume"] / f,
                "ret_1000": row["ret_0930_1000"],
            })
    hist = {}
    for row in frames:
        hist.setdefault(row["symbol"], []).append(row)
    for v in hist.values():
        v.sort(key=lambda x: x["day"])
    return hist


def trailing_checks(d, days_all, obs):
    print("D. trailing windows (sampled, full-history exact)")
    prior_days = [x for x in days_all if x < d]
    hist = sample_history(prior_days + [d], SAMPLE)
    obs_s = obs.filter(pl.col("display_symbol_on_day").is_in(SAMPLE))
    obs_full = pl.read_parquet(OUT / "daily_observation" / f"{d}.parquet").filter(
        pl.col("display_symbol_on_day").is_in(SAMPLE)
    )

    # Index cum-log maps for betas (built from SPY/QQQ/IWM histories,
    # prior days only — engine updates after build).
    cumlog = {}
    for idx_sym in ["SPY", "QQQ", "IWM"]:
        m, c = {}, 0.0
        rows = [x for x in hist.get(idx_sym, []) if x["day"] < d]
        for i, row in enumerate(rows):
            if i > 0 and rows[i - 1]["close"] > 0:
                c += math.log(row["close"] / rows[i - 1]["close"])
            m[row["day"]] = c
        cumlog[idx_sym] = m

    for sym in SAMPLE:
        rows = [x for x in hist.get(sym, []) if x["day"] < d]
        if len(rows) < 22:
            continue
        closes = [x["close"] for x in rows]
        highs = [x["high"] for x in rows]
        lows = [x["low"] for x in rows]
        opens = [x["open"] for x in rows]
        vols = [x["volume"] for x in rows]
        row = obs_full.filter(pl.col("display_symbol_on_day") == sym).to_dicts()[0]

        # ATR / ADV / realized vol
        trs = [
            max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
            for i in range(len(closes) - 14, len(closes))
        ]
        atr = sum(trs) / 14
        adv20 = sum(vols[-20:]) / 20
        rets = [math.log(closes[i] / closes[i - 1]) for i in range(len(closes) - 21, len(closes))]
        mean = sum(rets) / 21
        rv = math.sqrt(sum((x - mean) ** 2 for x in rets) / 20) * math.sqrt(252)
        r.check(f"{d} {sym} atr_14d", close_enough(row["atr_14d"], atr),
                f"engine={row['atr_14d']} indep={atr}")
        r.check(f"{d} {sym} adv_20d", close_enough(row["adv_20d"], adv20),
                f"engine={row['adv_20d']} indep={adv20}")
        r.check(f"{d} {sym} realized_vol_21d", close_enough(row["realized_vol_21d"], rv),
                f"engine={row['realized_vol_21d']} indep={rv}")

        # Yang-Zhang 21d
        n = 21
        o_r, c_r, rs = [], [], 0.0
        for i in range(len(closes) - n, len(closes)):
            o_r.append(math.log(opens[i] / closes[i - 1]))
            cc = math.log(closes[i] / opens[i])
            c_r.append(cc)
            u = math.log(highs[i] / opens[i])
            l = math.log(lows[i] / opens[i])
            rs += u * (u - cc) + l * (l - cc)

        def svar(v):
            mu = sum(v) / len(v)
            return sum((x - mu) ** 2 for x in v) / (len(v) - 1)

        k = 0.34 / (1.34 + (n + 1) / (n - 1))
        yz = math.sqrt((svar(o_r) + k * svar(c_r) + (1 - k) * rs / n) * 252)
        r.check(f"{d} {sym} yang_zhang_vol_21d",
                close_enough(row["yang_zhang_vol_21d"], yz),
                f"engine={row['yang_zhang_vol_21d']} indep={yz}")

        # 52w high/low (partial window served as-is)
        win = rows[-252:]
        r.check(f"{d} {sym} high_52w",
                close_enough(row["high_52w"], max(x["high"] for x in win)), "")
        r.check(f"{d} {sym} low_52w",
                close_enough(row["low_52w"], min(x["low"] for x in win)), "")

        # days-since-move / consecutive-up (+1 convention)
        for thr, col in zip(MOVE_THRESHOLDS,
                            ["days_since_last_5pct_move", "days_since_last_10pct_move",
                             "days_since_last_20pct_move"]):
            last = None
            for i in range(1, len(closes)):
                if abs(closes[i] / closes[i - 1] - 1.0) >= thr:
                    last = i
            expected = (len(closes) - last) if last is not None else None
            got = row[col]
            r.check(f"{d} {sym} {col}", got == expected, f"engine={got} indep={expected}")
        streak = 0
        for i in range(len(closes) - 1, 0, -1):
            if closes[i] > closes[i - 1]:
                streak += 1
            else:
                break
        r.check(f"{d} {sym} consecutive_up_days",
                row["consecutive_up_days_close_to_close"] == streak,
                f"engine={row['consecutive_up_days_close_to_close']} indep={streak}")

        # premarket median + spike (adjusted basis)
        pm_today = row["premarket_volume"]
        pms = [x["pm_volume"] for x in rows[-20:]]
        if len(pms) == 20:
            srt = sorted(pms)
            med = (srt[10] + srt[9]) / 2
            e_ratio = pm_today / med if (med > 0 and pm_today is not None) else None
            r.check(f"{d} {sym} premarket_volume_vs_20d_median",
                    close_enough(row["premarket_volume_vs_20d_median"], e_ratio),
                    f"engine={row['premarket_volume_vs_20d_median']} indep={e_ratio}")
            e_spike = (e_ratio > 5.0) if e_ratio is not None else None
            r.check(f"{d} {sym} pre_market_volume_spike_flag",
                    row["pre_market_volume_spike_flag"] == e_spike, "")

        # signal_first_in_5d (fired today + none in prior 5)
        today_ret = row["intraday_ret_0930_to_1000"]
        if today_ret is None:
            expected = None
        elif today_ret <= 0:
            expected = False
        elif len(rows) < 5:
            expected = None
        else:
            prior5 = [x["ret_1000"] for x in rows[-5:]]
            expected = not any(v is not None and v > 0 for v in prior5)
        r.check(f"{d} {sym} signal_first_in_5d",
                row["signal_first_in_5d"] == expected,
                f"engine={row['signal_first_in_5d']} indep={expected}")

        # betas (date-aligned log returns; ≥75% coverage)
        for idx_sym, col in [("SPY", "beta_spy_60d"), ("QQQ", "beta_qqq_60d"),
                             ("IWM", "beta_iwm_60d")]:
            cm = cumlog[idx_sym]
            xs, ys = [], []
            for i in range(len(rows) - 60, len(rows)):
                d0, d1 = rows[i - 1]["day"], rows[i]["day"]
                if d0 in cm and d1 in cm and closes[i - 1] > 0 and closes[i] > 0:
                    xs.append(cm[d1] - cm[d0])
                    ys.append(math.log(closes[i] / closes[i - 1]))
            if len(xs) < 45 or len(rows) < 61:
                expected = None
            else:
                mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
                cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
                var = sum((x - mx) ** 2 for x in xs)
                expected = cov / var if var > 0 else None
            r.check(f"{d} {sym} {col}", close_enough(row[col], expected),
                    f"engine={row[col]} indep={expected}")
    return hist


# ---------------------------------------------------------------------------
# E. earnings recount
# ---------------------------------------------------------------------------

def earnings_check(d, obs):
    print("E. earnings proximity (AAPL recount)")
    fin = pl.read_parquet(REF / "financials.parquet").filter(
        (pl.col("ticker") == "AAPL")
        & pl.col("timeframe").cast(pl.Utf8).is_in(["quarterly", "annual"])
    )
    backfill = pl.read_parquet(REF / "acceptance_datetime_backfill.parquet").select(
        "accession_number", pl.col("acceptance_datetime").alias("bf_ts")
    )
    fin = fin.with_columns(
        pl.col("source_filing_url").str.split("/").list.last().alias("accession_number")
    ).join(backfill, on="accession_number", how="left").with_columns(
        pl.coalesce(pl.col("acceptance_datetime"), pl.col("bf_ts")).alias("ts")
    )
    ann = (
        fin.filter(pl.col("ts").is_not_null())
        .with_columns(pl.col("ts").dt.convert_time_zone("America/New_York").dt.date().alias("ad"))
        .group_by("fiscal_year", "fiscal_period")
        .agg(pl.col("ad").min())
    )
    past = ann.filter(pl.col("ad") <= d)["ad"]
    expected = (d - past.max()).days if past.len() else None
    got = obs.filter(pl.col("display_symbol_on_day") == "AAPL")
    if got.height:
        got_v = got["days_since_last_earnings"][0]
        r.check(f"{d} AAPL days_since_last_earnings", got_v == expected,
                f"engine={got_v} indep={expected}")


# ---------------------------------------------------------------------------
# G. market_context_daily
# ---------------------------------------------------------------------------

def quantile_linear(vals, p):
    v = sorted(vals)
    if not v:
        return None
    pos = p * (len(v) - 1)
    lo = int(math.floor(pos))
    frac = pos - lo
    return v[lo] * (1 - frac) + v[lo + 1] * frac if lo + 1 < len(v) else v[lo]


def market_context_checks(d, obs, raw, bars, close_t, praw):
    print("G. market_context_daily")
    ctx = pl.read_parquet(OUT / "market_context_daily" / f"{d}.parquet").to_dicts()[0]

    # Index columns from raw (SPY/QQQ/IWM have factor 1.0 in this era,
    # asserted below so the comparison stays valid if that changes).
    for sym, px in [("SPY", "spy"), ("QQQ", "qqq"), ("IWM", "iwm")]:
        row = raw.filter(pl.col("display_symbol") == sym)
        if row.height == 0:
            continue
        row = row.to_dicts()[0]
        r.check(f"{d} {sym} factor==1 (era assumption)", row["factor"] == 1.0, "")
        for a, b in [("eod_open", "adj_open"), ("eod_high", "adj_high"),
                     ("eod_low", "adj_low"), ("eod_close", "adj_close"),
                     ("eod_volume", "adj_volume"), ("ret_0930_to_1000", "ret_0930_1000")]:
            r.check(f"{d} {px}_{a}", close_enough(ctx[f"{px}_{a}"], row[b]),
                    f"ctx={ctx[f'{px}_{a}']} indep={row[b]}")
        if praw is not None:
            p = praw.filter(pl.col("display_symbol") == sym)
            if p.height:
                pc = p.to_dicts()[0]["adj_close"]
                e_gap = row["adj_open"] / pc - 1.0 if pc else None
                r.check(f"{d} {px}_overnight_gap",
                        close_enough(ctx[f"{px}_overnight_gap"], e_gap), "")
        # cross-table: index realized_vol matches the (L2-verified) obs col
        ov = obs.filter(pl.col("display_symbol_on_day") == sym)
        if ov.height:
            r.check(f"{d} {px}_realized_vol_21d == obs",
                    close_enough(ctx[f"{px}_realized_vol_21d"], ov["realized_vol_21d"][0]), "")

    # SPY 10m list contents: recompute buckets over 04:00–20:00 ET.
    spy_bars = bars.filter(pl.col("display_symbol") == "SPY").sort("t")
    sb = spy_bars.filter(
        (pl.col("et_time") >= time(4, 0)) & (pl.col("et_time") < time(20, 0))
    ).to_dicts()
    buckets = {}
    for b in sb:
        mins = b["et_time"].hour * 60 + b["et_time"].minute - 240
        k = mins // 10
        cur = buckets.get(k)
        if cur is None:
            buckets[k] = dict(open=b["open"], high=b["high"], low=b["low"],
                              close=b["close"], volume=b["volume"])
        else:
            cur["high"] = max(cur["high"], b["high"])
            cur["low"] = min(cur["low"], b["low"])
            cur["close"] = b["close"]
            cur["volume"] += b["volume"]
    got = ctx["spy_intraday_10m"]
    r.check(f"{d} spy_intraday_10m length", len(got) == len(buckets),
            f"ctx={len(got)} indep={len(buckets)}")
    if got and buckets:
        ks = sorted(buckets)
        for pos, label in [(0, "first"), (len(ks) // 2, "mid"), (len(ks) - 1, "last")]:
            e = buckets[ks[pos]]
            g = got[pos]
            ok = all(close_enough(g[f], e[f]) for f in ["open", "high", "low", "close", "volume"])
            r.check(f"{d} spy_intraday_10m {label} bucket OHLCV", ok,
                    f"got={ {f: g[f] for f in ['open','close','volume']} } exp={ {f: e[f] for f in ['open','close','volume']} }")

    # VIX vs reference file.
    vix = pl.read_parquet(REF / "vix_daily.parquet")
    vrow = vix.filter(pl.col("date") == d)
    e_vix = vrow["vix_close"][0] if vrow.height else None
    r.check(f"{d} vix_close", close_enough(ctx["vix_close"], e_vix),
            f"ctx={ctx['vix_close']} ref={e_vix}")
    r.check(f"{d} vix_open null-by-design", ctx["vix_open"] is None, "")

    # Breadth + dispersion + liquidity (cross-table identities over obs).
    rets = [v for v in obs["intraday_ret_0930_to_1000"].to_list() if v is not None]
    rets_1030 = [v for v in obs["intraday_ret_0930_to_1030"].to_list() if v is not None]
    eod = obs.filter(
        pl.col("eod_day_open").is_not_null() & (pl.col("eod_day_open") > 0)
    ).with_columns(
        (pl.col("eod_day_close") / pl.col("eod_day_open") - 1.0).alias("eod_ret")
    )
    eod_rets = eod["eod_ret"].to_list()

    def share_pos(v):
        return sum(1 for x in v if x > 0) / len(v) if v else None

    checks = {
        "breadth_pct_universe_green_at_1000": share_pos(rets),
        "breadth_pct_universe_green_at_1030": share_pos(rets_1030),
        "breadth_total_universe_with_bars": obs.height,
        "cross_sectional_ret_iqr_at_1000":
            (quantile_linear(rets, 0.75) - quantile_linear(rets, 0.25)) if rets else None,
        "cross_sectional_ret_iqr_eod":
            (quantile_linear(eod_rets, 0.75) - quantile_linear(eod_rets, 0.25)) if eod_rets else None,
        "cross_sectional_ret_dispersion_eod":
            (math.sqrt(sum((x - sum(eod_rets) / len(eod_rets)) ** 2 for x in eod_rets) / len(eod_rets))
             if eod_rets else None),
        "universe_total_dollar_volume":
            sum(v for v in obs["eod_day_dollar_volume"].to_list() if v is not None),
        "universe_median_addv_20d":
            quantile_linear([v for v in obs["addv_20d"].to_list() if v is not None], 0.5),
        "breadth_count_movers_above_5pct_at_1000": sum(1 for x in rets if abs(x) >= 0.05),
        "breadth_count_movers_above_5pct_eod": sum(1 for x in eod_rets if abs(x) >= 0.05),
    }
    for name, e in checks.items():
        r.check(f"{d} {name}", close_enough(ctx[name], e), f"ctx={ctx[name]} indep={e}")

    # A/D ratios (null when zero decliners).
    for col, v in [("breadth_advance_decline_ratio_at_1000", rets),
                   ("breadth_advance_decline_ratio_eod", eod_rets)]:
        adv = sum(1 for x in v if x > 0)
        dec = sum(1 for x in v if x < 0)
        e = adv / dec if dec else None
        r.check(f"{d} {col}", close_enough(ctx[col], e), f"ctx={ctx[col]} indep={e}")

    # movers ≥ 1 ATR + above-premarket-vwap (cross-table via obs columns).
    m = obs.filter(
        pl.col("intraday_ret_0930_to_1000").is_not_null()
        & pl.col("atr_14d").is_not_null() & (pl.col("atr_14d") > 0)
        & pl.col("eod_day_open").is_not_null()
    ).with_columns(
        ((pl.col("eod_day_open") * pl.col("intraday_ret_0930_to_1000")).abs()
         / pl.col("atr_14d")).alias("mv")
    )
    e_atr = m.filter(pl.col("mv") >= 1.0).height if m.height else None
    r.check(f"{d} breadth_count_movers_above_1atr_at_1000",
            ctx["breadth_count_movers_above_1atr_at_1000"] == e_atr,
            f"ctx={ctx['breadth_count_movers_above_1atr_at_1000']} indep={e_atr}")
    av = obs.filter(
        pl.col("intraday_ret_0930_to_1000").is_not_null()
        & pl.col("premarket_vwap").is_not_null() & pl.col("eod_day_open").is_not_null()
    ).with_columns(
        ((pl.col("eod_day_open") * (1 + pl.col("intraday_ret_0930_to_1000")))
         > pl.col("premarket_vwap")).alias("above")
    )
    e_share = av["above"].sum() / av.height if av.height else None
    r.check(f"{d} breadth_pct_universe_above_premarket_vwap_at_1000",
            close_enough(ctx["breadth_pct_universe_above_premarket_vwap_at_1000"], e_share),
            f"ctx={ctx['breadth_pct_universe_above_premarket_vwap_at_1000']} indep={e_share}")


# ---------------------------------------------------------------------------
# H. sector aggregates via the independent SIC port
# ---------------------------------------------------------------------------

def sector_map_port():
    """Engine sector_by_key port: mapped rows only; sid + symbol keys;
    LAST row wins (HashMap insert semantics)."""
    tc = pl.read_parquet(REF / "tickers_classified.parquet")
    m = {}
    for row in tc.select("security_id", "display_symbol", "sic_code").to_dicts():
        sector, _ = sic_sector_industry(row["sic_code"])
        if sector is not None:
            if row["security_id"] is not None:
                m[row["security_id"]] = sector
            m[row["display_symbol"]] = sector
    return m


def sector_checks(d, obs, smap):
    print("H. sector_aggregates_daily (independent SIC port)")
    sec = pl.read_parquet(OUT / "sector_aggregates_daily" / f"{d}.parquet").with_columns(
        pl.col("sector_id").cast(pl.Utf8)
    )
    grp = {}
    rows = obs.select(
        "security_id", "display_symbol_on_day",
        "intraday_ret_0930_to_0950", "intraday_ret_0930_to_1000",
        "intraday_ret_0930_to_1030", "eod_day_open", "eod_day_close",
    ).to_dicts()
    for row in rows:
        sector = smap.get(row["security_id"]) or smap.get(row["display_symbol_on_day"]) or "Unknown"
        g = grp.setdefault(sector, {"n": 0, "sums": [0.0] * 4, "cnts": [0] * 4,
                                    "g1000": 0, "g1030": 0})
        g["n"] += 1
        eod_ret = (row["eod_day_close"] / row["eod_day_open"] - 1.0) \
            if (row["eod_day_open"] or 0) > 0 else None
        for k, v in enumerate([row["intraday_ret_0930_to_0950"], row["intraday_ret_0930_to_1000"],
                               row["intraday_ret_0930_to_1030"], eod_ret]):
            if v is not None:
                g["sums"][k] += v
                g["cnts"][k] += 1
                if v > 0:
                    if k == 1:
                        g["g1000"] += 1
                    elif k == 2:
                        g["g1030"] += 1
    bad = 0
    for row in sec.to_dicts():
        g = grp.get(row["sector_id"])
        if g is None:
            bad += 1
            continue
        mean = lambda k: g["sums"][k] / g["cnts"][k] if g["cnts"][k] else None
        ok = (
            row["sector_constituent_count_with_bars"] == g["n"]
            and close_enough(row["sector_ret_0930_to_0950"], mean(0))
            and close_enough(row["sector_ret_0930_to_1000"], mean(1))
            and close_enough(row["sector_ret_0930_to_1030"], mean(2))
            and close_enough(row["sector_eod_ret"], mean(3))
            and close_enough(row["sector_pct_green_at_1000"],
                             g["g1000"] / g["cnts"][1] if g["cnts"][1] else None)
            and close_enough(row["sector_pct_green_at_1030"],
                             g["g1030"] / g["cnts"][2] if g["cnts"][2] else None)
        )
        bad += 0 if ok else 1
    r.check(f"{d} sector rows recomputed", bad == 0,
            f"mismatching sectors={bad}/{sec.height}")
    r.check(f"{d} sector set matches", set(sec["sector_id"].to_list()) == set(grp),
            f"engine={sec.height} indep={len(grp)}")
    # rank: 1 = best mean ret@1000
    means = {s: (g["sums"][1] / g["cnts"][1] if g["cnts"][1] else None) for s, g in grp.items()}
    ordered = sorted((s for s in means if means[s] is not None), key=lambda s: -means[s])
    e_rank = {s: i + 1 for i, s in enumerate(ordered)}
    bad = sum(
        1 for row in sec.to_dicts()
        if row["sector_ret_0930_to_1000_rank_today"] != e_rank.get(row["sector_id"])
    )
    r.check(f"{d} sector rank", bad == 0, f"mismatches={bad}")


# ---------------------------------------------------------------------------
# I. security_classification_daily — caps, buckets, ranks, tag port
# ---------------------------------------------------------------------------

ETF_TYPES = {"ETF", "ETV", "ETS", "FUND"}
TECH_MEGA = {"Technology", "Communication Services", "Consumer Discretionary"}
TECH_LARGE = {"Technology", "Communication Services"}


def cap_bucket(c):
    return ("micro" if c < 300e6 else "small" if c < 2e9 else "mid" if c < 10e9
            else "large" if c < 200e9 else "mega")


def classification_checks(d, obs, jr, colls, ref_idx, shares_map, praw):
    print("I. security_classification_daily")
    cls = pl.read_parquet(OUT / "security_classification_daily" / f"{d}.parquet").with_columns(
        pl.col("ticker_type").cast(pl.Utf8), pl.col("sector").cast(pl.Utf8),
        pl.col("industry").cast(pl.Utf8),
        pl.col("market_cap_bucket").cast(pl.Utf8), pl.col("liquidity_bucket").cast(pl.Utf8),
        pl.col("price_bucket").cast(pl.Utf8), pl.col("volatility_bucket").cast(pl.Utf8),
        pl.col("style_bucket").cast(pl.Utf8), pl.col("listing_status").cast(pl.Utf8),
    )
    r.check(f"{d} classification rows == obs rows", cls.height == obs.height, "")

    # Prior-day closes (adjusted + raw) for caps/price buckets.
    prior = {}
    if praw is not None:
        for row in praw.filter(pl.col("rth_close").is_not_null()).select(
            "sid_key", "display_symbol", "adj_close", "rth_close"
        ).to_dicts():
            prior[(row["sid_key"], row["display_symbol"])] = (row["adj_close"], row["rth_close"])

    rows = cls.to_dicts()
    obs_dvol1000 = obs["intraday_dollar_volume_0930_to_1000"].to_list()
    dvol_rank, _ = ranks_desc(obs_dvol1000)
    obs_key = {(a, b): i for i, (a, b) in enumerate(
        zip(obs["security_id"].to_list(), obs["display_symbol_on_day"].to_list())
    )}

    bad = {k: 0 for k in [
        "ttype", "flags", "sector", "industry", "cap", "cap_bucket", "price_bucket",
        "liq_bucket", "vol_bucket", "style_bucket", "ipo", "status",
        "tag_energy", "tag_semis", "tag_biotech", "tag_regbank", "tag_china",
        "tag_recent_ipo", "tag_spac", "tag_megatech", "tag_largetech",
        "tag_lowfloat", "tag_meme",
    ]}
    n_cap_checked = 0
    for row in rows:
        sid, sym = row["security_id"], row["display_symbol_on_day"]
        ref = ref_idx.get(sid) or ref_idx.get(sym)
        t = ref["ticker_type"] if ref else None

        if row["ticker_type"] != t:
            bad["ttype"] += 1
        e_flags = {
            "is_common_stock": (t == "CS") if t is not None else None,
            "is_etn": (t == "ETN") if t is not None else None,
            "is_adr": t.startswith("ADR") if t is not None else None,
            "is_warrant": (t == "WARRANT") if t is not None else None,
            "is_preferred": (t == "PFD") if t is not None else None,
            "is_unit": (t == "UNIT") if t is not None else None,
            "is_etf": (t in ETF_TYPES) if t is not None else None,
        }
        if any(row[k] != v for k, v in e_flags.items()):
            bad["flags"] += 1

        e_sector, e_industry = sic_sector_industry(ref["sic_code"]) if ref else (None, None)
        if row["sector"] != e_sector:
            bad["sector"] += 1
        if row["industry"] != e_industry:
            bad["industry"] += 1

        # Market cap: filed shares × unadjusted prior close, plausibility-
        # checked against split-rebased snapshot; collision sids → null.
        # Rows absent from D-1's tape are SKIPPED (the engine's rolling
        # prior is the last TRADED day, possibly older than D-1).
        pr = prior.get((sid, sym))
        if pr is None and sid not in colls:
            e_cap = "skip"
        elif sid in colls:
            e_cap = None
        else:
            adj_c, raw_c = pr
            sh = shares_as_of(shares_map, sid, d) or shares_as_of(shares_map, sym, d)
            if sh is None:
                e_cap = None
            else:
                snap = ref["weighted_shares_outstanding_snapshot"] if ref else None
                if snap and snap > 0 and raw_c > 0:
                    f = adj_c / raw_c
                    ratio = sh / (snap * f)
                    if not (1 / 30 <= ratio <= 30):
                        sh = None
                e_cap = sh * raw_c if sh is not None else None
        if e_cap != "skip":
            if not close_enough(row["market_cap"], e_cap):
                bad["cap"] += 1
            else:
                n_cap_checked += 1
            e_bucket = cap_bucket(e_cap) if e_cap is not None else None
            if row["market_cap_bucket"] != e_bucket:
                bad["cap_bucket"] += 1

        # Price bucket from prior adjusted close (rolling basis); only
        # checkable for consecutive-day traders (see cap note above).
        if sid not in colls and pr is not None:
            pc = pr[0]
            e_pb = (None if pc is None else
                    "sub_1" if pc < 1 else "1_to_5" if pc < 5 else "5_to_20" if pc < 20
                    else "20_to_100" if pc < 100 else "above_100")
            if row["price_bucket"] != e_pb or not close_enough(row["prior_close_price"], pc):
                bad["price_bucket"] += 1

        # Self-consistent buckets from the table's own (L2-verified) cols.
        a = row["addv_20d"]
        e_lb = (None if a is None else "illiquid" if a < 1e6 else "thin" if a < 10e6
                else "normal" if a < 100e6 else "liquid" if a < 1e9 else "highly_liquid")
        if row["liquidity_bucket"] != e_lb:
            bad["liq_bucket"] += 1
        v = row["realized_vol_21d"]
        e_vb = (None if v is None else "low" if v < 0.25 else "medium" if v < 0.5
                else "high" if v < 1.0 else "extreme")
        if row["volatility_bucket"] != e_vb:
            bad["vol_bucket"] += 1
        b = row["beta_spy_60d"]
        e_sb = (None if b is None else "defensive" if b < 0.7 else "market" if b <= 1.3
                else "aggressive")
        if row["style_bucket"] != e_sb:
            bad["style_bucket"] += 1

        if ref and ref["list_date"] is not None:
            if row["days_since_ipo_or_first_bar"] != (d - ref["list_date"]).days:
                bad["ipo"] += 1
        if row["listing_status"] != "trading":
            bad["status"] += 1

        # Behavioral-tag port (momentum-classify v1 rules).
        dsi = row["days_since_ipo_or_first_bar"]
        i_key = obs_key.get((sid, sym))
        dv_rank = dvol_rank[i_key] if i_key is not None else None
        vol_pct = row["volatility_percentile_today"]
        vol_pct100 = vol_pct * 100 if vol_pct is not None else None
        ar = row["addv_rank_today"]
        atr_ratio = (row["atr_14d"] / row["prior_close_price"]
                     if row["atr_14d"] is not None and row["prior_close_price"] else None)
        tags = {
            "tag_energy": ("is_energy", e_sector == "Energy"),
            "tag_semis": ("is_semiconductor", e_industry == "Semiconductors"),
            "tag_biotech": ("is_biotech", e_industry in ("Biotechnology", "Pharmaceuticals")),
            "tag_regbank": ("is_regional_bank", e_industry == "Regional Banks"),
            "tag_china": ("is_china_adr", False),
            "tag_recent_ipo": ("is_recent_ipo", dsi is not None and dsi < 60),
            "tag_spac": ("is_spac", bool(ref) and ref["sic_code"] == "6770"),
            "tag_megatech": ("is_mega_cap_tech",
                             row["market_cap_bucket"] == "mega" and ar is not None
                             and row["beta_qqq_60d"] is not None
                             and e_sector in TECH_MEGA and ar <= 50
                             and row["beta_qqq_60d"] > 0.9),
            "tag_largetech": ("is_large_cap_tech",
                              row["market_cap_bucket"] in ("large", "mega")
                              and ar is not None and e_sector in TECH_LARGE and ar <= 500),
            "tag_lowfloat": ("is_low_float_candidate",
                             ar is not None and atr_ratio is not None
                             and row["prior_close_price"] is not None
                             and ar > 5000 and atr_ratio > 0.05
                             and row["prior_close_price"] > 1.0),
            "tag_meme": ("is_meme_candidate",
                         vol_pct100 is not None and vol_pct100 >= 95.0
                         and dv_rank is not None and dv_rank <= 50),
        }
        for key, (col, expected) in tags.items():
            if row[col] != expected:
                bad[key] += 1

    for key, n in bad.items():
        r.check(f"{d} classification {key}", n == 0, f"mismatches={n}/{cls.height}")
    r.check(f"{d} classification caps checked > 2000", n_cap_checked > 2000,
            f"checked={n_cap_checked}")
    # rank bijectivity for cap/addv/dvol ranks
    for col, parent in [("market_cap_rank_today", "market_cap"),
                        ("addv_rank_today", "addv_20d"),
                        ("dollar_volume_rank_today", None)]:
        if parent:
            erank, _ = ranks_desc(cls[parent].to_list())
            got = cls[col].to_list()
            n = sum(1 for a, b in zip(got, erank) if a != b)
            r.check(f"{d} classification {col}", n == 0, f"mismatches={n}")


# ---------------------------------------------------------------------------
# J. regime_definitions
# ---------------------------------------------------------------------------

def regime_checks(days_all):
    print(f"\n{'='*64}\nJ. regime_definitions (full recompute)")
    pf = pq.ParquetFile(OUT / "regime_definitions.parquet")
    meta = {k.decode(): v.decode() for k, v in pf.metadata.metadata.items()
            if not k.startswith(b"ARROW")}
    reg = pl.read_parquet(OUT / "regime_definitions.parquet").with_columns(
        pl.col("taxonomy").cast(pl.Utf8)
    )

    series = []
    for d in days_all:
        row = pl.read_parquet(
            OUT / "market_context_daily" / f"{d}.parquet",
            columns=["spy_eod_close", "vix_close", "breadth_advance_decline_ratio_eod",
                     "universe_median_addv_20d"],
        ).to_dicts()[0]
        row["day"] = d
        series.append(row)
    series.sort(key=lambda x: x["day"])

    expl = (date(2016, 6, 8), date(2020, 12, 31))

    def era_of(d):
        if d <= date(2019, 12, 31):
            return 1, "zirp_bull"
        if d <= date(2022, 6, 30):
            return 2, "covid_meme"
        if d <= date(2024, 12, 31):
            return 3, "rate_shock_ai"
        return 4, "recent"

    spy_ret63 = []
    for i, row in enumerate(series):
        if i < 63 or series[i - 63]["spy_eod_close"] in (None, 0) or row["spy_eod_close"] is None:
            spy_ret63.append(None)
        else:
            spy_ret63.append(row["spy_eod_close"] / series[i - 63]["spy_eod_close"] - 1.0)

    taxes = {
        "spy_trend": (spy_ret63, ["downtrend", "sideways", "uptrend"]),
        "vix_level": ([x["vix_close"] for x in series], ["low", "medium", "high"]),
        "breadth": ([x["breadth_advance_decline_ratio_eod"] for x in series],
                    ["narrow", "mixed", "broad"]),
        "liquidity": ([x["universe_median_addv_20d"] for x in series],
                      ["low", "medium", "high"]),
    }
    expected = {}
    for i, row in enumerate(series):
        expected[(row["day"], "era")] = era_of(row["day"])
    for name, (vals, labels) in taxes.items():
        ev = [v for x, v in zip(series, vals)
              if expl[0] <= x["day"] <= expl[1] and v is not None]
        if len(ev) < 9:
            r.check(f"regimes {name} stamped insufficient-data",
                    meta.get(f"regime_thresholds_{name}") == "insufficient-data", "")
            continue
        t1, t2 = quantile_linear(ev, 1 / 3), quantile_linear(ev, 2 / 3)
        stamp = f"tertiles:{t1:.6f},{t2:.6f}"
        r.check(f"regimes {name} threshold stamp",
                meta.get(f"regime_thresholds_{name}") == stamp,
                f"meta={meta.get(f'regime_thresholds_{name}')} indep={stamp}")
        for x, v in zip(series, vals):
            if v is not None:
                b = 0 if v <= t1 else 1 if v <= t2 else 2
                expected[(x["day"], name)] = (b + 1, labels[b])

    got = {(row["day"], row["taxonomy"]): (row["regime_id"], row["regime_name"])
           for row in reg.to_dicts()}
    r.check("regimes assignment count", len(got) == len(expected),
            f"engine={len(got)} indep={len(expected)}")
    bad = sum(1 for k, v in expected.items() if got.get(k) != v)
    r.check("regimes assignments match", bad == 0, f"mismatches={bad}/{len(expected)}")
    r.check("regimes window stamp",
            meta.get("regime_thresholds_window") == "2016-06-08..2020-12-31",
            f"meta={meta.get('regime_thresholds_window')}")
    r.check("regimes era stamp",
            meta.get("regime_thresholds_era") == "calendar:2019-12-31,2022-06-30,2024-12-31", "")


# ---------------------------------------------------------------------------
# K. property sweep over ALL days
# ---------------------------------------------------------------------------

def property_sweep(days_all):
    print(f"\n{'='*64}\nK. property sweep over {len(days_all)} days")
    viol = {k: 0 for k in [
        "high<low", "close outside [low,high]", "open outside [low,high]",
        "pct out of [0,1]", "rank not bijective", "bar_count>391",
        "first_30m>30", "first_hour>60", "stamp drift", "schema drift",
        "sector counts != obs rows", "cap rank not bijective",
    ]}
    n_fields = None
    for d in days_all:
        p = OUT / "daily_observation" / f"{d}.parquet"
        pf = pq.ParquetFile(p)
        meta = {k.decode(): v.decode() for k, v in pf.metadata.metadata.items()}
        if meta.get("engine_milestone") != "B2" or meta.get("daily_observation_version") != "v2":
            viol["stamp drift"] += 1
        if n_fields is None:
            n_fields = len(pf.schema_arrow.names)
        elif len(pf.schema_arrow.names) != n_fields:
            viol["schema drift"] += 1
        t = pl.read_parquet(p, columns=[
            "eod_day_open", "eod_day_high", "eod_day_low", "eod_day_close",
            "intraday_ret_0930_to_1000_rank_today",
            "intraday_ret_0930_to_1000_percentile_today",
            "bar_count_rth", "bar_count_first_30m", "bar_count_first_hour",
        ])
        viol["high<low"] += t.filter(pl.col("eod_day_high") < pl.col("eod_day_low")).height
        viol["close outside [low,high]"] += t.filter(
            (pl.col("eod_day_close") < pl.col("eod_day_low") - 1e-9)
            | (pl.col("eod_day_close") > pl.col("eod_day_high") + 1e-9)
        ).height
        viol["open outside [low,high]"] += t.filter(
            (pl.col("eod_day_open") < pl.col("eod_day_low") - 1e-9)
            | (pl.col("eod_day_open") > pl.col("eod_day_high") + 1e-9)
        ).height
        viol["pct out of [0,1]"] += t.filter(
            (pl.col("intraday_ret_0930_to_1000_percentile_today") < 0)
            | (pl.col("intraday_ret_0930_to_1000_percentile_today") > 1)
        ).height
        ranks = t["intraday_ret_0930_to_1000_rank_today"].drop_nulls()
        if ranks.len() and (ranks.n_unique() != ranks.len()
                            or ranks.min() != 1 or ranks.max() != ranks.len()):
            viol["rank not bijective"] += 1
        viol["bar_count>391"] += t.filter(pl.col("bar_count_rth") > 391).height
        viol["first_30m>30"] += t.filter(pl.col("bar_count_first_30m") > 30).height
        viol["first_hour>60"] += t.filter(pl.col("bar_count_first_hour") > 60).height

        sec = pl.read_parquet(
            OUT / "sector_aggregates_daily" / f"{d}.parquet",
            columns=["sector_constituent_count_with_bars"],
        )
        if sec["sector_constituent_count_with_bars"].sum() != t.height:
            viol["sector counts != obs rows"] += 1
        c = pl.read_parquet(
            OUT / "security_classification_daily" / f"{d}.parquet",
            columns=["market_cap_rank_today"],
        )["market_cap_rank_today"].drop_nulls()
        if c.len() and (c.n_unique() != c.len() or c.min() != 1 or c.max() != c.len()):
            viol["cap rank not bijective"] += 1
    for k, n in viol.items():
        r.check(f"property: {k}", n == 0, f"violations={n}")


# ---------------------------------------------------------------------------
# L. forward_outcomes (B3 short horizons) — sample exact recompute + sweep
# ---------------------------------------------------------------------------
#
# Independent recompute for the SAMPLE liquid names at early/mid/late
# offsets across the 8 short horizons. Adjustment is per-day pin factor
# (raw * factor_at(sym,day)) — the same common basis day_sessions applies,
# so a reintroduced cross-day rescale would surface here. Conventions
# mirror crates/momentum-engine/src/forward_outcomes.rs:
#   entry_price = open of the first RTH bar at/after the entry minute;
#   forward path = RTH bars, entry bar inclusive (index 0);
#   intraday horizons capped at D's close; 1d..5d end at D+k close.

FO_OFFSETS = ["0935", "1000", "1530"]
# (label, kind, k): kind i=intraday(minutes=k), eod, d=day(close of D+k)
FO_HORIZONS = [
    ("10min", "i", 10), ("30min", "i", 30), ("60min", "i", 60),
    ("EOD", "eod", 0),
    ("1d", "d", 1), ("2d", "d", 2), ("3d", "d", 3), ("5d", "d", 5),
]
FO_PCT = [("0_5", 0.005), ("1", 0.01), ("2", 0.02), ("3", 0.03), ("5", 0.05), ("10", 0.10), ("20", 0.20)]
FO_ATR = [("0_25", 0.25), ("0_5", 0.5), ("1", 1.0), ("1_5", 1.5), ("2", 2.0), ("3", 3.0)]
# (pair name, (up kind,val), (down kind,val), horizon) — the 8 B3 pairs;
# the 21d pair is B5 (null). stop_first on a same-bar double touch.
FO_PAIRS = [
    ("0_5pct_before_minus_0_5pct_30min", ("pct", 0.005), ("pct", 0.005), "30min"),
    ("1pct_before_minus_1pct_EOD", ("pct", 0.01), ("pct", 0.01), "EOD"),
    ("2pct_before_minus_1pct_EOD", ("pct", 0.02), ("pct", 0.01), "EOD"),
    ("3pct_before_minus_1_5pct_EOD", ("pct", 0.03), ("pct", 0.015), "EOD"),
    ("2pct_before_minus_2pct_1d", ("pct", 0.02), ("pct", 0.02), "1d"),
    ("3pct_before_minus_3pct_5d", ("pct", 0.03), ("pct", 0.03), "5d"),
    ("1atr_before_minus_0_5atr_EOD", ("atr", 1.0), ("atr", 0.5), "EOD"),
    ("2atr_before_minus_1atr_5d", ("atr", 2.0), ("atr", 1.0), "5d"),
]


def corpus_trading_days():
    return sorted(date.fromisoformat(p.stem) for p in BARS.glob("*.parquet"))


def _fo_splits():
    pin = date.fromisoformat(
        pq.ParquetFile(REF / "splits.parquet").metadata.metadata[b"splits_snapshot_date"].decode()
    )
    return pl.read_parquet(REF / "splits.parquet"), pin


def _fo_factor(sp_all, pin, sym, d):
    rows = sp_all.filter(
        (pl.col("display_symbol") == sym)
        & (pl.col("execution_date") > d)
        & (pl.col("execution_date") <= pin)
    )
    f = 1.0
    for fr, to in rows.select("split_from", "split_to").iter_rows():
        f *= fr / to
    return f


def _fo_adj_rth(daybars, close_t, sym, factor):
    """Adjusted RTH bars for sym on a day, as (minute, o,h,l,c,v) tuples,
    minute = ET hour*60+min; OHLC * factor, volume / factor (pin basis)."""
    b = (
        daybars.filter(
            (pl.col("display_symbol") == sym)
            & (pl.col("et_time") >= time(9, 30))
            & (pl.col("et_time") <= close_t)
        )
        .sort("t")
    )
    out = []
    for row in b.iter_rows(named=True):
        et = row["et_time"]
        out.append((
            et.hour * 60 + et.minute,
            row["open"] * factor, row["high"] * factor,
            row["low"] * factor, row["close"] * factor,
            row["volume"] / factor,
        ))
    return out


def _fo_resolve(per_day_bars, off, atr14=None, pm_vol=0.0, pm_dollar=0.0):
    """per_day_bars[k] = adjusted RTH bar list for D+k (k=0..5). Returns a
    dict of recomputed columns for one entry_offset, or None if no entry.
    pm_vol/pm_dollar = adjusted premarket volume/dollar for D (04:00-09:30)."""
    d0 = per_day_bars[0]
    if not d0:
        return None
    entry_min = int(off[:2]) * 60 + int(off[2:])
    eidx = next((i for i, b in enumerate(d0) if b[0] >= entry_min), None)
    if eidx is None:
        return None
    ebar = d0[eidx]
    ep = ebar[1]  # open
    if not ep > 0:
        return None
    rth_open = d0[0][1]

    tape = list(d0[eidx:])
    day_end = [len(tape) - 1 if tape else None] + [None] * 5
    for k in range(1, 6):
        if k < len(per_day_bars) and per_day_bars[k]:
            tape.extend(per_day_bars[k])
            day_end[k] = len(tape) - 1

    pref = []
    mh = (-1e18, 0); ml = (1e18, 0); mc = (-1e18, 0); nc = (1e18, 0)
    for i, b in enumerate(tape):
        if b[2] > mh[0]: mh = (b[2], i)
        if b[3] < ml[0]: ml = (b[3], i)
        if b[4] > mc[0]: mc = (b[4], i)
        if b[4] < nc[0]: nc = (b[4], i)
        pref.append((mh, ml, mc, nc))

    def at(end):
        if end is None or end >= len(tape):
            return None
        mh, ml, mc, nc = pref[end]
        return {
            "ret": tape[end][4] / ep - 1.0,
            "max_runup": mh[0] / ep - 1.0, "bars_to_max_runup": mh[1],
            "max_drawdown": ml[0] / ep - 1.0, "bars_to_max_drawdown": ml[1],
            "close_max_ret": mc[0] / ep - 1.0, "bars_to_close_max": mc[1],
            "close_min_ret": nc[0] / ep - 1.0, "bars_to_close_min": nc[1],
        }

    upper = day_end[0] or 0
    # Intraday horizons are measured from the ACTUAL entry bar, not the
    # nominal offset (engine: cutoff = entry_bar.t + N min). They differ
    # only when a gap/halt pushes the fill past the offset minute — e.g.
    # the 15:30 offset on the 2016-11-25 half day fills at the 16:00
    # auction print (is_halted_at_entry=True), and the 10/30/60min windows
    # collapse onto that single bar. Adjudicated: engine is correct.
    entry_bar_min = tape[0][0]
    horizons = {}
    ends = {}
    for label, kind, k in FO_HORIZONS:
        if kind == "i":
            cap = entry_bar_min + k
            end = None
            for i in range(0, upper + 1):
                if tape[i][0] <= cap:
                    end = i
                else:
                    break
        elif kind == "eod":
            end = day_end[0]
        else:
            end = day_end[k]
        ends[label] = end
        horizons[label] = at(end)

    # --- threshold crossings (mirror engine: high/ep-1>=v up, low/ep-1<=-v
    # down; 1-based index; 0=never within horizon; None=horizon absent) ---
    up_pct = {t: None for t, _ in FO_PCT}; dn_pct = {t: None for t, _ in FO_PCT}
    up_atr = {t: None for t, _ in FO_ATR}; dn_atr = {t: None for t, _ in FO_ATR}
    have_atr = atr14 is not None and atr14 > 0
    for i, bb in enumerate(tape):
        hr = bb[2] / ep - 1.0; lr = bb[3] / ep - 1.0
        for t, v in FO_PCT:
            if up_pct[t] is None and hr >= v: up_pct[t] = i
            if dn_pct[t] is None and lr <= -v: dn_pct[t] = i
        if have_atr:
            for t, m in FO_ATR:
                if up_atr[t] is None and bb[2] >= ep + m*atr14: up_atr[t] = i
                if dn_atr[t] is None and bb[3] <= ep - m*atr14: dn_atr[t] = i
    def rep(first, end):
        if end is None: return None
        if first is not None and first <= end: return first + 1
        return 0
    cross = {}
    for label, _, _ in FO_HORIZONS:
        e = ends[label]
        for t, _ in FO_PCT:
            cross[f"first_cross_up_{t}pct_{label}"] = rep(up_pct[t], e)
            cross[f"first_cross_down_{t}pct_{label}"] = rep(dn_pct[t], e)
        for t, _ in FO_ATR:
            cross[f"first_cross_up_{t}atr_{label}"] = rep(up_atr[t], e) if have_atr else None
            cross[f"first_cross_down_{t}atr_{label}"] = rep(dn_atr[t], e) if have_atr else None

    # --- target-before-stop labels (8 pairs) ---
    labels = {}
    for name, up, dn, hl in FO_PAIRS:
        e = ends[hl]
        upp = ep*(1+up[1]) if up[0] == "pct" else (ep + up[1]*atr14 if have_atr else None)
        dnp = ep*(1-dn[1]) if dn[0] == "pct" else (ep - dn[1]*atr14 if have_atr else None)
        if e is None or upp is None or dnp is None:
            labels[name] = ("no_data", None); continue
        ua = da = None
        for i in range(0, e + 1):
            if ua is None and tape[i][2] >= upp: ua = i
            if da is None and tape[i][3] <= dnp: da = i
            if ua is not None and da is not None: break
        if ua is None and da is None: ev = "neither"
        elif da is None: ev = "target_first"
        elif ua is None: ev = "stop_first"
        elif ua < da: ev = "target_first"
        else: ev = "stop_first"  # da<ua or same bar (pessimistic)
        labels[name] = (ev, ev == "target_first")

    # ---- Aux: day-0 segments/shape, time-underwater, next-day, gap-vs-RTH ----
    ebm = tape[0][0]
    dclose = d0[-1][4]
    # ret_to_<seg>: last D bar in [entry_bar_min, seg_min].close / ep - 1
    seg_mins = [630, 660, 690, 720, 780, 840, 900, 930, d0[-1][0]]
    ret_to = [None] * 9
    for i, sm in enumerate(seg_mins):
        if sm < ebm:
            continue
        cand = [bb for bb in d0 if ebm <= bb[0] <= sm]
        if cand:
            ret_to[i] = cand[-1][4] / ep - 1.0
    # day-0 session-shape (post-entry bars only)
    post = d0[eidx + 1:]
    def hilo(lo, hi):
        seg = [bb for bb in post if bb[0] >= lo and (hi is None or bb[0] < hi)]
        if not seg:
            return (None, None)
        return (max(b[2] for b in seg) / ep - 1.0, min(b[3] for b in seg) / ep - 1.0)
    mh, ml = hilo(570, 690); dh, dl = hilo(690, 840); ah, al = hilo(840, None)
    pw = [b for b in post if b[0] >= 900]
    post_h = max((b[2] for b in post), default=None)
    post_l = min((b[3] for b in post), default=None)
    shape = [mh, ml, dh, dl, ah, al,
             (dclose / pw[0][1] - 1.0) if pw else None,
             (dclose / post_h - 1.0) if post_h is not None else None,
             (dclose / post_l - 1.0) if post_l is not None else None]
    # time-underwater (close-based) per EOD,1d,2d,3d,5d
    uw = []
    for lab in ["EOD", "1d", "2d", "3d", "5d"]:
        e = ends[lab]
        if e is None:
            uw.append((None, None, None, None, None)); continue
        n = e + 1
        prof = under = cp = cu = mp = mu = 0
        fu = rec = None
        for i in range(n):
            c = tape[i][4]
            if c > ep:
                prof += 1; cp += 1; cu = 0; mp = max(mp, cp)
                if fu is not None and rec is None: rec = i
            elif c < ep:
                under += 1; cu += 1; cp = 0; mu = max(mu, cu)
                if fu is None: fu = i
            else:
                cp = cu = 0
                if fu is not None and rec is None: rec = i
        ttr = (rec - fu) if (fu is not None and rec is not None) else 0
        uw.append((prof / n, under / n, mp, mu, ttr))
    # next-day (D+1)
    nd = per_day_bars[1] if len(per_day_bars) > 1 else []
    next_day = [None] * 11
    if nd:
        o = nd[0][1]; c = nd[-1][4]
        hi = max(b[2] for b in nd); lo = min(b[3] for b in nd)
        m0 = nd[0][0]
        def ndp(mins):
            cand = [b for b in nd if b[0] <= m0 + mins]
            return cand[-1][4] / o - 1.0 if cand else None
        next_day = [o / ep - 1.0, o / dclose - 1.0, ndp(5), ndp(15), ndp(30),
                    hi / o - 1.0, lo / o - 1.0, c / o - 1.0,
                    ((c - lo) / (hi - lo)) if hi > lo else None,
                    c / hi - 1.0, c / o - 1.0]
    # gap-vs-RTH days 1..5
    gap_rth = [[None] * 4 for _ in range(5)]
    prev = dclose
    for k in range(1, 6):
        cur = per_day_bars[k] if (k < len(per_day_bars) and per_day_bars[k]) else None
        if cur and prev is not None:
            o = cur[0][1]; c = cur[-1][4]
            gap_rth[k - 1] = [o / prev - 1.0, c / o - 1.0, c / prev - 1.0, c / o - 1.0]
        prev = cur[-1][4] if cur else None

    # bar_gap_minutes_max (short horizons): max same-day consecutive bar gap.
    # tape minutes ascend within a day and reset at a day boundary, so
    # minute[i] > minute[i-1] ⟺ same day (overnight gaps excluded).
    pgap = [0] * len(tape)
    rung = 0
    for i in range(1, len(tape)):
        if tape[i][0] > tape[i - 1][0]:
            rung = max(rung, tape[i][0] - tape[i - 1][0])
        pgap[i] = rung
    bar_gap = {lab: (pgap[ends[lab]] if ends[lab] is not None else None) for lab, _, _ in FO_HORIZONS}

    pre = d0[:eidx]
    pre_vol = sum(b[5] for b in pre)
    pre_high = max((b[2] for b in pre), default=None)
    return {
        "entry_price": ep,
        "is_halted_at_entry": ebar[0] > entry_min,
        "bar_gap": bar_gap,
        "pre_entry_ret_from_open": ep / rth_open - 1.0,
        "pre_entry_volume_from_open": pre_vol,
        "pre_entry_high_return_so_far": (pre_high / rth_open - 1.0) if pre else None,
        "entry_1m_volume": ebar[5],
        "entry_1m_range": ebar[2] - ebar[3],
        "entry_price_location_in_1m_bar": (
            (ebar[4] - ebar[3]) / (ebar[2] - ebar[3]) if ebar[2] > ebar[3] else None
        ),
        "entry_open_to_close_1m_return": ebar[4] / ebar[1] - 1.0 if ebar[1] > 0 else None,
        "horizons": horizons,
        "cross": cross,
        "labels": labels,
        "ret_to": ret_to,
        "shape": shape,
        "uw": uw,
        "next_day": next_day,
        "gap_rth": gap_rth,
        # cumulative pre-entry volume: premarket + RTH from open through entry
        "cum_vol": pm_vol + sum(b[5] for b in d0[:eidx + 1]),
        "cum_dollar": pm_dollar + sum(b[4] * b[5] for b in d0[:eidx + 1]),
    }


def forward_outcomes_checks(d):
    fo_path = OUT / "forward_outcomes" / f"{d}.parquet"
    if not fo_path.exists():
        r.check(f"{d} forward_outcomes present", False, "missing — run write-forward-outcomes")
        return
    sp_all, pin = _fo_splits()
    corpus = corpus_trading_days()
    if d not in corpus:
        return
    i0 = corpus.index(d)
    fdays = corpus[i0:i0 + 6]
    daybars = {fd: load_day_bars(fd) for fd in fdays}
    dayclose = {fd: spy_session_close(daybars[fd]) for fd in fdays}

    do = pl.read_parquet(
        OUT / "daily_observation" / f"{d}.parquet",
        columns=["security_id", "display_symbol_on_day", "atr_14d"],
    )
    sym2sid = {}
    sym2atr = {}
    for row in do.iter_rows(named=True):
        if row["display_symbol_on_day"] in SAMPLE:
            sym2sid[row["display_symbol_on_day"]] = row["security_id"]
            sym2atr[row["display_symbol_on_day"]] = row["atr_14d"]
    eng = pl.read_parquet(fo_path).filter(
        pl.col("security_id").is_in(list(sym2sid.values()))
    )

    sym_bars = {
        sym: [
            _fo_adj_rth(daybars[fd], dayclose[fd], sym, _fo_factor(sp_all, pin, sym, fd))
            for fd in fdays
        ]
        for sym in sym2sid
    }
    # adjusted premarket (04:00-09:30) volume/dollar for D (cum-volume base);
    # adj vol = raw/factor, adj dollar = raw_close*raw_vol (factor cancels).
    d0day = fdays[0]
    sym_pm = {}
    for sym in sym2sid:
        fD = _fo_factor(sp_all, pin, sym, d0day)
        pmb = daybars[d0day].filter(
            (pl.col("display_symbol") == sym)
            & (pl.col("et_time") >= time(4, 0)) & (pl.col("et_time") < time(9, 30))
        )
        rv = pmb["volume"].sum() or 0.0
        rcd = (pmb["close"] * pmb["volume"]).sum() or 0.0
        sym_pm[sym] = (rv / fD, rcd)
    spy_ret = {}
    if "SPY" in sym_bars:
        for off in FO_OFFSETS:
            res = _fo_resolve(sym_bars["SPY"], off, sym2atr.get("SPY"))
            spy_ret[off] = {
                h: (res["horizons"][h]["ret"] if res and res["horizons"][h] else None)
                for h, _, _ in FO_HORIZONS
            } if res else {}

    for sym, sid in sym2sid.items():
        f_d = _fo_factor(sp_all, pin, sym, d)
        for off in FO_OFFSETS:
            pmv, pmd = sym_pm[sym]
            exp = _fo_resolve(sym_bars[sym], off, sym2atr.get(sym), pmv, pmd)
            row = eng.filter((pl.col("security_id") == sid) & (pl.col("entry_offset") == off))
            if row.height != 1:
                r.check(f"{d} {sym}@{off} row present", False, f"rows={row.height}")
                continue
            row = row.to_dicts()[0]
            tag = f"{d} {sym}@{off}"
            if exp is None:
                r.check(f"{tag} entry null", row["entry_price"] is None, f"eng={row['entry_price']}")
                continue
            r.check(f"{tag} entry_price", close_enough(row["entry_price"], exp["entry_price"]),
                    f"eng={row['entry_price']} indep={exp['entry_price']}")
            r.check(f"{tag} entry_unadjusted_price",
                    close_enough(row["entry_unadjusted_price"], exp["entry_price"] / f_d),
                    f"eng={row['entry_unadjusted_price']} indep={exp['entry_price'] / f_d}")
            r.check(f"{tag} is_halted_at_entry",
                    row["is_halted_at_entry"] == exp["is_halted_at_entry"], "")
            for col in ["pre_entry_ret_from_open", "pre_entry_volume_from_open",
                        "pre_entry_high_return_so_far", "entry_1m_volume", "entry_1m_range",
                        "entry_price_location_in_1m_bar", "entry_open_to_close_1m_return"]:
                r.check(f"{tag} {col}", close_enough(row[col], exp[col]),
                        f"eng={row[col]} indep={exp[col]}")
            for label, _, _ in FO_HORIZONS:
                h = exp["horizons"][label]
                for stat, col in [
                    ("ret", f"ret_{label}"),
                    ("max_runup", f"max_runup_{label}"),
                    ("max_drawdown", f"max_drawdown_{label}"),
                    ("close_max_ret", f"close_max_ret_{label}"),
                    ("close_min_ret", f"close_min_ret_{label}"),
                    ("bars_to_max_runup", f"bars_to_max_runup_{label}"),
                    ("bars_to_max_drawdown", f"bars_to_max_drawdown_{label}"),
                    ("bars_to_close_min", f"bars_to_close_min_{label}"),
                    ("bars_to_close_max", f"bars_to_close_max_{label}"),
                ]:
                    want = h[stat] if h else None
                    r.check(f"{tag} {col}", close_enough(row[col], want),
                            f"eng={row[col]} indep={want}")
                want_x = None
                if h and spy_ret.get(off, {}).get(label) is not None:
                    want_x = h["ret"] - spy_ret[off][label]
                r.check(f"{tag} ret_{label}_excess_spy",
                        close_enough(row[f"ret_{label}_excess_spy"], want_x),
                        f"eng={row[f'ret_{label}_excess_spy']} indep={want_x}")
            # threshold crossings (every filled column, all 8 short horizons)
            for col, want in exp["cross"].items():
                r.check(f"{tag} {col}", row[col] == want, f"eng={row[col]} indep={want}")
            # target-before-stop labels (8 B3 pairs): first_event + hit
            for name, (ev, h_) in exp["labels"].items():
                r.check(f"{tag} first_event_{name}", row[f"first_event_{name}"] == ev,
                        f"eng={row[f'first_event_{name}']} indep={ev}")
                r.check(f"{tag} hit_{name}", row[f"hit_{name}"] == h_,
                        f"eng={row[f'hit_{name}']} indep={h_}")
            # day-0 segments
            for i, seg in enumerate(["1030", "1100", "1130", "1200", "1300", "1400", "1500", "1530", "close"]):
                r.check(f"{tag} ret_to_{seg}", close_enough(row[f"ret_to_{seg}"], exp["ret_to"][i]),
                        f"eng={row[f'ret_to_{seg}']} indep={exp['ret_to'][i]}")
            # day-0 session-shape
            for i, name in enumerate([
                "post_entry_morning_high_return", "post_entry_morning_low_return",
                "post_entry_midday_high_return", "post_entry_midday_low_return",
                "post_entry_afternoon_high_return", "post_entry_afternoon_low_return",
                "post_entry_power_hour_return", "post_entry_close_vs_high_return",
                "post_entry_close_vs_low_return"]):
                r.check(f"{tag} {name}", close_enough(row[name], exp["shape"][i]),
                        f"eng={row[name]} indep={exp['shape'][i]}")
            # time-underwater
            for slot, hh in enumerate(["EOD", "1d", "2d", "3d", "5d"]):
                u = exp["uw"][slot]
                for col, want in [
                    (f"pct_bars_profitable_{hh}", u[0]), (f"pct_bars_underwater_{hh}", u[1]),
                    (f"max_consecutive_bars_profitable_{hh}", u[2]),
                    (f"max_consecutive_bars_underwater_{hh}", u[3]),
                    (f"time_to_recover_after_first_drawdown_{hh}", u[4]),
                ]:
                    ok = close_enough(row[col], want) if isinstance(want, float) or want is None else row[col] == want
                    r.check(f"{tag} {col}", ok, f"eng={row[col]} indep={want}")
            # next-day
            for i, name in enumerate([
                "next_day_open_return", "next_day_gap_return", "next_day_first_5m_return",
                "next_day_first_15m_return", "next_day_first_30m_return", "next_day_high_return",
                "next_day_low_return", "next_day_close_return", "next_day_close_location_in_range",
                "next_day_fade_from_open", "next_day_continuation_from_open"]):
                r.check(f"{tag} {name}", close_enough(row[name], exp["next_day"][i]),
                        f"eng={row[name]} indep={exp['next_day'][i]}")
            # gap-vs-RTH days 1..5
            for k in range(1, 6):
                for j, fam in enumerate(["gap_return", "rth_return", "close_to_close_return", "open_to_close_return"]):
                    col = f"{fam}_day_{k}"
                    r.check(f"{tag} {col}", close_enough(row[col], exp["gap_rth"][k - 1][j]),
                            f"eng={row[col]} indep={exp['gap_rth'][k-1][j]}")
            # bar_gap_minutes_max: 8 short horizons from the 1m tape; 5 long null
            for lab, _, _ in FO_HORIZONS:
                r.check(f"{tag} bar_gap_minutes_max_{lab}",
                        row[f"bar_gap_minutes_max_{lab}"] == exp["bar_gap"][lab],
                        f"eng={row[f'bar_gap_minutes_max_{lab}']} indep={exp['bar_gap'][lab]}")
            for lab in ["10d", "21d", "42d", "63d", "252d"]:
                r.check(f"{tag} bar_gap_minutes_max_{lab} null",
                        row[f"bar_gap_minutes_max_{lab}"] is None, f"eng={row[f'bar_gap_minutes_max_{lab}']}")
            # cumulative pre-entry volume
            r.check(f"{tag} cumulative_volume_to_entry",
                    close_enough(row["cumulative_volume_to_entry"], exp["cum_vol"]),
                    f"eng={row['cumulative_volume_to_entry']} indep={exp['cum_vol']}")
            r.check(f"{tag} cumulative_dollar_volume_to_entry",
                    close_enough(row["cumulative_dollar_volume_to_entry"], exp["cum_dollar"]),
                    f"eng={row['cumulative_dollar_volume_to_entry']} indep={exp['cum_dollar']}")

    # cross-sectional pre-entry ranks: full-universe self-consistency — the
    # rank/percentile columns must equal ranks_desc() of the (sample-validated)
    # pre_entry source columns, per entry_offset.
    full = pl.read_parquet(fo_path, columns=[
        "entry_offset", "pre_entry_ret_from_open", "pre_entry_dollar_volume_from_open",
        "pre_entry_ret_rank_today", "pre_entry_ret_percentile_today",
        "pre_entry_dollar_volume_rank_today"])
    rmm = pmm = dmm = 0
    for off in full["entry_offset"].unique().to_list():
        sub = full.filter(pl.col("entry_offset") == off)
        rk, pc = ranks_desc(sub["pre_entry_ret_from_open"].to_list())
        dk, _ = ranks_desc(sub["pre_entry_dollar_volume_from_open"].to_list())
        rmm += sum(1 for a, b in zip(rk, sub["pre_entry_ret_rank_today"].to_list()) if a != b)
        pmm += sum(1 for a, b in zip(pc, sub["pre_entry_ret_percentile_today"].to_list())
                   if not close_enough(a, b))
        dmm += sum(1 for a, b in zip(dk, sub["pre_entry_dollar_volume_rank_today"].to_list()) if a != b)
    r.check(f"{d} pre_entry_ret_rank_today (full universe)", rmm == 0, f"mismatches={rmm}")
    r.check(f"{d} pre_entry_ret_percentile_today (full universe)", pmm == 0, f"mismatches={pmm}")
    r.check(f"{d} pre_entry_dollar_volume_rank_today (full universe)", dmm == 0, f"mismatches={dmm}")


def forward_property_sweep(days_all):
    fo_dir = OUT / "forward_outcomes"
    swept = [d for d in days_all if (fo_dir / f"{d}.parquet").exists()]
    print(f"\n{'='*64}\nL2. forward_outcomes property sweep over {len(swept)} days")
    n_fields = None
    viol = {k: 0 for k in [
        "row count != universe*17", "entry_offset out of grid", "stamp drift",
        "schema drift", "max_runup<0", "max_drawdown>0",
        "ret_EOD outside [dd,runup]", "close_max<close_min",
        "is_halted null when entry present",
        "B5 horizon crossing not null", "first_event out of domain",
        "hit != (first_event==target_first)",
        "bar_gap 10d+ not null", "ret_252d outside [dd,runup]",
    ]}
    grid = {"0935", "0940", "0945", "0950", "0955", "1000", "1005", "1010",
            "1015", "1020", "1030", "1045", "1100", "1130", "1200", "1300", "1530"}
    for d in swept:
        p = fo_dir / f"{d}.parquet"
        pf = pq.ParquetFile(p)
        meta = {k.decode(): v.decode() for k, v in pf.metadata.metadata.items()}
        if meta.get("forward_outcomes_version") != "v2":
            viol["stamp drift"] += 1
        if n_fields is None:
            n_fields = len(pf.schema_arrow.names)
        elif len(pf.schema_arrow.names) != n_fields:
            viol["schema drift"] += 1
        n_univ = pl.read_parquet(
            OUT / "daily_observation" / f"{d}.parquet", columns=["security_id"]
        ).height
        t = pl.read_parquet(p, columns=[
            "entry_offset", "entry_price", "is_halted_at_entry",
            "max_runup_EOD", "max_drawdown_EOD", "ret_EOD",
            "close_max_ret_EOD", "close_min_ret_EOD",
        ])
        if t.height != n_univ * 17:
            viol["row count != universe*17"] += 1
        if set(t["entry_offset"].unique().to_list()) - grid:
            viol["entry_offset out of grid"] += 1
        viol["max_runup<0"] += t.filter(pl.col("max_runup_EOD") < -1e-9).height
        viol["max_drawdown>0"] += t.filter(pl.col("max_drawdown_EOD") > 1e-9).height
        viol["ret_EOD outside [dd,runup]"] += t.filter(
            (pl.col("ret_EOD") < pl.col("max_drawdown_EOD") - 1e-9)
            | (pl.col("ret_EOD") > pl.col("max_runup_EOD") + 1e-9)
        ).height
        viol["close_max<close_min"] += t.filter(
            pl.col("close_max_ret_EOD") < pl.col("close_min_ret_EOD") - 1e-9
        ).height
        viol["is_halted null when entry present"] += t.filter(
            pl.col("entry_price").is_not_null() & pl.col("is_halted_at_entry").is_null()
        ).height
        # B5: bar_gap is null for the 5 long horizons; ret_252d within [dd,runup]
        b5 = pl.read_parquet(p, columns=[
            "bar_gap_minutes_max_10d", "bar_gap_minutes_max_252d",
            "ret_252d", "max_drawdown_252d", "max_runup_252d"])
        viol["bar_gap 10d+ not null"] += b5.filter(
            pl.col("bar_gap_minutes_max_10d").is_not_null()
            | pl.col("bar_gap_minutes_max_252d").is_not_null()).height
        viol["ret_252d outside [dd,runup]"] += b5.filter(
            pl.col("ret_252d").is_not_null()
            & ((pl.col("ret_252d") < pl.col("max_drawdown_252d") - 1e-9)
               | (pl.col("ret_252d") > pl.col("max_runup_252d") + 1e-9))).height
        # B5b-deferred crossings (10d+) stay null; label domain + hit↔event
        lab = pl.read_parquet(p, columns=[
            "first_cross_up_1pct_10d", "first_cross_up_1pct_252d",
            "first_event_1pct_before_minus_1pct_EOD", "hit_1pct_before_minus_1pct_EOD",
        ])
        viol["B5 horizon crossing not null"] += lab.filter(
            pl.col("first_cross_up_1pct_10d").is_not_null()
            | pl.col("first_cross_up_1pct_252d").is_not_null()
        ).height
        ev = "first_event_1pct_before_minus_1pct_EOD"; hh = "hit_1pct_before_minus_1pct_EOD"
        viol["first_event out of domain"] += lab.filter(
            pl.col(ev).is_not_null()
            & ~pl.col(ev).is_in(["target_first", "stop_first", "neither", "no_data"])
        ).height
        viol["hit != (first_event==target_first)"] += lab.filter(
            (pl.col(ev).is_in(["target_first", "stop_first", "neither"]))
            & (pl.col(hh) != (pl.col(ev) == "target_first"))
        ).height
    for k, n in viol.items():
        r.check(f"fo property: {k}", n == 0, f"violations={n}")


# ---------------------------------------------------------------------------
# L5. forward_outcomes B5 multi-day (10d–252d) + ret_total + bar_gap
# ---------------------------------------------------------------------------

LONG_H = [("10d", 10), ("21d", 21), ("42d", 42), ("63d", 63), ("252d", 252)]
MD_DAYS = [("1d", 1), ("2d", 2), ("3d", 3), ("5d", 5), ("10d", 10),
           ("21d", 21), ("42d", 42), ("63d", 63), ("252d", 252)]
FO_HORIZON_LABELS = ["10min", "30min", "60min", "EOD", "1d", "2d", "3d", "5d",
                     "10d", "21d", "42d", "63d", "252d"]
_MD_CACHE = {}  # (day_str) -> {sym: (close,high,low) pin-adjusted RTH}


def _md_daily(d, syms):
    """Pin-adjusted RTH (close,high,low) for syms on day d, cached."""
    if d not in _MD_CACHE:
        sp_all, pin = _fo_splits()
        bars = load_day_bars(d)
        close_t = spy_session_close(bars)
        out = {}
        sub = bars.filter(
            pl.col("display_symbol").is_in(list(syms))
            & (pl.col("et_time") >= time(9, 30)) & (pl.col("et_time") <= close_t)
        ).sort("t")
        for sym, g in sub.group_by("display_symbol"):
            sym = sym[0]
            f = _fo_factor(sp_all, pin, sym, d)
            out[sym] = (g["close"][-1] * f, g["high"].max() * f, g["low"].min() * f)
        _MD_CACHE[d] = out
    return _MD_CACHE[d]


def _md_dividends(sym):
    sp_all, pin = _fo_splits()
    dv = pl.read_parquet(REF / "dividends.parquet",
                         columns=["display_symbol", "ex_dividend_date", "cash_amount"]).filter(
        pl.col("display_symbol") == sym)
    out = []
    for r in dv.iter_rows(named=True):
        ex = r["ex_dividend_date"]
        if ex is None or r["cash_amount"] is None:
            continue
        out.append((ex, r["cash_amount"] * _fo_factor(sp_all, pin, sym, ex)))
    return sorted(out)


def forward_multiday_checks(d):
    corpus = corpus_trading_days()
    if d not in corpus:
        return
    i0 = corpus.index(d)
    do = pl.read_parquet(OUT / "daily_observation" / f"{d}.parquet",
                         columns=["security_id", "display_symbol_on_day"])
    sym2sid = {r["display_symbol_on_day"]: r["security_id"] for r in do.iter_rows(named=True)
               if r["display_symbol_on_day"] in SAMPLE}
    eng = pl.read_parquet(OUT / "forward_outcomes" / f"{d}.parquet").filter(
        pl.col("security_id").is_in(list(sym2sid.values())))
    allsyms = set(sym2sid) | {"SPY", "QQQ", "IWM"}
    # forward daily series per sym (traded days after D): (date, close, high, low)
    fwd = {s: [] for s in allsyms}
    j = i0 + 1
    while j < len(corpus) and any(len(fwd[s]) < 252 for s in allsyms):
        dd = _md_daily(corpus[j], allsyms)
        dat = date.fromisoformat(corpus[j]) if isinstance(corpus[j], str) else corpus[j]
        for s in allsyms:
            if s in dd:
                fwd[s].append((dat, *dd[s]))
        j += 1
    divs = {s: _md_dividends(s) for s in sym2sid}
    bars_d = load_day_bars(d)
    ct = spy_session_close(bars_d)
    sp_all, pin = _fo_splits()
    rows = {(row["security_id"], row["entry_offset"]): row for row in eng.iter_rows(named=True)}

    def day0_extremes(sym, em):
        f0 = _fo_factor(sp_all, pin, sym, d)
        b0 = bars_d.filter((pl.col("display_symbol") == sym) & (pl.col("et_time") >= time(9, 30))
                           & (pl.col("et_time") <= ct)).sort("t")
        b0 = b0.filter(pl.col("et_time").map_elements(
            lambda x: x.hour * 60 + x.minute >= em, return_dtype=pl.Boolean))
        if not b0.height:
            return None
        return (b0["high"].max() * f0, b0["low"].min() * f0, b0["close"][-1] * f0)

    for sym, sid in sym2sid.items():
        for off in FO_OFFSETS:
            row = rows[(sid, off)]
            ep = row["entry_price"]
            tag = f"{d} {sym}@{off} MD"
            if ep is None:
                continue
            em = int(off[:2]) * 60 + int(off[2:])
            ext = day0_extremes(sym, em)
            if ext is None:
                continue
            d0_hi, d0_lo, d0_c = ext
            series = fwd[sym]
            spy_row = rows.get((sym2sid.get("SPY"), off))
            for label, H in LONG_H:
                if len(series) < H:
                    r.check(f"{tag} ret_{label} null", row[f"ret_{label}"] is None, f"eng={row[f'ret_{label}']}")
                    continue
                win = series[:H]
                ret = win[H - 1][1] / ep - 1
                runup = max([d0_hi] + [x[2] for x in win]) / ep - 1
                dd_ = min([d0_lo] + [x[3] for x in win]) / ep - 1
                cmax = max([d0_c] + [x[1] for x in win]) / ep - 1
                cmin = min([d0_c] + [x[1] for x in win]) / ep - 1
                for col, w in [(f"ret_{label}", ret), (f"max_runup_{label}", runup),
                               (f"max_drawdown_{label}", dd_), (f"close_max_ret_{label}", cmax),
                               (f"close_min_ret_{label}", cmin)]:
                    r.check(f"{tag} {col}", close_enough(row[col], w), f"eng={row[col]} indep={w}")
                # excess consistency: engine excess == eng ret(sym) - eng ret(SPY)
                if spy_row is not None and spy_row[f"ret_{label}"] is not None:
                    want_x = row[f"ret_{label}"] - spy_row[f"ret_{label}"]
                    r.check(f"{tag} ret_{label}_excess_spy",
                            close_enough(row[f"ret_{label}_excess_spy"], want_x),
                            f"eng={row[f'ret_{label}_excess_spy']} indep={want_x}")
            dv = divs[sym]
            for label, H in MD_DAYS:
                if len(series) < H:
                    continue
                day_h = series[H - 1][0]
                div_sum = sum(a for ex, a in dv if d < ex <= day_h)
                rt = (series[H - 1][1] + div_sum) / ep - 1
                anydiv = any(d < ex <= day_h for ex, a in dv)
                r.check(f"{tag} ret_{label}_total", close_enough(row[f"ret_{label}_total"], rt),
                        f"eng={row[f'ret_{label}_total']} indep={rt}")
                r.check(f"{tag} dividend_ex_date_within_{label}",
                        row[f"dividend_ex_date_within_{label}"] == anydiv, "")


# ---------------------------------------------------------------------------
# M. forward_path_short (B4) — sample exact recompute + property sweep
# ---------------------------------------------------------------------------

FP_CHECKPOINTS = ["1m", "2m", "3m", "5m", "10m", "15m", "20m", "30m", "45m", "60m",
                  "90m", "120m", "180m", "EOD", "1d_open", "1d_30m", "1d_close",
                  "2d_open", "2d_close", "3d_open", "3d_close", "5d_open", "5d_close"]


def _fp_resolve(per_day_bars, off, atr14=None):
    """Recompute the 23 path checkpoints for one (sym, offset). Mirrors
    crates/momentum-engine/src/forward_path.rs. Returns {cp: {col: val}}."""
    d0 = per_day_bars[0]
    if not d0:
        return None
    em = int(off[:2]) * 60 + int(off[2:])
    eidx = next((i for i, b in enumerate(d0) if b[0] >= em), None)
    if eidx is None:
        return None
    ep = d0[eidx][1]
    if not ep > 0:
        return None
    # tape + per-day (start, close) bounds
    tape = list(d0[eidx:])
    bounds = [(0, len(tape) - 1)] + [None] * 5
    for k in range(1, 6):
        if k < len(per_day_bars) and per_day_bars[k]:
            s = len(tape); tape.extend(per_day_bars[k]); bounds[k] = (s, len(tape) - 1)
    ebm = tape[0][0]
    daystart = {bounds[k][0] for k in range(1, 6) if bounds[k]}

    # prefix scans
    n = len(tape)
    mh = ml = mc = nc = None
    sv = sd = sr = sr2 = 0.0
    cprof = cund = 0
    halt = False
    P = []
    for i, b in enumerate(tape):
        mh = b[2] if mh is None else max(mh, b[2])
        ml = b[3] if ml is None else min(ml, b[3])
        mc = b[4] if mc is None else max(mc, b[4])
        nc = b[4] if nc is None else min(nc, b[4])
        sv += b[5]; sd += b[4] * b[5]
        if b[4] > ep: cprof += 1
        elif b[4] < ep: cund += 1
        if i >= 1:
            rr = tape[i][4] / tape[i - 1][4] - 1.0
            sr += rr; sr2 += rr * rr
            if i not in daystart and (tape[i][0] - tape[i - 1][0]) >= 5:
                halt = True
        P.append((mh, ml, mc, nc, sv, sd, cprof, cund, sr, sr2, halt))

    def cp_end(cp):
        if cp == "EOD": return bounds[0][1]
        if cp.endswith("_open"): k = int(cp[0]); return bounds[k][0] if bounds[k] else None
        if cp == "1d_30m":
            if not bounds[1]: return None
            s, c = bounds[1]; cut = tape[s][0] + 30; last = None
            for i in range(s, c + 1):
                if tape[i][0] <= cut: last = i
                else: break
            return last
        if cp.endswith("_close"): k = int(cp[0]); return bounds[k][1] if bounds[k] else None
        mins = int(cp[:-1]); cut = ebm + mins; last = None  # intraday Nm
        for i in range(0, bounds[0][1] + 1):
            if tape[i][0] <= cut: last = i
            else: break
        return last

    atr = atr14 if (atr14 is not None and atr14 > 0) else None
    out = {}
    prev = (0.0, 0)
    for cp in FP_CHECKPOINTS:
        e = cp_end(cp)
        if e is None or e >= n:
            out[cp] = {c: None for c in [
                "ret", "high_ret_so_far", "low_ret_so_far", "close_max_ret_so_far",
                "close_min_ret_so_far", "volume_since_entry", "dollar_volume_since_entry",
                "vwap_since_entry", "bars_elapsed", "pct_bars_profitable_so_far",
                "pct_bars_underwater_so_far", "volatility_within_trade", "rate_of_change",
                "current_ret_over_atr_14d", "halt_gap_crossed"]}
            continue
        mh, ml, mc, nc, sv, sd, cprof, cund, sr, sr2, halt = P[e]
        ret = tape[e][4] / ep - 1.0
        nbar = e + 1
        vw = None
        if e >= 2:
            nr = e
            var = (sr2 - sr * sr / nr) / (nr - 1)
            vw = max(var, 0.0) ** 0.5
        roc = (ret - prev[0]) / (e - prev[1]) if e > prev[1] else None
        prev = (ret, e)
        out[cp] = {
            "ret": ret, "high_ret_so_far": mh / ep - 1.0, "low_ret_so_far": ml / ep - 1.0,
            "close_max_ret_so_far": mc / ep - 1.0, "close_min_ret_so_far": nc / ep - 1.0,
            "volume_since_entry": sv, "dollar_volume_since_entry": sd,
            "vwap_since_entry": (sd / sv) if sv > 0 else None, "bars_elapsed": e,
            "pct_bars_profitable_so_far": cprof / nbar, "pct_bars_underwater_so_far": cund / nbar,
            "volatility_within_trade": vw, "rate_of_change": roc,
            "current_ret_over_atr_14d": ((tape[e][4] - ep) / atr) if atr else None,
            "halt_gap_crossed": halt,
        }
    return out


def forward_path_checks(d):
    fp_path = OUT / "forward_path_short" / f"{d}.parquet"
    if not fp_path.exists():
        r.check(f"{d} forward_path_short present", False, "missing")
        return
    sp_all, pin = _fo_splits()
    corpus = corpus_trading_days()
    if d not in corpus:
        return
    fdays = corpus[corpus.index(d):corpus.index(d) + 6]
    daybars = {fd: load_day_bars(fd) for fd in fdays}
    dayclose = {fd: spy_session_close(daybars[fd]) for fd in fdays}
    do = pl.read_parquet(OUT / "daily_observation" / f"{d}.parquet",
                         columns=["security_id", "display_symbol_on_day", "atr_14d"])
    sym2sid, sym2atr = {}, {}
    for row in do.iter_rows(named=True):
        if row["display_symbol_on_day"] in SAMPLE:
            sym2sid[row["display_symbol_on_day"]] = row["security_id"]
            sym2atr[row["display_symbol_on_day"]] = row["atr_14d"]
    eng = pl.read_parquet(fp_path).filter(pl.col("security_id").is_in(list(sym2sid.values())))
    cols = ["ret", "high_ret_so_far", "low_ret_so_far", "close_max_ret_so_far",
            "close_min_ret_so_far", "volume_since_entry", "dollar_volume_since_entry",
            "vwap_since_entry", "bars_elapsed", "pct_bars_profitable_so_far",
            "pct_bars_underwater_so_far", "volatility_within_trade", "rate_of_change",
            "current_ret_over_atr_14d", "halt_gap_crossed"]
    for sym, sid in sym2sid.items():
        pdb = [_fo_adj_rth(daybars[fd], dayclose[fd], sym, _fo_factor(sp_all, pin, sym, fd)) for fd in fdays]
        for off in FO_OFFSETS:
            exp = _fp_resolve(pdb, off, sym2atr.get(sym))
            sub = eng.filter((pl.col("security_id") == sid) & (pl.col("entry_offset") == off))
            if exp is None:
                r.check(f"{d} {sym}@{off} fp: no rows when no entry", sub.height == 0, f"rows={sub.height}")
                continue
            r.check(f"{d} {sym}@{off} fp: 23 checkpoints", sub.height == 23, f"rows={sub.height}")
            rowmap = {row["path_checkpoint"]: row for row in sub.iter_rows(named=True)}
            for cp in FP_CHECKPOINTS:
                row = rowmap.get(cp)
                if row is None:
                    r.check(f"{d} {sym}@{off} {cp} present", False, "")
                    continue
                for c in cols:
                    want = exp[cp][c]
                    got = row[c]
                    ok = (got == want) if (c in ("bars_elapsed", "halt_gap_crossed") or want is None or got is None) else close_enough(got, want)
                    r.check(f"{d} {sym}@{off} {cp}.{c}", ok, f"eng={got} indep={want}")


def forward_path_property_sweep(days_all):
    fp_dir = OUT / "forward_path_short"
    swept = [d for d in days_all if (fp_dir / f"{d}.parquet").exists()]
    print(f"\n{'='*64}\nM2. forward_path_short property sweep over {len(swept)} days")
    cpset = set(FP_CHECKPOINTS)
    n_fields = None
    viol = {k: 0 for k in [
        "stamp drift", "schema drift", "checkpoint out of set",
        "rows per (sid,offset) not multiple of 23", "ret outside [low,high]",
        "close_max<close_min", "pct out of [0,1]", "bars_elapsed<0",
        "cross-table ret != forward_outcomes",
    ]}
    for d in swept:
        p = fp_dir / f"{d}.parquet"
        pf = pq.ParquetFile(p)
        meta = {k.decode(): v.decode() for k, v in pf.metadata.metadata.items()}
        if meta.get("forward_path_checkpoints_version") != "v2" or meta.get("engine_milestone") != "B4":
            viol["stamp drift"] += 1
        if n_fields is None:
            n_fields = len(pf.schema_arrow.names)
        elif len(pf.schema_arrow.names) != n_fields:
            viol["schema drift"] += 1
        t = pl.read_parquet(p, columns=[
            "security_id", "entry_offset", "path_checkpoint", "ret",
            "high_ret_so_far", "low_ret_so_far", "close_max_ret_so_far",
            "close_min_ret_so_far", "pct_bars_profitable_so_far", "bars_elapsed"])
        if set(t["path_checkpoint"].unique().to_list()) - cpset:
            viol["checkpoint out of set"] += 1
        # 23 per (sid, offset); a vendor sid collision (two listings under one
        # FIGI) legitimately yields a multiple of 23 (build-state §6).
        grp = t.group_by("security_id", "entry_offset").len()
        viol["rows per (sid,offset) not multiple of 23"] += grp.filter(pl.col("len") % 23 != 0).height
        viol["ret outside [low,high]"] += t.filter(
            (pl.col("ret") < pl.col("low_ret_so_far") - 1e-9)
            | (pl.col("ret") > pl.col("high_ret_so_far") + 1e-9)).height
        viol["close_max<close_min"] += t.filter(
            pl.col("close_max_ret_so_far") < pl.col("close_min_ret_so_far") - 1e-9).height
        viol["pct out of [0,1]"] += t.filter(
            (pl.col("pct_bars_profitable_so_far") < 0) | (pl.col("pct_bars_profitable_so_far") > 1)).height
        viol["bars_elapsed<0"] += t.filter(pl.col("bars_elapsed") < 0).height
        # cross-table: fp.ret at aligning checkpoints == fo.ret_<H>
        fo = pl.read_parquet(OUT / "forward_outcomes" / f"{d}.parquet",
                             columns=["security_id", "entry_offset", "ret_EOD", "ret_5d"])
        for cp, h in [("EOD", "ret_EOD"), ("5d_close", "ret_5d")]:
            j = t.filter(pl.col("path_checkpoint") == cp).join(
                fo, on=["security_id", "entry_offset"], how="inner")
            viol["cross-table ret != forward_outcomes"] += j.filter(
                (pl.col("ret") - pl.col(h)).abs() > 1e-9).height
    for k, n in viol.items():
        r.check(f"fp property: {k}", n == 0, f"violations={n}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    days_all = all_trading_days()
    battery = ([date.fromisoformat(a) for a in sys.argv[1:]]
               or [date(2016, 12, 30), date(2016, 11, 25), date(2016, 11, 7), date(2016, 6, 9)])
    battery = [d for d in battery if d in set(days_all)]
    print(f"battery days: {battery} | swept days: {len(days_all)}")

    ref_idx = reference_lookup()
    shares_map = filed_shares_lookup()
    smap = sector_map_port()

    # Cumulative ever-collided sid set per day (engine purges trailing
    # state permanently on first collision).
    amb_cum = {}
    amb = set()
    for d in days_all:
        sids = pl.read_parquet(
            OUT / "daily_observation" / f"{d}.parquet", columns=["security_id"]
        )["security_id"]
        dup = sids.to_frame().group_by("security_id").len().filter(pl.col("len") > 1)
        amb |= set(dup["security_id"].to_list())
        amb_cum[d] = set(amb)

    # Per-day universe signal share (engine definition), for the
    # concentration-percentile history.
    share_history = {}
    for d in days_all:
        col = pl.read_parquet(
            OUT / "daily_observation" / f"{d}.parquet",
            columns=["intraday_ret_0930_to_1000"],
        )["intraday_ret_0930_to_1000"].drop_nulls()
        if col.len():
            share_history[d] = (col > 0).sum() / col.len()

    for d in battery:
        obs, raw, jr, colls, close_t, praw, prior_day = validate_day(
            d, days_all, ref_idx, shares_map, share_history, amb_cum[d]
        )
        trailing_checks(d, days_all, obs)
        earnings_check(d, obs)
        bars = load_day_bars(d)
        market_context_checks(d, obs, raw, bars, close_t, praw)
        sector_checks(d, obs, smap)
        classification_checks(d, obs, jr, colls, ref_idx, shares_map, praw)
        forward_outcomes_checks(d)
        forward_multiday_checks(d)
        forward_path_checks(d)

    regime_checks(days_all)
    property_sweep(days_all)
    forward_property_sweep(days_all)
    forward_path_property_sweep(days_all)

    print(f"\n{'='*64}")
    print(f"PASSED: {G}{r.passes}{X}   FAILED: {R}{r.fails}{X}")
    if r.fail_msgs:
        print(f"{R}Failures:{X}")
        for m in r.fail_msgs:
            print(f"  - {m}")
        sys.exit(1)
    print(f"{G}ALL INDEPENDENT CHECKS PASS{X}")


if __name__ == "__main__":
    main()
