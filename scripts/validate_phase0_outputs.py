#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow python3
"""Independent cross-validation of Phase 0 engine outputs (v0).

Recomputes engine-output columns FROM THE RAW TAPE in polars — a second
implementation, different language, written from the column definitions —
and diffs against the Rust engine's parquet output. Catches correlated
author errors that the Rust unit tests can't (same person wrote code and
tests).

v0 scope: one target day, full cross-section for day-local columns,
sampled securities for trailing windows, cross-table consistency
(daily_observation ↔ market_context_daily ↔ sector_aggregates_daily),
and an independent earnings-proximity recount. Grows into the B6 suite.

Usage: scripts/validate_phase0_outputs.py [DAY=2016-12-30]
"""

import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "outputs"
BARS = ROOT / "data" / "bars_1m_raw"
REF = ROOT / "data" / "reference"

DAY = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date(2016, 12, 30)

G, R, Y, X = "\033[32m", "\033[31m", "\033[33m", "\033[0m"


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


def et(frame, col="t"):
    return pl.col(col).dt.convert_time_zone("America/New_York")


def load_day_bars(d: date) -> pl.DataFrame:
    df = pl.read_parquet(BARS / f"{d}.parquet")
    return df.with_columns(
        et(df).dt.time().alias("et_time"),
        et(df).dt.date().alias("et_date"),
    ).filter(pl.col("et_date") == d)


def session_windows(bars: pl.DataFrame) -> pl.DataFrame:
    """Per-security UNADJUSTED day aggregates, recomputed from raw 1m bars.

    Universe = every (sid, symbol) with ANY session bar (the engine keeps
    premarket/AH-only securities — e.g. Nasdaq .TEST symbols — by the
    record-everything amendment). RTH aggregates are null when no RTH
    bars printed. RTH includes the 16:00 closing-auction print.
    """
    from datetime import time

    bars = bars.with_columns(
        pl.coalesce(pl.col("security_id"), pl.col("display_symbol")).alias("sid_key")
    )
    rth = bars.filter((pl.col("et_time") >= time(9, 30)) & (pl.col("et_time") <= time(16, 0)))
    pm = bars.filter((pl.col("et_time") >= time(4, 0)) & (pl.col("et_time") < time(9, 30)))
    pre1000 = rth.filter(pl.col("et_time") < time(10, 0))

    universe = bars.group_by("sid_key", "display_symbol").agg(pl.len().alias("n_bars"))
    agg = rth.group_by("sid_key", "display_symbol").agg(
        pl.col("open").first().alias("rth_open"),
        pl.col("high").max().alias("rth_high"),
        pl.col("low").min().alias("rth_low"),
        pl.col("close").last().alias("rth_close"),
        pl.col("volume").sum().alias("rth_volume"),
        pl.len().alias("bar_count_rth"),
        pl.col("et_time").first().alias("first_rth_time"),
    )
    agg = universe.join(agg, on=["sid_key", "display_symbol"], how="left")
    agg = agg.join(
        pre1000.group_by("sid_key", "display_symbol").agg(
            pl.col("close").last().alias("close_1000")
        ),
        on=["sid_key", "display_symbol"],
        how="left",
    )
    agg = agg.join(
        pm.group_by("sid_key", "display_symbol").agg(
            pl.col("volume").sum().alias("pm_volume")
        ),
        on=["sid_key", "display_symbol"],
        how="left",
    )
    return agg.with_columns(
        (pl.col("close_1000") / pl.col("rth_open") - 1.0).alias("ret_0930_1000"),
        pl.col("pm_volume").fill_null(0.0),
    )


def pin_factors(d: date) -> pl.DataFrame:
    """Independent re-derivation of pin-basis split factors from splits.parquet."""
    import pyarrow.parquet as pq

    f = pq.ParquetFile(REF / "splits.parquet")
    pin = date.fromisoformat(
        f.metadata.metadata[b"splits_snapshot_date"].decode()
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


print(f"=== independent validation of {DAY} ===\n")

# ---------- A. daily_observation vs raw tape, full cross-section ----------
print("A. daily_observation day-local columns vs raw-tape recomputation")
obs = pl.read_parquet(OUT / "daily_observation" / f"{DAY}.parquet").select(
    "security_id",
    "display_symbol_on_day",
    "eod_day_open",
    "eod_day_high",
    "eod_day_low",
    "eod_day_close",
    "eod_day_volume",
    "eod_unadjusted_day_close",
    "adjustment_factor_on_day",
    "premarket_volume",
    "intraday_ret_0930_to_1000",
    "bar_count_rth",
    "first_rth_bar_time",
    "overnight_gap",
    "prior_day_eod_close",
    "is_earnings_day",
    "days_since_last_earnings",
)
raw = session_windows(load_day_bars(DAY))
bysym, bysid = pin_factors(DAY)
raw = (
    raw.join(bysym, on="display_symbol", how="left")
    .join(bysid, left_on="sid_key", right_on="security_id", how="left")
    .with_columns(
        pl.coalesce(pl.col("sid_factor"), pl.col("sym_factor"), pl.lit(1.0)).alias("factor")
    )
    .with_columns(
        (pl.col("rth_open") * pl.col("factor")).alias("adj_open"),
        (pl.col("rth_close") * pl.col("factor")).alias("adj_close"),
        (pl.col("rth_high") * pl.col("factor")).alias("adj_high"),
        (pl.col("rth_low") * pl.col("factor")).alias("adj_low"),
        (pl.col("rth_volume") / pl.col("factor")).alias("adj_volume"),
    )
)
j = obs.join(
    raw,
    left_on=["security_id", "display_symbol_on_day"],
    right_on=["sid_key", "display_symbol"],
    how="inner",
)
r.check("row counts match (obs vs raw session universe)", obs.height == raw.height,
        f"obs={obs.height} raw={raw.height}")
r.check("every obs row joined exactly once", j.height == obs.height, f"joined={j.height}")
# eod checks apply to rows with RTH bars; the rest must be null both sides.
no_rth = j.filter(pl.col("bar_count_rth_right").is_null())
r.check("no-RTH rows have null eod on both sides",
        no_rth.filter(pl.col("eod_day_close").is_not_null()).height == 0,
        f"rows={no_rth.height}")
j = j.filter(pl.col("bar_count_rth_right").is_not_null())

REL = 1e-9
for out_col, raw_col in [
    ("eod_day_open", "adj_open"),
    ("eod_day_high", "adj_high"),
    ("eod_day_low", "adj_low"),
    ("eod_day_close", "adj_close"),
    ("eod_day_volume", "adj_volume"),
    ("eod_unadjusted_day_close", "rth_close"),
]:
    bad = j.filter(
        ((pl.col(out_col) - pl.col(raw_col)).abs()
         > REL * pl.max_horizontal(pl.col(out_col).abs(), pl.lit(1.0)))
    ).height
    r.check(f"{out_col} == recomputed", bad == 0, f"mismatches={bad}/{j.height}")

# Premarket volume is reported on the pin-adjusted basis, like every
# other volume column (adjudicated: engine semantics are uniform).
bad = j.filter(
    ((pl.col("premarket_volume") - pl.col("pm_volume") / pl.col("factor")).abs()
     > REL * pl.max_horizontal(pl.col("premarket_volume").abs(), pl.lit(1.0)))
).height
r.check("premarket_volume == recomputed (adjusted basis)", bad == 0, f"mismatches={bad}")

bad = j.filter(
    (pl.col("intraday_ret_0930_to_1000") - pl.col("ret_0930_1000")).abs() > 1e-9
).height
r.check("intraday_ret_0930_to_1000 == recomputed", bad == 0, f"mismatches={bad}")
bad = j.filter(pl.col("bar_count_rth") != pl.col("bar_count_rth_right")).height
r.check("bar_count_rth == recomputed", bad == 0, f"mismatches={bad}")
bad = j.filter(
    pl.col("first_rth_bar_time") != pl.col("first_rth_time").cast(pl.Utf8).str.slice(0, 5)
).height
r.check("first_rth_bar_time == recomputed", bad == 0, f"mismatches={bad}")

# adjustment factor: independent re-derivation
jf = j.filter(
    (pl.col("adjustment_factor_on_day") - pl.col("factor")).abs()
    > 1e-9 * pl.max_horizontal(pl.col("adjustment_factor_on_day").abs(), pl.lit(1.0))
).height
r.check("adjustment_factor_on_day == independent splits derivation", jf == 0,
        f"mismatches={jf}")

# ---------- B. overnight gap vs prior raw day ----------
print("B. overnight context vs prior day's raw tape")
prior_files = sorted(BARS.glob("*.parquet"))
prior_day = None
for p in reversed(prior_files):
    d = date.fromisoformat(p.stem)
    if d < DAY:
        prior_day = d
        break
praw = session_windows(load_day_bars(prior_day))
pbysym, pbysid = pin_factors(prior_day)
praw = (
    praw.join(pbysym, on="display_symbol", how="left")
    .join(pbysid, left_on="sid_key", right_on="security_id", how="left")
    .with_columns(
        pl.coalesce(pl.col("sid_factor"), pl.col("sym_factor"), pl.lit(1.0)).alias("pf")
    )
    .filter(pl.col("rth_close").is_not_null())
    .with_columns((pl.col("rth_close") * pl.col("pf")).alias("prior_adj_close"))
    .select("sid_key", "display_symbol", "prior_adj_close")
)
dups = (
    obs.group_by("security_id").len().filter(pl.col("len") > 1)["security_id"]
)
pdups = (
    praw.group_by("sid_key").len().filter(pl.col("len") > 1)["sid_key"]
)
jg = j.join(
    praw,
    left_on=["security_id", "display_symbol_on_day"],
    right_on=["sid_key", "display_symbol"],
    how="inner",
).filter(
    # Vendor sid collisions: engine keeps the rows but refuses trailing
    # state for ambiguous sids (documented DQ decision) — nothing to
    # compare against.
    ~pl.col("security_id").is_in(dups.implode())
    & ~pl.col("security_id").is_in(pdups.implode())
)
bad = jg.filter(
    (pl.col("prior_day_eod_close") - pl.col("prior_adj_close")).abs() > 1e-9
).height
r.check(f"prior_day_eod_close == {prior_day} recomputed", bad == 0, f"mismatches={bad}")
bad = jg.filter(
    (pl.col("overnight_gap") - (pl.col("adj_open") / pl.col("prior_adj_close") - 1.0)).abs()
    > 1e-9
).height
r.check("overnight_gap == adj_open/prior_close - 1", bad == 0, f"mismatches={bad}")

# ---------- C. cross-table consistency ----------
print("C. cross-table consistency (market_context / sector vs daily_observation)")
ctx = pl.read_parquet(OUT / "market_context_daily" / f"{DAY}.parquet").row(0, named=True)
valid = obs.filter(pl.col("intraday_ret_0930_to_1000").is_not_null())
green = valid.filter(pl.col("intraday_ret_0930_to_1000") > 0).height / valid.height
r.check("breadth_pct_universe_green_at_1000 == share from obs",
        abs(ctx["breadth_pct_universe_green_at_1000"] - green) < 1e-9,
        f"ctx={ctx['breadth_pct_universe_green_at_1000']:.6f} obs={green:.6f}")
disp = valid["intraday_ret_0930_to_1000"].std(ddof=0)
r.check("cross_sectional_ret_dispersion_at_1000 == population std from obs",
        abs(ctx["cross_sectional_ret_dispersion_at_1000"] - disp) < 1e-9,
        f"ctx={ctx['cross_sectional_ret_dispersion_at_1000']:.6f} obs={disp:.6f}")
r.check("breadth_total_universe_with_bars == obs rows",
        ctx["breadth_total_universe_with_bars"] == obs.height,
        f"ctx={ctx['breadth_total_universe_with_bars']} obs={obs.height}")

sec = pl.read_parquet(OUT / "sector_aggregates_daily" / f"{DAY}.parquet")
r.check("sector constituent counts sum to universe",
        sec["sector_constituent_count_with_bars"].sum() == obs.height,
        f"sum={sec['sector_constituent_count_with_bars'].sum()} obs={obs.height}")

# ---------- D. trailing windows, sampled securities ----------
print("D. trailing windows vs multi-day raw recomputation (sampled)")
SAMPLE = ["SPY", "AAPL", "MSFT", "XOM", "JPM"]
trail_days = [date.fromisoformat(p.stem) for p in prior_files
              if date.fromisoformat(p.stem) < DAY][-70:]
frames = []
sp_all = pl.read_parquet(REF / "splits.parquet").filter(
    pl.col("display_symbol").is_in(SAMPLE)
)
def factor_for(sym: str, d: date) -> float:
    rows = sp_all.filter(
        (pl.col("display_symbol") == sym) & (pl.col("execution_date") > d)
    )
    f = 1.0
    for fr, to in rows.select("split_from", "split_to").iter_rows():
        f *= fr / to
    return f

for d in trail_days:
    bars = load_day_bars(d).filter(pl.col("display_symbol").is_in(SAMPLE))
    w = session_windows(bars).with_columns(pl.lit(str(d)).alias("day"))
    frames.append(w.select("display_symbol", "day", "rth_high", "rth_low",
                           "rth_close", "rth_volume"))
hist = pl.concat(frames).sort("display_symbol", "day")
hist = hist.with_columns(
    pl.struct(["display_symbol", "day"]).map_elements(
        lambda x: factor_for(x["display_symbol"], date.fromisoformat(x["day"])),
        return_dtype=pl.Float64,
    ).alias("f")
).with_columns(
    (pl.col("rth_high") * pl.col("f")).alias("rth_high"),
    (pl.col("rth_low") * pl.col("f")).alias("rth_low"),
    (pl.col("rth_close") * pl.col("f")).alias("rth_close"),
    (pl.col("rth_volume") / pl.col("f")).alias("rth_volume"),
)
obs_s = pl.read_parquet(OUT / "daily_observation" / f"{DAY}.parquet").select(
    "display_symbol_on_day", "atr_14d", "adv_20d", "realized_vol_21d"
).filter(pl.col("display_symbol_on_day").is_in(SAMPLE))

for sym in SAMPLE:
    h = hist.filter(pl.col("display_symbol") == sym)
    if h.height < 22:
        continue
    closes = h["rth_close"].to_list()
    highs = h["rth_high"].to_list()
    lows = h["rth_low"].to_list()
    vols = h["rth_volume"].to_list()
    # ATR_14: Wilder TR mean over the last 14 days (vs prior close).
    trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
           for i in range(len(closes) - 14, len(closes))]
    atr = sum(trs) / 14
    adv = sum(vols[-20:]) / 20
    import math
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(len(closes) - 21, len(closes))]
    mean = sum(rets) / 21
    rv = math.sqrt(sum((x - mean) ** 2 for x in rets) / 20) * math.sqrt(252)
    row = obs_s.filter(pl.col("display_symbol_on_day") == sym).row(0, named=True)
    r.check(f"{sym} atr_14d", abs(row["atr_14d"] - atr) < 1e-9 * max(1, atr),
            f"engine={row['atr_14d']:.6f} indep={atr:.6f}")
    r.check(f"{sym} adv_20d", abs(row["adv_20d"] - adv) < 1e-6,
            f"engine={row['adv_20d']:.1f} indep={adv:.1f}")
    r.check(f"{sym} realized_vol_21d", abs(row["realized_vol_21d"] - rv) < 1e-9,
            f"engine={row['realized_vol_21d']:.6f} indep={rv:.6f}")

# ---------- E. earnings proximity, independent recount ----------
print("E. earnings proximity vs independent financials recount (AAPL)")
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
last = ann.filter(pl.col("ad") <= DAY)["ad"].max()
expected = (DAY - last).days
got = obs.filter(pl.col("display_symbol_on_day") == "AAPL").row(0, named=True)
r.check("AAPL days_since_last_earnings", got["days_since_last_earnings"] == expected,
        f"engine={got['days_since_last_earnings']} indep={expected} (last={last})")

# ---------- summary ----------
print(f"\n{'=' * 50}")
print(f"PASSED: {G}{r.passes}{X}   FAILED: {R}{r.fails}{X}")
if r.fail_msgs:
    print(f"{R}Failures:{X}")
    for m in r.fail_msgs:
        print(f"  - {m}")
    sys.exit(1)
print(f"{G}ALL INDEPENDENT CHECKS PASS{X}")
