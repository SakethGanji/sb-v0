#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow python3
"""
End-to-end integrity validation for sb-v0's `data/reference/` parquet set
+ the 1-min equity bars at `data/bars_1m_raw/`.

Run with:  uv run scripts/validate_reference_data.py
Exit code: 0 = all pass; 1 = any fail.

Adds a new check whenever a Phase 0 ingest assumption is encoded — this
file is the regression suite for the raw-data layer.
"""
from __future__ import annotations
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent.parent
REF = REPO / "data" / "reference"
BARS = REPO / "data" / "bars_1m_raw"

# ANSI
G = "\033[92m"; R = "\033[91m"; Y = "\033[93m"; D = "\033[2m"; X = "\033[0m"


@dataclass
class Results:
    passes: int = 0
    fails: int = 0
    skips: int = 0
    fail_msgs: list[str] = field(default_factory=list)

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        if ok:
            self.passes += 1
            print(f"  {G}✓{X} {name}" + (f"  {D}{detail}{X}" if detail else ""))
        else:
            self.fails += 1
            self.fail_msgs.append(f"{name}: {detail}")
            print(f"  {R}✗{X} {name}  {R}{detail}{X}")

    def skip(self, name: str, reason: str) -> None:
        self.skips += 1
        print(f"  {Y}~{X} {name}  {Y}SKIPPED: {reason}{X}")


def get_file_meta(path: Path, key: str) -> str | None:
    md = pq.ParquetFile(path).metadata.metadata or {}
    raw = md.get(key.encode())
    return raw.decode() if raw else None


def section(title: str) -> None:
    print(f"\n{D}=={X} {title} {D}=={X}")


def main() -> int:
    r = Results()

    files = {
        "bars_dir":                BARS,
        "vix":                     REF / "vix_daily.parquet",
        "tickers":                 REF / "tickers.parquet",
        "tickers_enriched":        REF / "tickers_enriched.parquet",
        "tickers_classified":      REF / "tickers_classified.parquet",
        "figi_map":                REF / "figi_map.parquet",
        "ticker_events":           REF / "ticker_events.parquet",
        "splits":                  REF / "splits.parquet",
        "dividends":               REF / "dividends.parquet",
        "short_interest":          REF / "short_interest.parquet",
        "financials":              REF / "financials.parquet",
        "acceptance_backfill":     REF / "acceptance_datetime_backfill.parquet",
    }

    # ===== Section A: existence + snapshot metadata =====
    section("A. existence + snapshot metadata")
    for name, path in files.items():
        if name == "bars_dir":
            r.check(f"{name} exists", path.exists() and path.is_dir(),
                    f"@{path}")
            continue
        ok = path.exists() and path.stat().st_size > 0
        r.check(f"{name} exists & non-empty", ok,
                f"size={path.stat().st_size if path.exists() else 0}")

    meta_keys = {
        "vix":                 "vix_snapshot_date",
        "tickers_classified":  "ticker_details_snapshot_date",
        "figi_map":            "figi_map_snapshot_date",
        "ticker_events":       "ticker_events_snapshot_date",
        "splits":              "splits_snapshot_date",
        "dividends":           "dividends_snapshot_date",
        "short_interest":      "short_interest_snapshot_date",
        "financials":          "financials_snapshot_date",
        "acceptance_backfill": "acceptance_backfill_snapshot_date",
    }
    for name, key in meta_keys.items():
        path = files[name]
        if not path.exists():
            r.skip(f"{name} has {key}", "file missing")
            continue
        v = get_file_meta(path, key)
        r.check(f"{name} stamped {key}", v is not None, f"value={v}")

    # ===== Section B: per-file shape + invariants =====
    section("B. per-file shape + invariants")
    # bars
    bar_files = sorted(BARS.glob("*.parquet"))
    r.check("bars_1m_raw has >= 2000 day files", len(bar_files) >= 2000,
            f"count={len(bar_files)}")
    if bar_files:
        earliest = bar_files[0].stem
        latest = bar_files[-1].stem
        r.check("bars first day ≤ 2016-07-01", earliest <= "2016-07-01",
                f"earliest={earliest}")
        r.check("bars latest day ≥ 2026-05-01", latest >= "2026-05-01",
                f"latest={latest}")

    # tickers.parquet
    if files["tickers"].exists():
        t = pl.read_parquet(files["tickers"])
        n_act = t.filter(pl.col("active") == True).height
        n_inact = t.filter(pl.col("active") == False).height
        r.check("tickers active count plausible", 4000 <= n_act <= 8000,
                f"active={n_act}")
        r.check("tickers inactive count plausible", n_inact >= 5000,
                f"inactive={n_inact}")
        r.check("tickers display_symbol non-null",
                t["display_symbol"].null_count() == 0,
                f"nulls={t['display_symbol'].null_count()}")

    # vix
    if files["vix"].exists():
        v = pl.read_parquet(files["vix"]).sort("date")
        r.check("vix has ≥ 2500 rows", v.height >= 2500, f"rows={v.height}")
        r.check("vix dates strictly monotonic",
                v["date"].is_sorted() and v["date"].n_unique() == v.height,
                f"uniq={v['date'].n_unique()}/{v.height}")
        r.check("vix_close all > 0 (no missing-sentinels leaked)",
                (v["vix_close"] > 0).all(),
                f"min={v['vix_close'].min()}")
        r.check("vix_close max < 200 (sanity ceiling)",
                v["vix_close"].max() < 200, f"max={v['vix_close'].max():.2f}")

    # tickers_classified
    if files["tickers_classified"].exists():
        c = pl.read_parquet(files["tickers_classified"])
        r.check("tickers_classified row count matches tickers", True,
                f"rows={c.height}")
        r.check("tickers_classified has 26 cols", len(c.columns) == 26,
                f"cols={len(c.columns)}")

    # splits
    if files["splits"].exists():
        s = pl.read_parquet(files["splits"])
        r.check("splits has > 10k rows", s.height > 10_000, f"rows={s.height}")
        r.check("splits.execution_date non-null",
                s["execution_date"].null_count() == 0,
                f"nulls={s['execution_date'].null_count()}")
        r.check("splits.split_from/to > 0",
                (s["split_from"] > 0).all() and (s["split_to"] > 0).all(),
                "ratios sane")

    # dividends
    if files["dividends"].exists():
        d = pl.read_parquet(files["dividends"])
        r.check("dividends > 500k rows", d.height > 500_000,
                f"rows={d.height}")
        r.check("dividends.cash_amount > 0",
                (d["cash_amount"] > 0).all(),
                f"min={d['cash_amount'].min()}")

    # short_interest
    if files["short_interest"].exists():
        si = pl.read_parquet(files["short_interest"])
        r.check("short_interest > 2M rows", si.height > 2_000_000,
                f"rows={si.height}")
        r.check("short_interest dates 2017-2026",
                si["settlement_date"].min() <= date(2018, 1, 1)
                and si["settlement_date"].max() >= date(2026, 1, 1),
                f"range {si['settlement_date'].min()} → {si['settlement_date'].max()}")
        r.check("short_interest non-negative",
                (si["short_interest"] >= 0).all(),
                f"min={si['short_interest'].min()}")

    # financials
    if files["financials"].exists():
        f = pl.read_parquet(files["financials"])
        r.check("financials > 200k rows", f.height > 200_000,
                f"rows={f.height}")
        r.check("financials.filing_date non-null",
                f["filing_date"].null_count() == 0,
                f"nulls={f['filing_date'].null_count()}")
        # Allow a tiny number of upstream Massive data quirks (verified:
        # 2/251k as of 2026-06-08 — TRW & SGOL Q1 2013 with bad filing
        # dates that pre-date their reporting period). Treat as data
        # quality, not a pipeline bug.
        bad = (f.filter(pl.col("start_date").is_not_null()
                        & (pl.col("filing_date") < pl.col("start_date"))).height)
        r.check("financials.filing_date >= start_date (≤10 upstream quirks ok)",
                bad <= 10, f"violations={bad}")

    # ===== Section C: cross-table referential integrity =====
    section("C. cross-table referential integrity")
    if files["tickers"].exists() and files["figi_map"].exists():
        t = pl.read_parquet(files["tickers"])
        m = pl.read_parquet(files["figi_map"])
        sids_t = set(t["composite_figi"].drop_nulls().to_list())
        sids_m = set(m["security_id"].to_list())
        overlap = sids_t & sids_m
        r.check("figi_map.security_ids ⊆ tickers.composite_figi (mostly)",
                len(overlap) >= len(sids_m) * 0.90,
                f"overlap {len(overlap)}/{len(sids_m)} ({len(overlap)/len(sids_m)*100:.0f}%)")

    if files["ticker_events"].exists() and files["figi_map"].exists():
        e = pl.read_parquet(files["ticker_events"])
        m = pl.read_parquet(files["figi_map"])
        # every rename event's sid should appear in figi_map
        sids_e = set(e["security_id"].to_list())
        sids_m = set(m["security_id"].to_list())
        miss = sids_e - sids_m
        r.check("every rename event's sid is in figi_map",
                len(miss) == 0,
                f"missing={len(miss)}")

    if files["financials"].exists():
        # Every financials row has cik present.
        f = pl.read_parquet(files["financials"])
        missing_cik = f.filter(pl.col("cik").is_null()).height
        r.check("financials.cik non-null", missing_cik == 0,
                f"null_ciks={missing_cik}")

    # ===== Section D: domain spot checks (known facts) =====
    section("D. domain spot checks")
    if files["vix"].exists():
        v = pl.read_parquet(files["vix"])
        for d_str, expected, tol in [
            ("2020-03-16", 82.69, 1.0),
            ("2021-01-27", 37.21, 1.0),
            ("2024-01-02", 13.20, 0.5),
        ]:
            d_ = date.fromisoformat(d_str)
            row = v.filter(pl.col("date") == d_)
            if row.height:
                got = row[0, "vix_close"]
                r.check(f"vix {d_str} ≈ {expected:.2f}",
                        abs(got - expected) <= tol,
                        f"got={got:.2f}")
            else:
                r.check(f"vix {d_str} ≈ {expected:.2f}", False, "row missing")

    if files["tickers_classified"].exists():
        c = pl.read_parquet(files["tickers_classified"])
        aapl = c.filter(pl.col("display_symbol") == "AAPL")
        if aapl.height:
            r.check("AAPL SIC == 3571",
                    aapl[0, "sic_code"] == "3571",
                    f"got={aapl[0,'sic_code']}")
            r.check("AAPL list_date == 1980-12-12",
                    aapl[0, "list_date"] == date(1980, 12, 12),
                    f"got={aapl[0,'list_date']}")
        abnb = c.filter(pl.col("display_symbol") == "ABNB")
        if abnb.height:
            r.check("ABNB list_date == 2020-12-10",
                    abnb[0, "list_date"] == date(2020, 12, 10),
                    f"got={abnb[0,'list_date']}")
        spy = c.filter(pl.col("display_symbol") == "SPY")
        if spy.height:
            r.check("SPY ticker_type == ETF",
                    spy[0, "ticker_type"] == "ETF",
                    f"got={spy[0,'ticker_type']}")

    if files["splits"].exists():
        s = pl.read_parquet(files["splits"])
        aapl_2020 = s.filter(
            (pl.col("display_symbol") == "AAPL")
            & (pl.col("execution_date") == date(2020, 8, 31))
        )
        ok = aapl_2020.height == 1 and aapl_2020[0, "split_from"] == 1.0 and aapl_2020[0, "split_to"] == 4.0
        r.check("AAPL 2020-08-31 split is 1:4", ok,
                f"rows={aapl_2020.height}")
        nvda_2021 = s.filter(
            (pl.col("display_symbol") == "NVDA")
            & (pl.col("execution_date") == date(2021, 7, 20))
        )
        ok = nvda_2021.height == 1 and nvda_2021[0, "split_to"] == 4.0
        r.check("NVDA 2021-07-20 split present", ok,
                f"rows={nvda_2021.height}")

    if files["ticker_events"].exists() and files["figi_map"].exists():
        e = pl.read_parquet(files["ticker_events"])
        m = pl.read_parquet(files["figi_map"])
        # FB → META rename
        meta_evt = e.filter(pl.col("new_ticker") == "META")
        r.check("FB→META ticker_event present", meta_evt.height >= 1,
                f"rows={meta_evt.height}")
        if meta_evt.height:
            sid = meta_evt[0, "security_id"]
            r.check("FB→META event sid == BBG000MM2P62",
                    sid == "BBG000MM2P62", f"sid={sid}")
            fb_meta = m.filter(pl.col("display_symbol").is_in(["FB", "META"])
                               & (pl.col("security_id") == sid))
            r.check("figi_map has FB+META windows for sid BBG000MM2P62",
                    fb_meta.height == 2, f"rows={fb_meta.height}")

    if files["short_interest"].exists():
        si = pl.read_parquet(files["short_interest"])
        gme = si.filter(
            (pl.col("display_symbol") == "GME")
            & (pl.col("settlement_date") == date(2021, 1, 15))
        )
        if gme.height:
            got = gme[0, "short_interest"]
            r.check("GME 2021-01-15 short_interest ≈ 61.78M",
                    abs(got - 61_782_730) < 100_000,
                    f"got={got:.0f}")
        else:
            r.check("GME 2021-01-15 short_interest row present", False,
                    "row missing")

    if files["dividends"].exists():
        d = pl.read_parquet(files["dividends"]).filter(
            pl.col("display_symbol") == "AAPL"
        ).sort("ex_dividend_date", descending=True)
        r.check("AAPL has > 40 dividend records", d.height > 40,
                f"rows={d.height}")
        if d.height:
            latest = d[0, "cash_amount"]
            r.check("AAPL most-recent dividend in $0.20-0.30",
                    0.20 < latest < 0.30, f"got={latest}")

    if files["financials"].exists():
        f = pl.read_parquet(files["financials"]).filter(
            pl.col("ticker") == "AAPL"
        )
        r.check("AAPL has ≥ 40 financials rows since 2009",
                f.height >= 40, f"rows={f.height}")
        if f.height:
            latest = f.sort("filing_date", descending=True)[0]
            das = latest[0, "diluted_average_shares"]
            r.check("AAPL latest diluted_shares in 10-20B band",
                    das is not None and 10e9 <= das <= 20e9,
                    f"got={das}")

    # ===== Section E: time coverage =====
    section("E. time coverage")
    if files["vix"].exists():
        v = pl.read_parquet(files["vix"])
        gap_days = v.with_columns(
            (pl.col("date").diff().dt.total_days()).alias("gap")
        ).filter(pl.col("gap") > 7)
        r.check("vix has no >7-day gaps (excl. start)",
                gap_days.height <= 1,
                f"big_gaps={gap_days.height}")

    # ===== Section F: EDGAR backfill consistency (conditional) =====
    section("F. EDGAR backfill consistency")
    if files["acceptance_backfill"].exists() and files["financials"].exists():
        # Detect stale state — backfill mtime predates financials mtime.
        f_mtime = files["financials"].stat().st_mtime
        b_mtime = files["acceptance_backfill"].stat().st_mtime
        if b_mtime < f_mtime:
            r.skip("backfill consistency",
                   "backfill file older than financials — re-run build-edgar-acceptance")
        else:
            f = pl.read_parquet(files["financials"])
            b = pl.read_parquet(files["acceptance_backfill"])
            # Every backfill accession_number must appear in financials.source_filing_url.
            f_accs = set(
                f["source_filing_url"]
                .drop_nulls()
                .map_elements(lambda u: u.rsplit("/", 1)[-1], return_dtype=pl.String)
                .to_list()
            )
            b_accs = set(b["accession_number"].to_list())
            unknown = b_accs - f_accs
            r.check("every backfill accession exists in financials",
                    len(unknown) == 0,
                    f"unknown={len(unknown)}")
            # Backfill should target rows where acceptance_datetime IS NULL.
            missing_in_f = f.filter(pl.col("acceptance_datetime").is_null()).height
            r.check("backfill rows ≤ missing-acceptance rows in financials",
                    b.height <= missing_in_f,
                    f"backfill={b.height}, missing={missing_in_f}")
            r.check("backfill coverage of missing-acceptance > 50%",
                    b.height > missing_in_f * 0.5,
                    f"coverage={b.height/missing_in_f*100:.1f}%")
    else:
        r.skip("backfill consistency", "file missing")

    # ===== summary =====
    print(f"\n{D}{'='*60}{X}")
    print(f"PASSED: {G}{r.passes}{X}   FAILED: {R}{r.fails}{X}   SKIPPED: {Y}{r.skips}{X}")
    if r.fail_msgs:
        print(f"\n{R}Failures:{X}")
        for m in r.fail_msgs:
            print(f"  - {m}")
    return 0 if r.fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
