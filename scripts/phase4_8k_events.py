#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow python3
"""
Phase 4 — Step 4: rebuild the earnings-event clock from 8-K item 2.02 filing dates.

DISCOVERY (this session): the dataset's earnings dates (earnings_calendar / days_since_
last_earnings) are 10-Q/10-K FILING dates — the press release (8-K item 2.02 "Results of
Operations and Financial Condition") lands 0-14+ days EARLIER. Every earnings-conditioned
result so far therefore measured post-FILING drift and may have missed PEAD's early phase.

This extracts, per deployable-universe CIK, all 8-K filings whose `items` include 2.02,
from the EDGAR submissions bulk (filings.recent + paged history files).

Output: data/phase1_analysis/edgar_8k_events.parquet (cik, event_date=filingDate, acceptance)

Run: scripts/phase4_8k_events.py
"""
from __future__ import annotations
import json, zipfile, datetime as dt
from pathlib import Path
import polars as pl

EDGAR = Path("/mnt/atlas/edgar")
OUT = "data/phase1_analysis/edgar_8k_events.parquet"


def main():
    t0 = dt.datetime.now()
    ciks = set(pl.read_parquet("data/phase1_analysis/edgar_sid_cik_map.parquet")["cik"].to_list())
    rows = []

    def harvest(doc_part, cik):
        forms = doc_part.get("form", [])
        dates = doc_part.get("filingDate", [])
        items = doc_part.get("items", [])
        acc = doc_part.get("acceptanceDateTime", [])
        for i in range(len(forms)):
            if forms[i] not in ("8-K", "8-K/A"):
                continue
            it = items[i] if i < len(items) else ""
            if "2.02" not in (it or ""):
                continue
            rows.append({"cik": cik, "event_date": dt.date.fromisoformat(dates[i]),
                         "acceptance": (acc[i] if i < len(acc) else None) or ""})

    with zipfile.ZipFile(EDGAR / "submissions.zip") as z:
        names = set(z.namelist())
        main_files = {f"CIK{c:010d}.json": c for c in ciks}
        present = [n for n in main_files if n in names]
        print(f"submissions files for our CIKs: {len(present):,} / {len(ciks):,}")
        for n_i, nm in enumerate(present):
            if (n_i + 1) % 400 == 0:
                print(f"  ...{n_i+1:,} CIKs, {len(rows):,} 8-K 2.02 events, "
                      f"{(dt.datetime.now()-t0).total_seconds():.0f}s")
            cik = main_files[nm]
            try:
                doc = json.loads(z.read(nm))
            except Exception:
                continue
            filings = doc.get("filings") or {}
            harvest(filings.get("recent") or {}, cik)
            for extra in (filings.get("files") or []):
                en = extra.get("name")
                if en and en in names:
                    try:
                        harvest(json.loads(z.read(en)), cik)
                    except Exception:
                        pass
    df = pl.DataFrame(rows).unique(subset=["cik", "event_date"])
    df.write_parquet(OUT)
    print(f"\nwrote {df.height:,} 8-K item-2.02 events for {df['cik'].n_unique():,} CIKs -> {OUT}")
    yr = df.with_columns(pl.col("event_date").dt.year().alias("y")).group_by("y").len().sort("y")
    print(yr.filter(pl.col("y").is_between(2015, 2022)))
    print(f"WALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
