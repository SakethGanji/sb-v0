#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow python3
"""
Phase 4 — Step 1: extract quarterly EPS facts from the SEC EDGAR companyfacts bulk file.

Input : /mnt/atlas/edgar/companyfacts.zip (1.39GB, one JSON per CIK)
        /mnt/atlas/edgar/company_tickers.json (current CIK<->ticker map)
Output: data/phase1_analysis/edgar_eps_facts.parquet  (as-filed EPS facts, all vintages)
        data/phase1_analysis/edgar_sid_cik_map.parquet (security_id FIGI -> CIK)

Join path: security_id in our tables is a FIGI; the point-in-time ticker lives in
security_classification_daily.display_symbol_on_day. We map every deployable-universe
(CS, mega/large/mid, our 3 liq buckets, 2016-2020) FIGI to its day-tickers, normalize
(strip . - /), and match against company_tickers.json.

Durations kept: quarterly (70-110d) and annual (330-400d) — annual enables Q4-by-differencing.
All vintages of a fact are kept (accn + filed preserved) so the SUE step can do point-in-time,
SAME-FILING seasonal differences (immune to splits/restatements by construction).

Known limitation (documented): company_tickers.json is a CURRENT snapshot, so names delisted
before today mostly fail to match -> SUE coverage is survivorship-tilted. Match rate by year
is reported downstream; conclusions must carry this caveat.

Run: scripts/phase4_edgar_eps_extract.py
"""
from __future__ import annotations
import glob, json, zipfile, datetime as dt
from pathlib import Path
import polars as pl

EDGAR = Path("/mnt/atlas/edgar")
OUTD = Path("data/phase1_analysis")
TAGS = ["EarningsPerShareDiluted", "EarningsPerShareBasic"]
EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]


def wf(t):
    out = []
    for f in sorted(glob.glob(f"data/outputs/{t}/*.parquet")):
        try:
            d = dt.date.fromisoformat(Path(f).stem)
        except ValueError:
            continue
        if EXP_START <= d <= EXP_END:
            out.append(f)
    return out


def norm(s: str) -> str:
    return s.upper().replace(".", "").replace("-", "").replace("/", "")


def main():
    t0 = dt.datetime.now()
    pairs = (pl.scan_parquet(wf("security_classification_daily"))
             .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                     & pl.col("liquidity_bucket").is_in(LIQS))
             .select("security_id", "display_symbol_on_day").unique().collect())
    print(f"deployable (sid, symbol) pairs 2016-2020: {pairs.height:,} "
          f"({pairs['security_id'].n_unique():,} FIGIs)")

    tick = json.loads((EDGAR / "company_tickers.json").read_text())
    cik_by_norm = {}
    for v in tick.values():
        cik_by_norm.setdefault(norm(v["ticker"]), v["cik_str"])

    sid_cik = (pairs.with_columns(pl.col("display_symbol_on_day")
                                  .map_elements(lambda s: cik_by_norm.get(norm(s)), return_dtype=pl.Int64)
                                  .alias("cik"))
               .filter(pl.col("cik").is_not_null())
               .group_by("security_id").agg(pl.col("cik").mode().first()))
    print(f"FIGIs matched to a CIK: {sid_cik.height:,} / {pairs['security_id'].n_unique():,} "
          f"({100*sid_cik.height/pairs['security_id'].n_unique():.1f}%)")
    sid_cik.write_parquet(OUTD / "edgar_sid_cik_map.parquet")

    ciks = set(sid_cik["cik"].to_list())
    want = {f"CIK{c:010d}.json": c for c in ciks}
    rows, n_seen = [], 0
    with zipfile.ZipFile(EDGAR / "companyfacts.zip") as z:
        names = [n for n in z.namelist() if n in want]
        print(f"CIK files present in bulk zip: {len(names):,} / {len(ciks):,}")
        for nm in names:
            n_seen += 1
            if n_seen % 500 == 0:
                print(f"  ...{n_seen:,} files, {len(rows):,} facts, {(dt.datetime.now()-t0).total_seconds():.0f}s")
            try:
                doc = json.loads(z.read(nm))
            except Exception:
                continue
            cik = doc.get("cik")
            gaap = (doc.get("facts") or {}).get("us-gaap") or {}
            for tag in TAGS:
                units = (gaap.get(tag) or {}).get("units") or {}
                for unit_name, facts in units.items():
                    if "shares" not in unit_name.lower():
                        continue
                    for f in facts:
                        st, en, val = f.get("start"), f.get("end"), f.get("val")
                        if st is None or en is None or val is None:
                            continue
                        try:
                            d0, d1 = dt.date.fromisoformat(st), dt.date.fromisoformat(en)
                        except ValueError:
                            continue
                        span = (d1 - d0).days
                        if not (70 <= span <= 110 or 330 <= span <= 400):
                            continue
                        rows.append({
                            "cik": cik, "tag": tag, "start": d0, "end": d1, "span_days": span,
                            "val": float(val), "accn": f.get("accn", ""), "form": f.get("form", ""),
                            "filed": dt.date.fromisoformat(f["filed"]) if f.get("filed") else None,
                            "fy": f.get("fy"), "fp": f.get("fp", ""),
                        })
    df = pl.DataFrame(rows)
    df.write_parquet(OUTD / "edgar_eps_facts.parquet")
    print(f"\nwrote {df.height:,} EPS facts for {df['cik'].n_unique():,} CIKs -> edgar_eps_facts.parquet")
    q = df.filter(pl.col("span_days") <= 110)
    print(f"quarterly facts: {q.height:,} · annual facts: {df.height - q.height:,}")
    print(f"WALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
