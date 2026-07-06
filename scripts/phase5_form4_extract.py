#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow python3
"""
Phase 5 — Step 1: extract open-market insider PURCHASES from SEC Form 345 datasets.

Input : /mnt/atlas/edgar/form345/YYYYqN_form345.zip (2015q3..2020q4)
Output: data/phase1_analysis/form4_purchases.parquet
        one row per (issuer_cik, filing_date, owner_cik): open-market common-stock purchase
        aggregates with owner relationship flags.

Filters: DOCUMENT_TYPE=4 · TRANS_CODE='P' (open-market purchase) · TRANS_ACQUIRED_DISP_CD='A'
· TRANS_SHARES>0. Grants (A), exercises (M/X), sales (S) excluded by construction.
is_od = owner is Officer and/or Director (RPTOWNER_RELATIONSHIP).

Run: scripts/phase5_form4_extract.py
"""
from __future__ import annotations
import glob, io, zipfile, datetime as dt
from pathlib import Path
import polars as pl

SRC = Path("/mnt/atlas/edgar/form345")
OUT = "data/phase1_analysis/form4_purchases.parquet"


def read_tsv(z: zipfile.ZipFile, name: str, cols: list[str]) -> pl.DataFrame:
    with z.open(name) as fh:
        return pl.read_csv(fh.read(), separator="\t", columns=cols,
                           infer_schema_length=0, quote_char=None,
                           truncate_ragged_lines=True)


def main():
    t0 = dt.datetime.now()
    zips = sorted(glob.glob(str(SRC / "*_form345.zip")))
    print(f"quarterly zips: {len(zips)}")
    parts = []
    for zp in zips:
        with zipfile.ZipFile(zp) as z:
            sub = read_tsv(z, "SUBMISSION.tsv",
                           ["ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK"])
            tr = read_tsv(z, "NONDERIV_TRANS.tsv",
                          ["ACCESSION_NUMBER", "TRANS_CODE", "TRANS_ACQUIRED_DISP_CD",
                           "TRANS_SHARES", "TRANS_PRICEPERSHARE"])
            ro = read_tsv(z, "REPORTINGOWNER.tsv",
                          ["ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNER_RELATIONSHIP"])
        sub = sub.filter(pl.col("DOCUMENT_TYPE") == "4").drop("DOCUMENT_TYPE")
        tr = (tr.filter((pl.col("TRANS_CODE") == "P") & (pl.col("TRANS_ACQUIRED_DISP_CD") == "A"))
              .with_columns(pl.col("TRANS_SHARES").cast(pl.Float64, strict=False),
                            pl.col("TRANS_PRICEPERSHARE").cast(pl.Float64, strict=False))
              .filter(pl.col("TRANS_SHARES") > 0)
              .with_columns((pl.col("TRANS_SHARES")
                             * pl.col("TRANS_PRICEPERSHARE").fill_null(0.0)).alias("dollars")))
        if tr.is_empty():
            continue
        ro = (ro.group_by("ACCESSION_NUMBER")
              .agg(pl.col("RPTOWNERCIK").first().alias("owner_cik"),
                   pl.col("RPTOWNER_RELATIONSHIP").str.concat(",").alias("rel")))
        j = (tr.join(sub, on="ACCESSION_NUMBER", how="inner")
             .join(ro, on="ACCESSION_NUMBER", how="inner")
             .group_by("ISSUERCIK", "FILING_DATE", "owner_cik")
             .agg(pl.col("TRANS_SHARES").sum().alias("shares"),
                  pl.col("dollars").sum().alias("dollars"),
                  pl.col("rel").first()))
        parts.append(j)
        print(f"  {Path(zp).name}: {j.height:,} purchase rows")
    df = (pl.concat(parts)
          .with_columns(pl.col("FILING_DATE").str.to_date("%d-%b-%Y", strict=False).alias("filing_date"),
                        pl.col("ISSUERCIK").cast(pl.Int64, strict=False).alias("issuer_cik"),
                        (pl.col("rel").str.contains("Officer") | pl.col("rel").str.contains("Director"))
                        .alias("is_od"))
          .drop("FILING_DATE", "ISSUERCIK", "rel")
          .drop_nulls(["filing_date", "issuer_cik"]))
    df.write_parquet(OUT)
    print(f"\nwrote {df.height:,} purchase rows · {df['issuer_cik'].n_unique():,} issuers · "
          f"O/D share {df['is_od'].mean()*100:.0f}% -> {OUT}")
    print(df.with_columns(pl.col('filing_date').dt.year().alias('y')).group_by('y').len().sort('y'))
    print(f"WALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
