#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 4 — Step 2: build point-in-time time-series SUE from the as-filed EPS facts.

Method (Bernard-Thomas time-series SUE, split/restatement-immune):
  * SAME-FILING seasonal difference: within one filing (accn), the current-quarter EPS and
    the year-ago comparative EPS are stated on a consistent share basis, so
    diff_q = EPS_q - EPS_{q-4} taken from a single accn is immune to splits/restatements.
  * Q4: most 10-Ks state only annual EPS. Q4 diff = annual same-filing diff minus the sum of
    the three same-fiscal-year quarterly diffs (requires all 3; else dropped). Where a 10-K
    does state Q4 quarterly EPS, that is used directly.
  * Point-in-time: per (cik, period_end) keep the EARLIEST-filed diff (original filing, not
    amendments). `filed` is preserved. NOTE the announced 8-K press-release EPS precedes the
    10-Q/10-K filing by days-weeks; we treat the number as public at the announcement date
    (standard in the PEAD literature; the press-release EPS ~always equals the filed EPS).
  * SUE_q = diff_q / std(diff_{q-8..q-1}), min 4 prior diffs, winsorized at ±6.
    Tag preference: Diluted over Basic within a filing.

Input : data/phase1_analysis/edgar_eps_facts.parquet
Output: data/phase1_analysis/edgar_sue.parquet (cik, period_end, filed, form, diff, sue, n_hist)

Run: scripts/phase4_sue_build.py
"""
from __future__ import annotations
import datetime as dt
import numpy as np
import polars as pl

IN = "data/phase1_analysis/edgar_eps_facts.parquet"
OUT = "data/phase1_analysis/edgar_sue.parquet"
MIN_HIST, WINSOR = 4, 6.0


def main():
    t0 = dt.datetime.now()
    f = pl.read_parquet(IN)
    # tag preference: within (cik, accn, start, end) keep Diluted if present
    f = (f.with_columns(pl.when(pl.col("tag") == "EarningsPerShareDiluted").then(0).otherwise(1).alias("_pref"))
         .sort("_pref").unique(subset=["cik", "accn", "start", "end"], keep="first"))
    q = f.filter(pl.col("span_days") <= 110)
    a = f.filter(pl.col("span_days") >= 330)

    def same_filing_diff(d: pl.DataFrame) -> pl.DataFrame:
        """within each (cik, accn): current period = max end; year-ago = end 350-380d earlier."""
        cur = d.group_by(["cik", "accn"]).agg(pl.col("end").max().alias("end")).join(
            d, on=["cik", "accn", "end"], how="left").unique(subset=["cik", "accn"], keep="first")
        ya = (cur.select("cik", "accn", "end").join(d.rename({"end": "ya_end", "val": "ya_val"})
                                                    .select("cik", "accn", "ya_end", "ya_val"),
                                                    on=["cik", "accn"], how="inner")
              .filter((pl.col("end") - pl.col("ya_end")).dt.total_days().is_between(350, 380))
              .sort("ya_end", descending=True).unique(subset=["cik", "accn"], keep="first")
              .select("cik", "accn", "ya_val"))
        return (cur.join(ya, on=["cik", "accn"], how="inner")
                .with_columns((pl.col("val") - pl.col("ya_val")).alias("diff"))
                .select("cik", "accn", "end", "filed", "form", "diff"))

    qd = same_filing_diff(q)
    ad = same_filing_diff(a)
    # PIT: earliest filed per (cik, end)
    qd = qd.sort("filed").unique(subset=["cik", "end"], keep="first")
    ad = ad.sort("filed").unique(subset=["cik", "end"], keep="first")
    print(f"quarterly same-filing diffs: {qd.height:,} · annual: {ad.height:,}")

    # Q4 synthesis: annual diff minus the 3 quarterly diffs inside the same fiscal year
    q3 = (ad.select("cik", pl.col("end").alias("a_end"), pl.col("filed").alias("a_filed"),
                    pl.col("form").alias("a_form"), pl.col("diff").alias("a_diff"))
          .join(qd.select("cik", pl.col("end").alias("q_end"), pl.col("diff").alias("q_diff")),
                on="cik", how="inner")
          .filter(((pl.col("a_end") - pl.col("q_end")).dt.total_days()).is_between(60, 340))
          .group_by(["cik", "a_end", "a_filed", "a_form", "a_diff"])
          .agg(pl.len().alias("nq"), pl.col("q_diff").sum().alias("sumq"))
          .filter(pl.col("nq") == 3)
          .with_columns((pl.col("a_diff") - pl.col("sumq")).alias("diff"))
          .select("cik", pl.col("a_end").alias("end"), pl.col("a_filed").alias("filed"),
                  pl.col("a_form").alias("form"), "diff"))
    # only where no direct quarterly Q4 diff exists
    q4 = q3.join(qd.select("cik", "end"), on=["cik", "end"], how="anti")
    print(f"Q4 synthesized from annual-minus-3Q: {q4.height:,}")
    allq = pl.concat([qd.select("cik", "end", "filed", "form", "diff"), q4]).sort(["cik", "end"])

    # SUE: rolling std of prior 8 seasonal diffs, min 4
    rows = []
    for (cik,), g in allq.group_by(["cik"], maintain_order=True):
        ends = g["end"].to_list(); filed = g["filed"].to_list()
        form = g["form"].to_list(); diff = np.array(g["diff"].to_list(), dtype=float)
        for i in range(len(diff)):
            hist = diff[max(0, i - 8):i]
            hist = hist[~np.isnan(hist)]
            if len(hist) < MIN_HIST or np.isnan(diff[i]):
                continue
            sd = hist.std(ddof=1)
            if not np.isfinite(sd) or sd <= 0:
                continue
            sue = float(np.clip(diff[i] / sd, -WINSOR, WINSOR))
            rows.append({"cik": cik, "period_end": ends[i], "filed": filed[i], "form": form[i],
                         "diff": float(diff[i]), "sue": sue, "n_hist": int(len(hist))})
    out = pl.DataFrame(rows)
    out.write_parquet(OUT)
    print(f"wrote {out.height:,} SUE observations for {out['cik'].n_unique():,} CIKs -> {OUT}")
    print(out.select(pl.col("sue").mean().alias("mean"), pl.col("sue").std().alias("std"),
                     pl.col("sue").quantile(0.1).alias("p10"), pl.col("sue").quantile(0.9).alias("p90")))
    yr = out.with_columns(pl.col("period_end").dt.year().alias("y")).group_by("y").len().sort("y")
    print(yr.filter(pl.col("y").is_between(2015, 2021)))
    print(f"WALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
