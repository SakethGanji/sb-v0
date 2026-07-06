#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 7 A2 — build volume_profile_daily from bars_1m_raw (phase7a2-preregistration.md,
frozen). Per (security_id, day), RTH 09:30-16:00 NY: 30-bin volume HHI + modal share
(bar volume at close-price bin), volume/dollar/transactions, avg trade sizes, top-40
1-min log returns per tail (Hill inputs). Resumable: skips existing daily outputs.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OUT = Path("data/phase1_analysis/volume_profile_daily")
OUT.mkdir(parents=True, exist_ok=True)
NBINS, TOPK = 30, 40


def one_day(f, day):
    d = (pl.scan_parquet(f)
         .select("security_id", "t", "close", "low", "high", "volume", "transactions")
         .with_columns(pl.col("t").dt.convert_time_zone("America/New_York").alias("t"))
         .filter((pl.col("t").dt.time() >= pl.time(9, 30)) & (pl.col("t").dt.time() < pl.time(16, 0)))
         .sort("security_id", "t")
         .with_columns(
             (pl.col("close") / pl.col("close").shift(1).over("security_id")).log().alias("r"),
             pl.col("low").min().over("security_id").alias("dlo"),
             pl.col("high").max().over("security_id").alias("dhi"))
         .with_columns(
             ((pl.col("close") - pl.col("dlo")) / (pl.col("dhi") - pl.col("dlo") + 1e-12) * NBINS)
             .floor().clip(0, NBINS - 1).cast(pl.Int8).alias("bin"))
         .collect())
    prof = (d.group_by("security_id", "bin").agg(pl.col("volume").sum().alias("v"))
            .with_columns((pl.col("v") / pl.col("v").sum().over("security_id")).alias("s"))
            .group_by("security_id")
            .agg((pl.col("s") ** 2).sum().alias("profile_hhi"), pl.col("s").max().alias("modal_share")))
    agg = (d.group_by("security_id")
           .agg(pl.col("volume").sum().alias("volume"),
                (pl.col("volume") * pl.col("close")).sum().alias("dollar_volume"),
                pl.col("transactions").sum().alias("transactions"),
                pl.len().alias("n_bars"),
                pl.col("r").top_k(TOPK).alias("top_r"),
                pl.col("r").bottom_k(TOPK).alias("bot_r"))
           .with_columns(
               (pl.col("volume") / pl.col("transactions").clip(1, None)).alias("avg_trade_size"),
               (pl.col("dollar_volume") / pl.col("transactions").clip(1, None)).alias("avg_dollar_trade")))
    (agg.join(prof, on="security_id")
        .with_columns(pl.lit(day).alias("day"))
        .write_parquet(OUT / f"{day}.parquet"))


def main():
    t0 = dt.datetime.now()
    files = []
    for f in sorted(glob.glob("data/bars_1m_raw/*.parquet")):
        try:
            day = dt.date.fromisoformat(Path(f).stem)
        except ValueError:
            continue
        if EXP_START <= day <= EXP_END:
            files.append((f, day))
    done = 0
    for i, (f, day) in enumerate(files):
        if (OUT / f"{day}.parquet").exists():
            continue
        one_day(f, day)
        done += 1
        if done % 100 == 0:
            el = (dt.datetime.now() - t0).total_seconds()
            print(f"  {i+1}/{len(files)} days · {el:.0f}s elapsed · ~{el/done*(len(files)-i-1):.0f}s left", flush=True)
    print(f"DONE: {len(files)} days present · built {done} new · WALL {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
