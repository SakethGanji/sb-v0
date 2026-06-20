#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — deliverable #3: ERA STABILITY × ENTRY-TIMEFRAME × HOLDING-HORIZON sweep.

Two questions the single frozen cell (10:00/1d) could NOT answer:
  (1) Did momentum work in a SINGLE era (e.g. 2020 COVID/meme) that pooling masked?
      (§2.5 — "positive in 1 era, negative in 3" averages to a fake null/edge.)
  (2) Is there alpha at ANY entry-delay × holding-period the single cell missed?

Sweep, on the DEPLOYABLE cohort (cap∈{mega,large,mid}, liq∈{highly_liquid,liquid,
normal}, CS, signal>0): entry offset ≥1000 (signal known) × horizon ∈ {60min, EOD,
1d, 2d, 5d, 21d} × {each exploration year 2016–2020, and pooled}. Metric =
daily-portfolio cost-adjusted excess/SPY, one-sided block-bootstrap LB (block =
horizon-days, §7.7.1 overlapping-horizon fix). Cost = flat 5 bps round-trip
(optimistic-realistic for liquid; gross also reported).

Exploration window ONLY — holdout (2023+) untouched. This is an EXPLORATORY
drill-down (§7.7.1 hierarchical tier), not an addition to the primary BY family;
a BY count over the grid is reported for honesty.

Read-only; writes data/phase1_analysis/stability_surface.parquet.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSETS = ["1000", "1005", "1010", "1015", "1020", "1030", "1045", "1100", "1130", "1200", "1300", "1530"]
HORIZONS = {"60min": 1, "EOD": 1, "1d": 1, "2d": 2, "5d": 5, "21d": 21}   # name -> block days
CAPS = ["mega", "large", "mid"]
LIQS = ["highly_liquid", "liquid", "normal"]
FLAT_COST_BPS = 5.0
MIN_NAMES, MIN_DAYS = 5, 60
ALPHA = 0.10
N_BOOT = 2000
SEED = 20260620
OUT = Path("data/phase1_analysis")


def window_files(tbl):
    out = []
    for f in sorted(glob.glob(f"data/outputs/{tbl}/*.parquet")):
        try:
            d = dt.date.fromisoformat(Path(f).stem)
        except ValueError:
            continue
        if EXP_START <= d <= EXP_END:
            out.append(f)
    return out


def harmonic(m):
    return float(np.sum(1.0 / np.arange(1, m + 1))) if m > 0 else 1.0


def by_count(pvals, alpha=ALPHA):
    m = len(pvals)
    if m == 0:
        return 0
    sp = np.sort(pvals)
    thr = (np.arange(1, m + 1) * alpha) / (m * harmonic(m))
    below = sp <= thr
    return int(np.max(np.where(below)[0]) + 1) if below.any() else 0


def block_boot(x, block, rng, B=N_BOOT):
    """One-sided lower bound (5th pct) + p-value for mean>0, circular block boot."""
    n = len(x)
    nblk = int(np.ceil(n / block))
    starts = rng.integers(0, n, size=(B, nblk))
    idx = (starts[:, :, None] + np.arange(block)[None, None, :]) % n
    idx = idx.reshape(B, -1)[:, :n]
    bm = x[idx].mean(axis=1)
    return float(np.quantile(bm, 0.05)), float(np.mean(bm <= 0.0))


def main():
    t0 = dt.datetime.now()
    OUT.mkdir(parents=True, exist_ok=True)
    print("=" * 90)
    print("Phase 1 — #3 ERA STABILITY × ENTRY-OFFSET × HORIZON sweep (deployable cohort)")
    print("=" * 90)

    cls = (pl.scan_parquet(window_files("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS")
                   & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select("day", "security_id"))
    sig = (pl.scan_parquet(window_files("daily_observation"))
           .filter(pl.col("intraday_ret_0930_to_1000") > 0.0).select("day", "security_id"))
    hz_cols = [f"ret_{h}_excess_spy" for h in HORIZONS]
    df = (pl.scan_parquet(window_files("forward_outcomes"))
          .filter(pl.col("entry_offset").is_in(OFFSETS))
          .select(["day", "security_id", "entry_offset"] + hz_cols)
          .join(cls, on=["day", "security_id"], how="inner")
          .join(sig, on=["day", "security_id"], how="inner")
          .with_columns(pl.col("day").dt.year().alias("yr"),
                        pl.col("entry_offset").cast(pl.Utf8))
          .collect())
    print(f"deployable signal-firing trades: {df.height:,}  "
          f"(cap∈{CAPS}, liq∈{LIQS})\n")

    rng = np.random.default_rng(SEED)
    years = [2016, 2017, 2018, 2019, 2020]
    recs = []
    cost = FLAT_COST_BPS / 1e4
    for h, block in HORIZONS.items():
        col = f"ret_{h}_excess_spy"
        for off in OFFSETS:
            sub = df.filter((pl.col("entry_offset") == off) & pl.col(col).is_not_null())
            for scope in ["pooled"] + years:
                s = sub if scope == "pooled" else sub.filter(pl.col("yr") == scope)
                daily = (s.group_by("day").agg(pl.col(col).mean().alias("g"), pl.len().alias("n"))
                         .filter(pl.col("n") >= MIN_NAMES).sort("day"))
                if daily.height < MIN_DAYS:
                    continue
                g = daily["g"].to_numpy()
                net = g - cost
                lb_net, p_net = block_boot(net, block, rng)
                lb_gr, p_gr = block_boot(g, block, rng)
                recs.append(dict(horizon=h, offset=off, scope=str(scope), n_days=daily.height,
                                 gross_bps=float(g.mean()*1e4), net_bps=float(net.mean()*1e4),
                                 net_lb_bps=lb_net*1e4, p_net_pos=p_net,
                                 gross_lb_bps=lb_gr*1e4, p_gross_pos=p_gr))
    res = pl.DataFrame(recs)
    res.write_parquet(OUT / "stability_surface.parquet")

    pooled = res.filter(pl.col("scope") == "pooled")
    # multiplicity over the pooled grid
    by_net = by_count(pooled["p_net_pos"].to_numpy())
    by_gr = by_count(pooled["p_gross_pos"].to_numpy())
    print(f"[pooled grid] {pooled.height} (offset×horizon) combos · "
          f"BY survivors: net-positive {by_net}, gross-positive {by_gr}")
    print(f"  raw (uncorrected) net_LB>0: {pooled.filter(pl.col('net_lb_bps')>0).height} · "
          f"gross_LB>0: {pooled.filter(pl.col('gross_lb_bps')>0).height}")

    print("\n--- pooled edge surface: net cost-adj excess (bps), entry-offset × horizon ---")
    print("    (net = gross − 5bps; * = one-sided bootstrap LB > 0)")
    hdr = "  offset " + "".join(f"{h:>9}" for h in HORIZONS)
    print(hdr)
    for off in OFFSETS:
        line = f"  {off:<7}"
        for h in HORIZONS:
            r = pooled.filter((pl.col("offset") == off) & (pl.col("horizon") == h))
            if r.height:
                v = r["net_bps"][0]; star = "*" if r["net_lb_bps"][0] > 0 else " "
                line += f"{v:>8.1f}{star}"
            else:
                line += f"{'—':>9}"
        print(line)

    # best gross combos (is there ANY raw alpha anywhere?)
    print("\n--- top 6 combos by GROSS excess (pre-cost), pooled ---")
    top = pooled.sort("gross_bps", descending=True).head(6)
    for r in top.iter_rows(named=True):
        print(f"  off {r['offset']:<5} {r['horizon']:>5}  gross={r['gross_bps']:+6.1f}b "
              f"(LB {r['gross_lb_bps']:+6.1f})  net={r['net_bps']:+6.1f}b "
              f"(LB {r['net_lb_bps']:+6.1f})  p_gross_pos={r['p_gross_pos']:.3f}")

    # era stability: per-year gross at a representative (1d) horizon, to spot a hidden era
    print("\n--- era stability: GROSS excess (bps) per year, horizon=1d, by offset ---")
    print("    (does any single year — e.g. 2020 — turn positive where pooled is flat?)")
    print("  offset " + "".join(f"{y:>8}" for y in years) + f"{'pooled':>9}")
    for off in OFFSETS:
        line = f"  {off:<7}"
        for sc in years + ["pooled"]:
            r = res.filter((pl.col("offset") == off) & (pl.col("horizon") == "1d")
                           & (pl.col("scope") == str(sc)))
            line += f"{r['gross_bps'][0]:>8.1f}" if r.height else f"{'—':>8}"
        print(line)

    print("\n--- verdict ---")
    anypos = pooled.filter(pl.col("net_lb_bps") > 0).height > 0
    print(f"Any deployable (net-LB>0) timeframe pooled: {'YES — investigate' if anypos else 'NONE'}.")
    print(f"BY-surviving net-positive combos: {by_net}.  If 0 and no single year flips")
    print(f"strongly/consistently positive, the null holds across timeframes AND eras —")
    print(f"momentum has no exploitable edge here at any entry/horizon/era we can resolve.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
