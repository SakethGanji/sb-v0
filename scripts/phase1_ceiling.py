#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 1 — §2.10 / §7.1 PREDICTABILITY-CEILING MAP (deliverable #2).

Per coarse cell, the MI-implied upper bound on how much ANY model could extract
from the recorded pre-entry features about the forward outcome (Fano: MI caps
achievable predictive accuracy regardless of modeling cleverness). Cells with a
near-zero ceiling are "genuinely random w.r.t. everything we measure" — research
effort stops there. The map sets the priority order for every step after this.

Outcome: y = (ret_1d_excess_spy > 0)  — does the signal-firing trade beat SPY at 1d.
Features: ~16 PRE-ENTRY (knowable by 10:00 ET, no look-ahead) ML-safe columns.
Estimator: binned plug-in MI (finite-sample biased UP) corrected against a
PERMUTATION NULL (shuffle y) — corrected_MI = max(0, MI − null_95) (§2.10 / §7.7.1).
Reported in bits; H(y) ≈ 1 bit so the fraction is directly interpretable.

Family: coarse `cap×liq×price`, signal `intraday_ret_0930_to_1000>0`, offset 1000,
exploration window. Read-only; writes data/phase1_analysis/ceiling_map.parquet.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CELL = ["market_cap_bucket", "liquidity_bucket", "price_bucket"]
MIN_TRADES = 2000
N_CAP = 30000
NBINS = 8
N_PERM = 50
SEED = 20260620
OUT = Path("data/phase1_analysis")

FEATURES = [
    "intraday_ret_0930_to_1000", "intraday_ret_0930_to_0950",
    "intraday_ret_from_first_30m_high_to_1000",
    "intraday_first_15m_volume_share_of_first_30m",
    "intraday_ret_0930_to_1000_percentile_today", "overnight_gap",
    "premarket_volume_vs_20d_median", "signal_concentration_percentile_today",
    "signal_concentration_hhi_today", "consecutive_up_days_close_to_close",
    "days_since_last_5pct_move", "beta_spy_60d", "realized_vol_21d_rank_today",
    "addv_20d_rank_today", "days_since_last_earnings", "atr_14d",
]


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


def mi_from_counts(tab):
    N = tab.sum()
    if N <= 0:
        return 0.0
    pxy = tab / N
    px = pxy.sum(1, keepdims=True)
    py = pxy.sum(0, keepdims=True)
    denom = px @ py
    mask = pxy > 0
    return float(np.sum(pxy[mask] * np.log2(pxy[mask] / denom[mask])))


def corrected_mi(x, y, rng):
    """Binned plug-in MI(x;y) in bits, minus the 95th-pct permutation null."""
    ok = ~np.isnan(x)
    x, y = x[ok], y[ok]
    if len(x) < 200:
        return 0.0, 0.0
    edges = np.unique(np.quantile(x, np.linspace(0, 1, NBINS + 1)))
    if len(edges) < 3:
        return 0.0, 0.0
    xb = np.clip(np.digitize(x, edges[1:-1]), 0, len(edges) - 2)
    nb = xb.max() + 1
    flat = xb * 2 + y
    tab = np.bincount(flat, minlength=nb * 2).reshape(nb, 2)
    mi = mi_from_counts(tab)
    null = np.empty(N_PERM)
    for p in range(N_PERM):
        ys = rng.permutation(y)
        tabp = np.bincount(xb * 2 + ys, minlength=nb * 2).reshape(nb, 2)
        null[p] = mi_from_counts(tabp)
    return mi, max(0.0, mi - float(np.quantile(null, 0.95)))


def main():
    t0 = dt.datetime.now()
    OUT.mkdir(parents=True, exist_ok=True)
    print("=" * 86)
    print("Phase 1 — PREDICTABILITY-CEILING MAP  (§2.10 / §7.1, deliverable #2)")
    print("=" * 86)

    cls = (pl.scan_parquet(window_files("security_classification_daily"))
           .filter(pl.col("ticker_type") == "CS").select(["day", "security_id"] + CELL))
    do = (pl.scan_parquet(window_files("daily_observation"))
          .filter(pl.col("intraday_ret_0930_to_1000") > 0.0)
          .select(["day", "security_id"] + FEATURES))
    df = (pl.scan_parquet(window_files("forward_outcomes"))
          .filter((pl.col("entry_offset") == OFFSET) & pl.col("ret_1d_excess_spy").is_not_null())
          .select("day", "security_id", "ret_1d_excess_spy")
          .join(cls, on=["day", "security_id"], how="inner").drop_nulls(CELL)
          .join(do, on=["day", "security_id"], how="inner")
          .with_columns((pl.col("ret_1d_excess_spy") > 0).cast(pl.Int8).alias("y"))
          .collect())
    print(f"signal-firing CS trades @1000: {df.height:,} · {len(FEATURES)} pre-entry features")

    cells = (df.group_by(CELL).len().filter(pl.col("len") >= MIN_TRADES)
             .sort("len", descending=True))
    keys = [tuple(r) for r in cells.select(CELL).iter_rows()]
    print(f"cells with ≥{MIN_TRADES} trades: {len(keys)}\n")

    rng = np.random.default_rng(SEED)
    feat_agg = {f: [] for f in FEATURES}
    recs = []
    for key in keys:
        cond = pl.lit(True)
        for c, v in zip(CELL, key):
            cond = cond & (pl.col(c) == v)
        sub = df.filter(cond)
        if sub.height > N_CAP:
            sub = sub.sample(N_CAP, seed=SEED)
        y = sub["y"].to_numpy().astype(np.int64)
        base = y.mean()
        Hy = 0.0 if base in (0.0, 1.0) else -(base*np.log2(base) + (1-base)*np.log2(1-base))
        per = {}
        for f in FEATURES:
            x = sub[f].to_numpy().astype(float)
            _, cmi = corrected_mi(x, y, rng)
            per[f] = cmi
            feat_agg[f].append(cmi)
        best_f = max(per, key=per.get)
        rec = dict(zip(CELL, key)) | dict(
            n=sub.height, base_rate=float(base), H_y=float(Hy),
            best_feature=best_f, best_mi_bits=per[best_f],
            n_signif=int(sum(v > 1e-4 for v in per.values())),
            sum_mi_bits=float(sum(per.values())))
        rec["ceiling_frac_of_Hy"] = rec["best_mi_bits"] / Hy if Hy > 0 else 0.0
        rec["noise_floor"] = rec["best_mi_bits"] < 1e-3
        recs.append(rec)

    res = pl.DataFrame(recs).sort("best_mi_bits", descending=True)
    res.write_parquet(OUT / "ceiling_map.parquet")

    nf = int(res["noise_floor"].sum())
    print(f"{'cap':<6}{'liq':<14}{'price':<11}{'n':>7}{'base%':>7}"
          f"{'best feature':<32}{'bestMI':>9}{'sumMI':>8}{'sig':>4}")
    for r in res.iter_rows(named=True):
        print(f"{str(r['market_cap_bucket']):<6}{str(r['liquidity_bucket']):<14}"
              f"{str(r['price_bucket']):<11}{r['n']:>7}{100*r['base_rate']:>6.0f}%"
              f"  {r['best_feature']:<30}{r['best_mi_bits']*1e3:>7.2f}m{r['sum_mi_bits']*1e3:>7.1f}m"
              f"{r['n_signif']:>4}")
    print(f"\n(MI in millibits; H(y)≈1 bit, so 1.00m ≈ 0.1% of outcome uncertainty)")
    print(f"noise-floor cells (best corrected MI < 1 millibit): {nf}/{res.height} "
          f"— no single feature beats the permutation null")

    print("\n--- feature ranking (mean corrected MI across cells, millibits) ---")
    fr = sorted(((np.mean(v) * 1e3, f) for f, v in feat_agg.items()), reverse=True)
    for mi, f in fr[:8]:
        print(f"  {f:<44}{mi:>7.2f}m")

    print("\n--- verdict ---")
    print(f"Cells above the noise floor are where ANY learnable structure exists — the")
    print(f"research priority queue. Near-zero ceilings everywhere would confirm the")
    print(f"outcome is ~random w.r.t. the recorded features (no meta-labeling headroom).")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
