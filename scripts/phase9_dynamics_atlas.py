#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 9 — FEATURE DYNAMICS ATLAS. Charter: phase9-dynamics-atlas.md.
Characterization only: no PASS/FAIL, no alpha claims; direction columns are expected
~0 (the Phases 0-6B measured prior) and printed anyway. Train era 2016-06..2020-12,
Phase 1 train universe. Outputs: data/phase1_analysis/atlas_l{1..5}_*.parquet + stdout.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
OUT = Path("data/phase1_analysis")

DO_COLS = ["day", "security_id", "intraday_ret_0930_to_1000", "overnight_gap",
           "prior_day_last_30m_return", "premarket_volume_vs_20d_median",
           "atr_5d", "atr_14d", "atr_42d", "realized_vol_21d",
           "adv_5d", "adv_20d", "adv_60d", "addv_20d", "signal_concentration_hhi_today",
           "high_52w", "days_since_last_5pct_move", "consecutive_up_days_close_to_close",
           "beta_spy_60d", "eod_day_close", "prior_day_eod_close", "eod_day_volume"]
FO_COLS = ["day", "security_id", "ret_1d_excess_spy", "ret_5d_excess_spy",
           "ret_21d_excess_spy", "max_runup_21d", "max_drawdown_21d"]
MC_COLS = ["day", "vix_close", "breadth_pct_universe_green_at_1000",
           "cross_sectional_ret_dispersion_at_1000", "spy_realized_vol_21d"]

# the curated dynamic variables (rank computed per day for each)
RANK_VARS = ["intraday_ret_0930_to_1000", "overnight_gap", "prior_day_last_30m_return",
             "premarket_volume_vs_20d_median", "volume_ratio", "vol_frac",
             "realized_vol_21d", "vol_trend", "addv_20d", "volm_trend",
             "dist_52w_high",
             "days_since_last_5pct_move", "beta_spy_60d", "ret_5d_trail", "ret_21d_trail"]
# signal_concentration_hhi_today excluded: market-wide constant within a day, so a
# cross-sectional rank is undefined; it belongs to the market layer if anywhere.
OUTCOMES = ["ret_1d_excess_spy", "ret_5d_excess_spy", "ret_21d_excess_spy", "fwd_range_21d"]


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


def build_panel():
    print("[panel] scanning tables (train era, train universe)…")
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select("day", "security_id", "market_cap_bucket"))
    do = pl.scan_parquet(wf("daily_observation")).select(DO_COLS)
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(FO_COLS))
    mc = pl.scan_parquet(wf("market_context_daily")).select(MC_COLS)
    df = (cls.join(do, on=["day", "security_id"], how="inner")
             .join(fo, on=["day", "security_id"], how="left")
             .join(mc, on="day", how="left")
             .with_columns(
                 (pl.col("eod_day_close") / pl.col("prior_day_eod_close") - 1).alias("ret_cc"),
                 (pl.col("atr_14d") / pl.col("prior_day_eod_close")).alias("vol_frac"),
                 (pl.col("atr_5d") / pl.col("atr_42d")).alias("vol_trend"),
                 (pl.col("adv_5d") / pl.col("adv_60d")).alias("volm_trend"),
                 (pl.col("eod_day_volume") / pl.col("adv_20d")).alias("volume_ratio"),
                 (pl.col("eod_day_close") / pl.col("high_52w")).alias("dist_52w_high"),
                 (pl.col("max_runup_21d") - pl.col("max_drawdown_21d")).alias("fwd_range_21d"))
             .collect())
    print(f"  rows {df.height:,} · days {df['day'].n_unique()} · names {df['security_id'].n_unique():,}")

    df = df.sort(["security_id", "day"])
    g = pl.col("ret_cc")
    df = df.with_columns(
        g.rolling_sum(5).over("security_id").alias("ret_5d_trail"),
        g.rolling_sum(21).over("security_id").alias("ret_21d_trail"),
        (g < 0).cast(pl.Int8).alias("red"))
    # CLOCK RULE: several conditioning variables (ret_cc, volume_ratio, dist_52w_high,
    # trailing returns) are only known at day t's CLOSE, but forward outcomes are
    # anchored at 10:00 entries. Attaching day t's 10:00 outcome to a close-of-t
    # condition leaks the event day's own afternoon into its "forward" return (the
    # Phase 4 clock lesson). Uniform fix: ALL outcomes are taken from the NEXT row's
    # 10:00 entry — every atlas number reads "what happens from the next morning on."
    df = df.with_columns([pl.col(c).shift(-1).over("security_id").alias(c) for c in OUTCOMES])
    # per-day cross-sectional percentile ranks for every curated variable
    df = df.with_columns([
        (pl.col(v).rank("average").over("day") / pl.col(v).count().over("day"))
        .cast(pl.Float32).alias(f"rk_{v}") for v in RANK_VARS])
    # per-stock leads/lags of ranks (trading-day steps within each stock's series)
    df = df.with_columns(
        [pl.col(f"rk_{v}").shift(21).over("security_id").alias(f"rk_{v}_lag21") for v in RANK_VARS]
        + [pl.col(f"rk_{v}").shift(-21).over("security_id").alias(f"rk_{v}_lead21") for v in RANK_VARS]
        + [pl.col(f"rk_{v}").shift(k).over("security_id").alias(f"rk_{v}_lag{k}") for v in RANK_VARS for k in (1, 5)]
        + [pl.col(f"rk_{v}").shift(-63).over("security_id").alias(f"rk_{v}_lead63") for v in RANK_VARS])
    return df


def clustered(df, keys, val):
    """day-clustered mean and SE of `val` by `keys`: per-day cell means, then across days."""
    per_day = df.group_by(keys + ["day"]).agg(pl.col(val).mean().alias("m"), pl.len().alias("n"))
    return (per_day.group_by(keys)
            .agg(pl.col("m").mean().alias(f"{val}_mean"),
                 (pl.col("m").std() / pl.len().sqrt()).alias(f"{val}_se"),
                 pl.col("n").sum().alias(f"{val}_N"))
            .sort(keys))


def layer1(df):
    print("\n=== L1 — univariate dynamics (rank persistence, half-life, jumps) ===")
    rows = []
    for v in RANK_VARS:
        acs = {}
        for lag, col in [(1, f"rk_{v}_lag1"), (5, f"rk_{v}_lag5"), (21, f"rk_{v}_lag21")]:
            per_day = (df.select("day", f"rk_{v}", col).drop_nulls()
                       .group_by("day").agg(pl.corr(f"rk_{v}", col).alias("ac")))
            acs[lag] = float(per_day["ac"].mean())
        # lead63 gives the 63d point (same quantity by stationarity)
        per_day = (df.select("day", f"rk_{v}", f"rk_{v}_lead63").drop_nulls()
                   .group_by("day").agg(pl.corr(f"rk_{v}", f"rk_{v}_lead63").alias("ac")))
        acs[63] = float(per_day["ac"].mean())
        hl = float(21 * np.log(0.5) / np.log(max(acs[21], 1e-6))) if 0 < acs[21] < 1 else float("inf")
        jump = float(df.select(((pl.col(f"rk_{v}") - pl.col(f"rk_{v}_lag1")).abs() > 0.25)
                               .cast(pl.Float64).mean()).item())
        rows.append({"variable": v, "ac1": acs[1], "ac5": acs[5], "ac21": acs[21],
                     "ac63": acs[63], "half_life_days": hl, "jump_rate_1d": jump})
        print(f"  {v:<34} AC1 {acs[1]:+.2f}  AC5 {acs[5]:+.2f}  AC21 {acs[21]:+.2f}  AC63 {acs[63]:+.2f}"
              f"  t½ {hl:7.1f}d  jump {jump*100:4.1f}%")
    t = pl.DataFrame(rows); t.write_parquet(OUT / "atlas_l1_dynamics.parquet")
    return t


def layer2(df, cut_by_vix=False):
    tag = "l5_volmap_by_vix" if cut_by_vix else "l2_maps"
    print(f"\n=== {'L5b — vol_trend map inside VIX terciles' if cut_by_vix else 'L2 — level×trend outcome maps'} ===")
    frames = []
    vars_ = ["vol_trend"] if cut_by_vix else RANK_VARS
    base = df
    if cut_by_vix:
        qs = df.select(pl.col("vix_close").quantile(1/3).alias("a"), pl.col("vix_close").quantile(2/3).alias("b"))
        a, b = qs["a"][0], qs["b"][0]
        base = df.with_columns(pl.when(pl.col("vix_close") <= a).then(pl.lit("vix_low"))
                               .when(pl.col("vix_close") <= b).then(pl.lit("vix_mid"))
                               .otherwise(pl.lit("vix_high")).alias("vix_t"))
    for v in vars_:
        d = base.with_columns(
            pl.min_horizontal((pl.col(f"rk_{v}") * 5).floor(), pl.lit(4)).cast(pl.Int8).alias("level_q"),
            (pl.col(f"rk_{v}") - pl.col(f"rk_{v}_lag21")).alias("chg"),
        ).drop_nulls(["level_q", "chg"])
        d = d.with_columns(
            pl.when(pl.col("chg") > 0.10).then(pl.lit("rising"))
              .when(pl.col("chg") < -0.10).then(pl.lit("falling"))
              .otherwise(pl.lit("flat")).alias("trend"),
            (pl.col(f"rk_{v}_lead21") - pl.col(f"rk_{v}")).alias("own_fwd_chg"))
        keys = (["vix_t"] if cut_by_vix else []) + ["level_q", "trend"]
        outs = [clustered(d, keys, o) for o in ["fwd_range_21d", "ret_21d_excess_spy", "own_fwd_chg"]]
        m = outs[0]
        for o in outs[1:]:
            m = m.join(o, on=keys)
        frames.append(m.with_columns(pl.lit(v).alias("variable")))
    t = pl.concat(frames); t.write_parquet(OUT / f"atlas_{tag}.parquet")
    print(f"  written {t.height} cells -> atlas_{tag}.parquet")
    return t


def layer3(df):
    print("\n=== L3 — event anatomy (before/after, day-clustered) ===")
    d = df.with_columns(
        (pl.col("red").shift(1).over("security_id") + pl.col("red").shift(2).over("security_id")
         + pl.col("red").shift(3).over("security_id")).alias("red3"),
        pl.col("vol_trend").shift(1).over("security_id").alias("vt_lag"),
        pl.col("volm_trend").shift(1).over("security_id").alias("mt_lag"),
        pl.col("vol_frac").shift(21).over("security_id").alias("vol_frac_m21"),
        pl.col("vol_frac").shift(-21).over("security_id").alias("vol_frac_p21"),
        pl.col("volume_ratio").shift(5).over("security_id").alias("vr_m5"),
        pl.col("volume_ratio").shift(-5).over("security_id").alias("vr_p5"))
    events = {
        "reversal_up   (3+ red then green)": (pl.col("red3") == 3) & (pl.col("red") == 0),
        "reversal_down (3+ green then red)": (pl.col("red3") == 0) & (pl.col("red") == 1),
        "vol_expansion (atr5/42 x-above 1.5)": (pl.col("vol_trend") > 1.5) & (pl.col("vt_lag") <= 1.5),
        "vol_collapse  (atr5/42 x-below .75)": (pl.col("vol_trend") < 0.75) & (pl.col("vt_lag") >= 0.75),
        "volume_surge  (adv5/60 x-above 2)": (pl.col("volm_trend") > 2.0) & (pl.col("mt_lag") <= 2.0),
    }
    rows = []
    for name, cond in events.items():
        e = d.filter(cond)
        row = {"event": name.split()[0], "N": e.height}
        for val, lbl in [("ret_21d_trail", "trail21"), ("ret_1d_excess_spy", "fwd1x"),
                         ("ret_5d_excess_spy", "fwd5x"), ("ret_21d_excess_spy", "fwd21x"),
                         ("fwd_range_21d", "fwd_range"), ("vol_frac_m21", "vol@-21"),
                         ("vol_frac", "vol@0"), ("vol_frac_p21", "vol@+21"),
                         ("vr_m5", "vr@-5"), ("volume_ratio", "vr@0"), ("vr_p5", "vr@+5")]:
            per_day = e.group_by("day").agg(pl.col(val).mean().alias("m"))
            row[lbl] = float(per_day["m"].mean())
            if lbl in ("fwd1x", "fwd5x", "fwd21x"):
                row[lbl + "_se"] = float(per_day["m"].std() / np.sqrt(per_day.height))
        rows.append(row)
        print(f"  {name:<38} N={row['N']:>7,}  trail21 {row['trail21']*100:+6.2f}%"
              f"  fwd excess 1d {row['fwd1x']*1e4:+6.1f}bp ±{row['fwd1x_se']*1e4:.1f}"
              f" | 5d {row['fwd5x']*1e4:+6.1f}±{row['fwd5x_se']*1e4:.1f}"
              f" | 21d {row['fwd21x']*1e4:+7.1f}±{row['fwd21x_se']*1e4:.1f}"
              f"  vol {row['vol@-21']*100:.2f}→{row['vol@0']*100:.2f}→{row['vol@+21']*100:.2f}%"
              f"  vr {row['vr@-5']:.2f}→{row['vr@0']:.2f}→{row['vr@+5']:.2f}")
    t = pl.DataFrame(rows); t.write_parquet(OUT / "atlas_l3_events.parquet")

    # market breadth flips (market-level, small)
    m = (df.group_by("day").agg(pl.col("breadth_pct_universe_green_at_1000").first().alias("br"),
                                pl.col("ret_1d_excess_spy").mean().alias("x1"),
                                pl.col("ret_5d_excess_spy").mean().alias("x5"),
                                pl.col("ret_cc").mean().alias("univ_ret"),
                                pl.col("fwd_range_21d").mean().alias("rng"))
           .sort("day")
           .with_columns(pl.col("br").rolling_mean(5).shift(1).alias("br5")))
    neg = m.filter((pl.col("br") < 0.35) & (pl.col("br5") > 0.50))
    pos = m.filter((pl.col("br") > 0.65) & (pl.col("br5") < 0.50))
    for nm, e in [("breadth_flip_down (<35% after >50%)", neg), ("breadth_flip_up (>65% after <50%)", pos)]:
        print(f"  {nm:<38} N={e.height:>4}  same-day univ ret {e['univ_ret'].mean()*100:+.2f}%"
              f"  fwd 5d excess {e['x5'].mean()*1e4:+.1f}bp  fwd 21d range {e['rng'].mean()*100:.1f}%")
    return t


def layer4(df):
    print("\n=== L4 — pre-declared grid: vol_trend × volm_trend × cap (27 cells) ===")
    d = df.with_columns(
        pl.when(pl.col("rk_vol_trend") < 1/3).then(pl.lit("volT_dn"))
          .when(pl.col("rk_vol_trend") > 2/3).then(pl.lit("volT_up")).otherwise(pl.lit("volT_md")).alias("vt"),
        pl.when(pl.col("rk_volm_trend") < 1/3).then(pl.lit("volmT_dn"))
          .when(pl.col("rk_volm_trend") > 2/3).then(pl.lit("volmT_up")).otherwise(pl.lit("volmT_md")).alias("mt"),
        (pl.col("ret_1d_excess_spy") > 0).cast(pl.Float64).alias("p_up1"))
    keys = ["market_cap_bucket", "vt", "mt"]
    m = clustered(d, keys, "fwd_range_21d")
    for o in ["ret_5d_excess_spy", "ret_21d_excess_spy", "p_up1"]:
        m = m.join(clustered(d, keys, o), on=keys)
    m.write_parquet(OUT / "atlas_l4_grid.parquet")
    show = m.filter(pl.col("vt") != "volT_md").filter(pl.col("mt") != "volmT_md")
    for r in show.iter_rows(named=True):
        print(f"  {r['market_cap_bucket']:<5} {r['vt']} × {r['mt']}:  fwd 21d range {r['fwd_range_21d_mean']*100:5.1f}%"
              f"  x5d {r['ret_5d_excess_spy_mean']*1e4:+6.1f}bp±{r['ret_5d_excess_spy_se']*1e4:.1f}"
              f"  x21d {r['ret_21d_excess_spy_mean']*1e4:+7.1f}bp±{r['ret_21d_excess_spy_se']*1e4:.1f}"
              f"  P(up1d) {r['p_up1_mean']*100:.1f}%")
    return m


def layer5(df):
    print("\n=== L5a — market variable dynamics ===")
    m = (df.group_by("day").agg([pl.col(c).first() for c in
          ["vix_close", "breadth_pct_universe_green_at_1000",
           "cross_sectional_ret_dispersion_at_1000", "spy_realized_vol_21d"]]).sort("day"))
    rows = []
    for c in ["vix_close", "breadth_pct_universe_green_at_1000",
              "cross_sectional_ret_dispersion_at_1000", "spy_realized_vol_21d"]:
        x = m[c].drop_nulls().to_numpy()
        def ac(k):
            return float(np.corrcoef(x[:-k], x[k:])[0, 1])
        hl = float(21 * np.log(0.5) / np.log(max(ac(21), 1e-6))) if 0 < ac(21) < 1 else float("inf")
        rows.append({"variable": c, "ac1": ac(1), "ac5": ac(5), "ac21": ac(21), "half_life_days": hl})
        print(f"  {c:<44} AC1 {ac(1):+.2f}  AC5 {ac(5):+.2f}  AC21 {ac(21):+.2f}  t½ {hl:6.1f}d")
    pl.DataFrame(rows).write_parquet(OUT / "atlas_l5_market.parquet")


def main():
    t0 = dt.datetime.now()
    print("=" * 96)
    print("Phase 9 · FEATURE DYNAMICS ATLAS — characterization, no PASS/FAIL, train era only")
    print("=" * 96)
    df = build_panel()
    layer1(df)
    layer2(df)
    layer3(df)
    layer4(df)
    layer5(df)
    layer2(df, cut_by_vix=True)
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
