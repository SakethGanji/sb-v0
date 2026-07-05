#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 1 — DIRECTION TEST: among names the volatility gate says WILL move, can the
                          remaining cheap features pick LONG vs SHORT?

Refines the L2 meta-label per a sharp critique: L2's label lumped non-movers ("neither")
in with down-moves, so AUC 0.63 partly measured RESOLUTION (magnitude), not direction.
This is the clean two-stage version:
  Stage 1 (magnitude, known to work): "will it hit either +2%/-2% barrier?"  -> movers.
  Stage 2 (DIRECTION, the open question): among RESOLVED MOVERS ONLY, predict
           target_first (+2% before -2% = UP) vs stop_first (DOWN). "neither" DROPPED.
Adds the last available untested features: sector-relative strength, distance-from-
52w-high, SPY/QQQ/IWM 20/50/200-MA regime. Then LONG/SHORT economics (long high P(up),
short low P(up), skip the middle), net of cost + a borrow note.

Universe: deployable/liquid, top-quintile morning momentum, enter 10:00. Walk-forward
OOS 2018-2020, permutation null. Read-only.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
TRAIN_TEST = [([2016, 2017], 2018), ([2016, 2017, 2018], 2019), ([2016, 2017, 2018, 2019], 2020)]
EMBARGO_DAYS = 2
N_PERM = 6
SEED = 20260705
TARGET_PCT, STOP_PCT = 0.02, -0.02
LABEL_COL = "first_event_2pct_before_minus_2pct_1d"
COSTS_BPS = [0.0, 10.0, 20.0]
BORROW_BPS_SHORT = 2.0   # ~1-day borrow for LIQUID large/mid caps (generous; easy-to-borrow)
CONF_BANDS = [0.00, 0.02, 0.05, 0.10]

DO_FEATS = ["intraday_ret_0930_to_0940","intraday_ret_0930_to_0950","intraday_ret_0930_to_1000",
    "intraday_volume_0930_to_1000","intraday_dollar_volume_0930_to_1000","intraday_first_30m_high_return",
    "intraday_first_30m_low_return","intraday_ret_from_first_30m_high_to_1000",
    "intraday_minutes_since_first_30m_high_at_1000","intraday_first_15m_volume_share_of_first_30m",
    "intraday_ret_0930_to_1000_rank_today","intraday_ret_0930_to_1000_percentile_today",
    "intraday_dollar_volume_0930_to_1000_rank_today","premarket_volume_rank_today",
    "premarket_dollar_volume_rank_today","overnight_gap_rank_today","addv_20d_rank_today",
    "realized_vol_21d_rank_today","premarket_volume","premarket_dollar_volume","overnight_gap",
    "prior_day_last_30m_return","prior_day_last_30m_volume_share","atr_5d","atr_14d","atr_42d",
    "realized_vol_21d","yang_zhang_vol_5d","yang_zhang_vol_14d","yang_zhang_vol_21d","yang_zhang_vol_42d",
    "adv_5d","adv_20d","adv_60d","addv_5d","addv_20d","addv_60d","beta_spy_60d","beta_qqq_60d",
    "beta_iwm_60d","days_to_next_known_earnings","days_since_last_earnings",
    "signal_concentration_percentile_today","signal_concentration_hhi_today",
    "premarket_volume_vs_20d_median","days_since_last_5pct_move","days_since_last_10pct_move",
    "days_since_last_20pct_move","consecutive_up_days_close_to_close","days_since_first_bar",
    "signal_first_in_5d","signal_first_in_10d","signal_first_in_20d","bar_count_premarket","bar_count_first_30m",
    # NEW available features:
    "high_52w","low_52w","prior_day_eod_close"]
FO_FEATS = ["pre_entry_ret_from_open","pre_entry_vwap_from_open","pre_entry_ret_from_high",
    "pre_entry_ret_from_low","pre_entry_minutes_since_high","pre_entry_minutes_since_low",
    "pre_entry_ret_rank_today","pre_entry_ret_percentile_today","pre_entry_dollar_volume_rank_today",
    "entry_bar_upper_wick_pct","entry_bar_lower_wick_pct","entry_slippage_proxy_bps","entry_1m_range",
    "entry_range_vs_atr_14d","entry_dollar_volume_vs_addv_20d","cumulative_volume_to_entry",
    "cumulative_dollar_volume_to_entry"]
CL_FEATS = ["market_cap","market_cap_rank_today","market_cap_percentile_today",
    "volatility_percentile_today","days_since_ipo_or_first_bar"]
MC_FEATS = ["vix_open","spy_overnight_gap","qqq_overnight_gap","iwm_overnight_gap","spy_ret_0930_to_1000",
    "qqq_ret_0930_to_1000","iwm_ret_0930_to_1000","spy_realized_vol_21d","qqq_realized_vol_21d",
    "iwm_realized_vol_21d","breadth_pct_universe_green_at_1000","breadth_pct_universe_above_premarket_vwap_at_1000",
    "breadth_advance_decline_ratio_at_1000","breadth_count_movers_above_5pct_at_1000",
    "breadth_count_movers_above_1atr_at_1000","cross_sectional_ret_dispersion_at_1000",
    "cross_sectional_ret_iqr_at_1000","universe_median_addv_20d","universe_total_dollar_volume"]
# engineered (added in-code): sector_rel_morning, sector_morning_ret, dist_52w_high, dist_52w_low,
#   spy/qqq/iwm _above_ma20/50/200
ENG = ["sector_rel_morning","sector_morning_ret","dist_52w_high","dist_52w_low",
       "spy_above_ma20","spy_above_ma50","spy_above_ma200",
       "qqq_above_ma50","iwm_above_ma50"]


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


def keep(c, h):
    return [x for x in c if x in h]


def gbm():
    return HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
        min_samples_leaf=200, l2_regularization=1.0, early_stopping=True,
        validation_fraction=0.1, n_iter_no_change=20, random_state=SEED)


def ma_regime():
    """SPY/QQQ/IWM above their own trailing 20/50/200d MA, leakage-safe (through prior day)."""
    mc = (pl.scan_parquet(wf("market_context_daily"))
          .select("day", "spy_eod_close", "qqq_eod_close", "iwm_eod_close").sort("day").collect())
    out = {"day": mc["day"]}
    for etf, cols in [("spy", [20, 50, 200]), ("qqq", [50]), ("iwm", [50])]:
        c = mc[f"{etf}_eod_close"]
        for w in cols:
            ma = c.rolling_mean(w)
            flag = (c > ma).cast(pl.Int8)
            out[f"{etf}_above_ma{w}"] = flag.shift(1)   # use through prior day
    return pl.DataFrame(out)


def main():
    t0 = dt.datetime.now()
    print("=" * 92)
    print("Phase 1 — DIRECTION TEST (two-stage): among movers, LONG vs SHORT?")
    print("=" * 92)
    do_h = set(pl.scan_parquet(wf("daily_observation")).collect_schema().names())
    fo_h = set(pl.scan_parquet(wf("forward_outcomes")).collect_schema().names())
    cl_h = set(pl.scan_parquet(wf("security_classification_daily")).collect_schema().names())
    mc_h = set(pl.scan_parquet(wf("market_context_daily")).collect_schema().names())

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select(["day", "security_id", "sector"] + keep(CL_FEATS, cl_h)))
    do = pl.scan_parquet(wf("daily_observation")).select(["day", "security_id"] + keep(DO_FEATS, do_h))
    mc = pl.scan_parquet(wf("market_context_daily")).select(["day"] + keep(MC_FEATS, mc_h))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + keep(FO_FEATS, fo_h) + [LABEL_COL, "ret_1d"]))
    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner").join(mc, on="day", how="left")
          .join(ma_regime().lazy(), on="day", how="left")
          .filter(pl.col("intraday_ret_0930_to_1000_percentile_today") >= 0.8)
          # engineered directional features
          .with_columns(
              (pl.col("intraday_ret_0930_to_1000")
               - pl.col("intraday_ret_0930_to_1000").mean().over(["day", "sector"])).alias("sector_rel_morning"),
              pl.col("intraday_ret_0930_to_1000").mean().over(["day", "sector"]).alias("sector_morning_ret"),
              (pl.col("prior_day_eod_close") / pl.col("high_52w") - 1.0).alias("dist_52w_high"),
              (pl.col("prior_day_eod_close") / pl.col("low_52w") - 1.0).alias("dist_52w_low"))
          .with_columns(pl.col("day").dt.year().alias("yr")).sort("day").collect())

    feats = (keep(DO_FEATS, do_h) + keep(FO_FEATS, fo_h) + keep(CL_FEATS, cl_h)
             + keep(MC_FEATS, mc_h) + ENG)
    feats = [f for f in feats if f in df.columns]
    for c in feats:
        if df[c].dtype not in (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int8):
            df = df.with_columns(pl.col(c).cast(pl.Float64, strict=False))
    feats = [f for f in feats if df[f].drop_nulls().n_unique() >= 2]

    X = df.select(feats).to_numpy()
    event = df[LABEL_COL].to_numpy().astype(object)
    yr = df["yr"].to_numpy(); day = df["day"].to_numpy().astype("datetime64[D]")
    # DIRECTION label among RESOLVED MOVERS ONLY (drop 'neither' and nulls)
    resolved = np.isin(event, ["target_first", "stop_first"])
    y = (event == "target_first").astype(np.int64)   # 1 = up-first, 0 = down-first
    print(f"cohort {df.height:,} · resolved movers {resolved.sum():,} "
          f"({100*resolved.mean():.0f}%) · {len(feats)} feats (incl {len(ENG)} new)")
    print(f"base rate among movers: up-first {100*y[resolved].mean():.1f}%  (50% = coin flip)\n")

    def run(perm=None):
        pr = np.full(df.height, np.nan)
        for tr_y, te_y in TRAIN_TEST:
            te = (yr == te_y) & resolved; trn = np.isin(yr, tr_y) & resolved
            if trn.any():
                trn = trn & (day <= day[trn].max() - np.timedelta64(EMBARGO_DAYS, "D"))
            if not trn.any() or not te.any():
                continue
            yy = y[trn].copy()
            if perm is not None:
                dtr = day[trn]; o = np.argsort(dtr, kind="stable")
                u, idx = np.unique(dtr[o], return_index=True); ends = np.append(idx[1:], len(o))
                sh = yy[o].copy()
                for i in range(len(u)):
                    sh[idx[i]:ends[i]] = perm.permutation(sh[idx[i]:ends[i]])
                yy = np.empty_like(yy); yy[o] = sh
            m = gbm().fit(X[trn], yy)
            pr[te] = m.predict_proba(X[te])[:, 1]
        return pr

    pr = run()
    oos = resolved & ~np.isnan(pr)
    auc = roc_auc_score(y[oos], pr[oos])
    nulls = []
    for k in range(N_PERM):
        prp = run(np.random.default_rng(SEED + 700 + k))
        m2 = resolved & ~np.isnan(prp)
        nulls.append(roc_auc_score(y[m2], prp[m2]))
    null95 = float(np.quantile(nulls, 0.95))
    print("── STAGE 2 DIRECTION: among movers, predict up-first vs down-first ──")
    print(f"  OOS AUC {auc:.4f}  | perm-null 95pct {null95:.4f}  -> "
          f"{'BEATS ✓ (directional signal!)' if auc > null95 else 'inside null ✗ (coin flip)'}\n")

    # decile: does higher P(up) => more up-first?
    sc = pr[oos]; yo = y[oos]; ev = event[oos]; yr_o = yr[oos]
    dec = np.clip((np.searchsorted(np.sort(sc), sc, side="right") - 1) * 10 // len(sc), 0, 9)
    print(f"  {'P(up) decile':<14}{'n':>8}{'up-first%':>11}")
    for d in [0, 1, 2, 8, 9]:
        mm = dec == d
        if mm.sum():
            print(f"  {'D'+str(d)+(' (top)' if d==9 else ' (bot)' if d==0 else ''):<14}{mm.sum():>8,}{100*yo[mm].mean():>10.1f}%")

    # ── LONG/SHORT economics: long high P(up), short low P(up), skip middle ──
    print("\n── LONG/SHORT economics (long P(up)>0.5+m, short <0.5-m, skip middle) ──")
    print(f"  barrier +2%/-2%; borrow {BORROW_BPS_SHORT:g}bp on shorts (liquid=easy-borrow)")
    print(f"  {'band m':<8}{'n long':>8}{'n short':>9}{'dir-acc%':>10}{'gross':>9}"
          + "".join(f"{'net@'+str(int(c))+'bp':>10}" for c in COSTS_BPS))
    up_first = (ev == "target_first")
    for m in CONF_BANDS:
        longs = sc > 0.5 + m; shorts = sc < 0.5 - m
        # long P&L: +2 if up-first else -2 ; short P&L: +2 if down-first else -2
        pnl = np.concatenate([np.where(up_first[longs], TARGET_PCT, STOP_PCT),
                              np.where(~up_first[shorts], TARGET_PCT, STOP_PCT)]) * 1e4
        acc = (np.concatenate([up_first[longs], ~up_first[shorts]]).mean() * 100
               if (longs.sum() + shorts.sum()) else np.nan)
        # borrow charged on the short leg
        nsh = shorts.sum()
        g = pnl.mean() if len(pnl) else np.nan
        row = f"  {m:<8.2f}{longs.sum():>8,}{nsh:>9,}{acc:>9.1f}%{g:>9.1f}"
        for c in COSTS_BPS:
            borrow = BORROW_BPS_SHORT * nsh / max(len(pnl), 1)
            row += f"{g - c - borrow:>10.1f}"
        print(row)

    # by-year at the tightest band
    print("\n── by year (band m=0.05): directional accuracy + net@10bp ──")
    m = 0.05
    for yv in [2018, 2019, 2020]:
        ymask = yr_o == yv
        longs = ymask & (sc > 0.5 + m); shorts = ymask & (sc < 0.5 - m)
        n = longs.sum() + shorts.sum()
        if not n:
            continue
        pnl = np.concatenate([np.where(up_first[longs], TARGET_PCT, STOP_PCT),
                              np.where(~up_first[shorts], TARGET_PCT, STOP_PCT)]) * 1e4
        acc = np.concatenate([up_first[longs], ~up_first[shorts]]).mean() * 100
        print(f"  {yv}: n={n:>6,}  dir-acc {acc:>5.1f}%  gross {pnl.mean():>6.1f}bp  net@10 {pnl.mean()-10:>6.1f}bp")

    print("\n--- verdict ---")
    print("If Stage-2 AUC beats the null AND P(up) deciles spread up-first% AND the long/short")
    print("net expectancy is positive & year-stable -> the cheap features DO turn magnitude into")
    print("direction: a real long/short edge. If AUC~0.50 / flat deciles / net<=0 -> direction is")
    print("NOT in the available features; magnitude is all there is, and it needs new data.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
