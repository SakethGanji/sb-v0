#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 1 — META-LABEL L2 ECONOMIC TEST: does the strong barrier-touch classifier
                                        actually make MONEY under its own exit?

The direct meta-label test found L2 ("+2% before -2% within 1d") is strongly, stably,
monotonically predictable (OOS AUC 0.63, beats null, +15pp top-decile win lift). But
buy-hold-1d excess of the top decile was NEGATIVE — suggesting the model predicts PATH
GEOMETRY (volatility/barrier order), not expected return, measured under the wrong exit.

L2's OWN exit is the BARRIER: exit at +2% (target), -2% (stop), or 1d close (neither).
This computes the realized RAW barrier P&L per meta-score decile, net of a round-trip
cost sweep, and asks whether GATING on the meta-label (accept top-K% only) yields a
positive, cost-clearing, year-stable expectancy that beats taking ALL momentum trades.
These are LIQUID/DEPLOYABLE names -> if it clears, capacity is not the blocker.

Primary cohort + features + walk-forward identical to phase1_metalabel.py (L2 only).
Read-only; prints tables.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingClassifier

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
TRAIN_TEST = [([2016, 2017], 2018), ([2016, 2017, 2018], 2019),
              ([2016, 2017, 2018, 2019], 2020)]
EMBARGO_DAYS = 2
SEED = 20260705
TARGET_PCT, STOP_PCT = 0.02, -0.02          # L2 barriers
COSTS_BPS = [0.0, 10.0, 20.0, 30.0]          # round-trip cost sweep (bps)
LABEL_COL = "first_event_2pct_before_minus_2pct_1d"

# feature lists identical to phase1_metalabel.py
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
    "signal_first_in_5d","signal_first_in_10d","signal_first_in_20d","bar_count_premarket","bar_count_first_30m"]
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


def barrier_pnl(event, ret1d):
    """RAW realized return of the +2%/-2%/close barrier exit, per trade (fractions)."""
    out = np.where(event == "target_first", TARGET_PCT,
          np.where(event == "stop_first", STOP_PCT, ret1d))  # neither -> hold to close
    return out


def main():
    t0 = dt.datetime.now()
    print("=" * 92)
    print("Phase 1 — META-LABEL L2 ECONOMIC TEST (barrier exit +2%/-2%/close, net of cost)")
    print("=" * 92)
    do_h = set(pl.scan_parquet(wf("daily_observation")).collect_schema().names())
    fo_h = set(pl.scan_parquet(wf("forward_outcomes")).collect_schema().names())
    cl_h = set(pl.scan_parquet(wf("security_classification_daily")).collect_schema().names())
    mc_h = set(pl.scan_parquet(wf("market_context_daily")).collect_schema().names())
    feats = keep(DO_FEATS, do_h) + keep(FO_FEATS, fo_h) + keep(CL_FEATS, cl_h) + keep(MC_FEATS, mc_h)

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select(["day", "security_id"] + keep(CL_FEATS, cl_h)))
    do = pl.scan_parquet(wf("daily_observation")).select(["day", "security_id"] + keep(DO_FEATS, do_h))
    mc = pl.scan_parquet(wf("market_context_daily")).select(["day"] + keep(MC_FEATS, mc_h))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + keep(FO_FEATS, fo_h) + [LABEL_COL, "ret_1d"]))
    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner").join(mc, on="day", how="left")
          .filter(pl.col("intraday_ret_0930_to_1000_percentile_today") >= 0.8)
          .with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in feats])
          .with_columns(pl.col("day").dt.year().alias("yr")).sort("day").collect())
    feats = [f for f in feats if df[f].drop_nulls().n_unique() >= 2]

    X = df.select(feats).to_numpy()
    event_all = df[LABEL_COL].to_numpy().astype(object)
    ret1d_all = df["ret_1d"].to_numpy()
    yr = df["yr"].to_numpy(); day = df["day"].to_numpy().astype("datetime64[D]")
    y = (event_all == "target_first").astype(np.int64)
    valid = np.isin(event_all, ["target_first", "stop_first", "neither"]) & ~np.isnan(ret1d_all)
    print(f"cohort {df.height:,} · {len(feats)} feats · barrier +{TARGET_PCT:.0%}/{STOP_PCT:.0%}/close\n")

    # walk-forward OOS proba
    proba = np.full(df.height, np.nan); got = np.zeros(df.height, bool)
    for train_yrs, test_yr in TRAIN_TEST:
        te = (yr == test_yr) & valid; tr = np.isin(yr, train_yrs) & valid
        if tr.any():
            tr = tr & (day <= day[tr].max() - np.timedelta64(EMBARGO_DAYS, "D"))
        if not tr.any() or not te.any():
            continue
        m = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
            min_samples_leaf=200, l2_regularization=1.0, early_stopping=True,
            validation_fraction=0.1, n_iter_no_change=20, random_state=SEED)
        m.fit(X[tr], y[tr])
        proba[te] = m.predict_proba(X[te])[:, 1]; got[te] = True

    oos = got & valid
    pnl = barrier_pnl(event_all[oos], ret1d_all[oos])   # raw fractional return, barrier exit
    sc = proba[oos]; yo = yr[oos]
    dec = np.clip((np.searchsorted(np.sort(sc), sc, side="right") - 1) * 10 // len(sc), 0, 9)

    print("── barrier P&L by meta-score decile (gross, bps) + stop/target/neither mix ──")
    print(f"  {'decile':<12}{'n':>8}{'target%':>9}{'stop%':>8}{'neither%':>10}{'gross bps':>11}")
    for d in range(10):
        mm = dec == d
        if not mm.sum():
            continue
        ev = event_all[oos][mm]
        pt = 100*np.mean(ev == "target_first"); ps = 100*np.mean(ev == "stop_first")
        pn = 100*np.mean(ev == "neither")
        print(f"  {'D'+str(d)+(' (top)' if d==9 else ''):<12}{mm.sum():>8,}{pt:>8.1f}%{ps:>7.1f}%"
              f"{pn:>9.1f}%{pnl[mm].mean()*1e4:>11.1f}")

    print("\n── gated strategy expectancy (raw bps/trade), net-of-cost sweep ──")
    print(f"  {'strategy':<22}{'n/day~':>8}{'gross':>9}" + "".join(f"{'net@'+str(int(c))+'bp':>10}" for c in COSTS_BPS))
    ndays = len(np.unique(day[oos]))
    for lab, mask in [("ALL momentum", np.ones(oos.sum(), bool)),
                      ("top-half (D5-9)", dec >= 5), ("top-quartile (D8-9)", dec >= 8),
                      ("top-decile (D9)", dec == 9)]:
        g = pnl[mask].mean() * 1e4
        row = f"  {lab:<22}{mask.sum()/ndays:>8.0f}{g:>9.1f}"
        for c in COSTS_BPS:
            row += f"{g - c:>10.1f}"
        print(row)

    print("\n── top-decile barrier P&L by year (gross bps, net@20bp) ──")
    top = dec == 9
    for yv in [2018, 2019, 2020]:
        m2 = top & (yo == yv)
        if m2.sum():
            g = pnl[m2].mean() * 1e4
            print(f"  {yv}: n={m2.sum():>6,}  gross {g:>7.1f} bps   net@20bp {g-20:>7.1f} bps")

    print("\n--- verdict ---")
    print("If top-decile/quartile barrier P&L is clearly POSITIVE net of a realistic (~10-20bp)")
    print("round-trip cost AND positive in all 3 years -> a real, tradeable, capacity-OK edge the")
    print("return-ranking test missed: meta-labeling WORKS here. If net P&L <= 0 / flips by year")
    print("-> the AUC-0.63 signal predicts barrier GEOMETRY (volatility), not money. Either way,")
    print("the accept/reject objective is now answered on its own terms + its own exit.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
