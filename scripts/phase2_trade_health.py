#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy --with scikit-learn python3
"""
Phase 2C — TRADE-HEALTH / REMAINING-PATH PREDICTABILITY (post-entry information).

Phase 1 used only pre-entry info and found no directional edge. This is the first test of
POST-ENTRY information: I've already entered at 10:00; at a mid-trade checkpoint (30m in),
does the intra-trade STATE predict the REMAINING path to EOD — out-of-sample?

Honest framing (avoids the tautology "winners are already up"): target = REMAINING return
from the checkpoint forward = ret[EOD] - ret[30m]. The banked gain is EXCLUDED. So this asks:
conditional on how the trade has behaved so far, is what happens NEXT predictable?

State @30m (from forward_path_short): return-so-far, drawdown-from-peak, VWAP distance,
%bars profitable/underwater, within-trade volatility, rate-of-change, ret/ATR, volume-since-
entry. Target: remaining return to EOD (regression = expectancy, per the ±R idea).
Baselines to beat: "ret-so-far alone" and "rate-of-change alone" (trivial momentum).
Walk-forward OOS, day-clustered rank-IC, permutation null. Deployable/liquid, entry 10:00.
Leak-free: features @10:30 strictly precede the 10:30->EOD target; intraday so no overlap.
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CKPT, TGT_CKPT = "30m", "EOD"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
TRAIN_TEST = [([2016, 2017], 2018), ([2016, 2017, 2018], 2019), ([2016, 2017, 2018, 2019], 2020)]
N_PERM = 8
N_BOOT = 2000
SEED = 20260705
STATE = ["ret", "high_ret_so_far", "low_ret_so_far", "close_max_ret_so_far", "close_min_ret_so_far",
         "pct_bars_profitable_so_far", "pct_bars_underwater_so_far", "volatility_within_trade",
         "rate_of_change", "current_ret_over_atr_14d", "volume_since_entry", "dollar_volume_since_entry"]
DERIVED = ["dist_above_vwap", "drawdown_from_peak", "runup_to_now"]


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


def daily_rank_ic(day, pred, act):
    order = np.argsort(day, kind="stable"); d, p, a = day[order], pred[order], act[order]
    u, idx = np.unique(d, return_index=True); ends = np.append(idx[1:], len(d))
    ics = []
    for i in range(len(u)):
        s, e = idx[i], ends[i]
        if e - s < 10:
            continue
        pr = np.argsort(np.argsort(p[s:e])).astype(float); ar = np.argsort(np.argsort(a[s:e])).astype(float)
        pr -= pr.mean(); ar -= ar.mean()
        den = np.sqrt((pr*pr).sum()*(ar*ar).sum())
        if den > 0:
            ics.append(float((pr*ar).sum()/den))
    return np.array(ics)


def boot_ci(x, rng):
    if len(x) == 0:
        return (np.nan, np.nan)
    bm = x[rng.integers(0, len(x), size=(N_BOOT, len(x)))].mean(axis=1)
    return float(np.quantile(bm, 0.025)), float(np.quantile(bm, 0.975))


def walk(X, y, day, yr, valid, perm_rng=None):
    pr = np.full(len(y), np.nan)
    for tr_y, te_y in TRAIN_TEST:
        te = (yr == te_y) & valid; trn = np.isin(yr, tr_y) & valid
        if trn.any():
            trn = trn & (day <= day[trn].max() - np.timedelta64(2, "D"))
        if not trn.any() or not te.any():
            continue
        ytr = y[trn].copy()
        if perm_rng is not None:
            dtr = day[trn]; o = np.argsort(dtr, kind="stable")
            u, idx = np.unique(dtr[o], return_index=True); ends = np.append(idx[1:], len(o))
            sh = ytr[o].copy()
            for i in range(len(u)):
                sh[idx[i]:ends[i]] = perm_rng.permutation(sh[idx[i]:ends[i]])
            ytr = np.empty_like(ytr); ytr[o] = sh
        m = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
            min_samples_leaf=200, l2_regularization=1.0, early_stopping=True,
            validation_fraction=0.1, n_iter_no_change=20, random_state=SEED)
        m.fit(X[trn], ytr); pr[te] = m.predict(X[te])
    return pr


def main():
    t0 = dt.datetime.now()
    print("=" * 92)
    print(f"Phase 2C — TRADE-HEALTH: state @{CKPT} -> REMAINING return {CKPT}->{TGT_CKPT} (post-entry info)")
    print("=" * 92)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id"))
    path = pl.scan_parquet(wf("forward_path_short")).filter(pl.col("entry_offset") == OFFSET)
    s30 = (path.filter(pl.col("path_checkpoint") == CKPT)
           .select(["day", "security_id", "vwap_since_entry", "bars_elapsed"] + STATE))
    eod = (path.filter(pl.col("path_checkpoint") == TGT_CKPT)
           .select(["day", "security_id", pl.col("ret").alias("ret_eod")]))
    ep = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "entry_price"))
    df = (s30.join(eod, on=["day", "security_id"], how="inner")
          .join(ep, on=["day", "security_id"], how="inner")
          .join(cls, on=["day", "security_id"], how="inner")
          .with_columns(
              (pl.col("ret_eod") - pl.col("ret")).alias("remaining"),                 # target
              (pl.col("ret") - (pl.col("vwap_since_entry") / pl.col("entry_price") - 1.0)).alias("dist_above_vwap"),
              (pl.col("ret") - pl.col("close_max_ret_so_far")).alias("drawdown_from_peak"),
              (pl.col("high_ret_so_far")).alias("runup_to_now"))
          .with_columns(pl.col("day").dt.year().alias("yr")).sort("day").collect())

    feats = STATE + DERIVED + ["bars_elapsed"]
    feats = [f for f in feats if f in df.columns and df[f].drop_nulls().n_unique() >= 2]
    y = df["remaining"].to_numpy()
    valid = ~np.isnan(y)
    day = df["day"].to_numpy().astype("datetime64[D]"); yr = df["yr"].to_numpy()
    rng = np.random.default_rng(SEED)
    print(f"trades @10:00 with a {CKPT} checkpoint: {df.height:,} · target = remaining {CKPT}->{TGT_CKPT} "
          f"(median |remaining| {np.nanmedian(np.abs(y))*1e4:.0f}bp)\n")

    # ---- descriptive (Phase 2B): remaining return conditioned on state, forward-looking ----
    print("── 2B descriptive: mean REMAINING return + P(remaining>0) by state @30m ──")
    ret30 = df["ret"].to_numpy(); dvwap = df["dist_above_vwap"].to_numpy(); roc = df["rate_of_change"].to_numpy()
    for label, cond in [("up so far (ret>0)", ret30 > 0), ("down so far (ret<0)", ret30 < 0),
                        ("above entry-VWAP", dvwap > 0), ("below entry-VWAP", dvwap < 0),
                        ("rising (roc>0)", roc > 0), ("falling (roc<0)", roc < 0)]:
        m = valid & cond
        if m.sum():
            print(f"  {label:<24} n={m.sum():>7,}  remaining {y[m].mean()*1e4:>+6.1f}bp  "
                  f"P(remain>0) {100*np.mean(y[m]>0):>4.1f}%")

    # ---- 2C: does the FULL state predict remaining return OOS? ----
    print(f"\n── 2C: OOS predictability of remaining return (rank-IC, day-clustered) ──")
    def ic_of(fs, name, perm=False):
        X = df.select(fs).to_numpy()
        pr = walk(X, y, day, yr, valid)
        m = valid & ~np.isnan(pr)
        ics = daily_rank_ic(day[m], pr[m], y[m]); lo, hi = boot_ci(ics, rng)
        nullmean = None
        if perm:
            nn = []
            for k in range(N_PERM):
                prp = walk(X, y, day, yr, valid, perm_rng=np.random.default_rng(SEED+900+k))
                mm = valid & ~np.isnan(prp); nn.append(daily_rank_ic(day[mm], prp[mm], y[mm]).mean())
            nullmean = (np.mean(nn), np.quantile(nn, 0.95))
        tag = ""
        if nullmean:
            tag = f"  null95 {nullmean[1]:+.4f} -> {'BEATS ✓' if ics.mean() > nullmean[1] else 'inside null ✗'}"
        print(f"  {name:<30}{ics.mean():>+8.4f}  CI[{lo:+.4f},{hi:+.4f}]{tag}")
        return ics.mean()

    ic_of(["ret"], "baseline: ret-so-far only")
    ic_of(["rate_of_change"], "baseline: rate-of-change only")
    full = ic_of(feats, "FULL post-entry state", perm=True)

    print("\n--- verdict ---")
    print("FULL-state OOS rank-IC clearly >0, beats the null AND beats the ret-so-far baseline")
    print("-> post-entry state predicts the REMAINING path: a real trade-management edge (build the")
    print("health model + simulate early-exit vs hold, Phase 2D). IC ~0 / inside null / no better")
    print("than ret-so-far -> even post-entry information carries no directional edge; the remaining")
    print("path is a coin flip too, and price/volume is exhausted for stock direction.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
