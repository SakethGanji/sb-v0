#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 4 — CONDITIONAL-SUBSET TEST (PRIM / bump hunting), pre-registered.

HYPOTHESIS (user): even though the GLOBAL direction edge is null, there may exist a small
subset (support ~0.5-5%) of (name, day) states where P(beat SPY) or net expectancy is
dramatically better than average — a region global-loss ML would smooth away.

WHY THIS ISN'T ALREADY ANSWERED: the joint probe optimized global loss (a 1%-support,
+25pp region moves log-loss ~0.25% — inside regularization noise; min_samples_leaf=200 +
early stopping actively suppress it) and evaluated by decile (10x dilution of a 1% region).
PRIM optimizes sup-over-regions directly. Same feature space, different objective.

PRE-REGISTERED PROTOCOL (frozen before first run; see phase4-conditional-subset-findings.md):
  * Data: deployable universe (CS, mega/large/mid, HL/L/N), entry 10:00, 2016-2020 ONLY.
    Features: the leak-audited 97-column joint-probe list. Target: ret_5d_excess_spy.
  * Split: SEARCH on 2016-2018, CONFIRM on 2019-2020. (2021-22 validation stays untouched
    unless a box passes 2019-20; 2023+ holdout sealed regardless.)
  * Criteria (run separately): NETEXP = train mean excess - 20bp; WINRATE = train P(excess>0).
  * PRIM: peel alpha 5%; support >= 0.5% of search rows, >= 100 distinct train days, >= 50
    names, quality good in >= 2/3 train years. K=5 boxes via cover-and-remove.
  * MULTIPLICITY: full-pipeline permutation null — y shuffled WITHIN train day, entire search
    re-run, best box evaluated on REAL 2019-20 y. N_PERM=25 (parallel across cores).
  * VERDICT RULE: a box is REAL only if (a) OOS daily net expectancy > 0 with CI excluding 0,
    (b) beats permutation-null 95th pct, (c) OOS win rate within 10pp of train (no cliff),
    (d) not >50% concentrated in 2020-Feb..Apr, (e) >= 40 OOS days and >= 30 names.

Search on a seeded 400k-row subsample for speed; ALL evaluation on full data.
Writes data/phase1_analysis/prim_subgroup_results.parquet. Read-only on data/outputs.

Run: scripts/phase4_prim_subgroup.py [--quick]   (--quick: N_PERM=2 smoke test)
"""
from __future__ import annotations
import glob, os, sys, datetime as dt
from multiprocessing import get_context
from pathlib import Path
import numpy as np
import polars as pl

QUICK = "--quick" in sys.argv
EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
TARGET = "ret_5d_excess_spy"
COST = 20e-4
SEARCH_YEARS = [2016, 2017, 2018]
CONFIRM_YEARS = [2019, 2020]
PEEL = 0.05
SUPPORT_MIN_FRAC = 0.005
MIN_TRAIN_DAYS, MIN_TRAIN_NAMES = 100, 50
MIN_OOS_DAYS, MIN_OOS_NAMES = 40, 30
K_BOXES = 5
N_PERM = 2 if QUICK else 25
N_SEARCH_ROWS = 400_000
N_BOOT = 2000
BLOCK = 5
SEED = 20260706

DO_FEATS = [
    "intraday_ret_0930_to_0940", "intraday_ret_0930_to_0950", "intraday_ret_0930_to_1000",
    "intraday_volume_0930_to_1000", "intraday_dollar_volume_0930_to_1000",
    "intraday_first_30m_high_return", "intraday_first_30m_low_return",
    "intraday_ret_from_first_30m_high_to_1000", "intraday_minutes_since_first_30m_high_at_1000",
    "intraday_first_15m_volume_share_of_first_30m",
    "intraday_ret_0930_to_1000_rank_today", "intraday_ret_0930_to_1000_percentile_today",
    "intraday_dollar_volume_0930_to_1000_rank_today", "premarket_volume_rank_today",
    "premarket_dollar_volume_rank_today", "overnight_gap_rank_today", "addv_20d_rank_today",
    "realized_vol_21d_rank_today", "premarket_volume", "premarket_dollar_volume",
    "overnight_gap", "prior_day_last_30m_return", "prior_day_last_30m_volume_share",
    "atr_5d", "atr_14d", "atr_42d", "realized_vol_21d",
    "yang_zhang_vol_5d", "yang_zhang_vol_14d", "yang_zhang_vol_21d", "yang_zhang_vol_42d",
    "adv_5d", "adv_20d", "adv_60d", "addv_5d", "addv_20d", "addv_60d",
    "beta_spy_60d", "beta_qqq_60d", "beta_iwm_60d",
    "days_to_next_known_earnings", "days_since_last_earnings",
    "signal_concentration_percentile_today", "signal_concentration_hhi_today",
    "premarket_volume_vs_20d_median", "days_since_last_5pct_move",
    "days_since_last_10pct_move", "days_since_last_20pct_move",
    "consecutive_up_days_close_to_close", "day_of_week", "days_since_first_bar",
    "signal_first_in_5d", "signal_first_in_10d", "signal_first_in_20d",
    "bar_count_premarket", "bar_count_first_30m",
]
FO_FEATS = [
    "pre_entry_ret_from_open", "pre_entry_vwap_from_open", "pre_entry_ret_from_high",
    "pre_entry_ret_from_low", "pre_entry_minutes_since_high", "pre_entry_minutes_since_low",
    "pre_entry_ret_rank_today", "pre_entry_ret_percentile_today",
    "pre_entry_dollar_volume_rank_today", "entry_bar_upper_wick_pct",
    "entry_bar_lower_wick_pct", "entry_slippage_proxy_bps", "entry_1m_range",
    "entry_range_vs_atr_14d", "entry_dollar_volume_vs_addv_20d",
    "cumulative_volume_to_entry", "cumulative_dollar_volume_to_entry",
]
CL_FEATS = ["market_cap", "market_cap_rank_today", "market_cap_percentile_today",
            "volatility_percentile_today", "days_since_ipo_or_first_bar"]
MC_FEATS = ["vix_open", "spy_overnight_gap", "qqq_overnight_gap", "iwm_overnight_gap",
            "spy_ret_0930_to_1000", "qqq_ret_0930_to_1000", "iwm_ret_0930_to_1000",
            "spy_realized_vol_21d", "qqq_realized_vol_21d", "iwm_realized_vol_21d",
            "breadth_pct_universe_green_at_1000", "breadth_pct_universe_above_premarket_vwap_at_1000",
            "breadth_advance_decline_ratio_at_1000", "breadth_count_movers_above_5pct_at_1000",
            "breadth_count_movers_above_1atr_at_1000", "cross_sectional_ret_dispersion_at_1000",
            "cross_sectional_ret_iqr_at_1000", "universe_median_addv_20d",
            "universe_total_dollar_volume"]

G = {}  # worker globals (fork-inherited)


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


def keep(cands, have):
    return [c for c in cands if c in have]


def block_ci(x, rng, block=BLOCK, B=N_BOOT):
    n = len(x)
    if n < block + 5:
        return (np.nan, np.nan)
    nb = int(np.ceil(n / block))
    idx = (rng.integers(0, n, size=(B, nb))[:, :, None] + np.arange(block)[None, None, :]) % n
    bm = x[idx.reshape(B, -1)[:, :n]].mean(axis=1)
    return float(np.quantile(bm, 0.025)), float(np.quantile(bm, 0.975))


def q_of(ysub, crit):
    return ysub.mean() - COST if crit == "NETEXP" else (ysub > 0).mean()


def era_ok(ysub, yrsub, crit):
    n_good = n_tot = 0
    for e in SEARCH_YEARS:
        m = yrsub == e
        if m.sum() < 200:
            continue
        n_tot += 1
        q = q_of(ysub[m], crit)
        if (crit == "NETEXP" and q > 0) or (crit == "WINRATE" and q >= 0.55):
            n_good += 1
    return n_tot >= 2 and n_good >= 2


MAXDIM_SHALLOW = 4


def prim_search(X, y, day, sid, yr, idx0, crit, support_min):
    """PRIM peel from the rows in idx0. Returns (best_deep, best_shallow), each (q, box) or
    None. best_shallow = best trajectory point using <= MAXDIM_SHALLOW features (the
    human-scale 'Condition A' variant). Deterministic."""
    idx = idx0
    box = {}
    best = None
    best4 = None
    floor = max(support_min, 200)
    p = X.shape[1]
    while len(idx) > floor:
        yi = y[idx]
        n_in = len(idx)
        sum_y, npos = yi.sum(), (yi > 0).sum()
        cur_q = (sum_y / n_in - COST) if crit == "NETEXP" else (npos / n_in)
        bg, bf, bside, bthr = 1e-12, None, None, None
        for f in range(p):
            v = X[idx, f]
            fin = ~np.isnan(v)
            nf = int(fin.sum())
            if nf < 100:
                continue
            lo_t, hi_t = np.quantile(v[fin], [PEEL, 1.0 - PEEL])
            if not (lo_t < hi_t):
                continue
            for side, rem, thr in (("lo", v < lo_t, lo_t), ("hi", v > hi_t, hi_t)):
                nr = int(rem.sum())
                ns = n_in - nr
                if ns < floor or nr == 0:
                    continue
                if crit == "NETEXP":
                    q2 = (sum_y - yi[rem].sum()) / ns - COST
                else:
                    q2 = (npos - (yi[rem] > 0).sum()) / ns
                if q2 - cur_q > bg:
                    bg, bf, bside, bthr = q2 - cur_q, f, side, thr
        if bf is None:
            break
        v = X[idx, bf]
        keep_m = ~(v < bthr) if bside == "lo" else ~(v > bthr)
        idx = idx[keep_m]
        lo, hi = box.get(bf, (-np.inf, np.inf))
        box[bf] = (max(lo, bthr), hi) if bside == "lo" else (lo, min(hi, bthr))
        if len(idx) >= support_min:
            if (len(np.unique(day[idx])) >= MIN_TRAIN_DAYS
                    and len(np.unique(sid[idx])) >= MIN_TRAIN_NAMES
                    and era_ok(y[idx], yr[idx], crit)):
                q = q_of(y[idx], crit)
                if best is None or q > best[0]:
                    best = (q, dict(box))
                if len(box) <= MAXDIM_SHALLOW and (best4 is None or q > best4[0]):
                    best4 = (q, dict(box))
    return best, best4


def apply_box(X, box):
    m = np.ones(X.shape[0], dtype=bool)
    for f, (lo, hi) in box.items():
        v = X[:, f]
        if np.isfinite(lo):
            m &= ~(v < lo)
        if np.isfinite(hi):
            m &= ~(v > hi)
    return m


def run_pipeline(X, y, day, sid, yr, crit, support_min):
    """returns (deep_boxes, shallow_boxes); cover-and-remove driven by the deep boxes."""
    deep, shallow = [], []
    avail = np.arange(len(y))
    for _ in range(K_BOXES):
        bd, bs = prim_search(X, y, day, sid, yr, avail, crit, support_min)
        if bd is None:
            break
        deep.append(bd)
        if bs is not None:
            shallow.append(bs)
        m = apply_box(X, bd[1])
        avail = avail[~m[avail]]
        if len(avail) < support_min * 4:
            break
    return deep, shallow


def eval_box_quick(box, crit):
    """OOS metric for the permutation null (no bootstrap)."""
    Xte, yte, dte, ste = G["Xte"], G["yte"], G["dte"], G["ste"]
    m = apply_box(Xte, box)
    if m.sum() < 50:
        return None
    if len(np.unique(dte[m])) < MIN_OOS_DAYS or len(np.unique(ste[m])) < MIN_OOS_NAMES:
        return None
    return (yte[m].mean() - COST) * 1e4 if crit == "NETEXP" else (yte[m] > 0).mean()


def perm_worker(args):
    pi, crit = args
    Xs, ys, ds, ss, yrs, support_min = (G["Xs"], G["ys"], G["ds"], G["ss"], G["yrs"],
                                        G["support_min"])
    prng = np.random.default_rng(SEED + 7000 + pi)
    ysh = ys.copy()
    o = np.argsort(ds, kind="stable")
    u, idx = np.unique(ds[o], return_index=True)
    ends = np.append(idx[1:], len(o))
    tmp = ysh[o]
    for i in range(len(u)):
        seg = slice(idx[i], ends[i])
        tmp[seg] = prng.permutation(tmp[seg])
    ysh[o] = tmp
    deep, shallow = run_pipeline(Xs, ysh, ds, ss, yrs, crit, support_min)

    def best_of(boxes):
        vals = [eval_box_quick(box, crit) for _, box in boxes]
        vals = [v for v in vals if v is not None]
        return max(vals) if vals else None

    return best_of(deep), best_of(shallow)


def eval_box_full(box, X, y, day, sid, yr, rng, label, feats):
    m = apply_box(X, box)
    if m.sum() == 0:
        print(f"    {label}: empty"); return None
    d, yy = day[m], y[m]
    order = np.argsort(d, kind="stable")
    d, yy = d[order], yy[order]
    u, idx = np.unique(d, return_index=True)
    ends = np.append(idx[1:], len(d))
    daily = np.array([yy[a:b].mean() for a, b in zip(idx, ends)])
    n_days, n_names = len(u), len(np.unique(sid[m]))
    gross = daily.mean()
    lo, hi = block_ci(daily, rng)
    win = (y[m] > 0).mean()
    crisis = ((d >= np.datetime64("2020-02-15")) & (d <= np.datetime64("2020-04-30"))).mean()
    yrs = {int(e): round(float(y[m & (yr == e)].mean() * 1e4), 1)
           for e in np.unique(yr) if (m & (yr == e)).sum() > 0}
    print(f"    {label}: rows={m.sum():,} days={n_days} names={n_names}  gross {gross*1e4:+.1f}bp  "
          f"net@20 {gross*1e4-20:+.1f} CI[{lo*1e4-20:+.1f},{hi*1e4-20:+.1f}]  "
          f"win {win*100:.1f}%  crisis {crisis*100:.0f}%", flush=True)
    print(f"      per-year gross bp: {yrs}", flush=True)
    print("      box: " + ", ".join(f"{feats[f]}∈[{lo0:.4g},{hi0:.4g}]"
                                    for f, (lo0, hi0) in box.items()), flush=True)
    return dict(label=label, n_rows=int(m.sum()), n_days=n_days, n_names=n_names,
                gross_bp=gross * 1e4, net_bp=gross * 1e4 - 20,
                ci_lo_bp=lo * 1e4 - 20, ci_hi_bp=hi * 1e4 - 20,
                win=float(win), crisis_frac=float(crisis))


def main():
    t0 = dt.datetime.now()
    print("=" * 96, flush=True)
    print(f"Phase 4 — CONDITIONAL-SUBSET (PRIM) pre-registered test (perms={N_PERM}"
          f"{' QUICK' if QUICK else ''})", flush=True)
    print("=" * 96, flush=True)
    do_have = set(pl.scan_parquet(wf("daily_observation")).collect_schema().names())
    fo_have = set(pl.scan_parquet(wf("forward_outcomes")).collect_schema().names())
    cl_have = set(pl.scan_parquet(wf("security_classification_daily")).collect_schema().names())
    mc_have = set(pl.scan_parquet(wf("market_context_daily")).collect_schema().names())
    do_f, fo_f = keep(DO_FEATS, do_have), keep(FO_FEATS, fo_have)
    cl_f, mc_f = keep(CL_FEATS, cl_have), keep(MC_FEATS, mc_have)
    feats = do_f + fo_f + cl_f + mc_f

    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS))
           .select(["day", "security_id"] + cl_f))
    do = pl.scan_parquet(wf("daily_observation")).select(["day", "security_id"] + do_f)
    mc = pl.scan_parquet(wf("market_context_daily")).select(["day"] + mc_f)
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select(["day", "security_id"] + fo_f + [TARGET]))
    df = (fo.join(cls, on=["day", "security_id"], how="inner")
          .join(do, on=["day", "security_id"], how="inner")
          .join(mc, on="day", how="left")
          .with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in feats])
          .with_columns(pl.col("day").dt.year().alias("yr"))
          .filter(pl.col(TARGET).is_not_null())
          .sort("day").collect())
    feats = [f for f in feats if df[f].drop_nulls().n_unique() >= 2]
    X = np.ascontiguousarray(df.select(feats).to_numpy())
    y = df[TARGET].to_numpy()
    day = df["day"].to_numpy().astype("datetime64[D]")
    sid_codes = df["security_id"].cast(pl.Categorical).to_physical().to_numpy()
    yr = df["yr"].to_numpy()
    print(f"panel: {df.height:,} rows · {len(feats)} features · target {TARGET}", flush=True)
    del df

    tr = np.isin(yr, SEARCH_YEARS)
    te = np.isin(yr, CONFIRM_YEARS)
    print(f"search 2016-18: {tr.sum():,} rows · confirm 2019-20: {te.sum():,} rows", flush=True)
    rng = np.random.default_rng(SEED)
    tr_idx = np.where(tr)[0]
    sub = tr_idx if len(tr_idx) <= N_SEARCH_ROWS else np.sort(
        rng.choice(tr_idx, N_SEARCH_ROWS, replace=False))
    support_min = int(SUPPORT_MIN_FRAC * len(sub))
    print(f"search subsample: {len(sub):,} rows · support floor {support_min:,} (0.5%)\n", flush=True)

    # fork-inherited globals for workers
    G["Xs"], G["ys"] = np.ascontiguousarray(X[sub]), y[sub].copy()
    G["ds"], G["ss"], G["yrs"] = day[sub], sid_codes[sub], yr[sub]
    G["Xte"], G["yte"] = np.ascontiguousarray(X[te]), y[te].copy()
    G["dte"], G["ste"] = day[te], sid_codes[te]
    G["support_min"] = support_min

    all_recs = []
    for crit in ["NETEXP", "WINRATE"]:
        print(f"### criterion: {crit} ###", flush=True)
        t1 = dt.datetime.now()
        deep, shallow = run_pipeline(G["Xs"], G["ys"], G["ds"], G["ss"], G["yrs"],
                                     crit, support_min)
        print(f"  boxes on train-search: {len(deep)} deep, {len(shallow)} shallow(≤{MAXDIM_SHALLOW}d) "
              f"({(dt.datetime.now()-t1).total_seconds():.0f}s)", flush=True)
        oos_best = {"deep": -np.inf, "shallow": -np.inf}
        for depth, boxes in (("deep", deep), ("shallow", shallow)):
            for k, (q, box) in enumerate(boxes):
                trq = f"net {q*1e4:+.1f}bp" if crit == "NETEXP" else f"win {q*100:.1f}%"
                print(f"  {depth.upper()} BOX {k+1} ({len(box)} dims, train-search {trq}):",
                      flush=True)
                rtr = eval_box_full(box, X[tr], y[tr], day[tr], sid_codes[tr], yr[tr], rng,
                                    "train  ", feats)
                rte = eval_box_full(box, X[te], y[te], day[te], sid_codes[te], yr[te], rng,
                                    "CONFIRM", feats)
                if rtr and rte:
                    rte.update(crit=crit, depth=depth, box_k=k + 1,
                               train_win=rtr["win"], train_net_bp=rtr["net_bp"])
                    all_recs.append(rte)
                    metric = rte["net_bp"] if crit == "NETEXP" else rte["win"]
                    if (rte["n_days"] >= MIN_OOS_DAYS and rte["n_names"] >= MIN_OOS_NAMES):
                        oos_best[depth] = max(oos_best[depth], metric)
        print(f"  permutation null ({N_PERM} full search re-runs, parallel)...", flush=True)
        nproc = max(1, min(N_PERM, (os.cpu_count() or 4) - 2))
        with get_context("fork").Pool(nproc) as pool:
            null_pairs = pool.map(perm_worker, [(pi, crit) for pi in range(N_PERM)])
        unit = "bp net" if crit == "NETEXP" else "win-rate"
        for depth, di in (("deep", 0), ("shallow", 1)):
            nv = [p[di] for p in null_pairs if p[di] is not None]
            if not nv:
                print(f"  {depth}: (no valid null boxes)", flush=True)
                continue
            n95 = float(np.quantile(nv, 0.95))
            rb = oos_best[depth]
            print(f"  {depth:>7} NULL best-OOS ({unit}): mean {np.mean(nv):+.2f}  95pct {n95:+.2f}"
                  f"  n={len(nv)}  |  REAL {rb:+.2f} -> "
                  f"{'BEATS null' if rb > n95 else 'inside null ✗'}", flush=True)
        print(flush=True)

    if all_recs:
        pl.DataFrame(all_recs).write_parquet("data/phase1_analysis/prim_subgroup_results.parquet")
    print("--- pre-registered verdict rule ---", flush=True)
    print("REAL only if: OOS net CI>0 AND beats perm-null 95pct AND win cliff <10pp AND", flush=True)
    print("crisis <50% AND >=40 OOS days / >=30 names. Else the subset door closes.", flush=True)
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s", flush=True)


if __name__ == "__main__":
    main()
