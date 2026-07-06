#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 5 — VOL-TARGETING THE INDEX (the "filter the noisy days" question).

Reframe (user): stop trying to out-pick the market; use the ONE validated signal (vol
predictability) at the portfolio level — own less SPY when expected vol is high.

PRE-REGISTERED DESIGN (frozen before first run):
  * Universe: SPY only, daily close-to-close returns from market_context_daily.
  * Signals σ̂_t use info through day t-1 close ONLY (weight applies to return t-1→t):
      RV21   = spy_realized_vol_21d (t-1)
      VIX    = vix_close (t-1) / 100 (annualized implied)
      EWMA10 = exp-weighted realized vol of daily returns, halflife 10d (t-1)
      BLEND  = sqrt(RV21 * VIX) (geometric mean of realized and implied)
  * Rules (long-only, NO leverage — retail honest):
      INV-VOL : w = min(1, σ*/σ̂),  σ* = 15% annualized (constant, chosen a priori)
      INV-VAR : w = min(1, (σ*/σ̂)^2)   (Moreira-Muir form)
      BINARY  : w = 0 if σ̂ > expanding trailing 80th pct (min 1yr history) else 1
  * Costs: 2bp per unit turnover (conservative for SPY). Cash yields 0 (conservative).
  * Window: TRAIN 2016-06-08..2020-12-31. Validation 2021-22 ONLY on PASS. Holdout sealed.
  * KILL CRITERIA (PASS = all four, else NULL):
      1. net Sharpe(strategy) - Sharpe(SPY) >= +0.15 over the full train window
      2. block-bootstrap (block=21) 95% CI of the daily-return-based Sharpe difference > 0
      3. per-year Sharpe higher than SPY in >= 4/5 train years
      4. max drawdown reduced by >= 20% vs buy-and-hold
  * Honest caveats printed with results: only ~4.5yr / two vol events (Q4-2018, COVID) in
    train; vol-targeting's literature edge is Sharpe/drawdown, NOT raw return; a bull-market
    train window penalizes any de-risking rule on raw return by construction.

Run: scripts/phase5_vol_target.py [--validate]
"""
from __future__ import annotations
import glob, sys, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

VALIDATE = "--validate" in sys.argv
EXP_START, EXP_END = ((dt.date(2021, 1, 1), dt.date(2022, 12, 31)) if VALIDATE
                      else (dt.date(2016, 6, 8), dt.date(2020, 12, 31)))
WARMUP_DAYS = 260          # extra history before EXP_START for signals/thresholds
SIGMA_STAR = 0.15
COST_PER_TURNOVER = 2e-4
BLOCK = 21
N_BOOT = 4000
SEED = 20260706
ANN = 252


def wf():
    lo = EXP_START - dt.timedelta(days=int(WARMUP_DAYS * 1.6))
    out = []
    for f in sorted(glob.glob("data/outputs/market_context_daily/*.parquet")):
        try:
            d = dt.date.fromisoformat(Path(f).stem)
        except ValueError:
            continue
        if lo <= d <= EXP_END:
            out.append(f)
    return out


def perf(r, label, years=None, yr=None):
    mu, sd = r.mean() * ANN, r.std(ddof=1) * np.sqrt(ANN)
    sharpe = mu / sd if sd > 0 else np.nan
    eq = np.cumprod(1 + r)
    dd = (eq / np.maximum.accumulate(eq) - 1).min()
    line = f"  {label:<22} ret {mu*100:+6.2f}%/yr  vol {sd*100:5.2f}%  Sharpe {sharpe:5.2f}  maxDD {dd*100:6.1f}%"
    per_yr = {}
    if years is not None:
        for y in years:
            m = yr == y
            if m.sum() > 30:
                s = r[m]
                per_yr[y] = (s.mean() * ANN) / (s.std(ddof=1) * np.sqrt(ANN)) if s.std(ddof=1) > 0 else np.nan
    return sharpe, dd, line, per_yr


def sharpe_diff_ci(rs, rb, rng, block=BLOCK, B=N_BOOT):
    """block bootstrap CI of Sharpe(rs) - Sharpe(rb), joint blocks (preserves correlation)."""
    n = len(rs)
    nb = int(np.ceil(n / block))
    idx = (rng.integers(0, n, size=(B, nb))[:, :, None] + np.arange(block)[None, None, :]) % n
    ii = idx.reshape(B, -1)[:, :n]
    s_s = rs[ii].mean(1) / rs[ii].std(1, ddof=1)
    s_b = rb[ii].mean(1) / rb[ii].std(1, ddof=1)
    d = (s_s - s_b) * np.sqrt(ANN)
    return float(np.quantile(d, 0.025)), float(np.quantile(d, 0.975))


def main():
    t0 = dt.datetime.now()
    tag = "VALIDATION 2021-2022" if VALIDATE else "TRAIN 2016-06..2020-12"
    print("=" * 96)
    print(f"Phase 5 — VOL-TARGETED SPY ({tag}) · σ*={SIGMA_STAR:.0%} · cost {COST_PER_TURNOVER*1e4:.0f}bp/turnover · no leverage")
    print("=" * 96)
    mc = (pl.scan_parquet(wf())
          .select("day", "spy_eod_close", "spy_realized_vol_21d", "vix_close")
          .sort("day").collect()
          .with_columns((pl.col("spy_eod_close") / pl.col("spy_eod_close").shift(1) - 1).alias("ret")))
    d = mc.drop_nulls(["ret"])
    day = d["day"].to_numpy().astype("datetime64[D]")
    ret = d["ret"].to_numpy()
    rv21 = d["spy_realized_vol_21d"].to_numpy()
    vix = d["vix_close"].to_numpy() / 100.0

    # EWMA10 vol from returns (through t): annualized
    lam = 0.5 ** (1 / 10)
    ew = np.empty(len(ret)); v = ret[:20].var()
    for i, r in enumerate(ret):
        v = lam * v + (1 - lam) * r * r
        ew[i] = np.sqrt(v * ANN)

    sigs = {
        "RV21": rv21,
        "VIX": vix,
        "EWMA10": ew,
        "BLEND": np.sqrt(np.clip(rv21, 1e-8, None) * np.clip(vix, 1e-8, None)),
    }
    in_win = (day >= np.datetime64(EXP_START)) & (day <= np.datetime64(EXP_END))
    yr = day.astype("datetime64[Y]").astype(int) + 1970
    years = sorted(set(yr[in_win].tolist()))
    rng = np.random.default_rng(SEED)

    rb = ret[in_win]
    sh_b, dd_b, line_b, py_b = perf(rb, "SPY buy-hold", years, yr[in_win])
    print("\n" + line_b)
    print(f"    per-year Sharpe: { {y: round(s,2) for y,s in py_b.items()} }\n")

    results = []
    for sname, sig in sigs.items():
        sig_lag = np.concatenate([[np.nan], sig[:-1]])  # info through t-1
        for rule in ["INV-VOL", "INV-VAR", "BINARY"]:
            if rule == "INV-VOL":
                w = np.minimum(1.0, SIGMA_STAR / sig_lag)
            elif rule == "INV-VAR":
                w = np.minimum(1.0, (SIGMA_STAR / sig_lag) ** 2)
            else:
                thr = np.full(len(sig_lag), np.nan)
                for i in range(len(sig_lag)):
                    hist = sig_lag[max(0, i - 1260):i]
                    hist = hist[~np.isnan(hist)]
                    if len(hist) >= 252:
                        thr[i] = np.quantile(hist, 0.80)
                w = np.where(sig_lag > thr, 0.0, 1.0)
                w[np.isnan(thr)] = 1.0
            w = np.nan_to_num(w, nan=1.0)
            turn = np.abs(np.diff(np.concatenate([[w[0]], w])))
            rs_all = w * ret - turn * COST_PER_TURNOVER
            rs = rs_all[in_win]
            sh, dd, line, py = perf(rs, f"{sname} {rule}", years, yr[in_win])
            lo, hi = sharpe_diff_ci(rs, rb, rng)
            n_better = sum(1 for y in years if py.get(y, np.nan) > py_b.get(y, np.nan))
            dd_red = (dd_b - dd) / abs(dd_b)
            avg_w = w[in_win].mean()
            t_yr = turn[in_win].sum() / (in_win.sum() / ANN)
            c1 = (sh - sh_b) >= 0.15
            c2 = lo > 0
            c3 = n_better >= 4
            c4 = dd_red >= 0.20
            verdict = "PASS" if (c1 and c2 and c3 and c4) else "null"
            print(line + f"  ΔSharpe {sh-sh_b:+.2f} CI[{lo:+.2f},{hi:+.2f}]  yrs {n_better}/{len(years)}  "
                  f"DD↓{dd_red*100:+4.0f}%  ⟨w⟩{avg_w:.2f}  turn {t_yr:.0f}x/yr  -> {verdict}")
            results.append(dict(signal=sname, rule=rule, sharpe=sh, d_sharpe=sh - sh_b,
                                ci_lo=lo, ci_hi=hi, yrs_better=n_better, dd_red=dd_red,
                                verdict=verdict))

    n_pass = sum(1 for r in results if r["verdict"] == "PASS")
    print(f"\n--- pre-registered verdict: {n_pass}/{len(results)} configurations PASS ---")
    print("PASS requires ALL of: ΔSharpe>=+0.15 · bootstrap CI(ΔSharpe)>0 · >=4/5 years better ·")
    print("maxDD reduced >=20%. Multiple-testing note: 12 configs — a single marginal PASS is")
    print("noise; a PASS cluster across signals/rules is structure. Validation 2021-22 only if")
    print("a cluster passes (and it contains an unseen bear market).")
    print("\nCaveats: ~4.5yr train, two vol events; de-risking penalized on raw return in a bull")
    print("window by construction; literature effect is Sharpe/DD, not raw return; cash yield 0.")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
