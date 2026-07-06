#!/usr/bin/env -S uv run --quiet --with polars --with pyarrow --with numpy python3
"""
Phase 6 — Step 3: EVENT × STATE interaction cells (the only alpha-shaped part).

Per phase6-preregistration.md: 4 knowable events × 6 frozen states × 2 horizons.
Events: SUE top-quintile / SUE bottom-quintile (8-K clock) · insider cluster-buy flag day
(2nd distinct O/D buyer in trailing 30d) · any 8-K item-2.02. Entry next trading day 10:00;
state = frozen state on entry day (prior-close data → knowable).

Per cell: daily calendar-time series of ret_{5d,21d}_excess_spy, block bootstrap
(block=horizon) mean/CI + two-sided bootstrap p. POWER GATE: cells with MDE95 > 35bp(5d) /
60bp(21d) are UNANSWERABLE (excluded from verdicts). BY-FDR at alpha=0.10 across all
answerable cells. Survivors get era-sign and placebo checks. Negative-drift survivors are
pre-labeled GATED (wall #2). Train 2016-06..2020-12 only.

Run: scripts/phase6_event_state.py
"""
from __future__ import annotations
import glob, datetime as dt
from pathlib import Path
import numpy as np
import polars as pl

EXP_START, EXP_END = dt.date(2016, 6, 8), dt.date(2020, 12, 31)
OFFSET = "1000"
CAPS, LIQS = ["mega", "large", "mid"], ["highly_liquid", "liquid", "normal"]
SEED = 20260706
N_BOOT = 4000
ALPHA = 0.10
MDE_LIM = {"5d": 35.0, "21d": 60.0}
K = 6


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


def boot(x, rng, block, B=N_BOOT):
    n = len(x)
    if n < block + 5:
        return None
    nb = int(np.ceil(n / block))
    idx = (rng.integers(0, n, size=(B, nb))[:, :, None] + np.arange(block)[None, None, :]) % n
    bm = x[idx.reshape(B, -1)[:, :n]].mean(axis=1)
    lo, hi = np.quantile(bm, [0.025, 0.975])
    p = 2 * min((bm < 0).mean(), (bm > 0).mean())
    return dict(mean=float(x.mean()), lo=float(lo), hi=float(hi),
                se=float(bm.std(ddof=1)), p=float(max(p, 1 / N_BOOT)))


def main():
    t0 = dt.datetime.now()
    print("=" * 96)
    print("Phase 6 — EVENT × STATE cells (gate → BY-FDR → era/placebo)")
    print("=" * 96)
    cls = (pl.scan_parquet(wf("security_classification_daily"))
           .filter((pl.col("ticker_type") == "CS") & pl.col("market_cap_bucket").is_in(CAPS)
                   & pl.col("liquidity_bucket").is_in(LIQS)).select("day", "security_id"))
    fo = (pl.scan_parquet(wf("forward_outcomes")).filter(pl.col("entry_offset") == OFFSET)
          .select("day", "security_id", "ret_5d_excess_spy", "ret_21d_excess_spy"))
    panel = (fo.join(cls, on=["day", "security_id"], how="inner").sort(["security_id", "day"])
             .collect())
    states = pl.read_parquet("data/phase1_analysis/phase6_states.parquet")
    panel = panel.join(states, on=["security_id", "day"], how="inner")
    sidmap = pl.read_parquet("data/phase1_analysis/edgar_sid_cik_map.parquet")
    tdays = panel.select("security_id", "day").unique().sort("day")

    def to_entry(ev: pl.DataFrame) -> pl.DataFrame:
        """event (security_id, ev_date) -> first trading day STRICTLY AFTER ev_date."""
        ev = ev.with_columns((pl.col("ev_date") + dt.timedelta(days=1)).alias("_after")).sort("_after")
        return (ev.join_asof(tdays.rename({"day": "entry"}), left_on="_after", right_on="entry",
                             by="security_id", strategy="forward")
                .drop_nulls("entry").select("security_id", "entry"))

    # events
    ev8k = (pl.read_parquet("data/phase1_analysis/edgar_8k_events.parquet")
            .join(sidmap, on="cik", how="inner")
            .filter(pl.col("event_date").is_between(EXP_START, EXP_END))
            .select("security_id", "cik", pl.col("event_date").alias("ev_date")))
    sue = pl.read_parquet("data/phase1_analysis/edgar_sue.parquet")
    evs = (ev8k.join(sue.select("cik", "period_end", "sue"), on="cik", how="left")
           .filter(((pl.col("ev_date") - pl.col("period_end")).dt.total_days()).is_between(0, 120))
           .sort("period_end", descending=True)
           .unique(subset=["security_id", "ev_date"], keep="first"))
    q20, q80 = evs["sue"].quantile(0.2), evs["sue"].quantile(0.8)
    buys = (pl.read_parquet("data/phase1_analysis/form4_purchases.parquet")
            .filter(pl.col("is_od"))
            .join(sidmap.rename({"cik": "issuer_cik"}), on="issuer_cik", how="inner")
            .select("security_id", "filing_date", "owner_cik").sort("filing_date"))
    # cluster flag day: 2nd distinct buyer within trailing 30d (first day threshold crossed)
    b = buys.join(buys.rename({"filing_date": "fd2", "owner_cik": "oc2"}),
                  on="security_id", how="inner")
    b = (b.filter(((pl.col("filing_date") - pl.col("fd2")).dt.total_days()).is_between(0, 30)
                  & (pl.col("owner_cik") != pl.col("oc2")))
         .group_by("security_id", "filing_date").len()
         .select("security_id", pl.col("filing_date").alias("ev_date")).unique())

    EVENTS = {
        "SUE-top": to_entry(evs.filter(pl.col("sue") >= q80).select("security_id", "ev_date")),
        "SUE-bot": to_entry(evs.filter(pl.col("sue") <= q20).select("security_id", "ev_date")),
        "INSIDER-cluster": to_entry(b),
        "8K-any": to_entry(ev8k.select("security_id", "ev_date")),
    }
    rng = np.random.default_rng(SEED)
    recs = []
    for enm, ev in EVENTS.items():
        j = ev.rename({"entry": "day"}).join(panel, on=["security_id", "day"], how="inner")
        print(f"\n### {enm}: {j.height:,} event-entries (state dist: "
              + " ".join(f"S{r[0]}:{r[1]}" for r in j.group_by('state').len().sort('state').iter_rows()) + ")")
        for hz, hd in [("5d", 5), ("21d", 21)]:
            col = f"ret_{hz}_excess_spy"
            for s in range(K):
                cell = j.filter((pl.col("state") == s) & pl.col(col).is_not_null())
                ds = (cell.group_by("day").agg(pl.col(col).mean().alias("m")).sort("day"))
                x = ds["m"].to_numpy()
                r = boot(x, rng, hd)
                if r is None:
                    continue
                mde = 1.96 * r["se"] * 1e4
                answerable = mde <= MDE_LIM[hz]
                yrs = ds["day"].to_numpy().astype("datetime64[Y]").astype(int) + 1970
                signs = [np.sign(x[yrs == y].mean()) for y in range(2016, 2021) if (yrs == y).sum() >= 5]
                pos = sum(1 for v in signs if v > 0)
                recs.append(dict(event=enm, state=s, hz=hz, n_ev=cell.height, nd=len(x),
                                 mean_bp=r["mean"] * 1e4, lo=r["lo"] * 1e4, hi=r["hi"] * 1e4,
                                 p=r["p"], mde=mde, answerable=answerable,
                                 eras=f"{pos}/{len(signs)}"))

    df = pl.DataFrame(recs)
    ans = df.filter(pl.col("answerable")).sort("p")
    print(f"\ncells: {df.height} total · answerable (power gate): {ans.height} · "
          f"unanswerable: {df.height - ans.height}")
    # BY-FDR at alpha over answerable cells
    m = ans.height
    hm = sum(1 / i for i in range(1, m + 1))
    pv = ans["p"].to_numpy()
    thr = np.array([ALPHA * (i + 1) / (m * hm) for i in range(m)])
    passed = pv <= thr
    kmax = np.max(np.where(passed)[0]) + 1 if passed.any() else 0
    print(f"BY-FDR alpha={ALPHA}: {kmax} cells survive\n")
    print(f"{'event':<16}{'S':>2} {'hz':>4} {'n_ev':>6} {'nd':>5} {'mean':>8} {'CI':>18} "
          f"{'p':>7} {'MDE':>6} {'eras':>5}  status")
    for i, r in enumerate(ans.iter_rows(named=True)):
        surv = i < kmax
        status = ("BY-SURVIVOR" + (" (GATED short)" if r["mean_bp"] < 0 else "")) if surv else ""
        print(f"{r['event']:<16}{r['state']:>2} {r['hz']:>4} {r['n_ev']:>6,} {r['nd']:>5} "
              f"{r['mean_bp']:>+7.1f}b [{r['lo']:>+6.1f},{r['hi']:>+6.1f}] {r['p']:>7.4f} "
              f"{r['mde']:>5.0f}b {r['eras']:>5}  {status}")
    df.write_parquet("data/phase1_analysis/phase6_event_state.parquet")
    print("\nunanswerable cells (reported, no verdict): "
          + ", ".join(f"{r['event']}/S{r['state']}/{r['hz']}"
                      for r in df.filter(~pl.col("answerable")).iter_rows(named=True)) or "none")
    print(f"\nWALL: {(dt.datetime.now()-t0).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
