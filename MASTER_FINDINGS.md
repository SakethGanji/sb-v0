# MASTER FINDINGS — Price/Volume Alpha Research (consolidated)

**Updated:** 2026-07-06 · Consolidates Phase 0 → Phase 3. This is the single-page record of
what the whole project discovered, with exact numbers. Per-step detail in the `phase*-findings.md`
docs and `PHASE1_CHARACTERIZATION.md`; scripts in `scripts/phase*_*.py`.

---

## 0. Headline verdict (one paragraph)

Across ~30 experiments on 2016–2020 US equities (price/volume data only), **there is no tradeable
directional edge in liquid US stocks from price/volume alone — pre-entry, post-entry, or across the
full trade lifecycle.** What the data *does* contain is **magnitude** (how much a stock will move),
which is real but **trivial** (~pure volatility persistence, already priced into options). Direction
(up vs down) is a **~50% coin flip at every timeframe**, and the only place it flickers is when you
condition on *why* a stock moves (earnings → PEAD). Every genuine structure we found (a size/low-vol
factor, PEAD, short-term reversal, the vol signal) is **real in gross terms and uncapturable net of
friction / shorting limits / crisis-concentration.** The 2023+ holdout was never touched.

**What we won:** a rigorously characterized null, four documented "real-but-behind-glass" structures,
a validated 657-column engine + full analysis stack, a precise map of where information is and isn't,
and money not lost to an overfit backtest.

---

## 1. What was built

- **Phase 0:** a validated **657-column Rust data engine**, 8 parquet tables (daily_observation,
  forward_outcomes, forward_path_short, market_context_daily, sector_aggregates_daily,
  security_classification_daily, earnings_calendar, regime_definitions), 2016–2026, all checks green.
- **Analysis stack:** cost model + daily cost-adjusted excess + BY-FDR + block-bootstrap + power/
  placebo/synthetic validators + joint ML + meta-labels + barrier construction + walk-forward OOS +
  lifecycle/path analysis. All reusable for a new dataset.

---

## 2. The results ledger (exact numbers)

### Phase 1 — pre-entry directional edge → NULL
| Test | Result |
|---|---|
| Power/MDE | window detects only Sharpe ≈ **1.4–2** after BY-FDR; **0%** of cells reachable at Sharpe 0.5 |
| Structural blacklist | **55 cells** untradeable by cost/exitability alone (durable) |
| Predictability ceiling (univariate MI) | near-zero; best liquid cell **0.27 millibits**; *(later corrected: univariate only)* |
| Momentum sweep | 16 signals × timeframes × 6 horizons × 5 eras × 9 exit pairs × cross-fit DP → **all null** |
| Factor scan | 98 BY + 5/5-era-stable hits (low-vol/size/liquidity) — but **concentrated in illiquid micro/small corner, collapses on deployable** (real-but-uncapturable #1) |
| Validation | L2 recompute exact · placebo caught a real BY bug · synthetic power **92% @ Sharpe 2** |

### Phase 1 frontier (this session)
| Test | Result |
|---|---|
| Joint ML probe (95 feats + market-state) | OOS rank-IC **1d +0.0070** (beats noise null +0.0023, but CI incl 0, ~3bp gross); **5d null**. → corrected the "no ML headroom" claim: joint models find a faint, untradeable trace |
| Meta-label L1 "beat SPY 1d" | AUC **0.5048** (inside null) — direction null |
| Meta-label L2 "+2% before −2%" | AUC **0.6310** (beats null, monotonic, year-stable) — but it's **volatility, not direction** |
| L2 barrier economics | top-decile **47.8% target / 45.4% stop** (ratio 1.05 = coin flip); gross **+4.1bp**, net@20 **−15.9**; flips by year (2018 −31 / 2019 +10 / 2020 +22) |
| Controls | linear AUC **0.6291** ≈ GBM **0.6310** (model-agnostic); importances = vol/dispersion/activity |
| Barrier sweep (8 thresholds) | ATR AUC_move **0.65–0.74** (genuine stock-normalized vol-expansion), fixed-% **0.78–0.85** (partly static "always volatile"); **direction contributes exactly 0.5 at every threshold** → the 0.63 = magnitude ⊕ null-direction blend |
| Momentum reliability | raw win **51.5–57.3%**, excess-over-SPY **~49% every year** (coin flip); symmetric target-before-stop ~50%; favorable-asymmetric (2:1) loses (E −8 to −16bp) |
| Vol-forecast non-triviality | realized-vol IC: static-vol **0.576** → informed **0.604** → full-ML **0.612** (lift **+0.0076**) → vol signal is ~persistence, **would not beat implied vol** → options path not supported |
| **PEAD** (first real directional signal) | post-earnings 5d decile spread **+58.9bp**, CI[8,112], 5/5 eras; placebo −12.4 (null); beats look-ahead-safe momentum control (null). **BUT** long leg **+5.8bp** (net −9), short leg **−53bp** (gated), mid-cap → real-but-uncapturable #2 |

### Phase 2 — post-entry / trade management → NULL
| Test | Result |
|---|---|
| Green/magnitude by timeframe | P(beat SPY) **~49.5% at 30m/60m/EOD/1d/5d** (flat coin flip); raw green 50→54% (drift only); "stayed green" **25% by 1d, 14% by 5d**; median range **0.71%@30m, 1.66%@EOD, 2.80%@1d, 5.53%@5d**; crosses ±1% ~65% by EOD, ~90% by 1d |
| Config search (408 configs, honest OOS) | **not hardcoded**: naive best-OOS +13bp was **−3.8 train** (overfit mirage); best-train→OOS **+0.8bp**; top-5 avg **−4.8bp**; rank-corr(train,oos) +0.44 |
| Trade-health 2C (state@30m → remaining) | OOS rank-IC **+0.0103** (beats noise, trivial); descriptive ~50%/−1bp every state cut |
| Simulate mgmt 2D | managed vs hold: **wash at 0 cost, negative after**; win% up (48→51%) but truncates winners=losers (the win-rate trap); PF < 1 |

### Phase 2.5 / 3 — lifecycle, reversal, Bayesian
| Test | Result |
|---|---|
| Momentum-length breakdown | the persistent structure is **REVERSAL** not momentum: recent winners underperform losers, all lookbacks × horizons, OOS CIs exclude 0, up to **−80bp/21d**; overnight-gap the only continuation (**+9.9bp 1d**) |
| **Reversal verification** | **MIRAGE**: per-year crisis-driven (2016 +98 / 2017 −4 / 2018 −41 / 2019 +37 / 2020 +105 bp — 2017/18 negative); loser-decile spread **14.7 vs 11.3** universe → net **−15.8bp (5d) / +1.7bp (21d)**; doesn't survive in liquid names → real-but-uncapturable #4 |
| Green survival | holding a proven-green trade: remaining **~0/negative net of cost** at every buffer/checkpoint/split (banked gain, no hold edge) |
| Lifecycle / loser ID | losers finish red but **remaining from any down-state ~0** (60m down-states bounce **+4.2**) → no exit rule adds expectancy; winner MAE **−0.34% / 9% underwater** vs loser **−1.25% / 90%** (hindsight-clean, ex-ante-useless) |
| Bayesian update (P(win\|evidence)) | P(win) climbs **50% → ~85%** with stacked evidence (real!); P(never revisit) → 81%. **But excess-remaining within ±1.5bp of 0, negative net of cost even for max stack** → banked gains, not a hold edge. High-RVOL alone: P(win) **50.5%** (volume = magnitude, not direction) |

---

## 3. The recurring theme: 4 real-but-uncapturable structures

Every genuine structure found is real gross and dies at a wall:
1. **Low-vol / size / liquidity factor** — real, 5/5 eras, but lives in the illiquid micro/small corner (friction wall).
2. **PEAD** (post-earnings drift) — real, clean, directional, but the tradeable long leg is null and the −53bp is the short leg (shorting gated).
3. **Volatility-expansion signal** — real, stock-normalized (ATR AUC 0.65–0.74), but ~persistence already in implied vol (options wall; and no IV data to test).
4. **Short-term reversal** — real gross (−80bp/21d), but crisis-driven and eaten by the loser decile's wider spread (cost wall).

Direction is only ever predictable by conditioning on **why** a stock moves (the earnings event) — which price/volume doesn't contain.

---

## 4. Durable, usable takeaways (not alpha, but real)

- **Magnitude yes, direction no.** You can forecast *how much* and *how often* a stock moves (median 1d range 2.8%, ±1% ~90% of days), never *which way* (excess win ~49% at every horizon).
- **Stops should be WIDE, not tight.** Winners rarely dip past ~0.35% (MAE); a tight stop cuts winners, and the remaining from a down-state is ~0, so it adds no expectancy. Risk control ≠ alpha.
- **"Green" is mostly the market's.** Raw green rate rises with horizon only because the market drifts up; beat-SPY is flat.
- **Optimize expectancy, not win rate.** Every management/exit rule tested raised win-rate and left expectancy flat-to-worse.

---

## 5. Trust / audit

- **Leakage audit** of all ML scripts: 6 clean / 3 minor (embargo<horizon on 5d labels) / 1 leak (PEAD *control*, already fixed). **Leakage inflates signal → every NULL is conservative; no conclusion is a leakage artifact.** `high_52w` verified prior-close-based.
- Permutation nulls / placebos throughout (real ≈ shuffled for the nulls; PEAD placebo ~0).
- Holdout **2023-01 → 2026** never read (no candidate cleared the bar).

---

## 6. Forward path (Phase 3 decision)

The dataset is exhausted for a long-only price/volume stock edge. Two doors, both need NEW data:
- **Branch A — directional data (top lead).** Real earnings-surprise (SUE) + analyst revisions
  (e.g. FMP, ~hundreds/yr). **Cheapest sharp test:** re-run PEAD with a true SUE, target the long
  leg net of cost. Higher prior because the price-proxy PEAD already gives a clean drift.
- **Branch B — options/IV (downgraded).** The vol signal is ~persistence; only worth a ThetaData/ORATS
  slice if a delta-hedged straddle shows realized > implied net of the option spread.
- **Baseline to beat: just index.** The honest default if the cheap directional test is null.

---

## 7. Index

**Docs:** `PHASE1_CHARACTERIZATION.md` · `phase1-*-findings.md` (power-mde, tracer, cost-model,
blacklist, validation, ceiling, stability, sweep, dp, factor, metalabel, frontier) ·
`phase2-findings.md` · `phase3-findings.md` · `phase1-research-strategy.md` (frozen plan).
**Scripts:** `scripts/phase1_*.py` (18) · `scripts/phase2_*.py` (5) · `scripts/phase3_*.py` (2).
**Outputs:** `data/phase1_analysis/*.parquet`. **Data:** `data/outputs/` (8 tables).
