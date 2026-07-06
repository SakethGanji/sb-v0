# MASTER FINDINGS — Price/Volume Alpha Research (consolidated)

**Updated:** 2026-07-06 (evening: +Phase 4) · Consolidates Phase 0 → Phase 4. This is the
single-page record of what the whole project discovered, with exact numbers. Per-step detail in
the `phase*-findings.md` docs and `PHASE1_CHARACTERIZATION.md`; scripts in `scripts/phase*_*.py`.

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

### Phase 4 — Branch A executed at $0 + the conditional-subset door → BOTH NULL
| Test | Result |
|---|---|
| Power gate (new discipline: run BEFORE the test) | phase-1 daily-decile design MDE95 **38–44bp** (can't adjudicate the long leg); pooled calendar-time top-quintile design MDE95 **~23bp**; verdict rule pre-registered |
| EDGAR pipeline ($0) | FIGI→CIK via `display_symbol_on_day` **98.6%** match · 431k as-filed EPS facts · 59.9k PIT SUE (same-filing seasonal diff = split-immune) · 97k 8-K item-2.02 events |
| **CLOCK DISCOVERY** | dataset earnings dates are 10-Q/K **FILING** dates (filed−event ≈ 0d); press release is 0–14+d earlier → all prior earnings conditioning was on a late clock; true clock now in `edgar_8k_events.parquet` |
| True-SUE PEAD, filing clock | top-quintile long leg **−5.1bp gross**, placebo ~0; SUE decile spread **+8.6** vs proxy's +58.9 → the proxy spread was reaction-magnitude sorting |
| True-SUE PEAD, corrected 8-K clock | SUE is REAL at the event (quintile event-window swing **−43 → +72bp**, monotone) but **fully priced within ~1 day**: earliest-safe entry long leg **+5.5 gross / −9.5 net@15** CI[−28.8,+10.2]; all windows null → real-but-already-priced |
| GBM score-tail (top 5/1/0.5/0.1% of joint-probe OOS scores) | nominal net grows to **+33bp @0.1%** but win rate **~50% at every tail** (pure skew: avg win +809 vs loss −702), day-clustered CI **[−107,+189]**, 2018 negative → **vol mirage #5** |
| PRIM subgroup search (pre-registered, 25-perm full-pipeline null) | train boxes 62–65% win / +48–105bp net collapse OOS to 41–58% / −130…+11bp; best valid OOS **−31.6bp net** vs null 95pct **+12.7** → **inside the null**; **zero ≤4-condition box ever qualified in-sample** (real or any of 50 perm runs) |

### Phase 5 — the free queue emptied (vol-targeting, momentum gate, insiders)
| Test | Result |
|---|---|
| Vol-targeted SPY (4 signals × 3 rules, no leverage, 2bp) | **all 12 configs**: Sharpe +0.13…+0.64, maxDD −50…−82% (best −6.4% vs −34.6% through COVID) — but every ΔSharpe CI includes 0 and the payoff is insurance-shaped (wins only 2018/2020) → **real risk overlay, unprovable alpha**; 0/12 formal PASS |
| Long-horizon momentum power gate (12-1/6-1 × 21/42/63d) | **FAIL — unanswerable**: MDE95 56–168bp vs plausible 10–90bp; 252d lookback leaves ~14 independent 63d obs; test not run, no claim either way |
| Insider cluster-buying (Form 345, 96.8k open-market purchases, $0) | gate MARGIN (MDE ~75bp) · registered LONG hypothesis **NULL** · post-hoc surprise: CLUSTER **−81.4bp/21d** CI[−155,−5], and **−85.4 CI[−141,−31] vs decline-matched controls** — insiders bought falling knives and were wrong; **short-gated, suggestive-only** (candidate wall #6) |

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

## 6. Forward path (updated after Phase 4)

- **Branch A — EXECUTED at $0 and NULL** (see Phase 4 table). The "cheapest sharp test" was
  run with a free time-series SUE from EDGAR: the surprise is real at the event and priced
  within ~1 day. Buying consensus data (FMP) is now a long shot with a concrete bar: it must
  find **≥ ~25bp/5d net@15** in the top quintile where time-series SUE found **−9.5**.
  Recommendation: **don't buy** unless that bar is accepted as the explicit bet.
- **Conditional-subset door — CLOSED with evidence** (score-tail + PRIM + permutation null;
  `phase4-conditional-subset-findings.md`). No further price/volume conditional searches.
- **Branch B — options/IV (unchanged, downgraded).** Only worth a slice if a delta-hedged
  straddle shows realized > implied net of spread; the vol signal being ~persistence says no.
- **Baseline: just index.** This is now the standing default, not a fallback. Optional:
  the vol-targeting overlay (Phase 5) if drawdowns matter — risk tool, not alpha.
- **The free queue is EMPTY as of Phase 5** (momentum timeline unanswerable; insider
  long-edge null, short-side suggestive but gated). Remaining moves: fund Branch B
  (~$50–100/mo, one pre-registered month) or write the final report and seal.

---

## 7. Index

**Docs:** `PHASE1_CHARACTERIZATION.md` · `phase1-*-findings.md` (power-mde, tracer, cost-model,
blacklist, validation, ceiling, stability, sweep, dp, factor, metalabel, frontier) ·
`phase2-findings.md` · `phase3-findings.md` · `phase4-findings.md` (true-SUE PEAD + clock) ·
`phase4-conditional-subset-findings.md` (PRIM + score-tail) · `phase1-research-strategy.md`.
**Scripts:** `scripts/phase1_*.py` (18) · `scripts/phase2_*.py` (5) · `scripts/phase3_*.py` (2) ·
`scripts/phase4_*.py` (8). **Outputs:** `data/phase1_analysis/*.parquet` (+ `edgar_*.parquet`).
**Data:** `data/outputs/` (8 tables) · `/mnt/atlas/edgar/` (bulk zips).
