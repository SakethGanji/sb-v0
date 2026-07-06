# Phase 9 findings — the Feature Dynamics Atlas

**Date:** 2026-07-06 · Charter: `phase9-dynamics-atlas.md` (declared before build).
Build: `scripts/phase9_dynamics_atlas.py`, train era 2016-06-08→2020-12-31, train
universe (994,694 stock-days, 1,131 days, 1,267 names), wall 81s. Tables:
`data/phase1_analysis/atlas_l{1..5}_*.parquet`.
**Standing:** characterization. No PASS/FAIL. Direction columns were expected ≈0 and
the graduation rule applies to every cell below (atlas → power gate → pre-registration
before belief; 225 L2 cells + 27 grid cells + 45 VIX cells were examined, so 2-SE
excursions are guaranteed by volume).

## 0. The one-line answer

The variables are not "just numbers": they sort into three species with measured
lifetimes — **identities** (who the stock is), **weather** (states lasting ~a month),
and **flashes** (gone in days) — and essentially all of their information about the
future is about **how much things will move and about themselves**, not about
direction. What direction structure the maps do show decomposes into arithmetic
(beta × bull-window drift) and structure the project already closed (door 6's
low-vol/crisis effect), plus small-N noise.

## 1. Three species of variable (L1 — rank persistence)

| Species | Variables (21d rank AC → half-life) | Meaning |
|---|---|---|
| **Identity** | addv_20d (0.96 → ~345d), beta_spy_60d (0.82 → 75d), vol_frac (0.74 → 49d), dist_52w_high (0.75 → 51d) | "liquid stock," "high-beta stock" are near-permanent facts; a liquidity rank today is still mostly there next quarter |
| **Weather** | realized_vol_21d (0.56 → 25d), days_since_last_5pct_move (0.65 → 34d) — mirrored at market level by VIX (t½ 29d) and SPY RV21 (t½ 21d) | volatility is a *state* with a ~one-month half-life, at both stock and market level — the physical basis of every Phase 1-7 magnitude result |
| **Flash** | intraday/overnight returns (AC1 ≈ 0.00, jump rate ~54%), volume_ratio (AC1 0.49, dead by 5d), vol_trend (AC1 0.85 → AC5 0.17), volm_trend (t½ 3.8d), trailing 5d/21d returns (zero memory beyond their own window) | event markers, not states. Notably: **past return has no rank persistence at all** once its window rolls off — cross-sectional momentum literally is not in this panel at these horizons |

Market internals are flashes too: breadth-at-10:00 has AC1 = −0.07 (t½ 2.9d) and
cross-sectional dispersion AC1 ≈ 0.00 — the market's daily internals are a coin flip
day to day, even while its vol level is a monthly state.

Footnotes: `signal_concentration_hhi_today` was dropped (market-wide constant within a
day; cross-sectional rank undefined). `prior_day_last_30m_return` produced NaN rank
autocorrelations (degenerate/tied ranks) — parked.

## 2. What the levels and trends mean (L2 — 225 cells)

- **Magnitude gradients are the real content, and they are big and monotone.** Top-vs-
  bottom quintile of vol_frac or realized_vol_21d = **+12.7pp of future 21d trading
  range**; beta +7.8pp; recent-mover (days_since_5pct_move low) +10pp; near-52w-high
  stocks are structurally *quieter* (−7.3pp). Every family's strongest content is a
  forecast of movement size.
- **Mean reversion sorts by species:** from the top quintile, flashes give back ~0.39
  of rank within 21d (near-full reversion toward median); identities give back 0.01-0.13.
  "High volume today" is a wave; "high ADV" is the shoreline.
- **Direction cells:** 67/225 exceed 2 SE (vs ~11 expected by chance), so the panel
  does contain systematic direction *structure* — but its pattern is explanatory, not
  exploitable-new: high-vol/high-beta cells run positive excess-vs-SPY (+60…105bp/21d)
  and low-vol cells negative (−48bp) **in a 2016-2020 bull window, which is beta
  arithmetic** (β>1 mechanically beats SPY when SPY rises); the VIX-cut (L5b) flips it
  exactly where the record says it should — in high-VIX regimes the LOW-vol quintile
  is the strong cell (+107…137bp/21d), i.e. door 6's crisis/low-vol effect
  re-derived descriptively. The largest single cell (days_since_5pct_move q4-falling,
  −197bp ± 67) has N = 300 — multiplicity bait, cited here as the exhibit for why the
  graduation rule exists.

## 3. Reversal anatomy (L3) — the question the atlas was built for

**When a stock reverses, its volatility state changes and its expected direction does
not.**

- reversal-up (≥3 red closes then green, N=53,076): trailing 21d −2.6%; from the next
  morning's 10:00: **+1.9bp ± 3.4 (1d), −1.1 ± 5.8 (5d), +5.4 ± 11.2 (21d)** — zero.
  But vol was already elevated and rises further (2.78 → 3.06% of price) and is still
  elevated a month later (2.85%).
- reversal-down (mirror, N=68,694): −2.4bp ± 3.5 (1d) — symmetric zero. Vol texture
  differs: green streaks form in *falling* vol (2.73 → 2.61%), which then reverts up.
- **The clock lesson, live:** the first build attached outcomes at the event day's own
  10:00 and showed reversal-up "+77bp next-day, 16σ." Moving the outcome to the first
  tradeable clock after the condition is knowable collapsed it to +1.9bp ± 3.4. The
  entire apparent reversal edge was the event day measuring itself. (Charter §Clock
  rule; this is Phase 4's filing-clock lesson in intraday form.)
- vol expansion (ATR5/42 crossing 1.5, N=17k): a spike, not a step — vol 2.60 → 3.36 →
  2.86%, volume 1.27 → 1.60 → then **undershoot to 0.94** (activity hangover).
  Direction ≈ 0.
- volume surge (ADV5/60 crossing 2, N=6.6k): same shape, stronger — volume 1.38 → 1.83
  → 0.89. Direction ≈ 0.
- vol collapse (crossing 0.75, N=42k): the one mildly positive event corner
  (+31.9 ± 14.4 bp/21d) — same family as the low-vol/quality tilt already mapped;
  graduation rule applies.
- breadth flips: N = 6 per direction in five years (breadth has no memory, so
  sustained-then-flipped days barely exist); anecdotal only.

## 4. The pre-declared 27-cell grid (L4)

- **Cap dominates trend for future magnitude:** mid ≈ 16%, large ≈ 13%, mega ≈ 11%
  fwd 21d range in every trend cell — who you are beats what you're doing lately.
- Direction: P(up next day) sits in 47-52% everywhere. The one loud cell:
  **large-cap, vol-trend-up × volume-trend-down** (+17.7 ± 4.2 bp/5d, +29.8 ± 8.6
  bp/21d) — "quiet vol expansion" (vol rising without volume). It is the most
  interesting candidate the atlas produced, it is also a delta-one directional
  conditioning claim (Channel E, closed as a class), and the Phase 4 record predicts
  exactly this kind of searched-adjacent cell dies against a full-pipeline permutation
  null. If it is ever taken seriously, the path is the pipeline, not a backtest.

## 5. What the atlas adds to the record

1. A measured taxonomy (identity / weather / flash) with half-lives for every curated
   variable — the "do these numbers mean anything" question now has a quantitative
   answer per variable.
2. The magnitude story gets its kinematics: vol is a ~25-50d state at stock level,
   ~21-29d at market level; expansions are spikes with volume hangovers; reversals are
   vol events, not direction events.
3. The apparent direction structure in descriptive cuts is accounted for by beta
   arithmetic + the known crisis/low-vol structure — nothing here reopens Phases 1-6B.
4. Two parked items: prior_day_last_30m_return rank degeneracy (investigate ties), and
   the quiet-vol-expansion cell (graduation-rule candidate, prior: permutation-null).

---

## Phase 9b — graduation test of the quiet-vol-expansion cell → **FAIL (atlas-noise, closed)**

Pre-registration `phase9b-preregistration.md` (frozen; post-hoc candidate declared as
such, selection-adjusted design). Script: `scripts/phase9b_quietvol_test.py`, 1,000
within-(day, cap) permutations of the (vol-trend, volume-trend) pair, family-max |t|
over 27 cells × 2 horizons, seed 20260708.

- Observed cell (large, volT_up, volmT_dn): +17.7bp/5d (t = 4.18), +29.8bp/21d
  (t = 3.45) — the numbers that looked loud in the atlas.
- **Null family-max |t|: median 2.54, 95th percentile 3.58.** The observed 3.45 does
  not clear the bar: **selection-adjusted p = 0.073**. Being the loudest of 54
  statistics, a t of 3.45 is unremarkable.
- **Era stability: FAIL** — 21d cell means by year: −22.6 / +13.9 / −27.5 / +35.1 /
  +49.3 bp (2016→2020). The "effect" is two good late years, not a structure.
- Net economics context (+14.8bp/21d after 15bp RT) is moot given the above.

**REGISTERED VERDICT: FAIL.** The registered prior ("dies like every searched cell,
per the Phase 4 precedent") is confirmed. The cell is recorded as atlas-noise; no
variants, no neighboring cells. This is now the third time a "found" conditional cell
died against a selection-honest null in this project — the atlas's graduation rule
works as designed.
