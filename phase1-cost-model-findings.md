# Phase 1 — Cost Model Findings (§7.8 step 3 / §6.1)

**Date:** 2026-06-20 · **Script:** `scripts/phase1_cost_model.py` · **Output:**
`data/phase1_analysis/cost_model.parquet` (1,411 rows, offset × cell). **Status:**
FROZEN v1 — usable for the blacklist, with two documented caveats below.

Frozen, cost-side-only (never derived from the edge distribution — §6.1). Retail
assumptions per the §6 RESOLVED block (2026-06-20). Window: exploration only.

## Model
- **Cell:** `market_cap_bucket × liquidity_bucket × price_bucket`, CS universe, per
  entry_offset (17 offsets). 1,411 (offset × cell) rows.
- **Spread proxy:** `(entry_1m_high − entry_1m_low)/entry_price` in bps, median +
  p75 per cell.
  - **Top-200-ADV cap** (`addv_rank_today ≤ 200`): `min(raw, 2× stock's midday-median
    1m range)` — the raw high-low reflects the price walk, not the round-trip cost,
    and overstates spread for liquid names (§6.1). Midday range ≈ median of
    `entry_1m_range/price` at offsets {1130,1200,1300} per security.
  - **1-cent tick floor:** `effective = max(capped proxy, 0.01/price·1e4)`. Added
    after v1 showed the proxy reads **0.0 bps for illiquid/thin names** (they barely
    trade → entry minute high==low → fake-free, backwards for the widest-spread
    cells). The floor is frozen on a hard fact (min tick).
- **Market impact:** `c·√(size/ADV)`, c=20. Flat per %-ADV: **0.63 / 2.00 / 4.47 bps**
  at 0.1 / 1 / 5% ADV. Cell-invariant at fixed %-of-ADV sizing.
- **Commission:** 0 (retail).
- **`frac_zero_range` per cell:** share of entry bars with zero range (no trading) —
  an exitability flag (§3.4), independent of the cost number.
- `total_cost_bps_<size>_median = spread_bps_median + impact_bps(size)`.

## What the run showed
- Plumbing clean, 127s, 3,329 securities' midday caps computed.
- **0-bps bug fixed:** illiquid/thin raw median 0.0b (57% zero-range) → final **8.9b**
  (floor binds 67%). Low-price illiquid now priced correctly: micro·illiquid·sub_1
  = **143 bps**, ·1_to_5 = 41 bps, small·illiquid·1_to_5 = 27 bps.
- Liquid/highly-liquid: raw 21.5b → final 19.1b (cap binds 30%). Mega/large
  highly-liquid ~10–17 bps.

## Two documented caveats (consumption rules for downstream)
1. **Liquid cells are a conservative UPPER BOUND.** ~10–17 bps for mega/large
   highly-liquid vs a true ~1–3 bps. §6.1 calibration (SEC MIDAS or a quotes month)
   would tighten the cap multiplier; deferred per the 2026-06-20 decision. Any
   "fails the cost hurdle" verdict on a liquid cell is therefore conservative
   (real edge headroom is larger than the model says) — safe direction for a
   blacklist, pessimistic for a whitelist.
2. **High-price illiquid cells undercount cost** (penny floor ≪ true many-tick
   spread; e.g. micro·illiquid·above_100 = 0.7 bps). **Rule: the blacklist treats
   any cell with `frac_zero_range` above a pre-set threshold (~0.3) as non-exitable
   regardless of computed cost** — the exitability flag, not the cost number, is
   authoritative for these. (Cells are also tiny: n_sec 15–36.)

## Reproduce
```
scripts/phase1_cost_model.py
```
