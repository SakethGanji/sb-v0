# Phase 8 pre-registration — short-interest positioning (new information source)

**Frozen:** 2026-07-06, before any signal-vs-outcome computation. Scripts:
`scripts/phase8_si_gate.py` (power gate, outcomes-only, specified in Decision rules
below) is written AFTER this freeze and runs first; the test script runs only on a
gate PASS.
**Standing:** opened under `FINAL_REPORT.md` §8's reopening condition — FINRA bi-weekly
short interest is **positioning data**, genuinely new bits, not a re-representation of
price/volume. This does not reopen any Phase 0–7 door and does not alter the Phase 7
program or its exhaustion statement (which is about magnitude conversion). It follows
the EDGAR template: bulk (already local) → PIT construction → power gate → one
pre-registered test.

**Why this exists:** `phase1-research-strategy.md` catalogued `short_interest.parquet`
and question X.1 ("does high short interest / days-to-cover predict downside vs
squeeze?") and it was never run; the "free queue is EMPTY" line in
`MASTER_FINDINGS.md` §6 overlooked it. This registration closes that gap.

## What was examined before this freeze (marginals only — no signal-outcome contact)

Row counts, settlement-date cadence (202 dates, median 15d, 2017-12-29 → 2026-05-15),
null map (2.82M unmapped `security_id` rows — OTC/funds; mapped rows ≈ 0.9M),
deployable-universe coverage on one sample date (840/896), and the vendor
`days_to_cover` distribution (capped at 999.99 → vendor field rejected). No forward
return was joined to any signal value before this document was frozen.

## Data and point-in-time clock

- Source: `data/reference/short_interest.parquet` (security_id, settlement_date,
  short_interest, avg_daily_volume, days_to_cover). Vendor `days_to_cover` and
  `avg_daily_volume` are NOT used (cap artifacts; unknown window) — recomputed from our
  own tables.
- **Availability clock (frozen):** FINRA disseminates roughly 7 business days after
  settlement. Registered rule: a settlement-date record becomes usable at the first
  trading day **T ≥ settlement_date + 11 calendar days**, at the 10:00 entry
  (`entry_offset = "1000"`). Deliberately conservative — errors are toward staleness,
  never lookahead (the Phase 4 filing-clock lesson).
- Universe at day T: `ticker_type = CS`, market_cap ∈ {mega, large, mid}, liquidity ∈
  {highly_liquid, liquid, normal} (the Phase 1 train universe). The deployable corner
  (mega/large × highly_liquid/liquid) is additionally reported as a subset.
- Splits: train era = settlements **2017-12-29 → 2020-11-15** (~71 dates; the cap
  ensures day T and the full 21d outcome window complete inside 2020 — later 2020
  settlements would leak outcomes into the sealed validation era). Validation 2021-22
  touched only on a registered PASS (§7.7 protocol); 2023+ holdout sealed.

## Signal family (frozen — exactly three, no additions)

- **S1 — DTC:** short_interest / adv_20d (our `daily_observation.adv_20d` at day T).
- **S2 — SIR:** short_interest / shares_outstanding, shares ≈ market_cap /
  prior_day_eod_close at day T. *Caveat recorded: true float is unavailable; this is
  the shares-outstanding proxy flagged since Phase 1.*
- **S3 — ΔSI:** log(short_interest_t / short_interest_{t−1}) per name (consecutive
  settlements only; first date dropped).

Horizons: forward **5d** and **21d** excess-vs-SPY at the 10:00 clock
(`ret_5d_excess_spy`, `ret_21d_excess_spy` from `forward_outcomes` at day T).
**Family = 3 signals × 2 horizons = 6 tests. BY-FDR q = 0.10 within the family.**

## Estimands and inference (frozen)

- Primary per test: mean per-settlement-date Spearman rank-IC (signal vs forward
  excess). CI: moving-block bootstrap over settlement dates, block = 2 (covers the
  21d-horizon overlap of 15d-spaced dates), B = 4000, seed 20260707.
- Economic read: per-date decile spread (top − bottom), gross and net at the frozen
  cost model (15bp RT liquid-cell bound). Long leg and short leg reported separately;
  **the short leg is pre-labeled GATED** (borrow/margin wall, standing rule — a real
  short-side effect is map-knowledge, not deployable alpha).
- Era stability: IC sign agreement across 2018 / 2019 / 2020.
- Placebo: signal permuted across names within settlement date, 200 draws, full
  pipeline (the Phase 4 lesson) — observed |mean IC| must exceed the permutation 95th
  percentile.

## Registered priors (written first)

- **S1/S2 level effects: null at retail-visible size.** The classic SIR anomaly is a
  pre-2010 result that decayed with securities-lending efficiency; 2018-2020 includes
  the meme-era squeeze regime that actively punishes naive short-the-high-SI.
- **Short side (high SI underperforms): weakly negative if anything, and gated** —
  expected to rhyme with PEAD/insider short legs (real, uncapturable).
- **S3 (ΔSI): null.**
- Global prior: this dataset has produced 19 closed doors; the honest expectation is a
  20th. The value either way is that positioning data stops being the untested row in
  the inventory.

## Decision rules (frozen)

1. **Power gate first** (`scripts/phase8_si_gate.py`): MDE95 (80% power) for mean
   rank-IC and for the decile spread, computed from outcome dispersion, cross-section
   sizes, and date count ONLY — random signals, never the real ones. Plausible-effect
   bars, frozen: |IC| ≥ 0.03; spread ≥ 20bp/21d and ≥ 10bp/5d gross. MDE95 above the
   bar on a test ⇒ that test is UNANSWERABLE and is not run. All six unanswerable ⇒
   Phase 8 ends with the gate report, verdict UNANSWERABLE-at-this-window.
2. On gate PASS: one registered run over the train era. PASS per test requires ALL of:
   BY-FDR-surviving bootstrap CI excluding 0, era sign-stability 3/3, and clearing the
   permutation null. Economic materiality is then judged net-of-costs, long leg only.
3. Any PASS → 2021-22 confirmation per `phase1-research-strategy.md` §7.7 before
   belief. No re-tuning of universe, clock, family, horizons, or thresholds after
   results; any variant is a new registration.
4. Deliverable: `phase8-findings.md` + one line in `MASTER_FINDINGS.md` §6.

## Anti-goals (frozen)

- No conditional-subset or interaction searches (Phase 4 closure inherited). No
  regime/state conditioning (Phase 6). No additional signals, horizons, or universes.
- No expectancy claim from the short leg regardless of outcome (wall-class).
- Data-contract substitutions at run time are recorded verbatim in the findings doc;
  no other silent deviation.
