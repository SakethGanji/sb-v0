# Phase 9b pre-registration — graduation test for the "quiet vol expansion" cell

**Frozen:** 2026-07-06, before the permutation test ran. **POST-HOC candidate,
declared as such:** the cell (large-cap × vol-trend-top-tercile ×
volume-trend-bottom-tercile) showed +17.7 ± 4.2 bp/5d and +29.8 ± 8.6 bp/21d excess-SPY
in the Phase 9 atlas L4 grid — i.e. it was SELECTED as the loudest of 27 pre-declared
cells. A naive re-test of the same cell on the same data would be circular. This
registration therefore tests it against a **selection-adjusted null**.

**Cell claim:** delta-one directional conditioning = Channel E, closed as a class
(`phase7-magnitude-program.md` §2/§3). Graduation would therefore require
extraordinary evidence; the registered prior is that it **dies against the
family-max permutation null**, like every searched cell in Phase 4.

## Design (frozen)

- Panel identical to the atlas build: train era 2016-06-08→2020-12-31, train universe,
  vol_trend = atr_5d/atr_42d and volm_trend = adv_5d/adv_60d as per-day rank terciles,
  outcomes = ret_5d/21d_excess_spy anchored at the NEXT day's 10:00 (atlas clock rule).
- **Statistic:** day-clustered t = (mean of per-day cell means) / (std across days /
  √D), per (cap × vt × mt) cell × horizon.
- **Selection-adjusted null:** 1,000 permutations; each draw shuffles the
  (vt, mt) label PAIR jointly across stocks **within (day, cap-bucket)** (preserves
  per-day outcome distributions, cap structure, and the vt-mt dependence), recomputes
  all 27 cells × 2 horizons, records **max |t| over the 54-cell family**. Seed 20260708.
- **PASS requires ALL of:** (1) observed cell |t| > 95th percentile of the null
  max-|t| distribution, for the SAME horizon it was selected on (21d primary, 5d
  reported); (2) era sign-stability 5/5 (calendar years 2016–2020) of the cell mean;
  (3) descriptive net economics: cell mean 21d excess must exceed the 15bp RT cost of
  a 21d-rebalance implementation (context, not adjudication).
- Any PASS → this remains map-knowledge only unless it further survives the §7.7
  2021-22 confirmation; and even then Channel E's closure means the finding would be
  reported as an anomaly against the closure theorem's premise, triggering a re-audit
  of the premise measurement before any other action.
- FAIL ⇒ the cell is recorded as atlas-noise; no variants, no neighboring cells, no
  re-tuning (anti-goals inherited verbatim from `phase8-preregistration.md`).
