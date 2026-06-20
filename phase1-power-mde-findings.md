# Phase 1 — Power / MDE Pass Findings (§7.8 step −1)

**Date:** 2026-06-14 · regime rows filled **2026-06-20** · **Script:**
`scripts/power_mde_pass.py` (read-only on `data/outputs/`, writes
`data/phase1_analysis/`). **Status: FINAL.** B6 landed the full 2016–2026
regime sweep (regime_definitions now covers all 2,513 days × 5 taxonomies),
so the regime-conditioned rows are now valid and computed below. Both the
classification-only and the `×regime` rows are final.

This is the deliverable of `phase1-research-strategy.md` §7.8 **step −1** and a
direct confirmation of the §1.5.1 power reality, computed from the real
`security_classification_daily` distribution over the exploration window —
**before** any discovery run.

---

## Setup (all frozen per §1.6)

| Knob | Value |
|---|---|
| Exploration window | 2016-06-08 → 2020-12-31 · **1,129 trading days · ~4.48 yr** |
| Universe | `ticker_type = CS`, fully-classified rows only |
| Test | one-sided (headline rule = lower bound > 0, §7.8 step 9) |
| Inference | day-clustered ⇒ `t ≈ Sharpe_annual × √years` (§1.5.1) |
| FDR | Benjamini-Yekutieli, α = 0.10 (discovery) |
| MDE | `Sharpe_min = z*(m) / √(N/252)`; `z*` from the BY band `α/(m·H_m)` (strict) … `α/H_m` (loose) |
| Hurdle | Sharpe **0.5** (§2.8) |

`N` = distinct days a cell is active (≥5 names). This is an **upper bound** on
effective N — the long-only signal filter only removes days, so true power is
≤ what's shown (a 0.5× haircut column is in the parquet).

---

## Headline result

> **A true Sharpe-0.5 cell gives one-sided `t ≈ 1.06` (p ≈ 0.145) — it cannot
> clear even an *uncorrected* α = 0.05 over this window**, before any
> multiplicity penalty. This reproduces §1.5.1 exactly.

### Family-granularity ladder (FULL — classification × regime)

`med N` = median active days per cell. MDE = minimum annualized Sharpe a cell
must carry to clear the BY band; **strict** = rank-1 (`α/(m·H_m)`), **loose** =
rank-m (`α/H_m`). `%≤0.5` / `%≤1.0` = fraction of cells reachable at that
detectable Sharpe under the strict end. Hurdle = **0.5**.

| Family | m | med N | MDE @strict | MDE @loose | %≤0.5 | %≤1.0 |
|---|---:|---:|---:|---:|---:|---:|
| `cap` | 5 | 1129 | **1.12** | 0.81 | 0% | 0% |
| `cap×vix` | 15 | 373 | 2.36 | 1.54 | 0% | 0% |
| `cap×vix×trend` | 45 | 127 | 4.63 | 2.82 | 0% | 0% |
| `cap×vix×trend×breadth` | 115 | 43 | 8.70 | 5.03 | 0% | 0% |
| `cap×liq` | 16 | 1129 | **1.37** | 0.89 | 0% | 0% |
| `cap×liq×vix` | 46 | 372 | 2.71 | 1.65 | 0% | 0% |
| **`cap×liq×vix×trend`** (PRIMARY) | 128 | 125 | **5.15** | 2.96 | 0% | 0% |
| `cap×liq×vix×trend×breadth` | 312 | 42 | 9.52 | 5.26 | 0% | 0% |
| `cap×liq×price` | 46 | 1104 | **1.57** | 0.96 | 0% | 0% |
| `cap×liq×price×vix` | 119 | 371 | 2.97 | 1.72 | 0% | 0% |
| `cap×liq×price×vix×trend` | 335 | 125 | 5.55 | 3.06 | 0% | 0% |
| `cap×liq×price×vix×trend×breadth` | 789 | 42 | 10.14 | 5.40 | 0% | 0% |
| `cap×liq×price×vol` | 121 | 448 | **2.71** | 1.56 | 0% | 0% |
| `cap×liq×price×vol×vix` | 279 | 276 | 3.68 | 2.05 | 0% | 0% |
| `cap×liq×price×vol×vix×trend` | 695 | 88 | 6.95 | 3.72 | 0% | 0% |
| `cap×liq×price×vol×vix×trend×breadth` | 1330 | 39 | 10.86 | 5.67 | 0% | 0% |

**The regime axis is now quantified, and it is brutal.** Adding `vix` to any
classification base roughly *doubles* the MDE (e.g. `cap` 1.12 → `cap×vix` 2.36;
`cap×liq` 1.37 → `cap×liq×vix` 2.71); adding `vix×trend` roughly *quadruples* it
(`cap×liq` 1.37 → PRIMARY `cap×liq×vix×trend` **5.15**). The mechanism is exactly
§1.5.1's: each regime axis partitions a cell's days, so `med N` collapses
(1129 → 372 → 125 → 42) while `m` grows (16 → 46 → 128 → 312) — N down and the
BY `z*` up, both pushing MDE the wrong way. **`%≤0.5` is 0% on every single row,
and `%≤1.0` is 0% everywhere too.** Even the most generous (loose-band,
coarsest) reading floors at Sharpe ≈ 0.81.

---

## What this means for Phase 1 (decision-relevant)

1. **Per-cell mean-expectancy discovery at the 0.5 hurdle is not powered on this
   window — at any granularity.** Even the coarsest family (`cap`, m=5) needs a
   true annualized Sharpe of **~1.1 (strict) / ~0.8 (loose)** to be detectable;
   the 0.5 hurdle sits *below the detectable floor everywhere*. Finer slicing
   only makes it worse (more cells → higher `z*`, and finer cells → fewer days).
   `%cells ≤ 0.5` is **0% across the entire ladder.**

2. **This is a confirmation, not a surprise — and it validates the doc's own
   pivot.** §5.0 already orders the deliverables as (1) blacklist, (2)
   predictability-ceiling, (3) stability, with mean-expectancy whitelisting as
   the *final-mile gate*, not the success metric. The power math says: lean on
   the high-power instruments —
   - **blacklist / negative space (§2.2)** — provable from the cost side, high power by construction;
   - **hierarchical-Bayes pooling + surface-smoothness (§1.5.1, §2.4)** — share strength across neighbors instead of testing islands;
   - **second-moment & structure claims** (hit-rate structure, dispersion, MI, cross-cell rank stability) — where this dataset *does* have power.
   A per-cell BY whitelist of Sharpe-0.5 edges is the one thing the window
   cannot deliver; don't build the phase around it.

3. **Regime belongs in the day-gate, not the per-cell BY split — now measured.**
   Conditioning a cell on a regime taxonomy *partitions its days*, cutting N
   (and raising MDE) by roughly the number of regime buckets. The full ladder
   prices this exactly: one regime axis (`vix`) ≈ **2× MDE**, two
   (`vix×trend`) ≈ **4× MDE**. Spending that power as another BY axis is
   strictly dominated by spending it as the §2.1 two-stage **day-gate** — a
   single classifier over *all* days, which keeps N intact and adds *zero*
   hypotheses to the discovery family. The decision is no longer directional;
   it is quantified: **never put regime in the per-cell BY family.**

4. **The family that gets pre-registered should be coarse.** Consistent with
   §7.7.1: the primary BY family is `(classification × regime)` kept small, with
   finer slices as hierarchical-FDR drill-down on survivors only. `cap×liq`
   (m=16) is the sweet spot among the valid rows — lowest MDE that still carries
   real classification resolution.

---

## Caveats

- **Regime rows now final (filled 2026-06-20 post-B6).** The full regime sweep
  landed (regime_definitions: 2,513 days × 5 taxonomies), so the `×regime` rows
  are computed from full-window coverage, not the old 144-day smoke artifact.
  The classification-only rows did not move, as predicted.
- N is an upper bound (no signal-fire haircut applied in the headline; 0.5×
  haircut column in the parquet brackets the downside).
- MDE uses the normal approximation for `z*`; with heavy tails the true
  threshold is marginally higher (worse), reinforcing the conclusion.

## Reproduce

```
scripts/power_mde_pass.py        # prints the ladder; writes data/phase1_analysis/
```
