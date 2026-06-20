# Phase 1 — Era-Stability × Timeframe Sweep Findings (deliverable #3)

**Date:** 2026-06-20 · **Script:** `scripts/phase1_stability.py` · **Output:**
`data/phase1_analysis/stability_surface.parquet`. **Status:** FINAL — the
momentum-edge question is exhausted across timeframe AND era.

Deployable cohort (cap∈{mega,large,mid}, liq∈{highly_liquid,liquid,normal}, CS,
signal>0), exploration window only (holdout untouched). 5.9M trades. Swept entry
offset (10:00→15:30, signal known) × horizon {60min,EOD,1d,2d,5d,21d} × {each
year 2016–2020, pooled}. Metric: daily-portfolio cost-adj excess/SPY, one-sided
block-bootstrap LB (block = horizon-days, §7.7.1). Cost = flat 5 bps round-trip.

## Entry timeframe × horizon — nothing
- **0 of 72** (offset×horizon) pooled combos have a positive lower bound — not net,
  not gross (pre-cost), not even uncorrected. BY survivors: 0 net, 0 gross.
- Net surface uniformly −3 to −7 bps (cost-dominated). Best gross *anywhere* =
  +1.7 bps, p_gross_pos=0.37. No edge hides at a different entry time or hold.

## Era stability — the meme-era cautionary tale, made real
- **2020 (COVID/meme) shows elevated gross momentum**: +5 to +8 bps/day at 1d
  across offsets, while 2018–2019 are NEGATIVE (−2 to −3.5) and pooled ≈ 0. Exactly
  the §2.5 "positive in 1 era, negative in others, averages to a fake null" pattern
  that pooling would mask.
- **But 2020 doesn't clear the bar.** Best 2020 combo (offset 1100, 1d): +8.2 bps
  gross, LB −0.97, **p_gross=0.07** (insignificant even gross/one-sided, and it's
  the best of 72 → selection-inflated). Net +3.2 bps, p=0.28. **0/72 2020 combos
  significant gross OR net.** Non-persistent (2018/19 negative).
- → 2020 is the §1.3.20 "meme-era inflation": a real elevated point estimate,
  statistically indistinguishable from zero, net-negative, non-persistent. A
  characterization (momentum is regime-contingent), NOT an opportunity. The
  methodology caught exactly what it was built to catch.

## Blacklist persistence
The structural blacklist (cost/liquidity/exitability) is era-independent by
construction — liquidity buckets and spread proxies are structural properties, not
signal-conditional. It holds across all years.

## Conclusion
No exploitable momentum edge at **any entry time, any horizon, or any era** the
data can resolve. The one elevated era (2020) is the meme-era artifact, not a
hidden strategy. Combined with #1 (blacklist) + #2 (ceiling) + the MDE null, the
Phase-1 momentum-edge question is exhausted on the frozen signal. Remaining
untested (separate signal-definition families, each multiplying multiplicity):
other signal definitions, the §1.3.21–23 exploratory tiers (swing, short-side
patterns, archetype clustering).

## Reproduce
```
scripts/phase1_stability.py
```
