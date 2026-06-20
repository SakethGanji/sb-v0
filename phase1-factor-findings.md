# Phase 1 — Systematic Cross-Sectional Factor Scan Findings

**Date:** 2026-06-20 · **Scripts:** `scripts/phase1_factor_scan.py` (broad scan),
`scripts/phase1_factor_verify.py` (clean-universe + block-bootstrap verification).
**Outputs:** `data/phase1_analysis/factor_scan.parquet`. **Status:** FINAL.

The systematic answer to "is there ANY consistent structure" — every pre-entry
feature × horizon, cross-sectional decile spread (top−bottom forward excess/SPY),
era-stability + BY FDR. Signal-agnostic, full universe, 9.4M trades, 258 hypotheses.

## Result: consistent structure EXISTS — in the untradeable corner
- **Broad scan:** 130/180 BY-significant, **98 also same-sign in all 5 eras.** The
  coherent family: **low volatility (atr/realized-vol), smaller size, lower liquidity
  (addv), longer-since-big-move, and more pullback-from-high → higher forward excess**;
  higher morning volume → higher. The classic **low-vol / size / liquidity anomalies**,
  era-stable. (Big magnitudes, e.g. atr 21d −10%, partly overlap-inflated.)
- **Verification (block bootstrap + clean universe):**
  - Top-ATR decile is **0% ETF / 36% micro+small cap** — the structure is concentrated
    in small/micro, high-vol, illiquid names.
  - On the FULL universe it survives block-bootstrap CIs (real, not overlap artifact).
  - On the **DEPLOYABLE universe** (CS, mega/large/mid, liquid/normal) it **COLLAPSES**:
    atr_14d 21d −8.5b CI[−94,+72] 3/5; realized-vol −81b CI incl. 0; addv +43b n.s.;
    overnight_gap +1.8b 3/5. Not significant, not era-consistent → noise.
  - Closest near-survivor: `pre_entry_ret_from_high` 1d is 5/5 same-sign on deployable
    (pulled-back names underperform next day) but **not significant** (CI includes 0).

## Interpretation (answers "find consistency somewhere")
**Yes, the dataset has real, era-stable cross-sectional factor structure — but it lives
in the small/micro-cap, high-vol, illiquid corner that the blacklist already flags as
untradeable, and it is a long-short factor (shorting the high-vol leg is gated, §1.6).
In the deployable universe the cross-section is efficient — no era-stable structure.**
This mirrors the blacklist exactly: structure concentrates where friction prevents
capture. The data is meaningful (the known anomalies show up cleanly), but the
tradeable slice gives the structure up.

This neither contradicts nor rescues the cycle-1 null: there is no *tradeable* edge,
and now we also know *why the structure that exists is uncapturable* — it's the
small-cap/low-vol factor in the friction corner, not novel alpha in liquid names.

## Caveats
- Cross-sectional decile spreads are long-short factor exposures, not long-only
  strategies (§1.6 execution is long-only; the high-vol short leg is gated).
- Magnitudes on the full universe are partly inflated by multi-day overlap; the
  *deployable-collapse* conclusion uses block-bootstrap CIs and is robust.
- Conditional on recorded features; the deployable-universe efficiency is "no
  structure we can see," not a proof of none.

## Reproduce
```
scripts/phase1_factor_scan.py · scripts/phase1_factor_verify.py
```
