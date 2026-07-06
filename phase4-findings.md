# Phase 4 findings — Branch A executed free: true-SUE PEAD (and the clock discovery)

**Date:** 2026-07-06 · Scripts: `scripts/phase4_*.py` · Data: `/mnt/atlas/edgar/` (bulk),
`data/phase1_analysis/edgar_*.parquet` · Cost of this branch: **$0** (SEC EDGAR bulk files).

## 0. Headline

**The MASTER_FINDINGS §6 Branch A "cheapest sharp test" has now been run for free, and it is
NULL on both event clocks.** With a real (time-series Bernard–Thomas) SUE built from as-filed
EDGAR EPS, the PEAD long leg in the deployable universe is **+5.5bp gross / −9.5bp net@15 per
5d** entering the first information-safe day after the 8-K announcement. The surprise
information is real and large **measured from the event** (bottom-to-top SUE quintile spans
−43bp to +72bp over the event-window 5d) — it is simply **fully priced within ~1 day** in
liquid mid+ names, 2016–2020. Classic post-2015 PEAD decay, confirmed on our own data with a
true surprise measure. Real-but-uncapturable #2 is upgraded to **real-but-already-priced**.

Along the way we found a dataset-level issue that matters beyond PEAD: **the project's
earnings dates are 10-Q/10-K FILING dates, not announcement dates** (§3).

## 1. What was built (all reusable)

| Artifact | Contents |
|---|---|
| `/mnt/atlas/edgar/companyfacts.zip` (1.39GB) | all XBRL facts, every SEC filer |
| `/mnt/atlas/edgar/submissions.zip` (1.4GB) | full filing index incl. 8-K items |
| `edgar_sid_cik_map.parquet` | FIGI→CIK via `display_symbol_on_day` — **98.6%** of the 1,267 deployable FIGIs matched |
| `edgar_eps_facts.parquet` | 431k as-filed EPS facts (all vintages kept: accn + filed) |
| `edgar_sue.parquet` | 59.9k point-in-time SUE observations (~4k/yr) |
| `edgar_8k_events.parquet` | 97.2k 8-K item-2.02 announcement dates (~4/company-yr) |

SUE construction (split/restatement-immune by design): **same-filing seasonal difference** —
current quarter EPS minus the year-ago comparative *from the same accn*, so both sit on one
share basis; Q4 synthesized as annual-minus-3Q when not stated; PIT = earliest-filed vintage;
SUE = diff / std(last 8 diffs, min 4), winsorized ±6.

**Mechanical validation:** SUE monotonically predicts the event-window reaction
(Q1 −11bp → Q5 +66bp on the filing clock; −43 → +72 on the 8-K clock). The instrument works;
the null below is not a broken-join artifact.

## 2. Power gate (run BEFORE the test — new discipline, worth keeping)

The phase-1 daily-decile design **cannot adjudicate the long leg**: requiring 30+ valid
post-earnings names per day keeps only 191/1147 days → MDE95 ≈ 38–44bp at realistic match
rates. Redesigned to the standard **pooled calendar-time top-quintile** (≥3 names/day):
nd=568–589, **MDE95 ≈ 23bp**. Pre-registered rule: PASS = net@15>0 with CI excl 0, ≥4/5 eras,
placebo ~0; a NULL only rules out effects ≳23bp.

## 3. The clock discovery (affects prior results' interpretation)

Matched events show `filed − event_date ≈ −1..0 days`: the calendar's earnings dates (and
therefore `days_since_last_earnings` everywhere in the dataset) are **10-Q/10-K filing
dates**. The press release (8-K item 2.02) lands **0–14+ days earlier**. Consequences:

- Phase-1 "PEAD" (+58.9bp proxy spread) measured **post-filing** drift, not classic PEAD.
- Any feature keyed to `days_since_last_earnings` is on the late clock.
- Fixed for Phase 4 by rebuilding events from 8-K item-2.02 dates (`edgar_8k_events.parquet`);
  a future engine pass could backfill the calendar from this table.

## 4. The test results (exact numbers, train 2016-2020 only)

**Filing clock** (`phase4_sue_pead.py`), top-SUE-quintile long leg, 5d excess-over-SPY:
gross **−5.1bp** CI[−25.8,+14.6], net@15 **−20.1**, eras 3/5, placebo −6.4 (~0).
Quintile ladder Q1→Q5: −20.4 / −21.0 / −14.7 / −5.1 / −5.1 (weakly monotone, all ≤0 gross).
SUE decile *spread* +8.6bp vs the proxy's +58.9 → the proxy spread is **reaction-magnitude
sorting** (mostly short-leg), not surprise-direction drift.

**Corrected 8-K clock** (`phase4_sue_pead_8k.py`), primary = entry days 1–5 post-announcement:
gross **+5.5bp** CI[−13.8,+25.2], net@15 **−9.5** CI[−28.8,+10.2], eras 3/5, placebo +1.1.
Windows 2–6 / 1–10 / 6–20 days: +1.8 / −2.8 / +0.4 gross. Ladder U-shaped (Q1 +8.4 … Q5 +5.5).

**Verdict: NULL (both clocks), MDE caveat: only effects ≳23bp are excluded.** The
2021-22 validation split and 2023+ holdout were not touched (no PASS to confirm).

## 5. What this does to the forward path

- **Branch A price update.** The free test came back null with working machinery. Buying
  consensus-estimate data (FMP) is now a *long shot with a concrete bar*: consensus SUE must
  produce what time-series SUE could not — ≥ ~25bp/5d net@15 in the top quintile. The
  literature says consensus SUE sorts somewhat better; it does not usually resurrect a
  dead-in-liquid-names anomaly. **Recommendation: do not buy yet.**
- **"Just index" remains the honest baseline** per MASTER_FINDINGS §6.
- The EDGAR pipeline (facts + 8-K clock + PIT SUE) is a permanent, free asset: any future
  fundamental-event idea (guidance, restatements, filing-lag signals, insider Form 4s) starts
  from here instead of from a vendor invoice.
