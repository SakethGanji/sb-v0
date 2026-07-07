# Phase 8 findings — short-interest positioning

**Date:** 2026-07-06 · Pre-registration frozen before any signal-outcome contact
(`phase8-preregistration.md`, commit `20c585e`). Scripts: `phase8_si_gate.py`,
`phase8_si_test.py`.

## Power gate (outcomes + synthetic persistent signals only)

70 usable settlements (2018-01-09→2020-11-24 signal days at the +11-calendar-day
FINRA clock), median 908 names/day. **Mean-IC estimands ANSWERABLE** (MDE95@80%:
0.0116/5d, 0.0149/21d vs bar 0.03). **Decile-spread estimands UNANSWERABLE**
(MDE 22bp/5d, 61bp/21d vs bars 10/20bp — a persistent signal clusters spread
variance). Per the frozen rule, spreads below are descriptive only.

## Registered test — **0/6 PASS**

| signal | hz | mean IC | 95% CI | p | eras 18/19/20 | verdict |
|---|---|---|---|---|---|---|
| DTC | 5d | −0.009 | [−0.026, +0.009] | 0.32 | −/−/0 | NULL |
| DTC | 21d | −0.016 | [−0.040, +0.006] | 0.15 | −/−/+ | NULL |
| SIR | 5d | −0.015 | [−0.035, +0.005] | 0.14 | −/−/− | NULL |
| SIR | 21d | −0.015 | [−0.047, +0.016] | 0.32 | −/−/+ | NULL |
| **ΔSI** | **5d** | **−0.019** | **[−0.030, −0.008]** | **0.002 (FDR-surviving)** | **−0.022 / −0.038 / +0.005** | **NULL (era-unstable)** |
| ΔSI | 21d | −0.010 | [−0.021, +0.005] | 0.17 | −/−/+ | NULL |

The one texture worth recording: **rising short interest → next-week underperformance
was real in 2018–2019** (ΔSI/5d survived BY-FDR and the within-date permutation bar)
**and inverted in the 2020 squeeze regime**, failing the frozen 3/3 era rule. It is
exactly the regime-fragile, short-gated whisper the registered prior predicted — the
conjunctive rule killed it, and the 2021 meme era (out of window, sealed) would
presumably have punished it harder. Descriptive spreads all negative (high-SI names
underperform on average, −14…−64bp), all short-gated, none adjudicable at this window.

**Verdict:** positioning (FINRA short interest) joins the map with a measured answer:
no adjudicable edge at this window; short-side texture real-but-regime-fragile and
behind the borrow wall regardless. The phase1 X.1 gap is closed. Reopening requires
either borrow-fee/float data (true squeeze mechanics) or the 2021-22 window via a
registered confirmation design — neither is recommended.
