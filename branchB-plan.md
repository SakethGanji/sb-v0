# Branch B plan — realized vs implied (Test 7.2 + A2 extension)

**Date:** 2026-07-06 · Governs Phase 7 Step 3. This is the plan and the
pre-registration SKELETON; the formal freeze (`branchB-preregistration.md`) happens
before any IV data is opened, with only data-contract blanks filled in.

## STATUS 2026-07-07 — DATA ACCESS ACQUIRED, hooked up, nothing unblinded

- **Purchased:** ORATS **Delayed Data API** ($99/mo, **20,000 requests/month**,
  15-min delay — irrelevant, we use hist EOD endpoints only). History coverage
  verified via metadata call: 2007-01-03 → present.
- **Access:** token in `~/.orats_token` (mode 600, NOT in repo; if it ever needs
  rotating, the ORATS dashboard reissues it and only that file changes). Base URL
  `https://api.orats.io/datav2`, auth via `token` query param. Client helper:
  **`scripts/orats_client.py`** (tested; retry/backoff; reads the token file).
- **`@orats/cli` v1.1.0 is broken upstream** (its `incur` dep imports
  `StdioServerTransport` from `@modelcontextprotocol/server`, which no published
  version exports — tried beta.2 and alpha.4). Uninstalled; the REST path via the
  client helper is the supported route. Revisit the CLI/MCP only if they fix it.
- **Request budget (20k/mo, binding constraint of the tier):** per-ticker
  hist range calls for `hist/smvsummaries` + `hist/monies/implied` over ~700
  tickers ≈ 1.4–3k requests (chunk by year if response caps bite → ≤ 6k). Strikes
  history is the expensive one — pull it ONLY for the straddle/spread audit on a
  pre-registered SAMPLE (e.g. 100 names × month-ends ≈ 3.6k requests), not the full
  panel. Log request counts in the downloader; abort at 15k used.
- **Discipline unchanged:** no IV/skew VALUES for 2018-2020 have been fetched
  (auth check was ticker metadata only). Order remains freeze → pull → gate → test.

### Next-session bootstrap (start here)

1. Read this file + `phase7a2-preregistration.md` (monetization map) +
   `phase7-findings.md` §7.1/§A2. Evaluate/adjust this plan BEFORE the freeze.
2. Pull ORATS field docs / one out-of-window sample row (e.g. 2016 or 2021 excluded
   ticker) to pin exact field names for: ATM IV₃₀ (smvsummaries), 25-delta IVs or
   slope/deriv (monies/implied or summaries), per-strike bid/ask (hist/strikes).
3. Freeze `branchB-preregistration.md` (T-B1 + T-B2 from §4, blanks filled, BY-FDR
   q=0.10, spread-anchored bars). Commit before the bulk pull.
4. `scripts/branchB_pull.py` (resumable, request-counting) → `data/orats/`.
5. `scripts/branchB_gate.py` → gate verdict → on PASS run the registered tests.

## 1. Why this door, and why it just got wider

Branch B is the closure theorem's unique expectancy-shaped loophole (hatch H2:
convex instruments — option prices are first-order in magnitude, the one thing we
predict). The evidence ledger going in, honestly stated:

**Against (vol level):** the forecast's lift over persistence is +0.036 rank-IC and
persistence is IV's own first input; Test 7.1 Arm A showed our market-level ML lost
outright to BLEND = √(RV·VIX) — a crude implied blend — on RMSE. Registered prior for
the level test: **null**.

**For (new since FINAL_REPORT — today's A2 results):** two persistent stock
invariants exist beyond the vol level: **tail shape** (Hill index, rank-AC ~0.28/mo,
both tails, 5/5 eras) and **trade-size fingerprint** (AC 0.72). Options markets price
tails via **skew/smile**, not just ATM level. Whether implied skew already embeds the
persistent realized-tail ranking is a genuinely open question that did not exist
yesterday. Registered prior for the skew test: **weak — the first test in this
project whose prior is not flatly null.**

## 2. What to buy (decision for the user)

Need, for the liquid-optionable subset of the deployable universe (mega/large ×
highly_liquid/liquid, expect ~400-700 optionable names), **EOD data, 2018-01→2020-12
(the OOS window; train-era-only discipline preserved):**

1. ATM implied vol at ~30d constant maturity (the level test),
2. 25-delta put/call IVs or a fitted smile (risk reversal / skew — the A2 extension),
3. option NBBO quotes or bid/ask IVs for at least the near-ATM strikes (the cost
   side: no net verdict without spreads).

Vendor candidates (verify current pricing at purchase; both historically ~$50-150 for
one month of access):
- **ORATS** (recommended first look): fitted smoothed surfaces + bid/ask IVs +
  earnings-aware term structure; clean for skew; one month of API access with
  historical download typically covers the whole window.
- **ThetaData**: raw EOD (and intraday) option quotes + greeks/IV; heavier lifting
  (we fit the smile) but true NBBO spreads.
- Fallback: CBOE DataShop one-off EOD IV summary purchase.
Bar: whichever source is bought, the SAME source must provide both the IV and the
spread measure, or the spread side is bought separately — no mixing forecasts from
one vendor with spreads assumed from another.

## 3. Power gate (runs first, on purchased data, before the test freeze unblinds anything)

`branchB_gate.py`, outcomes-and-contract-only, same discipline as `phase8_si_gate.py`:
- Coverage audit: names × days actually matched to our universe (join rate, IV
  nulls, min history).
- MDE95@80% for the two registered estimands (below) from cross-section sizes and
  date counts, using synthetic persistent signals — never the real forecast.
- **Cost reality check:** distribution of quoted straddle round-trip spreads in
  vega-adjusted vol points; the plausible-effect bar for the level test is set AT the
  median spread (an edge smaller than the spread is untradeable by construction).
- Frozen bars (to finalize numerically at freeze): level test |IC| ≥ 0.03; skew test
  |IC| ≥ 0.03; straddle economics adjudicable only if MDE ≤ median quoted spread.
- Gate FAIL on an estimand ⇒ UNANSWERABLE-at-this-budget, reported as such; buying
  more history is a new, explicit bet (per `phase7-implementation-plan.md` Step 3).

## 4. The registered tests (freeze verbatim at purchase)

**T-B1 — vol level (the original Branch B):** rank-IC of (our FULL-ML RV forecast −
IV₃₀) on (realized fwd vol − IV₃₀), per day, day-clustered, block bootstrap B=4000.
Plus delta-hedged ~30d ATM straddle deciles by (forecast − IV): gross and **net of
quoted spreads**. Prior: null. PASS → §7.7 confirmation before belief.

**T-B2 — tail/skew (the A2 extension, one test, frozen by the A2 monetization map):**
rank-IC of (our residualized Hill-tail rank, trailing month) on (realized forward
tail asymmetry − implied-skew-implied asymmetry), i.e. does persistent realized tail
shape add anything AFTER conditioning on the market's skew. Implementation detail
frozen at purchase once the skew field is known. Prior: weak. Family: T-B1 + T-B2,
BY-FDR q=0.10, 2 tests — nothing else may be added post-purchase.

**Short-premium side (C2):** measured for the record if the data supports it,
pre-labeled GATED (margin/assignment/tail = wall-class) — never deployable at retail.

## 5. Sequence, cost, decision points

| step | actor | cost | wall |
|---|---|---|---|
| Buy 1 month vendor access, download 2018-2020 EOD IV+skew+quotes | **user** | ~$50–150 | an evening |
| Freeze `branchB-preregistration.md` (blanks filled, nothing else changed) | session | $0 | ½ session |
| Gate (`branchB_gate.py`) | session | $0 | minutes |
| On gate PASS: T-B1 + T-B2, one run | session | $0 | one session |
| findings + MASTER_FINDINGS + FINAL_REPORT_ADDENDUM per Step 4 | session | $0 | one session |

**Declining is a valid outcome** (recorded as "untested by choice, prior poor for
T-B1 / weak for T-B2") — but note the calculus changed today: before A2, Branch B was
one poor-prior test; now it is the only place two measured invariants can possibly
pay, and the same ~$100 answers both. If Branch B lands null on both, Step 4's
exhaustion statement gains its final line and the project seals with every door
measured, none assumed.
