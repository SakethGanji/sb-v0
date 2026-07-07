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
- **UPGRADED 2026-07-07: Live Data API, 100,000 requests/month** (Basic License).
- **`@orats/cli` v1.1.0 installed and WORKING after a local one-line patch:** its
  `incur` dep imports `StdioServerTransport` from `@modelcontextprotocol/server`
  root, but every published version exports it only at the `/stdio` subpath. Patch
  (re-apply after any `npm update -g @orats/cli`):
  `.../@orats/cli/node_modules/incur/dist/Mcp.js` line 1 → split into
  `import { McpServer } from '@modelcontextprotocol/server';` +
  `import { StdioServerTransport } from '@modelcontextprotocol/server/stdio';`
- **Hooked up:** `ORATS_TOKEN` exported from `~/.bashrc` (reads `~/.orats_token`);
  `orats` MCP server registered for Claude Code; `orats-data` / `orats-glossary`
  skills synced. **All 26 endpoint schemas + field docs dumped to
  `~/.orats/endpoints/`** (`*.schema.json`, `*.fields.json`, no-quota lookups) plus
  `~/.orats/llms-manifest.json` — the freeze pins field names from THESE files.
  REST fallback remains `scripts/orats_client.py`.
- **Contract facts already pinned** (from the field docs, no values fetched):
  `hist-eod-summaries` has `iv30d` (DECIMAL, 0.30=30%), `exErnIv30d`
  (earnings-cleaned), `rSlp30`/`rDrv30` (30d skew slope/curvature ≡ cores
  slope/deriv); **unit gotcha:** `*-cores` endpoints quote IV in PERCENTAGE POINTS
  (25.36 = 25.36%) while summaries are decimal — never mix without /100.
- **Request budget (100k/mo — comfortable, still logged):** summaries + monies
  per-ticker range calls ≈ 3–6k total; strikes for the spread audit can now afford
  ~700 names × month-ends with `--dte`/`--delta` filters (near-ATM ~30d only)
  ≈ 25k. Downloader logs request counts; abort at 80k used.
- Efficient-CLI conventions for the pull: `--fields` to trim payloads, `--format
  jsonl` piped to files, `--trade-date` ranges per ticker, `--dte 20,45 --delta
  0.35,0.65` for the straddle legs; `orats glossary <field>` for any field question
  (no quota).
- **Discipline unchanged:** no IV/skew VALUES for 2018-2020 have been fetched
  (auth check was ticker metadata only). Order remains freeze → pull → gate → test.

### Storage & no-refetch policy (added 2026-07-07 — the API is called ONCE per datum, ever)

- **Raw-response cache is the source of truth:** every API response is written
  verbatim, gzipped, BEFORE parsing: `data/orats/raw/<endpoint>/<ticker>__<range>.json.gz`.
  The downloader is idempotent — an existing raw file is never re-fetched (same
  resumable pattern as `phase7a2_build_profiles.py`); parser bugs are fixed by
  re-parsing the cache, never by re-calling the API.
- **Canonical layer:** `data/orats/parquet/<endpoint>/…` built purely FROM the raw
  cache by a separate script. **Analysis scripts read ONLY the parquet layer; the
  ONLY thing allowed to call the API is `branchB_pull.py`.** Once the pull is done,
  session usage of the orats CLI/MCP is restricted to `glossary`/`--schema`/
  `--list-fields` (no-quota); any data question is answered from local parquet.
- **Request ledger:** every call appends (endpoint, params, rows, timestamp) to
  `data/orats/request_log.jsonl` — quota usage stays auditable against the 100k cap.
- **Pull breadth (one month of access → take everything we could ever need):**
  summaries + monies-implied for the FULL available history (2007→present) for the
  ~700-name universe — per-ticker range calls make this barely more expensive than
  2018-2020 alone, and it puts the 2021-22 §7.7 confirmation data on disk in the
  same pass (a PASS must not require a second subscription month). Strikes: 2018-2020
  test sample + the matching 2021-22 sample. **Having windows on disk does not
  unseal them** — tests read only the windows their pre-registration allows; the
  seal is enforced by the scripts' date filters and the standing protocol, exactly
  as with the equity holdout (on disk since 2023, never opened).
- Backup note: `data/` lives on the T7; `data/orats/` raw+parquet is small (likely
  < 2-3 GB) — copy `raw/` somewhere second if paranoid; it is the $199 artifact.

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
