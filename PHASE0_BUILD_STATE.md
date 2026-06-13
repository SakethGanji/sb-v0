# Phase 0 Engine — Build State & Handoff Guide

**Last updated:** 2026-06-12 (after the B1+B2 validation session, commit `f4ddc19`).
**Purpose:** single cold-read for a fresh session. Read this first, then the
three design docs below as needed.

---

## 0. Orientation — what this project is

A Rust engine that turns ~10 years of 1-minute US equity bars into eight
"observation-pivot" Parquet tables for momentum research. **Phase 0 = record
everything deterministically, decide nothing.** No strategy logic, no
cutoffs, no edge-hunting — that all lives in Phase 1+ (a separate analytical
layer that consumes these tables).

**Read order for a new session:**
1. **This file** — current state, architecture, how to run, what's next.
2. `implementation-plan.md` (repo root) — the B0→B6 milestone plan + the
   §4.5 validation plan. **This is the plan of record.**
3. `phase0-observation-pivot-rfc.md` (repo root, RFC v7) — the schema spec
   (§7 daily_observation, §8 market_context, §9 forward_outcomes, §9.5
   forward_path_short, §10 regimes, §11 earnings, §11.5 sector, §11.6
   classification).
4. `phase1-research-strategy.md` (repo root) — §3.7 is the frozen schema
   delta; §7.8 is the downstream Phase 1 order. Only relevant once the
   engine is done.
5. Auto-memory at `~/.claude/projects/-home-saketh-Projects-playground-sb-v0/memory/`
   — `MEMORY.md` index + per-topic files. `project_sb_v0_phase1_strategy.md`
   has the running engine build log.

---

## 1. State at a glance

| Thing | Status |
|---|---|
| Schema (8 tables, v2 / §3.7) | ✅ coded, RFC v7, version-stamped |
| B0 engine skeleton + benchmark | ✅ done |
| B1 `daily_observation` + `market_context_daily` | ✅ complete & L2-validated |
| B2 `earnings_calendar`, `sector_aggregates_daily`, `security_classification_daily`, `regime_definitions` | ✅ complete & L2-validated |
| B3 `forward_outcomes` short horizons | ⬜ **NEXT** |
| B4 `forward_path_short` | ⬜ |
| B5 multi-day horizons + dividends + terminal events | ⬜ |
| B6 golden-day fixtures + **full 2016–2026 sweep** | ⬜ |
| Independent validation battery | ✅ 896/896 on B1+B2 |
| Workspace tests | ✅ 124 passing |
| Determinism + resume-equality | ✅ byte-identical |

**Important:** only **144 days (2016-06-08 → 2016-12-30)** have been swept so
far — a smoke window for fast iteration. The full 2,513-day sweep is B6. Six
of the eight tables exist on disk for those 144 days; `forward_outcomes` and
`forward_path_short` don't exist yet (B3–B5).

All work through B2+validation is committed and clean. The only uncommitted
files are `predmarket-*.md` (a different project — leave them).

---

## 2. Data layout

**Inputs (`data/`, gitignored, already validated by `scripts/validate_reference_data.py`):**
- `bars_1m_raw/YYYY-MM-DD.parquet` — 2,513 days, per-day, sorted
  `(display_symbol, t)`, raw unadjusted tape, full extended hours 04:00–20:00 ET.
- `reference/` — 11 tables: `tickers_classified.parquet` (CS-only!),
  `figi_map.parquet`, `splits.parquet` (with pin), `dividends.parquet`,
  `financials.parquet`, `acceptance_datetime_backfill.parquet`,
  `vix_daily.parquet`, etc.
- `_engine_state.sqlite` — the resume cursor (one row per (table, day) done).

**Outputs (`data/outputs/`):**
- Per-day tables: `daily_observation/`, `market_context_daily/`,
  `sector_aggregates_daily/`, `security_classification_daily/` (144 files each).
- Whole-corpus derivations: `earnings_calendar.parquet`,
  `regime_definitions.parquet`.
- Every file stamps `engine_milestone` + the relevant `*_version` keys into
  Parquet file metadata.

---

## 3. Architecture — how the engine works

**One crate, `momentum-engine`, driven by `bin/write-phase0`.** Pure
column-builders (testable, no I/O) + a sweep driver that does I/O.

### The chronological sweep (the core idea)

Days are processed **in calendar order** because trailing state accumulates
as it goes. There is **NO separate preload pass** — this was a deliberate
simplification over the original plan.

```
for each day D in order:
    sessions   = reader.day_sessions(D)        # bulk one-scan read, split-adjusted
    aggs       = compute per-security daily aggregates
    <build all per-day tables for D, reading trailing state>   # reads [start, D-1]
    <push D's aggregates into rolling state>                    # now state covers [start, D]
```

**The leakage contract (RFC §6.2) is structural:** features are read from
`RollingState` *before* day D is pushed into it. So any bare-name column can
only ever see `[D-N, D-1]`. This is the single most important invariant.

### Engine source files (`crates/momentum-engine/src/`)

| File | Role |
|---|---|
| `slices.rs` | ET/DST-correct session-window slicing + 1m→10m aggregation |
| `aggregates.rs` | per-(security, day) RTH/premarket/last-30m aggregates (`DailyAgg`) |
| `rolling.rs` | 252-day per-security window: ATR, realized vol, ADV/ADDV, Yang-Zhang, betas, 52w, premarket-median + O(1) counters (first-bar, consecutive-up, days-since-move, signal-fired). **`RollingState`** holds it all. |
| `daily_observation.rs` | the big builder — every `daily_observation` column |
| `market_context.rs` | `market_context_daily` (index paths, breadth, dispersion, VIX) |
| `earnings.rs` | `earnings_calendar` derivation + `EarningsLookup` (sweep join) |
| `sector.rs` | `sector_aggregates_daily` |
| `sic.rs` | SIC code → (sector, industry) v1 mapping |
| `classification.rs` | `security_classification_daily` + `SharesLookup` (market-cap) |
| `regimes.rs` | `regime_definitions` derivation (post-pass) |
| `cursor.rs` | SQLite resume cursor |
| `stamps.rs` | canonical per-table metadata stamps + zstd writer props |

### Supporting bins

- `write-phase0` — the sweep. Writes the 4 per-day tables.
- `build-earnings-calendar` — derives `earnings_calendar.parquet` from SEC
  filings (run BEFORE the sweep; the sweep joins it in).
- `build-regimes` — post-pass over written `market_context_daily/` →
  `regime_definitions.parquet` (run AFTER the sweep).

### Store-side additions (`crates/momentum-store/`)

- `bar_reader.rs`: `day_sessions()` (bulk one-scan read), `adjustment_factor()`,
  `split_event_within()`.
- `dividends.rs`: `read_ex_dividend_dates()`. `vix.rs`: `read_vix_closes()`.
- `figi_map.rs`: `renamed_on()`. `financials.rs`: `read_filings_lite()` +
  `FilingLite`. `tickers_classified.rs`: `read_classified_lite()` +
  `ClassifiedLite`. `acceptance_backfill.rs`: `read_acceptance_by_accession()`.

---

## 4. Per-table status

**`daily_observation`** (milestone B2) — fully populated EXCEPT
`days_to_next_known_earnings` which is **permanently null in Phase 0** (SEC
data records announcements, not schedules — filling it needs lookahead or a
scheduled-earnings feed on the deferred-ingest list). Everything else: signal
snapshots, EOD/premarket aggregates + unadjusted, overnight context, all
trailing windows, shape descriptors, ranks, signal freshness/concentration,
data-quality flags, earnings proximity. **Fully L2-validated.**

**`market_context_daily`** (B1) — index 10m paths over full 04:00–20:00
session, index EOD/gap/vol, VIX close (`vix_open` null by design — FRED VIXCLS
is close-only), 9 breadth metrics, 4 dispersion + 2 liquidity columns. **L2-validated.**

**`earnings_calendar`** (B2) — 249,738 events derived from `financials.parquet`
+ EDGAR backfill; earliest acceptance per (sid, fiscal period); ET-clock
`report_timing`; amendments→`revision_count`. **L2-validated (AAPL recount).**

**`sector_aggregates_daily`** (B2) — equal-weighted sector means via the SIC
v1 mapping; "Unknown" bucket for unmapped names (no silent filtering).
**L2-validated via an independent Python SIC port.**

**`security_classification_daily`** (B2) — structure flags, 5 buckets,
cross-sectional ranks, 10 behavioral tags (pinned to `momentum-classify` v1).
Market cap = **filed shares (point-in-time) × unadjusted prior close**, with a
>30× plausibility cross-check; null for funds/foreign issuers without filings.
**L2-validated incl. a full Python port of the tag rules.**

**`regime_definitions`** (B2) — 5 taxonomies (era + spy_trend + vix_level +
breadth + liquidity), tertiled on the **exploration window only**
(2016-06-08→2020-12-31), thresholds stamped. **L2-validated.** ⚠️ Thresholds
are currently from the 2016 smoke window — re-run `build-regimes` after the
full sweep.

---

## 5. The validation system (READ THIS — it's the project's discipline)

`scripts/validate_phase0_outputs.py` (~1,450 lines) is a **second
implementation in polars**, written from the column definitions, that
recomputes engine output from the raw tape and diffs. It exists because Rust
unit tests share an author with the code they test (correlated blind spots).

**Three levels (implementation-plan.md §4.5):**
- L1 — Rust unit tests (124, hand-computed expectations).
- L2 — the independent polars battery.
- L3 — external ground truth (real closes, VIX, earnings dates, SPY-beta≡1).

**The battery runs:** sections A–I on **4 stress days** (baseline 2016-12-30,
half-day 2016-11-25, post-DST 2016-11-07, dataset-start 2016-06-09) + section
J (regimes full recompute) + section K (property sweep over all 143 days).
**896/896 pass.**

**Also:** `scripts/check_determinism.sh` — same day written twice is
byte-identical, and a cursor-cleared resume reproduces the file byte-for-byte.

### ⚠️ THE BINDING GATE RULE

> **Every B-milestone extends the validator to its new columns IN THE SAME
> COMMIT.** Coverage never trails the engine. Every L2 mismatch is root-caused
> to ENGINE or VALIDATOR before any further table is built.

This rule was set after B2 shipped without validator coverage; the catch-up
session (#8) then found 2 more engine bugs. Do not skip it for B3.

---

## 6. Adjudicated semantics — decisions that look like bugs but are NOT

These were settled by raw-tape inspection. **Do not "fix" them without
re-adjudicating** — the validator encodes them deliberately:

- **RTH includes the 16:00 closing-auction print** (it's the official EOD close).
- **Session membership is by ET date, not UTC** (winter 19:00–20:00 ET
  after-hours bars carry the next UTC date — comparing UTC silently dropped them).
- **Split factors are sid-first, symbol-fallback** (old-symbol splits only
  aggregate under the FIGI; correct for renamed tickers).
- **All volume columns are on the pin-adjusted basis** (uniform).
- **`.TEST` / premarket-only securities belong in the universe** (record
  everything; no filtering).
- **Vendor sid collisions** (~16/day — one FIGI under two symbols, e.g.
  GOLD/AMRK, COMM/VISN): rows are kept, but the sid is **permanently
  ambiguous** once detected — its rolling trailing state is purged and never
  accumulates again, so trailing columns stay honestly null for both listings.
- **Rolling "prior day" = last TRADED day**, not strictly D-1 (gappy names).
- **market_context count columns are null-when-zero-valid.**

---

## 7. Bugs the validation found (the track record — why the discipline matters)

Five real engine bugs that 124 unit tests never caught:
1. **UTC-vs-ET date** dropped ~740 winter after-hours bars/day.
2. **Same-day sid collision** mixed two securities' EOD rows.
3. **Intermittent sid collision** leaked past the same-day guard — COMM's
   market cap landed on VISN's row (only caught by the full battery).
4. **Non-deterministic share tie-break** (vendor file order) → unstable market caps.
5. **Market-cap basis errors** during B2 spot checks (snapshot-shares fallback
   produced a $2.8×10¹⁷ market cap; the absurdity guard produced a $5T 2016
   Seadrill). Now: filed shares × unadjusted close + plausibility cross-check.

Also a latent reader bug found at B0: `MaterializedBarReader::open` hard-
errored on the real `splits.parquet` (vendor snapshots carry
announced-but-not-yet-executed future splits) — now excluded-with-warn.

---

## 8. Known gaps / deferred (documented, not forgotten)

- **`tickers_classified.parquet` is CS-only** → `ticker_type` is null for ~69%
  of the daily universe, `is_etf`≈0, ~71% of sector rows are "Unknown". Fix =
  cheap re-pull of the tickers list WITHOUT the CS filter. On deferred-ingest list.
- **`days_to_next_known_earnings`** — permanently null in Phase 0 (no scheduled
  feed).
- **`is_china_adr`** — structurally false (no country field on disk).
- **Meme-tag arm B** — disabled (needs a percentile history).
- **Float/short-interest, index membership, catalysts, NBBO quotes** — all
  out of scope per RFC; joins reserved for Phase 1+.

---

## 9. What's NEXT — B3 and the rest

### B3 — `forward_outcomes` short horizons (start here)

Intraday/EOD/1d–5d horizons (returns, max drawdown/runup, bars-to-extremes),
fixed-% + ATR threshold crossings, market-relative (excess) returns, day-0
session segments, next-day outcomes, gap-vs-RTH days 1-5, materialized
target-before-stop labels, and `first_event_<pair>` (exact at 1m resolution).
Schema already coded (657 columns total in `forward_outcomes_schema`).

**Architecture note:** this needs a **forward-looking window**. The sweep is
chronological, so day D's forward outcomes finalize once D+5's bars have
passed. The standard approach: keep a ~7-trading-day ring buffer of full 1m
bars. Long horizons (10d+) come later (B5) and should resolve from daily
aggregates, not 1m (confirm against RFC §9 — there's nothing between
`5d_close` and the wide horizons, which supports daily resolution).

**Tracer bullet:** once B3 writes one month, the Phase 1 §5.1 step-0 vertical
slice can fork off (one pre-chosen question, plain conditional sorts, full
discipline) to de-risk the power question early.

**GATE:** extend `scripts/validate_phase0_outputs.py` with a
`forward_outcomes` section in the same commit.

### B4 — `forward_path_short`

23 path checkpoints (1m → 5d_close) long-format, with the +4 v2 Markov-state
columns (`volatility_within_trade`, `rate_of_change`, `current_ret_over_atr_14d`,
`halt_gap_crossed`). Schema coded (19 columns).

### B5 — multi-day horizons + dividends + terminal events

10d–252d horizons, `ret_<H>_total` (dividend-adjusted), ex-date flags,
`bar_gap_minutes_max`, terminal events (merger-vs-delist split). One open
design decision: 1m vs daily resolution for 10d+ runup/drawdown (recommend
daily; document it).

### B6 — golden days + full sweep

Hand-verified fixtures (AAPL 2020-08-31 split, FB→META 2022-06-09 rename,
an ex-div, a LULD halt, a delisting), then the **full 2016–2026 sweep** (all
8 tables, ~0.5s/day → a few hours). Re-run `build-regimes` afterward so
thresholds reflect the true exploration window.

---

## 10. How to run things

```bash
# Full sweep (current smoke window). Writes the 4 per-day tables.
cargo run --release --bin write-phase0 -- --from 2016-06-08 --to 2016-12-30 --force

# Single day (resumes; --force to rewrite).
cargo run --release --bin write-phase0 -- --day 2016-12-30 --force

# Earnings calendar (run BEFORE the sweep so it joins in).
cargo run --release --bin build-earnings-calendar

# Regimes (run AFTER the sweep — post-pass over market_context_daily).
cargo run --release --bin build-regimes

# Validation battery (4 stress days + property sweep over all days).
scripts/validate_phase0_outputs.py
# or specific days:  scripts/validate_phase0_outputs.py 2016-12-30

# Determinism + resume-equality.
scripts/check_determinism.sh

# Tests + lint.
cargo test --workspace
cargo clippy --workspace --all-targets

# Inspect a parquet (pyarrow via uv, no system install needed).
uv run --quiet --with pyarrow python3 -c "import pyarrow.parquet as pq; ..."
```

Sweep order for a clean full run: `build-earnings-calendar` → `write-phase0`
→ `build-regimes` → `validate_phase0_outputs.py`.

---

## 11. Commit map (the build history)

```
f4ddc19  validation(#8): full B1+B2 battery — 896/896, 2 engine fixes
b491a90  validation: determinism + resume-equality script
4c2c8a7  cleanup: clippy nits
51da501  B2(complete): security_classification_daily + regime_definitions
76fe7d9  validation(B1): adjudicate diffs to zero — 1 engine bug fixed
0d9fa4a  B6-early(validation): independent polars suite v0
17b5d4d  B2(sector): sector_aggregates_daily
e5480de  B2(earnings): earnings_calendar + proximity join
8452653  cleanup: let-chains
394cd3f  B1: complete daily_observation + market_context_daily
3f033e3  B1: chronological sweep + rolling trailing state
a1f45db  phase0: §3.7 v2 schemas + RFC v7 + momentum-engine B0
```

Master branch. PRs go to `main`.

---

## 12. Working-style notes for the next session

- **Build releases for sweeps** (`--release`); debug is too slow on real data.
- **Long sweeps/builds run in the background** — use the task tools, don't
  block. A 144-day sweep is ~1.5 min release.
- **Parallel subagents share the worktree** — if you fan out, tell them
  NEVER to run `git stash`/`restore`/`checkout` (an agent once stashed all
  uncommitted work mid-session). Prefer worktree isolation for parallel edits.
- **The §4.5 gate is non-negotiable**: validator coverage lands with the
  engine code, same commit. Every diff adjudicated before moving on.
- The user values **honest nulls over fabricated data** and **spot-checking
  against external truth** — every validation burst this project has run found
  a real bug.
