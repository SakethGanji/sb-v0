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
| B3 `forward_outcomes` short horizons | ✅ **complete & L2-validated** (stamped `B5`) |
| B4 `forward_path_short` | ✅ **complete & L2-validated** (stamped `B4`) |
| B5 multi-day horizons + dividends + terminal events | ✅ **complete** — `forward_outcomes` at all 657 cols |
| B6a golden-day fixtures (split / ex-div / delisting / LULD halt / rename) | ✅ **complete & L2-validated** — `scripts/golden_days.py` + validator §N, 15/15 |
| B6b **full 2016–2026 sweep** | ✅ **complete & L2-validated** — all 8 tables, 2,513 days |
| B6c terminal-event rename fix + widened exact-recompute | ✅ **complete & L2-validated** — 399,798/2, 15/15 golden |
| Independent validation battery | ✅ 158,033/158,033 (full corpus + stress regimes + 15/15 golden) |
| Workspace tests | ✅ 139 passing |
| Determinism + resume-equality | ✅ byte-identical (B1/B2) |

**B6b complete (2026-06-15): the full 2,513-day corpus (2016-06-08 → 2026-06-05)
is swept for all eight tables.** `forward_outcomes` = 2,513 day-files spanning
the corpus (no partial/0-byte files); `daily_observation` matches at 2,513.
`build-regimes` re-stamped thresholds against the true full exploration window.
Honest nulls only: `bar_gap` for 10d+ (no 1m beyond D+5), terminal-detail for
non-delisting names, and truncated long horizons for the last ~year of entries
(252d forward runs past the 2026-06-05 corpus end). **Storage:** the dataset
now lives on the Samsung T7 (`/mnt/atlas`, ext4); the repo's `data/` is a
symlink there. forward_outcomes ~333 GB, forward_path_short ~280 GB.

**Stage 3 validation (158,033 checks, 0 fail):** per-day exact recompute on the
stress regimes the smoke window never covered — the 2020-03 COVID
circuit-breaker week (03-09/16/18), the AAPL 2020-08-31 split straddle, and the
FB→META 2022-06-09 rename — plus full-corpus property sweeps over all 2,513 days
(daily_observation, forward_outcomes, forward_path_short) and the 15/15 golden
battery. The smoke-window coverage gap is closed.

**B6c — terminal-event-on-rename fix (2026-06-16):** a *widened* exact-recompute
battery (18 days across every vol regime: Volmageddon, GME, SVB, yen-carry, the
COVID week) caught a second identity bug the 7-day sample missed.
`tickers_enriched.delisted_utc` also fires on a ticker **rename** (the old
symbol's cessation), so renamed names (ABIO→ORKA, AGFY→RYM, AGAE→AIFA, AGH→PUSA)
were tagged a spurious `delisted_unknown`. Fix: `compute_terminal` now suppresses
the event when the sid has **any bar after `dl`** within the ≤252-day matrix
window (a true delisting has none; FIGI reuse by an unrelated security only
recurs months/years later, outside the window). The validator's narrow
figi_map-rename proxy was replaced by the authoritative bar-based
`_delisted_membership()` continuation signal that mirrors the engine (catches
same-symbol restructures like AAN that the proxy missed). Re-swept
`forward_outcomes` corpus-wide via yearly chunks with a new `--no-path` flag
(forward_path_short is unaffected — left in place). **Result: 399,798 PASS / 2
FAIL / 0 SKIP** on the widened battery + golden. The 2 fails are documented
validator-side residuals where the **engine is provably correct**: (1) ADOM
`terminal_event_return` — sid-keyed reverse split under a *reused* ticker (EVTV);
the engine's sid-first split factor is right, the validator's `_fo_factor` is
symbol-only; (2) `breadth_count_movers_above_1atr` off-by-one — a float tie at
the exact 1.0-ATR boundary (446 vs 447).

**B6c.1 — entry-microstructure coverage closure (2026-06-20):** the coverage
audit had flagged 15 entry-quality columns as deterministic transforms of
validated inputs but with *no* independent recompute. Closed: `_fo_resolve`
now re-derives all 15 from the raw 1m tape + `daily_observation[D]`
denominators (`adv_20d`/`addv_20d`/`yang_zhang_vol_14d`) — pre_entry
vwap/low_return/ret_from_high/ret_from_low/minutes_since_high/minutes_since_low,
entry-bar upper/lower wick, slippage-proxy-bps, both participation-capacity
proxies, 1m dollar-volume, and the range-vs-atr/yz/addv ratios. Formulas were
checked against the engine `resolve_entry` source (not a paraphrase), including
the Rust tie-break asymmetry (`max_by`->last for `minutes_since_high`,
`min_by`->first for `minutes_since_low`). Widened 18-day battery + golden now
**406,188 PASS / 2 FAIL / 0 SKIP** (same 2 documented residuals). Every
forward_outcomes column the audit could name now has independent recompute.

**B6c.2 — validator-residual hardening → clean 0-fail baseline (2026-06-20):**
the 2 long-standing residuals (engine always correct, validator the limited
side) are now closed in the validator, so any future fail is unambiguously a
real regression. (1) `_fo_factor` is now **sid-first** (optional `sid`,
mirroring the engine `adjustment_factor` at bar_reader.rs:259:
`splits_by_sid` ∨ `splits_by_symbol`); the terminal series rescales to the
sid-first factor so a *reused* ticker's split can no longer leak into a
delisting security's close — ADOM `terminal_event_return` now matches exactly.
`sid=None` reproduces the legacy symbol-only path byte-for-byte. This closes a
real coverage hole: symbol-only keying could have *agreed with* an engine that
had the same symbol-vs-sid bug (false PASS), not just disagreed with a correct
one. (2) the breadth `count_movers_above_1atr` check now accepts an fp tie-band
at the exact 1.0-ATR threshold (a mover within ~1 ULP of the cutoff may be
counted either way; engine count accepted if in `[strictly_above,
strictly_above + on_boundary]`). Widened 18-day battery + golden:
**406,190 PASS / 0 FAIL / 0 SKIP**.

All work through B5+validation is committed and clean. The only uncommitted
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

- `write-phase0` — the trailing sweep. Writes the 4 per-day B1/B2 tables.
- `write-forward-outcomes` — **(B3+B4)** separate forward pass with a ~6-day
  ring buffer; writes BOTH `forward_outcomes/` (wide) and
  `forward_path_short/` (long) in one walk (run AFTER `write-phase0`, since it
  reads `daily_observation[D]` for entry context). Engine modules:
  `forward_outcomes.rs` + `forward_path.rs`.
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

**`daily_observation`** (milestone B1) — fully populated EXCEPT
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
J (regimes full recompute) + section K (property sweep over all days) +
**section L (B3 `forward_outcomes`: sample exact recompute × 8 names ×
{0935,1000,1530} of every family — horizons, crossings, labels, day-0,
next-day, gap, time-underwater, cumulative volume — plus full-universe rank
self-consistency and a 12-invariant property sweep over all 144 days)** +
**section L5 (B5 multi-day: 10d–252d stats + daily crossings + 21d label +
ret_total + bar_gap recomputed against an independent forward daily series,
cached; + terminal events on real delisting securities),** **section M (B4
`forward_path_short`: per-checkpoint exact recompute of all 15 columns + a
cross-table `fp.ret == forward_outcomes.ret_<H>` check over all 144 days).**
**85,552/85,552 pass.**

**Also:** `scripts/check_determinism.sh` — same day written twice is
byte-identical, and a cursor-cleared resume reproduces the file byte-for-byte.

### ⚠️ THE BINDING GATE RULE

> **Every B-milestone extends the validator to its new columns IN THE SAME
> COMMIT.** Coverage never trails the engine. Every L2 mismatch is root-caused
> to ENGINE or VALIDATOR before any further table is built.

This rule was set after B2 shipped without validator coverage; the catch-up
session (#8) then found 2 more engine bugs. Honored through B3 (3 increments,
each with validator coverage; honored through B5); do not skip it for B6.

---

## 6. Adjudicated semantics — decisions that look like bugs but are NOT

These were settled by raw-tape inspection. **Do not "fix" them without
re-adjudicating** — the validator encodes them deliberately:

- **RTH includes the 16:00 closing-auction print** (it's the official EOD close).
- **Session membership is by ET date, not UTC** (winter 19:00–20:00 ET
  after-hours bars carry the next UTC date — comparing UTC silently dropped them).
- **Split factors are sid-first, symbol-fallback** (old-symbol splits only
  aggregate under the FIGI; correct for renamed tickers).
- **(B6a) Security identity is stabilized to the FIGI.** ~55% of symbols/day
  ship a NULL `security_id` in the vendor 1m bars; `day_sessions` now backfills
  the stable FIGI from `figi_map` by `(display_symbol, day)`
  (`FigiMap::resolve_sid`), falling back to the display symbol only when the
  map has no unambiguous coverage (~21% missing FIGI, plus ambiguous ticker
  reuse → honest symbol fallback). **Why it matters:** before this, a renamed
  security flipped identity at the rename (e.g. `FB`→`BBG000MM2P62` on
  2022-06-09), so the FIGI-keyed forward matrix could not match the
  symbol-keyed pre-rename entry and **all multi-day forward returns for the
  ~year before the rename went null** — bounded to ~2,664 in-corpus renames,
  invisible in the calm smoke window. Found by the B6 rename golden day; fixed
  engine-wide (one site stabilizes both `daily_observation` and the forward
  matrix). The forward dividend lookup keys through the same resolution so a
  renamed security's pre-rename dividends still attach. Smoke window re-swept;
  85,553/85,553 still pass.
- **All volume columns are on the pin-adjusted basis** (uniform).
- **`.TEST` / premarket-only securities belong in the universe** (record
  everything; no filtering).
- **Vendor sid collisions** (~16/day — one FIGI under two symbols, e.g.
  GOLD/AMRK, COMM/VISN): rows are kept, but the sid is **permanently
  ambiguous** once detected — its rolling trailing state is purged and never
  accumulates again, so trailing columns stay honestly null for both listings.
  **(B5) `forward_outcomes` nulls ALL outcome columns for collided sids and
  `forward_path_short` emits no rows** (forward bars can't be attributed to
  either listing) — same honest-ambiguity principle. Caught by the B5 E2E:
  the daily matrix kept one listing while the 1m tape kept the other.
- **Rolling "prior day" = last TRADED day**, not strictly D-1 (gappy names).
- **market_context count columns are null-when-zero-valid.**
- **(B3) forward windows need NO cross-day split rescale.** `day_sessions`
  already adjusts every day to the PIN basis (`factor_at(day)` folds in all
  splits after `day`), so entry day D and forward days D+k are already on one
  consistent basis — including windows that straddle a split. (705 splits fall
  inside the smoke window; the validator recomputes per-day pin factors so a
  reintroduced rescale would fail.) `entry_unadjusted = entry_price / factor_at(D)`.
- **(B3) `forward_outcomes` entry = open of the first RTH bar at/after the
  entry-offset minute.** When that minute is absent (gap/halt — e.g. the 15:30
  offset on the 2016-11-25 half day, which fills at the 16:00 auction print),
  `is_halted_at_entry=true` and intraday horizons are measured from the ACTUAL
  fill bar, not the nominal offset (so 10/30/60min can collapse onto one bar).
- **(B5) 10d–252d horizons are DAILY resolution.** A 252-day 1m buffer is
  infeasible, so multi-day stats combine the day-D intraday extreme (from the
  1m tape) with daily highs/lows D+1..D+H; `bars_to_*` and daily crossings are
  in TRADING-DAY units. "D+H" = the H-th TRADED day after D (= calendar trading
  days for non-gappy names). Dividends are pin-adjusted (`cash × factor_at(ex)`).
- **(B5) terminal events never claim a delisting reason.** There is no
  reason field on disk (only `delisted_utc`), so `terminal_event_type` is only
  `none` or `delisted_unknown`; `terminal_event_return` carries the entry→last
  price signal (e.g. +29% likely-merger vs −56% likely-distress) without
  fabricating the cause. Detected from `delisted_utc`; confidence `high`.

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

## 9. What's NEXT — B6 (the full sweep)

### B3 — `forward_outcomes` short horizons (✅ DONE)

**Architecture (built):** a **separate forward pass**,
`bin/write-forward-outcomes`, NOT folded into the chronological sweep (the
sweep was left untouched). It walks a **~6-day ring buffer** of full 1m
sessions (entry day D + 5 successors); day D emits once its 5 successors are
buffered. Entry-day trailing context (atr_14d / yz_vol_14d / adv_20d /
addv_20d, for the entry-quality normalizers) is **read back from the
validated `daily_observation[D]`** rather than recomputed, so the pass needs
no `RollingState`. Forward days need no split rescale (see §6). Run:
`cargo run --release --bin write-forward-outcomes -- --from … --to …`.

**DONE (stamped `B3`, L2-validated) — every short-horizon family for the 8
horizons `10min,30min,60min,EOD,1d,2d,3d,5d` × 17 offsets:** identity + entry
pricing, pre-entry parametric features (incl. cross-sectional ranks +
`cumulative_volume_to_entry`), entry-quality proxies, per-horizon
ret/drawdown/runup/close-extremes/bars-to-extreme, `ret_<H>_excess_{idx}`,
fixed-% + ATR threshold crossings (1-based; 0=never; null=truncated),
`hit_`/`first_event_` target-before-stop labels (8 of 9 pairs; the 21d pair is
B5), day-0 `ret_to_<seg>` + post-entry session-shape, close-based
time-underwater, next-day outcomes, and gap-vs-RTH days 1-5. Built across 3
increments (commits `8958bcd`, `502320a`, + final); 12 L1 tests; validator §L
recomputes every family for the sample + full-universe rank self-consistency +
property sweep (incl. B5-horizons-null, label domain, hit↔first_event).

(The B5 columns — 10d–252d horizons, ret_total, dividend flags, bar_gap,
terminal events, the 21d pair — are now filled; see the B5 section below.)

**Adjudicated conventions** are documented at the top of
`forward_outcomes.rs` and §6 above (entry = open of first RTH bar at/after the
offset; no cross-day rescale; intraday cap from the fill bar; same-bar
target+stop ⇒ `stop_first`; session-segment boundaries; `time_to_recover` = 0
when never underwater/never recovered).

**Tracer bullet:** `forward_outcomes` is now complete for the smoke window, so
the Phase 1 §5.1 step-0 vertical slice can fork off to de-risk the power
question.

### B4 — `forward_path_short` (✅ DONE, stamped `B4`)

23 path checkpoints (1m → 5d_close) long-format, 19 columns incl. the +4 v2
Markov-state columns (`volatility_within_trade`, `rate_of_change`,
`current_ret_over_atr_14d`, `halt_gap_crossed`). Built in
`crates/momentum-engine/src/forward_path.rs` and **written in the same pass as
`forward_outcomes`** by `bin/write-forward-outcomes` (one ring-buffer walk →
both tables; cursor marks both `forward_outcomes` + `forward_path_short`).
Shares the tape/bounds via `forward_outcomes::build_tape_with_bounds`. Emits 23
checkpoints per (sid, offset) with a valid entry (none for null-entry pairs;
vendor sid collisions yield a multiple of 23). Smoke window swept: 433M rows /
~16 GB. Conventions (documented at the top of `forward_path.rs`): intraday
checkpoint ends mirror the forward_outcomes caps; `bars_elapsed` = tape index;
`volatility_within_trade` = sample stddev of per-bar returns (null < 2);
`rate_of_change` = Δret/Δbars vs the previous checkpoint; `halt_gap_crossed`
latches on any same-day ≥5-min bar gap (overnight gaps excluded).

### B5 — multi-day horizons + dividends + terminal events (✅ DONE, stamp `B5`)

Two increments (`B5a` `ebbcc49`, `B5b`). Needs forward data the 6-day ring
buffer can't reach, so `write-forward-outcomes` **preloads a pin-adjusted
daily-aggregate matrix** (close/high/low per security over [first entry, last
entry + 252 trading days]) + a pin-adjusted dividend lookup + a delisting
lookup (`tickers_enriched.delisted_utc`). The daily matrix is ~70s and reads
each day's bars once just for daily OHLC — **for B6 this can read
`daily_observation` (eod_day_*) instead, which exists corpus-wide.**

- **10d–252d horizons** at DAILY resolution: ret / drawdown / runup /
  close-extremes / `bars_to_*` (TRADING-DAY units; 0 = day-D intraday extreme,
  combined with daily extremes D+1..D+H) / excess / daily threshold crossings
  (1-based DAY index) / the `3atr_before_minus_1_5atr_21d` label.
- **`ret_<H>_total`** (9 horizons) = (close(D+H) + pin-adjusted dividends with
  ex in (D, D+H]) / entry − 1; `dividend_ex_date_within_<H>`.
- **`bar_gap_minutes_max`**: ≤5d from the 1m tape; null for 10d+ (no 1m).
- **Terminal events** (build-state §6): `delisted_unknown` (no reason field on
  disk — never claims merger/bankruptcy), `terminal_event_date` =
  `delisted_utc`, `terminal_event_return` (entry → last valid price),
  `last_valid_trade_date`, `days_with_missing_forward_bars`, confidence `high`.

Validator §L5 + a delisting-security terminal check. `forward_outcomes` is now
complete at **all 657 columns** (only honest nulls remain: `bar_gap` 10d+ and
the terminal-detail columns for non-delisting names).

### B6a — golden days (DONE)

Five hand-verified fixtures in `scripts/golden_days.py`, each grounded in real
reference data + the raw tape, validated by `validate_phase0_outputs.py §N`
(run `… --golden`; sweep recipe in `golden_days.SWEEP_RECIPE` → a separate
`data/outputs_golden/` tree so they never perturb the smoke battery):

| Fixture | Entry | Validates |
|---|---|---|
| AAPL 4:1 split | 2020-08-28 | straddle return on one consistent pin basis (+2.8%, not −74% / not double-adjusted) |
| AAPL ex-div | 2020-08-06 | `ret_total − ret_price == div/entry`; `within_1d` flag |
| LOGM delisting | 2020-08-28 | `delisted_unknown` dated to the vendor delist, no fabricated reason |
| 2020-03-09 LULD halt | 0935 entry | `is_halted_at_entry`, fills at the 09:49 resumption bar |
| FB→META rename | 2022-06-08 | FIGI identity stable across rename; forward window continuous |

**15/15 PASS.** The rename day surfaced + drove the B6a identity-stabilization
fix (see §6: null bar-sid → FIGI backfill). Result: 4 corporate-action types
were already correct on real 2020 stress data; the 5th exposed a real gap, now
fixed.

### B6b — full 2016–2026 sweep (DONE 2026-06-15)

The **full 2,513-day sweep** of all 8 tables is complete (write-phase0 → forward
sweep, chunked by year → build-regimes). Validation: 158,033/158,033 on the
stress regimes + full-corpus property sweeps + golden (see §1). The forward
pass still re-reads raw bars for the daily matrix (the `daily_observation`
source optimization was not needed — yearly chunks bounded the in-RAM matrix
and the run fit in ~13 h). Operational artifacts: `scripts/_b6b_resume.sh` /
`_b6b_resume2.sh` (chunked, disk-guarded, resumable — they survived a mid-run
machine crash with zero data loss; one 0-byte partial was the only casualty).

**Phase 0 engine is now complete: all 8 tables, full corpus, every layer
validated.** Remaining work is Phase 1 (the analytical layer), not engine.

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
(this)    B5b(forward_outcomes): daily crossings + 21d label + terminal events → B5
ebbcc49  B5a(forward_outcomes): multi-day horizons (10d-252d) + ret_total + bar_gap
2807e3b  B4(forward_path_short): long-format 23 checkpoints + same-pass writer + §M
ae81838  B3(forward_outcomes): pre-entry ranks + cumulative volume — B3 COMPLETE
502320a  B3(forward_outcomes): day-0/next-day/gap/time-underwater (increment B)
8958bcd  B3(forward_outcomes): threshold crossings + target-before-stop labels (A)
9f43467  B3(forward_outcomes): core short-horizon columns + forward pass + L2
5cdc863  docs: reconcile RFC/strategy drift (split dates, 657 cols, censor_type)
775e235  docs: comprehensive Phase 0 build-state & handoff guide
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
