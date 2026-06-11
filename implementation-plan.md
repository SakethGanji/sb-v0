# Implementation Plan — §3.7 schema v2 → Phase 0 engine → Phase 1 research stack

**Status:** Active plan of record (2026-06-11). Derived from
`phase1-research-strategy.md` §3.7 (the frozen v2 schema delta) and §7.8
(the Phase 1 implementation order). Read those first; this document adds
the engineering decisions — stack, storage, architecture, milestones —
not methodology.

**Read after:** `momentum-hold-phase0-frozen-v2.md`,
`phase0-observation-pivot-rfc.md`, `phase1-research-strategy.md`.

---

## 1. Tech stack

| Layer | Choice | Rationale |
|---|---|---|
| Engine (everything touching the 1-minute tape) | Rust, existing cargo workspace; new `momentum-engine` crate + `bin/write-phase0` | 2,513 days × ~7k names × 657-column combinatorics is a throughput problem; the ingest layer already proved this stack |
| Columnar I/O | arrow/parquet 58, zstd | Pinned workspace-wide, version-matched to duckdb's bundled arrow for zero-copy handoff |
| In-process SQL | DuckDB (bundled) where a join/window reads better as SQL | Already a dep; used by `BarReader` |
| Engine resume state | SQLite (`rusqlite`) cursor, same pattern as `_ingest_state.sqlite` | Frozen Phase 0 decision; idempotent re-runs |
| Analytics (Phase 1) | Python in a new `research/` dir (uv-managed): duckdb + polars (query), scikit-learn (MI), causal-learn (BN), numpy + xgboost (DP/FQI), pymc or numpyro (hierarchical Bayes), lifelines/statsmodels (competing-risks hazards), matplotlib (surface heatmaps) | Matches the library choices §7 names; "the stack runs on a laptop" |

**No database server — deliberate.** The store *is* day-partitioned
Parquet queried through DuckDB. 450–650 GB of write-once, scan-many,
single-machine analytical data is exactly the shape Parquet+DuckDB wins
at; a DB server would add operational surface and a second copy of the
data for zero query benefit. SQLite stays scoped to tiny mutable state
(cursors; later the pre-registration ledger).

**Disk risk:** volume is 930 GB, ~204 GB used, ~726 GB free. Output
budget is 450–650 GB → worst case ~92% full. Decided by measurement in
milestone B0 (bytes/day from the first benchmark day, extrapolate, then
choose zstd level / extra disk *before* the full run, per the §3.6
"measured disk number" clause).

---

## 2. Workstream A — land the §3.7 schema delta (~1 day) — FIRST

Everything gates on this: retrofitting after the engine writes means
regenerating 200–300 GB. All changes in
`crates/momentum-core/src/phase0_outputs.rs` + the momentum-store
roundtrip test + RFC changelog.

1. **Constants**
   - New per-table version keys: `daily_observation_version = v2`,
     `forward_outcomes_version = v2`, `market_context_daily_version = v2`.
   - Bump `FORWARD_PATH_CHECKPOINTS_VERSION` v1 → v2 (checkpoint *set*
     stays the full 23 per the §3.6 decision; only the stamp bumps).
   - `SIGNAL_DEFINITION_KEY` +
     `SIGNAL_DEFINITION_V1 = "intraday_ret_0930_to_1000 > 0"` + version
     key (the `signal_*` columns embed the default signal definition).
   - `TERMINAL_EVENT_TYPES_V1` — 6 values per §3.6: `none`,
     `delisted_merger_acquisition`, `delisted_bankruptcy_liquidation`,
     `delisted_exchange_compliance`, `delisted_unknown`,
     `extended_halt_no_bars`. (String constants here, NOT the
     `enums.rs` derived-layer enums — see the warning at the top of
     `enums.rs`.)
   - `FIRST_EVENT_VALUES_V1` — `target_first / stop_first / neither /
     no_data`.
   - Extract the 9 inline target/stop label pairs into
     `TARGET_STOP_PAIRS_V6` so `hit_<pair>` and `first_event_<pair>`
     generate from one source and can never drift. Suffix = the label
     name with `hit_` stripped.
   - `MULTIDAY_HORIZONS` — the 9-horizon `1d…252d` suffix of
     `FORWARD_HORIZONS_V2`, used by the dividend columns.
   - Regime-threshold metadata key format for `regime_definitions`
     stamps (per-taxonomy bucket boundary values + computed-on window).
2. **Columns** (counts verified against §3.7)
   - `daily_observation` +14, `forward_outcomes` +42 (615 → 657),
     `forward_path_short` +4 (15 → 19), `market_context_daily` +6.
   - Note `censor_type` does NOT land in the path table — it lands in
     `forward_outcomes` as `first_event_<pair>` per the §3.2 refinement.
3. **Tests**
   - Update count assertions (657, 19) with extended arithmetic
     comments; assert new column families, enum constants, version
     bumps, grid lengths (`TARGET_STOP_PAIRS_V6.len() == 9`, etc.).
   - Extend the momentum-store roundtrip to write the version stamps +
     `signal_definition` as Parquet file-level key-value metadata and
     assert they survive read-back (the DoD's metadata clause).
4. **Docs** — v7 changelog entry in `phase0-observation-pivot-rfc.md`
   Appendix C referencing §3.7; add the per-table version keys to the
   §0.6 versioned-defaults table.

**Definition of done (from §3.7):** schemas + constants updated;
roundtrip test asserts new column counts, enum values, and metadata keys
(incl. `signal_definition`); RFC changelog v7 entry. Engine writer work
starts only after this lands.

---

## 3. Workstream B — the Phase 0 writer engine (~5–6 weeks)

### Architecture: single chronological sweep, day-major, bounded memory

The engine reads `data/bars_1m_raw/YYYY-MM-DD.parquet` in date order
(matching the per-day storage amendment) and maintains two pieces of
rolling state:

- **A ~7-trading-day ring buffer of full 1-minute bars** (≈1 GB/day) —
  computes all intraday/EOD/1d–5d forward outcomes and the sub-5d path
  checkpoints at exact 1m resolution (including `first_event_<pair>`
  ordering, which §3.7 requires exact at 1m).
- **A full in-memory per-security daily-aggregate matrix**
  (OHLCV+vwap per (security, day): ~12k securities × 2,513 days × a few
  floats ≈ 1–2 GB) — serves all trailing windows (ATR, ADV, betas,
  52w high/low) and all multi-day horizons (10d–252d), which need daily
  resolution, not 1m. Pre-loaded in a cheap first pass.

Day D's `forward_outcomes` row finalizes when D+5 has passed through the
ring buffer (short-horizon columns); long-horizon columns fill from the
daily matrix. Days near the end of the window get genuine nulls for
unfilled horizons — correct, not a bug. Splits applied at read time via
`BarReader` against the pinned `splits.parquet`; dividends joined as
events for the new `ret_<H>_total` columns.

Concurrency: rayon across securities within a day, days sequential
(rolling state forces order); a prefetch thread keeps the next day's
file decoded. Output mirrors input layout:
`data/outputs/<table>/YYYY-MM-DD.parquet`, every file stamped with the
v2 version constants + `signal_definition`.

### Milestones (each ends green-tested and committed)

| # | Milestone | Output | Est. |
|---|---|---|---|
| B0 | Engine skeleton: day iterator, ring buffer, daily-aggregate preload, SQLite resume cursor, metadata stamping, **1-day benchmark** | `write-phase0 --day 2021-03-15` writes a valid stamped file; measured sec/day + bytes/day → runtime & disk forecast | 3–4 d |
| B1 | `daily_observation` v2 + `market_context_daily` v2 (day-local + trailing state, no forward deps) | full columns incl. the +14/+6 | 1 wk |
| B2 | Small tables: `regime_definitions` (+threshold stamps), `earnings_calendar`, `sector_aggregates_daily`, `security_classification_daily` (rules in `momentum-classify`) | 4 tables | 1 wk |
| B3 | `forward_outcomes` intraday/EOD/1d–5d horizons + crossings + labels + `first_event_<pair>` | short-horizon outcomes | 1 wk |
| B4 | `forward_path_short` v2 (23 checkpoints, +4 state columns) | path table | 3–4 d |
| B5 | Multi-day horizons, dividend total returns, `bar_gap_minutes_max`, terminal events (merger-vs-delist split from Phase 0) | `forward_outcomes` complete at 657 | 1 wk |
| B6 | **Golden-day tests** (2–3 hand-verified days incl. a split day, an ex-div day, a halt, a delisting) + `scripts/validate_phase0_outputs.py` (cross-table checks, same style as the 67-check suite) + full historical run | all 8 tables, 2016-06 → 2026-06 | 1 wk |

**Tracer bullet runs early, not after B6:** the moment B3 writes one
month of output, the §5.1 step-0 vertical slice runs in Python (one
pre-chosen question, plain conditional sorts, full discipline). It
de-risks the §1.5.1 power question and the pipeline plumbing while
B4/B5 continue.

---

## 4. Workstream C — Phase 1 analytical layer (Python, ~3.5 weeks effort, 6-week box)

New `research/` directory. First deliverable is a thin **guard library**,
not an analysis: a data-access layer that (a) exposes only ML-safe
columns by allowlist, (b) mechanically refuses to read holdout dates
(2023-01-01+) unless a committed pre-registration file exists — the
§2.5/§8 leakage rules enforced in code, not willpower.

Then the §7.8 order exactly: step −1 power pass → step 0
synthetic-signal validation (§7.9) → step 0.5 placebo runs → MI matrix →
BN structure → cost-model derivation (§6.1) → gated DP/FQI per cell →
Markov-adequacy check → hierarchical Bayes → walk-forward stability →
BY at α = 0.10 → pre-registration YAMLs
(`pre_registrations/<cell_id>.yaml`, git-tagged) → holdout → live
shadow. Artifacts (`mi_matrix.parquet`, `cost_model.parquet`, DAGs,
policies) are versioned outputs in the repo or `data/research/`.

---

## 4.5 Validation plan (added 2026-06-11 after correctness review)

Three verification levels; every engine output column must reach L2
before the table it lives in is declared milestone-complete:

- **L1 — Rust unit tests.** Hand-computed expectations. Necessary, not
  sufficient: code and tests share an author (correlated blind spots).
- **L2 — Independent recomputation.** `scripts/validate_phase0_outputs.py`:
  a second implementation in polars, written from the column definitions,
  recomputing from the raw tape and diffing the full cross-section.
- **L3 — External ground truth.** Known market facts: real closes, VIX
  prints, earnings dates, split ratios, SPY-beta-vs-itself ≡ 1.

**Adjudication protocol (binding):** every L2 mismatch is root-caused to
ENGINE or VALIDATOR by raw-tape inspection before any further table is
built. Score so far: round 1 found 30 failure classes → 3 validator
bugs, round 2 found 1 real engine bug (UTC-midnight defense dropped
winter 19:00–20:00 ET bars — fixed) + 4 engine-vindicated semantics
(auction close in RTH, sid-first split factors, adjusted-basis volumes,
PM/AH-only rows kept) + 1 data-quality fact (16 vendor sid collisions
per day, both rows preserved).

**Plan:**

1. **Per-milestone gate (standing rule):** each B-milestone extends the
   validator to its new columns in the SAME commit — coverage never
   trails the engine by more than one milestone.
2. **Dedicated B1 validation session (next session, task #8):** extend
   L2 to every not-yet-covered B1 column — remaining snapshots
   (0940/0950/1010/1030), shape descriptors, ranks/percentiles,
   Yang-Zhang (exact recompute), betas (exact), signal
   freshness/concentration/HHI, gap-filled, days-since-move,
   market_context index columns + 10m list contents, sector means
   recomputed from obs + an independent SIC port — and run the battery
   over stress days: a half day (2016-11-25), both DST transitions, a
   rename day, a dup-sid day.
3. **Cheap full-sweep property checks, every day swept:** rank
   bijectivity, percentiles ∈ [0,1], high ≥ low, close within [low,
   high], bar counts ≤ window sizes, uniform milestone stamps.
4. **Golden-day fixtures (B6, unchanged):** hand-verified split
   (AAPL 2020-08-31), rename (FB→META 2022-06-09), ex-div, LULD halt,
   delisting days, once the full-window sweep exists.
5. **Determinism check:** sweep the same day twice, byte-compare.

## 5. Risks & open decisions

1. **Disk headroom** — decided by B0's measured bytes/day, not guessed.
2. **Engine runtime** — 2,513 days at unknown sec/day; B0's benchmark
   decides whether wider parallelism is needed. Re-runs are cheap by
   design (resume cursor).
3. **Long-horizon resolution** — assertion: 10d+ horizons compute from
   daily aggregates (1m resolution for runup/drawdown beyond 5d would
   force a 252-day bar buffer). The RFC checkpoint set (nothing between
   `5d_close` and the wide horizons) supports this; confirm against RFC
   §9 wording during B0 and flag if it disagrees.
4. **Cost-model calibration anchor** (§6.1: one month of a quotes tier
   vs. published spread studies) — a Workstream C decision, not blocking
   anything before §7.8 step 3.

---

## 6. Sequencing

**A (1 day) → B0…B6 (5–6 wks; tracer bullet forks off after B3) →
C (6-wk box).** Workstream A is the only thing on the critical path
today and is fully specified by §3.7. The only naming judgment call:
`first_event_<pair>` suffixes derive by stripping `hit_` from the frozen
label names, so the pairs can never drift.
