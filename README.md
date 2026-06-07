# Momentum Hold — Phase 0 Build Plan

This repo implements the experiment specified in
[`momentum-hold-phase0-frozen-v2.md`](./momentum-hold-phase0-frozen-v2.md).

Phase 0 is **not** a trading verdict — it builds a reusable trade-path dataset
so we can later evaluate giveback/trailing/profit-target rules without
re-running the simulation.

Stack: **Rust (2024 edition)**, Massive (Polygon) REST API + S3 flat files, Parquet/DuckDB for storage.

> **Language note.** An earlier revision of this plan targeted C++20. It was switched to Rust before any non-scaffolding code was written. The motivation lives in §3.0; the spec (`momentum-hold-phase0-frozen-v2.md`) is frozen and unchanged. Python is *not* used in Phase 0 — the downstream Parquet contract still has to round-trip cleanly to PyArrow (§4), and we may stand up Python services later (analyst notebooks, ML pipelines) once they're actually needed, but no Phase 0 milestone depends on a Python toolchain being installed.

---

## 1. What we're actually building

Two deliverables, in order:

1. **Ingestion + cache** — pull every required raw input from Massive once,
   land it on disk in an analytics-friendly format. The cache is the
   "source of truth" the spec demands ("Store the raw bar data used to
   generate every signal and every trade-path observation").
2. **Backtest engine** — pure function of the cache. Produces the two output
   tables defined by the spec:
   - `per_trade_bar_path` (one row per trade per 10-min bar for the full
     252-day horizon, stored as a thin join-index into `bars_1m` — see §6.5)
   - `per_trade_summary` (one row per trade, with materialized peak/trough/
     giveback aggregates so common analytical queries don't rescan the
     full path)

Everything else (giveback histograms, equity curves, etc.) is downstream
analytics on those two tables. We do **not** hard-code profit targets or
trailing exits — the spec is explicit about that.

---

## 2. Massive (Polygon) API surface we will use

We are on **Stocks Advanced ($199/mo)**. That entitles us to everything
except NBBO Quotes / Last Quote — neither of which Phase 0 needs.

### Required endpoints

| Purpose | Endpoint | Notes |
|---|---|---|
| Universe — list every common stock ever traded in window | `GET /v3/reference/tickers` | Paginate. Use `type=CS`, `market=stocks`, **and call once with `active=true` and once with `active=false`** to capture delisted names (mandatory per spec, otherwise label survivorship-biased). |
| Per-ticker metadata (delist date, type, primary exchange) | `GET /v3/reference/tickers/{ticker}` | Used to confirm common-stock status and pull `delisted_utc`. |
| Ticker type taxonomy | `GET /v3/reference/tickers/types` | One-time lookup to confirm CS vs ETF vs preferred vs warrant vs right vs unit codes. |
| **Historical 1-minute OHLCV bars, market-wide** | **S3 Flat Files** — endpoint `https://files.massive.com`, bucket `flatfiles`, path `us_stocks_sip/minute_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz`. Columns: `ticker, volume, open, close, high, low, window_start, transactions` (`window_start` is Unix **ns**). | **Primary historical ingest path.** Each daily file contains *every* ticker's 1-min bars for that day. ~2,750 files for the 2015→present window vs. ~15–20k REST calls per-ticker — two orders of magnitude fewer requests and effectively zero "API budget." **Stored as 1-min**; the engine rolls up to 10-min at read time (§6.2). Flat files are unadjusted raw tape; split adjustment is applied client-side per §5.3. See §11. |
| Same window, last 1–2 days only | `GET /v2/aggs/ticker/{ticker}/range/1/minute/{from}/{to}` (`t` is Unix **ms**) | **Fallback only**, for the lagging window before today's flat file publishes (typically ~T+1). 1-min raw, written into the 1-min tail partition (§5.1 step 5). |
| SPY 1-minute bars | Same flat files (SPY is one ticker among many in the daily file) | Falls out for free from the market-wide flat files. Drives `market_return_at_signal` and the trading-day calendar. |
| Splits | `GET /stocks/v1/splits` | Newer endpoint (older `/v3/reference/splits` is deprecated). Used both for split-adjustment audit and as a corporate-action event source. |
| Dividends | `GET /stocks/v1/dividends` | Pulled, *not* folded into bars. Dividends are joined to paths as events (§5.3.1) so a real ex-dividend price drop fires a real breakeven stop, matching what an actual broker order would do. Total-return analytics get a DuckDB view downstream. |
| Ticker events (renames, etc.) | `GET /vX/reference/tickers/{id}/events` | Needed to chain symbols across renames so a trade opened under `FB` follows through to `META`. Experimental endpoint — handle missing data gracefully. |
| Market holidays / status | `GET /v1/marketstatus/upcoming` | Forward-looking only — **insufficient for historical backtest**. We derive the historical trading calendar from SPY's own bar timestamps instead. The holidays endpoint is used at runtime only for scheduling future ingestion cron. |
| Exchange reference | `GET /v3/reference/exchanges` | Lookup table; used to filter to NYSE/Nasdaq listings if we want to exclude OTC pink sheets, and to interpret MIC codes in trade conditions. |

### Endpoints we explicitly do **not** use in Phase 0

- **Trades / Last Trade** — tick-level. Phase 0 is bar-level only; pulling
  ticks would balloon our API budget by ~3 orders of magnitude with no
  benefit.
- **Quotes / Last Quote** — not in our plan anyway, and Phase 0 has no
  spread/liquidity logic.
- **Technical Indicators (SMA/EMA/MACD/RSI)** — Phase 0 indicator is
  trivial (`price/open − 1`); we compute it from bars.
- **Snapshots / Top Movers** — live/near-live data, irrelevant to a
  historical backtest.
- **Fundamentals, Filings, Short Interest/Volume, Float, News, IPOs,
  Related Tickers, 13-F, Form 3/4** — explicitly out of scope per spec
  §Explicitly Deferred.

### Helpful Massive resources

- All-endpoints LLM index: `https://massive.com/docs/rest/llms.txt`
- Stocks section: `https://massive.com/docs/rest/stocks/llms.txt`
- Full stocks docs (for code-gen / sanity checks): `https://massive.com/docs/rest/stocks/llms-full.txt`

---

## 3.0 Why Rust (and what changes vs. the original C++ plan)

The original plan targeted C++20. Three concrete things made Rust the better fit for this workload before any non-scaffolding C++ was written:

1. **The §4 dependency pain disappears.** The original §4 warned that Arrow's transitive deps (Thrift, Protobuf, jemalloc, gRPC) make a from-source build a "3-day toolchain wrestle," that vcpkg setup is mandatory, and that DuckDB has to be vendored as a `duckdb.hpp` + `duckdb.cpp` amalgamation to avoid CMake hell. In Rust, `arrow`, `parquet`, `duckdb`, and `polars` are first-class Cargo crates with native implementations (`arrow-rs` is not an FFI binding). `cargo build` resolves the whole graph; no vcpkg, no FetchContent, no amalgamation.
2. **The type system enforces the schema discipline §4 already cares about.** Newtype wrappers for `SecurityId` / `DisplaySymbol`, `#[non_exhaustive]` enums for `Phase0ExitReason` / `PathEndReason` / `TerminalValueSource`, and `arrow`-derived schemas make "no implicit casting to PyArrow" a compile-time invariant rather than a runtime golden test. Mixing up `security_id` and `display_symbol` in a join becomes a type error.
3. **Concurrent ingest is ~1/5 the code.** `tokio` + `reqwest` (gzip feature) + `aws-sdk-s3` + a bounded `tokio::sync::Semaphore` replaces the libcurl-multi loop, custom 429 circuit breaker, and hand-rolled S3 client in the original §5.2.

What does **not** change:
- The spec (`momentum-hold-phase0-frozen-v2.md`) — frozen, byte-identical.
- The on-disk artifact set (§5.4 cache layout, the two output tables, the Arrow/Parquet schemas) — Parquet is a language-agnostic contract.
- The ingest design (§5), backtest design (§6), milestones structure (§7), and the property/golden tests (§8). Their *implementations* swap from C++ to Rust; the *plans* are the same.

What changes besides §3/§4:
- Pseudocode in §6.3 and §6.4 is now Rust.
- M0 (§7) drops "CMake + vcpkg" and becomes "Cargo workspace + hello-world REST + S3 fetch."
- M8 (§7) moves *back into Rust* (was Python notebook in the C++ plan, justified by avoiding C++ date math). With `polars` available natively, equal-allocation portfolio aggregation is ~150 lines of Rust over `per_trade_summary.parquet` — no Python toolchain dependency for Phase 0.

## 3. Repository layout (proposed)

```
sb-v0/
├── README.md                          # this file
├── momentum-hold-phase0-frozen-v2.md  # frozen spec (do not edit)
├── Cargo.toml                         # workspace manifest
├── Cargo.lock                         # checked in
├── rust-toolchain.toml                # pin to stable + 2024 edition
├── deny.toml                          # cargo-deny: license + advisory gates
├── .cargo/config.toml                 # build profile tweaks, target dir
├── crates/
│   ├── momentum-core/        # ID newtypes, schema, errors, bar/time types
│   ├── momentum-api/         # massive REST client + S3 flat-file fetcher
│   ├── momentum-calendar/    # trading day + ET/DST + half-day session
│   ├── momentum-store/       # arrow/parquet writers, DuckDB wrapper, bar reader
│   ├── momentum-universe/    # eligibility filter, rename-chain resolution
│   ├── momentum-signal/      # 10:00 AM ET signal
│   ├── momentum-engine/      # backtest loop, position book, path recorder
│   └── momentum-portfolio/   # equal-allocation aggregator (M8)
├── bin/                      # thin CLI front-ends (each = one bin crate)
│   ├── ingest/               # pull/refresh cache
│   ├── backtest/             # run Phase 0 over cache
│   ├── inspect/              # DuckDB queries over output tables
│   └── portfolio/            # M8 aggregator → equity curve + metrics
├── tests/                    # workspace-level integration + golden tests
├── benches/                  # criterion benches for the engine hot path
├── data/                     # gitignored cache root (unchanged from C++ plan)
│   ├── reference/            # tickers, splits, dividends, events
│   ├── bars_1m_raw/         # immutable 1-min, source-of-truth (per §5.4)
│   ├── bars_1m_deltas/      # sibling: incremental days (post-bulk), no glob collision
│   ├── bars_1m_tail/        # sibling: REST tail-window only
│   # (no derived bars_1m/ — split adjustment is applied at read time by BarReader; see §5.3 amended 2026-06-06)
│   └── output/               # per_trade_bar_path, per_trade_summary, portfolio/
└── xtask/                    # cargo-xtask: schema codegen, golden-test fixtures
```

**Crate boundaries are real and enforced.** Each `crates/momentum-*` is a separate library crate with its own `Cargo.toml`; modules inside re-export only what's needed publicly. This keeps incremental compile times sane (a change to the path recorder shouldn't rebuild the S3 client) and forces clear ownership of types — `SecurityId` is defined exactly once, in `momentum-core`, and every other crate depends on it.

### Bins currently present

| Bin | Purpose | Default output | When to run |
|---|---|---|---|
| `hello-rest` | M0 sanity: GET `/v3/reference/tickers/AAPL`, print decoded JSON. | stdout | Once, to verify API auth. |
| `hello-s3` | M0 sanity: GET one flat-file day, gunzip, print first 5 CSV rows. | stdout | Once, to verify S3 auth. |
| `smoke-universe` | List `type=CS&market=stocks` active + delisted, write to `data/_smoke/`. **No enrichment.** Reports coverage diagnostics (FIGI %, delisted_utc on inactive %). | `data/_smoke/reference/tickers.parquet` | Anytime, to re-measure list-endpoint coverage. |
| `smoke-day` | Download one flat file, decode, partition by ticker into `data/_smoke/`. Reports null-`security_id` count (universe-miss diagnostic). | `data/_smoke/bars_1m_raw/{T}.parquet` | Anytime, to spot-check a date end-to-end. |
| `build-universe` | **Production universe build.** Page list endpoint, run scoped per-ticker enrichment (only rows missing FIGI or delisted_utc), write to real path. | `data/reference/tickers.parquet` | Monthly per §11.3 reference cadence, plus any time renames/delistings happen. |
| `bulk-download` | **M3 stage 1.** Download every weekday's flat file in `[START_DATE, END_DATE]` into the staging dir. Bounded concurrency, retry+jitter, NoSuchKey → empty-success, SQLite resume cursor. `START_DATE` and `END_DATE` are required env vars (no accidental 80 GB pulls). | `data/_staging/flat_files/YYYY/MM/{date}.csv.gz` + `data/_ingest_state.sqlite` | Once for the historical backfill (effective window starts **2016-06-08** due to entitlement boundary, not the spec's 2015-01-01); incrementally for daily updates if a separate path doesn't take over. |
| `build-splits` | **M4.** Paginate `/stocks/v1/splits`, attach `security_id` via tickers FIGI map, write `splits.parquet` with `splits_snapshot_date` stamped in file-level Parquet metadata (= the read-time pin source per §5.3). | `data/reference/splits.parquet` | Whenever splits change (rare) or as part of a coordinated reference refresh. |
| `build-dividends` | **M4.** Paginate `/stocks/v1/dividends`, attach `security_id`, write `dividends.parquet`. Stored as events joined to per-trade summaries (§5.3.1); NOT folded into bars. | `data/reference/dividends.parquet` | Monthly with other reference refreshes. |
| `build-ticker-events` | **M4.** For every active+FIGI row in `tickers.parquet`, call `/vX/reference/tickers/{id}/events`. Bounded concurrency (default 10). Output is one row per rename event; figi_map can be derived from it. | `data/reference/ticker_events.parquet` | Monthly with other reference refreshes. |

`smoke-*` bins write under `data/_smoke/` so iterating doesn't clobber the production cache. `build-universe` writes to `data/reference/` and is the one wired to the cache the rest of the pipeline reads. Both share the same code path under the hood — `RestClient`, `enrich_universe_gap`, `write_tickers` — so the smoke output and the production output have identical Parquet schema and identical row semantics modulo the enrichment pass.

Env overrides on `build-universe`: `MASSIVE_API_KEY` (required), `MASSIVE_REST_BASE`, `OUT_PATH`, `ENRICH_CONCURRENCY` (default 10), `ENRICH_MAX_ATTEMPTS` (default 5), `SKIP_ENRICH=1` to disable the second pass.

**Measured coverage (2026-06-07).** After enrichment, FIGI is present on **79% of active** (4,191 / 5,289) and **43% of delisted** (2,837 / 6,512); `delisted_utc` is present on 96% of inactive rows. The enrichment gap that remains is bounded by Massive's detail endpoint — 3,845 of 4,958 enrichment calls returned 404, meaning the symbol isn't carried at all (typically old delisted tickers from before Massive's FIGI assignment era). **Engineering implication:** ~1,098 active tickers (21%) fall back to `display_symbol` as the join key and would silently lose data on rename. Phase 0 accepts the gap; the engine should LOG fallbacks so golden tests can pin the loss.

**Why a workspace, not a single crate.** A single-crate layout is fine for ≤10 modules; this design has ~8 logical components with non-trivial dependency surface (the engine depends on store + calendar + signal but not on the REST client). Workspace incremental rebuilds during M5–M7 (engine churn) will not retrigger Arrow C++ → Rust FFI compilation in the store crate.

---

## 4. Dependencies (Rust)

All deps below are Cargo crates resolved into a single `Cargo.lock`. No system package manager, no vcpkg, no FetchContent. The pinned MSRV is recorded in `rust-toolchain.toml`; CI verifies `cargo build --locked` on that exact toolchain.

| Crate | Why |
|---|---|
| **`tokio`** (full features) | Async runtime for the ingest pipeline. The S3 flat-file fetcher and the REST tail-window fetcher run thousands of concurrent transfers; a synchronous runtime would force a thread-pool model that's harder to rate-limit cleanly. The backtest engine itself (§6.3) is single-threaded — `tokio` is an ingest-only concern. |
| **`reqwest`** + `rustls-tls`, `gzip`, `json`, `stream` features | HTTP client for Massive REST. `gzip` feature replaces `CURLOPT_ACCEPT_ENCODING` (transparent ~10× JSON bandwidth saving); `stream` lets us pipe responses straight into Arrow record batches without buffering whole responses in memory. |
| **`aws-sdk-s3`** + **`aws-config`** | S3 client for the flat-file path. `BehaviorVersion::latest()` is required. We use `get_object` with byte-range streaming, not `download_file`, so the gunzip pipeline can be driven from the `ByteStream` without staging full files in RAM. |
| **`arrow`** (apache/arrow-rs) | First-class native Arrow implementation — *not* an FFI binding to Arrow C++. RecordBatch, Schema, Field, dictionary arrays, timestamp arrays-with-timezone all live here. We hold the canonical schemas (`per_trade_summary`, `per_trade_bar_path`, `bars_1m`, `bars_1m_raw`) as `OnceLock<SchemaRef>` constants in `momentum-core::schema`. |
| **`parquet`** + `arrow`, `async`, `zstd`, `snappy` features | Parquet reader/writer. `ArrowWriter` writes RecordBatches directly. `AsyncArrowWriter` is used for the daily-deltas writer (§5.1 step 4) so the ingest loop never blocks the runtime. Embeds the schema-version stamp in file metadata as required by §10. |
| **`polars`** + `lazy`, `parquet`, `temporal`, `dtype-categorical`, `dtype-struct` features | DataFrame layer for the M8 portfolio aggregator and the engine's read-time 1-min → 10-min rollup (§6.2). `LazyFrame::group_by_dynamic` over a 10-min window with `first/last/max/min/sum` aggs replaces a hand-written scan-with-state-machine. Polars uses `arrow` internally, so RecordBatch handoff to/from the Parquet writer is zero-copy. (Ingest itself no longer aggregates — 1-min flat-file rows land in Parquet unchanged; rollup is per-read in the engine.) |
| **`duckdb`** + `bundled` feature | Embedded SQL over Parquet for the `inspect` CLI and the §6.5 path-join view. `bundled` means the DuckDB amalgamation builds as part of the crate — no separate install step, no toolchain wrestling. |
| **`chrono`** + `serde` feature, **`chrono-tz`** | America/New_York timezone math. The signal time (10:00 ET), the entry-bar boundary (10:00–10:10 ET), the half-day session close, and the DST-transition days all go through `chrono_tz::US::Eastern`. **Never** call `Utc::now()` for session math; convert to/from ET explicitly at each boundary. |
| **`clap`** + `derive`, `env` features | CLI argument parsing for the four bin crates. `env` lets `MASSIVE_API_KEY` and `AWS_*` come from the environment without per-app glue. |
| **`tracing`** + **`tracing-subscriber`** + `env-filter`, `json` features | Structured logging. JSON output mode is used in CI / production; `pretty` for local dev. `RUST_LOG=momentum_api=debug` is the standard filter knob. |
| **`serde`** + `derive`, **`serde_json`** | JSON parsing for REST responses. Reference data (universe, splits, dividends, ticker events) is small enough that `serde_json` is fast enough; we do *not* pre-optimize with `simd-json` until a profiler asks for it. |
| **`rusqlite`** + `bundled` feature | SQLite for the persistent ingest cursor (§5.2). `bundled` avoids a system-libsqlite dependency. The cursor schema is `(ticker TEXT, last_completed_window_to INTEGER, status TEXT, last_error TEXT, attempt_count INTEGER)` — kept deliberately minimal. |
| **`flate2`** | Gzip decoder for the flat-file CSV streams. Used in tandem with `arrow::csv::ReaderBuilder` so a gzipped flat file is decompressed and parsed into RecordBatches in one streaming pipeline, never materialized in RAM. |
| **`anyhow`** (in `bin/` crates), **`thiserror`** (in `crates/`) | Library crates define typed error enums; binary crates wrap them in `anyhow::Result` at the top level. This keeps the `?` operator working everywhere without leaking `Box<dyn Error>` into the public API. |
| **`bytes`**, **`futures-util`** | Streaming plumbing. Required by `reqwest` and `aws-sdk-s3` for `Stream<Item = Result<Bytes>>` handling in the byte-range download path. |

**Dev-dependencies:**

| Crate | Why |
|---|---|
| **`proptest`** | Property-based testing. Exit-rule invariants (`phase0_exit_fill_price <= entry_price`), schema round-trips, adjustment kernel roundtrips (§5.3, §8). |
| **`insta`** + `yaml`, `json` features | Golden-snapshot tests. The hand-verified trades from §8 become `assert_yaml_snapshot!` calls; diffs are reviewed via `cargo insta review`. |
| **`pretty_assertions`** | Diff-friendly assertion output for the golden tests. |
| **`tempfile`** | Scratch directories for integration tests that exercise the on-disk cache. |
| **`mockito`** *or* **`wiremock`** | HTTP mocking for the Massive REST client; lets us exercise pagination + retry + circuit-breaker logic without real API calls. |
| **`criterion`** | Benchmarks for the path-join hot path and the engine inner loop. Optional, only wired up if a profiling question comes up. |

**Workspace policy crates:**

| Crate | Why |
|---|---|
| **`cargo-deny`** (config in `deny.toml`) | License + advisory + duplicate-version gate. Run in CI. |
| **`cargo-nextest`** | Faster test runner than `cargo test`; used by CI. Local `cargo test` still works. |
| **`cargo-xtask`** pattern (the `xtask/` crate) | Schema codegen + golden-fixture generation; replaces a `Makefile` or shell scripts. |

**Build is `cargo build --workspace --locked`.** No external build system. Target stable Rust, 2024 edition, pinned in `rust-toolchain.toml`.

**Schema discipline (cross-language contract).** The Parquet/Arrow output files are the only contract downstream tooling sees — Phase 0 has no downstream consumer *now*, but the contract is what future Python notebooks, PyArrow ML pipelines, and the §10 agentic-sim work will read. Hold one canonical Arrow schema per output table as a `OnceLock<SchemaRef>` in `momentum-core::schema`, embed its version in Parquet file metadata, and have one place that asserts the schema before any write. Type choices round-trip cleanly to PyArrow with **zero implicit casting**:

- timestamps as `timestamp[ns, tz="UTC"]` (never `int64` epoch millis) — in Rust this is `DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))`;
- monetary values as `float64` — *not* `decimal`. DuckDB/PyArrow handling of `decimal` diverges on edge cases and Phase 0 doesn't need decimal precision. The `f64` type ensures this at write time;
- enums (`phase0_exit_reason`, `path_end_reason`, `terminal_value_source`) as Arrow `dictionary<string, int32>`. In Rust these are `#[non_exhaustive]` enums with a `&'static str` discriminant and an `arrow::array::DictionaryArray<Int32Type>` writer — the enum-to-dictionary mapping is the single source of truth, not duplicated per write site;
- categorical IDs (`trade_id`, `security_id`) as `string`, sortable lexically.

A schema change bumps the version field in the file metadata; the reader refuses files outside its compatibility range. **The version is derived from a stable hash of the canonical schema bytes** (`blake3(schema.to_canonical_bytes())`, first 8 hex chars), not a hand-bumped integer — so changing the schema and forgetting to bump the version is unrepresentable; the version moves automatically with the schema.

**Identifier discipline — `security_id` is identity, `symbol` is decorative.** Stock tickers rename (FB→META), get reused (defunct ticker → new company later), and change share class (GOOG/GOOGL). **Treat the ticker symbol as a display attribute, not a key.** In Rust this is enforced by newtypes:

```rust
// in momentum-core::ids
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SecurityId(String);   // composite FIGI (or share-class FIGI fallback)

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DisplaySymbol(String); // the ticker as it appeared at a given timestamp
```

`SecurityId` and `DisplaySymbol` are *not* interchangeable — a function that joins on identity takes `&SecurityId`, a function that renders for a human takes `&DisplaySymbol`, and the compiler enforces the distinction. The rename chain from `/vX/reference/tickers/{id}/events` is the only code path that converts between them, exposed as `RenameChain::resolve(symbol: &DisplaySymbol, at: DateTime<Utc>) -> SecurityId`.

`per_trade_summary` therefore has both `security_id` and `entry_display_symbol`; `bars_1m/` files are still partitioned by *current* display symbol for fast file-level lookup, but every row carries `security_id` as the durable identity. Engine joins use `security_id`.

**No file is rewritten on rename.** Pre-rename bars stay in their original `bars_1m/{old_symbol}.parquet`; post-rename bars accumulate in `{new_symbol}.parquet`. The bar reader takes `(SecurityId, t_range)`, resolves the affected display symbols via the rename chain in `figi_map.parquet`, and UNIONs the matching files. The only operation that rewrites a `bars_1m/` file is split-snapshot recomputation (§5.3); renames never trigger one.

**Closed-set enums catch the path-end ambiguity at compile time.** Per §6.6, bankruptcies and acquisitions must not share a terminal code path. In Rust:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PathEndReason {
    HorizonReached,
    Delisted,           // bankruptcy / dropped — last reliable pre-delist bar
    Halted,             // last reliable bar + halted_flag = true
    Merged,             // last bar + terminal_value_source = LastBarUnderDeal
    EndOfData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TerminalValueSource {
    LastBarHorizon,
    LastBarPreDelist,
    LastBarHalted,
    LastBarUnderDeal,   // §6.6: acquisitions; deal price backfill later
    LastBarEndOfData,
}
```

Forgetting a branch in a `match` is a compile error; the spec's symmetric-terminal goal becomes a type-system invariant.

---

## 5. Ingestion design

The ingest step is the riskiest and most expensive — design it to be
**idempotent, resumable, and cheap to re-run**.

### 5.1 Order of operations

1. **Universe snapshot**
   - Page through `/v3/reference/tickers?type=CS&market=stocks&active=true`
   - Page through same with `active=false` (delisted)
   - Persist to `data/reference/tickers.parquet` with `as_of_date`.
   - For each ticker, hit `/v3/reference/tickers/{ticker}` to capture
     `delisted_utc`, `primary_exchange`, `cik`, `composite_figi` (needed
     to chain renames). Cache this; only refresh on a schedule.

2. **Corporate actions, universe-wide**
   - `/stocks/v1/splits` (paginate, no ticker filter needed — pull all).
     Used both for the split-adjustment audit and as an event source for
     re-pull triggering (§5.3).
   - `/stocks/v1/dividends` (paginate). Snapshotted to
     `dividends.parquet`; **not** folded into bars. Joined to paths as
     events per §5.3.1.
   - For each ticker that renamed, hit `/vX/reference/tickers/{id}/events`.
     The rename chain is keyed on composite FIGI so the engine can
     resolve symbol → canonical entity at path lookup time (§6.3).

3. **Historical 1-min bars, market-wide, via S3 flat files (bulk batch — *not* daily append)**

   Parquet is a write-once file format. You cannot efficiently append
   rows to an existing `.parquet`; the only options are full rewrite or
   proliferating small delta files. So the historical ingest is one big
   batched scan, not a per-day write loop.

   - **Stage 1 — bulk download.** Download every flat file in
     `[2015-01-01, T−1]` into `data/_staging/flat_files/YYYY/MM/YYYY-MM-DD.csv.gz`.
     Concurrent S3 transfers (§5.2). The SQLite ingest cursor (§5.2)
     keys on `(date, status)` since one file == one day.
   - **Stage 2 — one-pass per-ticker pivot.** After staging is complete,
     run a single end-of-bulk scan: stream every daily gzipped CSV
     through DuckDB into a `(ticker, window_start, open, high, low,
     close, volume, transactions)` partitioned writer. Each per-ticker
     Parquet file `data/bars_1m_raw/{ticker}.parquet` is written
     **exactly once**, sorted by `t`, with no per-year rewrite. This
     avoids the ~5.8-day wall-clock cost of yearly rebuilds; a single
     end-of-bulk pivot is ~hours of streaming I/O on a laptop. No 1-min
     → 10-min aggregation runs at ingest — rows land in Parquet
     unchanged; engine rollup is per-read (§6.2).
   - **Stage 3 — purge staging.** Once Stage 2 finishes successfully,
     drop the staged CSV.gz files from `_staging/` — they're
     reproducible from S3 if needed and they're ~50 GB/year uncompressed.

   - Flat files are **unadjusted raw tape**. Split adjustment runs as a
     deterministic post-step (§5.3); raw is never modified.

4. **Daily incremental (T onward) via `bars_1m_deltas/`**

   Once the historical bulk load is done, *new* trading days do **not**
   get rewritten into per-ticker partitions on arrival — that would
   force ~10k rewrites per day. Instead:

   - Each new trading day's flat file lands *whole* (still 1-min, no
     aggregation) at `data/bars_1m_deltas/YYYY-MM-DD.parquet` — one
     file per day, all tickers, sorted by `(ticker, t)`. **Note the
     sibling-directory layout, not a sub-folder of `bars_1m_raw/`** —
     this is deliberate so that `read_parquet('bars_1m_raw/*.parquet')`
     in the engine view never accidentally globs delta or tail files.
   - The bar reader exposes a DuckDB view that UNIONs the per-ticker
     historical partitions with all of `bars_1m_deltas/*.parquet`:
     ```sql
     CREATE VIEW bars_1m_raw_v AS
       SELECT * FROM read_parquet('bars_1m_raw/*.parquet')
       UNION ALL
       SELECT * FROM read_parquet('bars_1m_deltas/*.parquet')
       UNION ALL
       SELECT * FROM read_parquet('bars_1m_tail/*.parquet');
     ```
     Predicates on `(security_id, t)` push down into all three halves
     (every row carries `security_id` per §4, so engine joins go
     through that key, not the display symbol).
   - **Monthly compaction:** fold accumulated deltas back into per-ticker
     partitions (one rewrite per ticker), then clear `bars_1m_deltas/`.
     Deterministic, idempotent, safe to interrupt and resume.

5. **Tail-window bars via REST** (flat files lag by ~T+1)

   For the most-recent ~1 trading day before the flat file publishes,
   hit `GET /v2/aggs/ticker/{T}/range/1/minute/{from}/{to}?adjusted=false&limit=50000`
   for tickers in the eligible universe. Write to its own sibling dir
   `data/bars_1m_tail/_rest_tail.parquet` (not under `bars_1m_raw/`).
   Clear once the next flat file lands and the delta replaces the data.
   This is the only REST call in the bar-ingest path.

   **Timestamp precision.** REST aggregates ship `t` as Unix **ms**;
   flat files ship `window_start` as Unix **ns**. At write time, cast
   `t * 1_000_000` and store the column as
   `timestamp[ns, tz="UTC"]` so the tail file is bit-compatible with
   the per-ticker and delta partitions before the UNION view sees it.
   Without this cast, a `(security_id, t)` join against flat-file bars
   silently drops or duplicates rows at the boundary day.

### 5.2 Rate-limit + concurrency

- **REST surface is now narrow** (reference data + tail-window aggregates only) so concurrency tuning matters less than it did when REST was the bulk path. Still: start at ~10 inflight, measure, ramp. The cap is a `tokio::sync::Semaphore` with `acquire_owned()` held for the full request lifetime, so backpressure propagates naturally through `tokio::spawn`'d tasks.
- **Always request `Accept-Encoding: gzip`** on REST calls. JSON responses compress ~10×; `reqwest`'s `gzip` feature decompresses transparently when the client is built with `.gzip(true)`. Free bandwidth — this replaces the `CURLOPT_ACCEPT_ENCODING` of the original C++ plan with zero additional code.
- Retry on 429/5xx with exponential backoff + jitter. A sustained 429 storm trips a circuit breaker that halves the inflight cap and pauses for 60s before resuming. The breaker is a single `Mutex<BreakerState>` shared across spawn tasks; we deliberately do *not* reach for a fancier reactive-streams library, since the state is small and contention is negligible at 10 inflight.
- **S3 flat-file downloads** use a separate, larger concurrency pool (start at ~16 transfers) since the bottleneck there is bandwidth, not API quota. The pool is a second `Semaphore` and a second `aws_sdk_s3::Client` with `HttpClient` tuned for higher per-connection throughput (`s2n-tls` defaults are fine; do *not* downgrade to native-tls).
- Cap memory: stream JSON → Arrow `RecordBatch` → Parquet writer; never collect a whole response into a `String` or `Vec<u8>`. `reqwest::Response::bytes_stream()` plus `futures_util::StreamExt::next()` feeds bytes incrementally. Same rule for flat-file ingest: `aws_sdk_s3::primitives::ByteStream` → `flate2::read::GzDecoder` → `arrow::csv::ReaderBuilder` → `parquet::arrow::AsyncArrowWriter`. No aggregation pass — 1-min rows pass through unchanged. Never materialize a full day's market-wide CSV in RAM (one day is ~50 GB uncompressed for the full universe).
- **Persistent ingest cursor (SQLite via `rusqlite`).** Recomputing what's missing from the JSON manifest on every restart is O(universe) and gets slow on partial failures. Maintain a `data/_ingest_state.sqlite` with `(ticker TEXT, last_completed_window_to INTEGER, status TEXT, last_error TEXT, attempt_count INTEGER)`, written transactionally after each successful Parquet flush. The `rusqlite` connection lives behind a `Mutex<Connection>` in the ingest crate; cross-task access is via `tokio::task::spawn_blocking` because `rusqlite` is sync. (We do not use `sqlx` here — its async-from-the-start design buys us nothing for a single-process, write-mostly cursor table, and `rusqlite` + `bundled` is a smaller dependency surface.) Reboot resumes from the cursor in milliseconds; the JSON manifest remains the human-readable summary of what's on disk.

### 5.3 Split adjustment is applied at read time, not materialized (frozen, amended 2026-06-06)

Flat files ship **unadjusted raw tape**. We do **not** materialize a
derived `bars_1m/` directory. The split factor is applied **at read
time** by the bar reader, against a pinned `splits.parquet` snapshot.
Cash dividends remain stored as events, not folded into the price line.

**This section was amended 2026-06-06 (decision 0a).** Earlier drafts
required a derived `bars_1m/{T}.parquet` directory written by an
offline split-adjustment kernel. We dropped that for three reasons:
(1) the multiply is sub-millisecond per ticker, so the materialized
file saves no engine wall-clock; (2) it halves disk (~50 GB → ~25 GB
for the full historical window) and removes a whole pipeline stage;
(3) per-file `splits_snapshot_date` metadata stamps were the messy
part of the old design — a single run-level pin against
`splits.parquet`'s own snapshot date is simpler and equally safe.

Why splits are applied, dividends are not:

- The Phase 0 exit rule is a price-based breakeven stop: `bar.low <=
  entry_price`. On ex-dividend day, the price *really does* drop by
  approximately the dividend amount — that's mechanical, not noise.
  A real stop order at a real broker would fire on that drop.
- If we dividend-adjust the price line so the drop disappears, the
  simulated stop becomes systematically *more lenient* than a real
  stop on the same stock. The path no longer reflects "what would a
  resting breakeven stop have done"; it reflects "what would the path
  look like if dividends were free money the trader collected without
  any price consequence." Those are different counterfactuals.
- The trader does receive the dividend, but that's an income stream,
  not a property of the price path. Modeling it separately (annotated
  on the path, not adjusted into the bars) preserves both views:
  engine mechanics see real price drops; total-return analytics can
  add the dividend stream back downstream.
- The spec sanctions this directly: "Dividend adjustment is documented
  separately (note whether it was applied)." We document: **not applied
  to bars; tracked as events.**

Pipeline:

```
bars_1m_raw/{T}.parquet  (canonical, from flat files, immutable)
        + splits.parquet  (snapshot, stamped at file level via parquet metadata)
        ↓
        BarReader::session_bars(sid, day)  — multiplies raw OHLC × split_factor_cum,
                                              divides raw volume ÷ split_factor_cum,
                                              against the run-pinned snapshot
        ↓
        (in-memory adjusted bars; never written to disk)

dividends.parquet  (snapshot, stamped — joined alongside paths in analytics, never into bars)
```

**Adjustment math (applied at read time):**

```
adjusted_price  =  raw_price  ×  split_factor_cum
adjusted_volume =  raw_volume ÷  split_factor_cum
```

- A 2-for-1 split halves price *and* doubles share count → both raw
  price and raw volume need restating onto the current-share basis.
- Dividends do not enter either equation.

Rules:

- **Splits applied at BarReader read time; dividends not applied at
  all.** Both reference parquets are pulled, snapshotted, and stamped;
  only splits feed the read-time multiply.
- **`splits.parquet` carries `splits_snapshot_date` in its file-level
  Parquet metadata.** This is the single source of truth for the pin.
  No per-bar-file stamps; raw bars are immutable and have no
  adjustment basis to stamp.
- The eligibility comparison `bar.low <= entry_price` operates on
  read-time-adjusted bars under a single snapshot. Phantom-split-gap
  risk is eliminated by construction; ex-dividend drops are *not*
  eliminated and that is the intended behavior.
- **Snapshot-pin per backtest run (frozen).** A backtest run pins
  `splits_snapshot_date` at startup (read from `splits.parquet`
  metadata) and **refuses to apply any split row with
  `effective_date > pin`**. Reason: a stored `entry_price` is
  denominated in the adjustment basis that was current when the trade
  was opened. If `splits.parquet` is refreshed mid-run with a fresh
  split, every open trade's `entry_price` is now in stale units and
  the breakeven check silently misfires. Pinning the snapshot keeps
  `entry_price` and every subsequent bar in the same units for the
  life of the run. To incorporate a new split: re-snapshot
  `splits.parquet`, then start a fresh run against the new snapshot
  date — no bar files need rebuilding.
- Audit roundtrip (tested as property §8):
  - `adjusted_price ÷ split_factor_cum == raw_price`
  - `adjusted_volume × split_factor_cum == raw_volume`
  - Both within float64 epsilon.
  - Now a property over the read-time multiply function, not over an
    offline kernel.

### 5.3.1 Dividend handling downstream

Cash dividends are joined to paths as events, not bars:

- For every trade open across an ex-dividend date for its symbol,
  `per_trade_summary` carries:
  - `dividend_events_during_path` — count of ex-dates the position
    crossed.
  - `total_cash_dividends_per_share` — sum of dividends paid per share
    during the path (in dollars).
- A DuckDB view exposes a `total_return_view` that adds dividends back
  to the price-only return for analytics that want total return. The
  view is downstream; the engine path stays price-only.
- If a future phase wants dividend-adjusted bars (e.g., to compare to a
  hypothetical dividend-aware stop), that view can be promoted to a
  materialized table without touching the engine.

### 5.4 Cache layout

```
data/bars_1m_raw/{ticker}.parquet          # unadjusted 1-min, immutable, source of truth — engine reads this and applies split factor at read time (§5.3)
data/bars_1m_deltas/YYYY-MM-DD.parquet     # incremental days, sibling dir so the bars_1m_raw/*.parquet glob never picks them up (§5.1 step 4)
data/bars_1m_tail/_rest_tail.parquet       # REST tail-window only, sibling dir (§5.1 step 5)
data/reference/tickers.parquet
data/reference/splits.parquet               # stamped with snapshot date (file-level Parquet metadata) — pin source for the run (§5.3)
data/reference/dividends.parquet            # stamped with snapshot date; joined to paths as events (§5.3.1), not folded into bars
data/reference/ticker_events.parquet        # rename chain: (display_symbol, t) → security_id
data/reference/figi_map.parquet             # (security_id, display_symbol, valid_from, valid_to) — sourced from ticker_events
data/output/eligibility_log.parquet         # (date, security_id, eligible, reason) — persisted per-day decisions
data/_staging/flat_files/YYYY/MM/...        # transient bulk-download staging (purged after partitioning)
data/_manifest.json                         # human-readable summary of coverage
data/_ingest_state.sqlite                   # resume cursor (§5.2)
```

SPY no longer needs its own file — it's just one ticker among many in
the daily flat files, so it lives in `bars_1m_raw/SPY.parquet` like
everything else. The trading-day calendar is derived from
`SELECT DISTINCT date FROM bars_1m_raw/SPY` at engine startup.

Partition-by-ticker is fine for Phase 0's scale (~10k tickers). If we
later scale to ticks, switch to date-partitioned.

**`eligibility_log.parquet` (persisted decisions, not re-derived).**
For every (date, security_id) the engine considered, we write one row
with `eligible` (bool) and `reason` (dictionary-encoded: e.g.
`'ok'`, `'not_common_stock'`, `'delisted_before_date'`,
`'missing_0930_bar'`, `'missing_1010_open'`, `'currently_held'`,
`'not_listed_yet'`). Size: ~10k tickers × ~2,750 trading days = 27M
rows × ~30 bytes each = ~800 MB compressed — trivial. Why persist
rather than re-derive on demand:
- The reference data the engine used at run time may have been updated
  since (universe lists shift, delist dates get corrected). Re-deriving
  later gives a *different* eligibility set than the engine actually
  saw. Persisting locks in the audit trail.
- "Why didn't this ticker fire a signal on day D?" becomes a one-line
  DuckDB query instead of a re-run.
- Required for the survivorship golden test (§8): assert that a
  known-delisted name has rows in `eligibility_log` with the right
  pre-delist / post-delist transitions.

---

## 6. Backtest engine design

### 6.1 Trading-day calendar

Take the set of distinct ET-dates appearing in `bars_1m_raw/SPY.parquet`. Those
are our trading days. This sidesteps the holidays-endpoint-is-only-forward
limitation entirely.

### 6.2 Bar-boundary semantics (per spec)

- `session_open_price` = open of the **09:30–09:40** bar.
- `price_at_10am` = close of the **09:50–10:00** bar.
- Entry fill = open of the **10:00–10:10** bar.
- Stop becomes live on the **10:10–10:20 bar onward** — i.e. the very next
  10-min bar after the entry bar. An earlier draft said "11:10," which
  was wrong and would have left every position unprotected for its first
  hour, silently changing which trades survive their first hour and
  therefore the entire trade distribution. Fixed.

Encode this as an enum/struct of canonical bar indices for a session;
do not hand-roll timestamp math at each call site.

**Read-time 1-min → 10-min rollup.** Storage is 1-min (§5.4); the
engine reads 10-min. `BarReader::session_bars(sid, day)` returns 10-min
bars built on the fly from the underlying 1-min rows:

```rust
// inside momentum-store::BarReader
fn session_bars(&self, sid: &SecurityId, day: NaiveDate) -> Result<Session> {
    let one_min = self.scan_1m(sid, day)?;  // ≤390 rows (≤210 on a half-day)
    one_min
        .lazy()
        .group_by_dynamic(col("t"), [], DynamicGroupOptions {
            every:  Duration::parse("10m"),
            period: Duration::parse("10m"),
            offset: Duration::parse("0"),
            ..Default::default()
        })
        .agg([
            col("open").first(),
            col("high").max(),
            col("low").min(),
            col("close").last(),
            col("volume").sum(),
        ])
        .collect()
        .map(Session::from_frame)
}
```

The rollup is microseconds per (ticker, day) on 390 rows; the engine's
hot path is Parquet I/O on the 1-min file, not aggregation CPU. If a
profiler ever flags this we can materialize a derived 10-min cache, but
Phase 0 doesn't need it.

### 6.3 Daily loop (pseudocode)

```rust
// The engine is single-threaded — no async, no tokio. Ingest is async;
// backtest is a hot inner loop over Arrow RecordBatches read from
// Parquet, and synchronous code is straightforwardly the fastest
// shape for it. (Per §6.3-rationale, profiling can revisit later.)

for day in trading_days.iter() {
    // 1. Universe at this date — yields (SecurityId, DisplaySymbol) pairs
    //    for common stocks not yet delisted as of `day`.
    let eligible: Vec<(SecurityId, DisplaySymbol)> = universe.snapshot(*day);

    // 2. Score every eligible security once at 10:00 ET. Key everything on
    //    SecurityId; DisplaySymbol is carried only for the entry record.
    for (sid, disp) in &eligible {
        let bars = bar_store.session_bars(sid, *day)?;     // ≤39 bars, half-day aware (§6.8)
        let (Some(open_0930), Some(close_1000), Some(open_1010)) = (
            bars.open_at(BarBoundary::Open0930),
            bars.close_at(BarBoundary::Close1000),
            bars.open_at(BarBoundary::Open1010),
        ) else { continue; };

        let signal_return = close_1000 / open_0930 - 1.0;
        if signal_return > 0.0 && !book.is_open(sid) {
            book.enter(EntryRecord {
                security_id:    sid.clone(),
                display_symbol: disp.clone(),
                entry_date:     *day,
                entry_price:    open_1010,
                signal_return,
                market_return_at_signal: spy_return_0930_to_1000(*day),
            });
        }
    }

    // 3. Advance every open position by one trading day; record path; check exit.
    //    `advance_to(day)` walks every 10-min bar on `day` that is strictly AFTER
    //    each position's entry bar — for a same-day entry that means the 10:10–10:20
    //    bar onward (per §6.2). Positions entered on `day` therefore CAN stop out
    //    on `day` itself; the entry bar (10:00–10:10) cannot trigger the stop
    //    because the spec's entry-bar guard exempts it.
    book.advance_to(*day)?;
}
```

A few things this snippet expresses that the C++ version did not:

- **`BarBoundary` is an enum, not an integer.** `bars.close_at(1000)` in the original C++ pseudocode is a hand-rolled lookup keyed on an int. The Rust version makes `BarBoundary::{Open0930, Close1000, Open1010, …}` a closed-set enum defined in `momentum-calendar`; a typo turns into a compile error instead of a silently-missing bar.
- **`session_bars` returns `Result<Session>`.** A missing or malformed bar file is a typed error (`StoreError::MissingBars { security_id, date }`), not a silent empty return. The signal scan continues with `?`-propagation; the daily loop logs and proceeds with the next security.
- **`book.is_open(sid)` takes `&SecurityId`.** Passing a `DisplaySymbol` here is a compile error — the rename-continuity bug class (an entry registered under FB but the open-check done under META) cannot be expressed.

**Symbol continuity across renames.** Because the engine keys positions, the path index, and `bars_1m` joins on `SecurityId` (composite FIGI), a mid-path rename (FB→META, share-class rejigs, etc.) is invisible to the daily loop: `bar_store.session_bars(sid, day)` resolves the affected display symbols via the rename chain (§4) and UNIONs the matching `bars_1m/{sym}.parquet` files. If the engine ever keyed on `DisplaySymbol` instead, every path would silently go empty after a rename date and the trade would look terminated when it wasn't. The newtype distinction makes this a type error rather than a runtime golden-test catch; golden test on FB→META in §8 stays as a belt-and-braces check.

### 6.4 Exit logic (verbatim from spec — pin it in a single function)

```rust
// Called once per 10-min bar AFTER the entry bar.
// Lives in momentum-engine::position; every exit decision in the
// entire codebase goes through this function — there is no other
// place that compares bar.low to entry_price.
impl Position {
    pub fn check_breakeven_stop(&self, b: &Bar) -> Option<Fill> {
        if b.low <= self.entry_price {
            Some(Fill {
                price: self.entry_price.min(b.open),
                when:  b.t,
            })
        } else {
            None
        }
    }
}
```

`phase0_exit_date` is recorded as a **marker**; the path continues for 252 trading days regardless, per spec §Trade Path Tracking.

The Rust signature carries one invariant the C++ version did not: `&self` is borrowed immutably and `&Bar` is borrowed immutably, so the exit check provably cannot mutate the position or the bar. Position state changes (`mark_exited`) are separate methods on `&mut self`, called by the path recorder only after the `Option<Fill>` is examined. The accidental "stop check overwrites entry_price" bug class is unrepresentable.

**Label this rule honestly: bar-level idealized stop-fill model.** It
is *not* a real-broker fill simulation. Specifically:

- A real stop-market order can fill *below* `entry_price` due to
  slippage, especially on a gap-through. The Phase 0 rule caps the fill
  at `min(entry_price, bar.open)` — a small gap-through is modeled, but
  intrabar slippage on a non-gap touch is **assumed to be zero**.
- A bar that opens above entry, touches below entry intrabar, and
  closes above entry fires the stop at `entry_price` in our model.
  Whether a real broker stop would have actually filled depends on
  liquidity and bar microstructure that we deliberately don't model.

Phase 0 is observational; this idealization is consistent across all
trades, deterministic, and frozen. When a future phase wants
real-broker-realism, it adds a slippage model on top of these paths
without rerunning the simulation.

### 6.5 Path recording (frozen: full 10-min resolution, stored as a join-index)

The whole point of Phase 0 is to make *future* exit rules (trailing stops,
giveback exits, profit targets) evaluable on this dataset without rerunning.
Those rules are intrinsically intrabar — a daily-rollup path would silently
defeat them: a 2%-giveback rule on daily bars gives a different and wrong
answer than the same rule on 10-min bars, because the giveback and the
recovery can both happen inside a single day that closes flat.

**Decision:** record the path at **full 10-min bar resolution for the full
252-day horizon.** No daily-rollup tier.

**Storage layout — thin join-index, not duplicated bars.** A naive
duplication ("OHLCV per trade per bar") re-stores the same bar once per
overlapping open position; with hundreds of concurrent positions on busy
days, the path table blows up 100×+ for zero information gain. Since
`bars_1m` is already source of truth (and the spec says path metrics are
"recomputable from" the raw bars), store the path as a thin index that
references it:

```
per_trade_bar_path columns:
  trade_id, security_id, entry_date, t, periods_held
  -- (optional) halted_flag, split_event_id, dividend_event_id for context
```

OHLCV is recovered by joining `(security_id, t)` back to a 10-min
view over `bars_1m` (via the rename chain at file-resolution time —
§4). The view is the same `time_bucket(window_start, INTERVAL '10
minutes')` rollup the engine uses at read time (§6.2). `entry_price`
and `entry_display_symbol` live in `per_trade_summary` and are joined
in when needed; the path index itself carries no symbol column,
because `security_id` is the durable identity and `display_symbol`
varies along the path under renames. DuckDB joins hundreds of millions
of keyed rows against partitioned Parquet comfortably; this is
strictly cheaper than duplication, in both storage and typical
analytical query time.

**Convenience columns** (`current_return`, `peak_return`,
`giveback_pct_of_peak`, etc.) are a DuckDB view over the join. The
expensive-once / read-many ones (`peak_return`, `trough_return`,
`time_to_peak`, `max_giveback_pct_of_peak`, `days_to_first_profit`) are
**materialized into `per_trade_summary`** so the typical analytical query
doesn't run window functions over ~250M rows every time.

### 6.6 Terminal accounting

- `phase0_exit_reason`: `breakeven_rule` if stop hit during horizon,
  `open_at_end` if it never did.
- `path_end_reason`: `horizon_reached | delisted | halted | merged | end_of_data`.
- `terminal_return` source by `path_end_reason` — **bankruptcies and
  acquisitions must not share a code path**, or the spec's symmetric-
  terminal goal silently breaks (the last-bar rule clips winners but
  fairly captures losers):
  - `horizon_reached`, `end_of_data`: mark-to-market at the last bar in
    `bars_1m`.
  - `delisted` (bankruptcy / dropped — **not** an acquisition): last
    reliable bar before the delist date. For genuine liquidations the
    final tape print is approximately the terminal value, so this is
    fine.
  - `halted`: same rule, plus `halted_flag = true` so future reopens can
    be re-examined without rerunning.
  - `merged` (cash or stock acquisition): the tape's final print is
    typically a few cents *under* the deal price; the real terminal
    value is the deal consideration, which isn't in the bar cache.
    Phase 0 records the last-bar value but sets
    `terminal_value_source = 'last_bar_under_deal'` so the deal price
    can be backfilled from corporate-action data without rerunning the
    sim. Accepted bias for Phase 0: acquisition payoffs are slightly
    understated, and we know exactly which trades that affects.
- **Delist-date boundary:** a stock delisting on date D typically still
  has bars *through* D, and an open trade is valid through D. The
  universe-snapshot predicate is `delisted_date IS NULL OR
  delisted_date > day_being_evaluated`. Off-by-one here either drops a
  valid final day or fabricates a phantom one — golden-test it.

### 6.7 Portfolio layer — Rust bin crate using polars-rs

Trade-level returns ignore allocation entirely; `momentum-engine` emits `per_trade_summary` and stops there. Portfolio aggregation is a pure tabular operation on that table.

In the original C++ plan this module was offloaded to a Python notebook (`notebooks/portfolio.ipynb`) on the argument that re-doing date math and accumulator logic in C++ would cost ~1.5 days versus 100 lines of Polars. **That trade-off does not apply to Rust.** `polars-rs` is the same engine the Python notebook would use — same Arrow internals, same vectorized groupbys, same `LazyFrame::group_by_dynamic` window operator. The whole module is ~150 lines of Rust, gets compile-time schema checking against `per_trade_summary`'s `OnceLock<SchemaRef>`, and runs without a Python interpreter or a notebook server.

So **the portfolio module lives in `crates/momentum-portfolio` + `bin/portfolio`** and reads `per_trade_summary.parquet` directly:

```rust
let summary = LazyFrame::scan_parquet(
    "data/output/per_trade_summary.parquet",
    ScanArgsParquet::default(),
)?;
// Equal-allocation across same-day entries, accumulator over signal days, …
```

It produces:

- Equal-allocation of available cash across all entries on each signal day (per spec §Portfolio Accounting).
- Cash freed at each exit becomes available on subsequent signal days.
- No rebalancing of existing positions.
- Output: equity curve, CAGR, max drawdown, concurrent-positions chart.

Outputs land in `data/output/portfolio/` as Parquet (the equity curve + concurrent-positions table) plus a CSV of summary metrics. **Plots are not part of Phase 0.** The Parquet artifacts are the durable interface; if/when an analyst wants charts, they spin up a Python notebook over the Parquet — that's the kind of "spin up Python services when we need them" the language choice explicitly allows. We just don't need them in M8 to call the milestone done. This is M8 (§7).

### 6.8 Half-day sessions (early closes)

The U.S. equity market closes early at 13:00 ET on roughly
~6 days/year (day after Thanksgiving, Christmas Eve, Independence Day
eve, etc.). On these days there are fewer than 39 ten-minute bars in
the session. **Do not hardcode "39 bars" or "16:00 close" anywhere.**

- The trading-day calendar (derived from SPY in `bars_1m_raw/SPY`)
  already gives us every trading date *and*, by inspecting the
  per-date max `t`, the session-close time for that date.
- `session_bars(symbol, day)` returns only the bars actually present;
  consumers iterate, they don't index.
- End-of-day mark-to-market uses the actual last bar of the session
  for that date (12:50–13:00 close on half-days, 15:50–16:00 close
  otherwise), pulled from the same SPY-derived calendar.
- Path-row count per trade-day is variable (39 on a normal day,
  21 on a typical 1pm early close) and that variability is fine —
  the path is stored as `(trade_id, security_id, entry_date, t)` rows,
  not as a fixed-stride matrix.

Golden test for half-days is in §8.

---

## 7. Milestones

| # | Milestone | Output | Approx scope |
|---|---|---|---|
| M0 | Cargo workspace skeleton; hello-world `reqwest` GET against `/v3/reference/tickers/AAPL` and hello-world `aws-sdk-s3` GET of one flat file from the dashboard-provided bucket. CI: `cargo build --workspace --locked` + `cargo nextest run`. | both work end-to-end; CI green on a fresh clone | **0.5 day** (down from 1 day; no CMake/vcpkg setup) |
| M1 | Universe snapshot end-to-end (active + delisted, paginated, Parquet) via `momentum-api::tickers`. `serde`-decoded → `arrow::RecordBatchBuilder` → `parquet::ArrowWriter`. | `tickers.parquet` | 1–2 days |
| M2 | Flat-file ingest for a single recent day, written as 1-min Parquet partitioned by ticker (no aggregation). End-to-end byte-stream from `aws-sdk-s3::ByteStream` through `flate2` into `arrow::csv::ReaderBuilder` → `parquet::arrow::AsyncArrowWriter` without staging. | `bars_1m_raw/*.parquet` for one date | 0.5–1 day |
| M3 | Bulk flat-file ingest at scale: download every day in `[2015, T-1]` into `_staging/`, then a single end-of-bulk per-ticker pivot pass writing each `bars_1m_raw/{T}.parquet` exactly once. `tokio::sync::Semaphore`-bounded S3 concurrency, `rusqlite` cursor, plus the REST tail-filler for the lagging window. Sibling `bars_1m_deltas/` / `bars_1m_tail/` dirs (no glob collision with the historical partition glob). | `bars_1m_raw/*.parquet` for full 2015→T window | 2 days |
| M4 | Splits / dividends / ticker-events ingest **+ read-time split adjustment in `BarReader` (one `split_factor_cum`, applied to price *and* volume against pinned `splits.parquet` snapshot — no derived bars file, see §5.3 amended 2026-06-06). Dividends pulled but NOT folded into bars — joined to paths as events per §5.3.1.** Audit roundtrip is a `proptest` over the read-time multiply function. Snapshot-pin enforced at run startup (§5.3). | reference parquets + dividend-event annotations + `BarReader` impl over `bars_1m_raw/` with split-multiply + audit roundtrip passing | 2 days |
| M5 | Backtest engine v0 in `momentum-engine`: signal + entry only, no exit logic. Single-threaded, synchronous, reads via `momentum-store::BarReader`. | `per_trade_summary.parquet` with entry-only rows | 2 days |
| M6 | Exit logic + path recorder (10-min bar resolution, full 252-day horizon, half-day calendar aware). `Position::check_breakeven_stop` is the single exit predicate (§6.4). | full `per_trade_bar_path.parquet` | 2–3 days |
| M7 | Terminal accounting (delist/merge/halt/horizon). `PathEndReason` + `TerminalValueSource` `#[non_exhaustive]` enums enforce the §6.6 separation. | `path_end_reason` populated | 1–2 days |
| M8 | **Portfolio module — Rust bin crate over `polars-rs`** (§6.7). `bin/portfolio` reads `per_trade_summary.parquet` and writes equity curve + concurrent-positions + summary metrics as Parquet/CSV. No plots in Phase 0; plots become a downstream Python notebook the day someone needs one. | `data/output/portfolio/{equity_curve.parquet, summary.csv}` | **~1 day in Rust** (no Python toolchain on the critical path) |
| M9 | Golden tests via `insta` snapshots — handful of hand-verified trades including a half-day session, a rename, and a delisting. `inspect` bin crate exposes DuckDB queries. | confidence; `cargo insta review` clean | 2 days |

**Estimate calibration.** Best case ~2.5–3 weeks of focused work; the realistic-expected case is **4–5 weeks of part-time work** (down from 5–6 in the C++ plan) once you account for API auth setup, S3 access provisioning, a few inevitable calendar / corporate-action surprises, and the first round of golden tests catching bugs you have to chase. Three concrete reasons the budget shrinks:

- **M0 drops from 1 day to ~0.5 day.** No CMake setup, no vcpkg dance, no choosing between Arrow-from-source and Arrow-from-package-manager. `cargo new --bin`, add four deps, two hello-world async functions; done.
- **M3 ingest plumbing shrinks.** `tokio::sync::Semaphore` + `aws-sdk-s3` + `reqwest`(gzip) replaces ~200 LOC of libcurl-multi + custom backoff state machine. The semantic content stays the same; the line count is roughly a fifth. Removing the ingest-time 1-min → 10-min aggregation step (now done at engine read time) trims another day off M2+M3 combined.
- **M8 stays in-language.** No `pyproject.toml`, no `uv` or `poetry`, no Python venv inside the build artifact tree. The artifact contract (Parquet) is preserved; the implementation language is consolidated.

Treat 4–5 weeks as the planning budget. The 2.5–3 week floor is unchanged because the calendar-time risk (S3 access provisioning, the first delisting golden test catching a real bug) is language-independent.

**Why Rust for M8 (not Python notebook).** The C++ plan offloaded M8 to a Python notebook because re-doing date math and accumulator logic in C++ would cost ~1.5 days vs. 100 lines of Polars. That argument was sound for C++ and is *not* sound for Rust: `polars-rs` is the same engine that Python's `polars` wraps, with the same `LazyFrame` API. The spec's stated outputs (equity curve, CAGR, max drawdown, concurrent positions over time) are all one-liners against `per_trade_summary` in `polars-rs` too. Implementing them in Rust costs ~1 day (vs. ~0.5 day in a Python notebook), but saves us from carrying a Python toolchain dependency on the critical path for Phase 0. When Phase 1 needs interactive charts, ML pipelines, or agentic-sim work, a Python service can read the same Parquet outputs — that's the right time to introduce it.

---

## 8. Testing strategy

**Tooling.** Unit tests are `#[cfg(test)] mod tests` inside each crate, run via `cargo nextest run --workspace`. Property tests use `proptest` and live alongside the unit tests. Golden tests use `insta` snapshots (`assert_yaml_snapshot!` against a `PerTradeSummary` row or against a slice of the path table) — diffs are reviewed via `cargo insta review`, accepted snapshots commit to the repo as `*.snap` files. Integration tests that touch the on-disk cache live in `tests/` at the workspace root and use `tempfile::TempDir` for scratch space. HTTP mocking is via `wiremock`; never hit the real Massive API from a test.

- **Unit**: bar boundary math, ET/DST conversions (`chrono_tz::US::Eastern` round-trips across the spring-forward / fall-back days), exit rule (intrabar touch vs gap-through), eligibility filter.
- **Golden**: pick 5–10 well-known historical trades (e.g. NVDA on a known gap-up day, a known split day, a known delisting). Hand-compute the path. Engine output must match to the cent, captured as an `insta` snapshot.
- **Property**: `peak_price >= entry_price` always (equivalently
  `peak_return >= 0` measured from entry) — note the naive
  `peak_return >= signal_return` is *wrong*: `signal_return` is measured
  09:30→10:00 (pre-entry) and `peak_return` is measured from the
  10:00–10:10 open (post-entry), so they're in different reference
  frames and the inequality doesn't hold (a stock that tops out exactly
  at entry would fail it spuriously); `terminal_return` consistent with
  the last raw bar joined from `bars_1m`; `phase0_exit_fill_price <=
  entry_price` always.
- **Survivorship coverage** (catches the failure mode where the universe
  pull succeeds but the bar pull silently filtered to active names):
  pick a known pre-2020 bankruptcy and assert it has both a row in
  `tickers.parquet` *and* non-empty rows in `bars_1m`. A trade opened
  in that name terminates with `path_end_reason = delisted` and a
  non-null, finite `terminal_return`.
- **Rename continuity**: a trade entered under `FB` before the META
  rename has a path that continues unbroken through and past the rename
  date, with bars resolved from the post-rename rows. (Any well-known
  rename works — FB→META, GOOG/GOOGL share-class quirks, etc.)
- **Adjustment-baseline mismatch**: deliberately construct a path
  whose `bars_1m` partition was pulled on a different
  adjustment-as-of date than the trade's entry; assert the build
  errors loudly rather than silently producing a phantom-gap stop.
- **Half-day session**: a trade open on a known early-close day
  (e.g. **2023-11-24**, day after Thanksgiving; market closes 13:00 ET)
  produces a path-row count for that date consistent with the actual
  session length (~21 ten-min bars, not 39), and EOD mark-to-market
  uses the 12:50–13:00 close, not a 16:00 phantom value. Catches any
  hardcoded `39` or `16:00` in session-bar logic.
- **Split-vs-dividend factor separation**: pick a ticker with a known
  split *and* a known cash dividend in the window (e.g. AAPL 2020
  4-for-1 split + quarterly divs). Assert (a) raw volume on the
  split day is ~¼ of adjusted volume; (b) raw volume on a dividend
  ex-day equals adjusted volume (no division); (c) raw price on
  ex-dividend day equals adjusted price + dividend × `split_factor_cum`.
  Catches the "single factor on volume" bug class.
- **Timestamp-precision invariant**: every Parquet file under `bars_1m_raw/`, `bars_1m_deltas/`, and `bars_1m_tail/` has column `t` typed exactly `timestamp[ns, tz="UTC"]` (i.e. `DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))`). Property test reads each file's Arrow schema via `parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?.schema()` and asserts the `t` field's `DataType` matches exactly. Catches a REST-tail writer that forgot to cast Unix-ms to ns (§5.1 step 5) the moment it ships, before the UNION view silently drops boundary-day rows.
- **10-min rollup parity** (cheap sanity check): for a handful of (ticker, day) pairs, call `BarReader::session_bars(sid, day)` and compare against the REST `/v2/aggs/.../range/10/minute/...` response. Open/high/low/close/volume must match to the cent / share — catches any drift in the read-time rollup or the bar-boundary alignment.
- **Delist-date boundary**: pick a known bankruptcy with delist date
  `D`. Open a synthetic trade 5 trading days before `D`. Assert:
  (a) the last `t` in `per_trade_bar_path` equals the close of `D`'s
  final session bar (16:00 if normal, early-close timestamp if `D` is
  a half-day); (b) `path_length` equals the exact bar count over the
  5-day inclusive window (half-day-aware); (c) `path_end_reason =
  'delisted'`; (d) `per_trade_bar_path` has *no* rows with date >
  `D`. Catches the off-by-one §6.6 flags as the highest-probability
  failure mode (drops a valid final day, or fabricates a phantom one).

---

## 9. Scope decisions (frozen)

All previously-open items are now resolved. The ingest manifest can be
frozen and M0 can begin.

| Item | Decision | Rationale |
|---|---|---|
| **Backtest window** | `2015-01-01 → most recent complete month` | Polygon's pre-2015 intraday data has more micro-structure anomalies, irregular 10-min prints, and wider effective spreads; starting 2015 buys higher-fidelity 10-min boundaries without manual outlier scrubbing. The lost 2010–2014 window is not load-bearing for Phase 0's descriptive goals. |
| **ADR handling** | **Excluded** | Foreign-market holiday mismatches, FX noise, and structural ADR-vs-ordinary gaps dilute the pure intraday momentum signal. Filter on the `type` taxonomy. |
| **OTC tickers** | **Excluded** | Pink-sheet liquidity invalidates the resting breakeven-stop fill assumptions (the spec's exit rule assumes a stop fill at the touched level; OTC books don't guarantee that). Filter by `primary_exchange ∈ {"XNYS", "XNAS", "XASE", "ARCX", "BATS"}` (MIC codes — that's the form `/v3/reference/tickers` returns; the human names "NYSE / Nasdaq / NYSE American / NYSE Arca / Cboe BZX" are *not* what the field contains). Codify the set as a `const &[&str]` in `momentum-universe`, unit-test the filter against real reference rows, and reject the rest. |
| **Path resolution** | Full 10-min × 252 days, stored as join-index — §6.5 | Correctness, not scope: a daily rollup silently defeats intrabar future-exit studies. |
| **Adjustment baseline** | Split-adjusted bars for engine mechanics (one `split_factor_cum` factor, applied to both price and volume); dividends **not** folded into bars, stored as joined events for total-return analytics — §5.3 / §5.3.1 | Correctness, not scope: dividend-adjusted bars would make the breakeven stop systematically more lenient than a real broker order, since ex-dividend price drops are real and would fire a real stop. Reversed from an earlier draft. |

### Known signal-formulation artifact to expect in the output

`signal_return = close_at_10am / open_at_0930 - 1 > 0` will mark as
"green" a stock that gapped up +40% at 09:30, **bled out continuously
through the first half hour**, and is still nominally above the 09:30
open at 10:00. That trade exhibits sharply negative intraday momentum
but the signal can't see it. This is **expected** Phase 0 behavior, not
a bug — the spec is explicit that we record paths, not chase signal
quality. The giveback distribution should make this cohort visible
(big peak inside the entry bar, deep early drawdown). If we want a
follow-up phase to filter it, the right place is "open→10:00 monotonic
descent" or a 09:50→10:00 sub-window check, not changes to Phase 0.

## 10. Downstream contract (Phase 1 sketch — schemas locked now, code later)

Phase 0 ships two Parquet outputs and the `bars_1m/` cache. That's it
for Phase 0. **But the schema decisions made now lock in how cheap or
expensive downstream agentic simulation work will be**, so we sketch the
intended consumption shape here and use it to validate the schemas
before they're written.

Three downstream access patterns to design for:

1. **Static analytical queries** (notebooks, ad-hoc research). DuckDB
   over the Parquet files. No new code needed — works the day Phase 0
   finishes if the schemas are clean. In-tree acceptance test is an
   `arrow-rs` schema round-trip; PyArrow zero-copy read is the Phase 1+
   cross-language cross-check, run once a downstream Python consumer
   actually exists.

2. **Trade-path replay iterator** (per-trade or per-cohort). A thin
   library API that yields `RecordBatch`es of `(trade_id, t, bar)` in
   chronological order across the join. Same query, just packaged. The
   Rust side ships as `momentum-store::PathReplay` (an `Iterator<Item =
   arrow::RecordBatch>` over a DuckDB query); a future Python wrapper
   would wrap the same Parquet via `pyarrow` — no Rust↔Python FFI
   needed, the Parquet file is the wire format.

3. **Synchronized-clock replay** (multi-position, wall-clock-aligned —
   the shape an agentic / hive simulation actually wants). Given a
   simulated `t`, yield every open position's bar at `t` as a single
   `RecordBatch`. This is just a different join order on the same
   data: instead of `GROUP BY trade_id ORDER BY t`, it's
   `GROUP BY t ORDER BY t`. **Arrow Flight** is the natural wire format
   if/when this needs to cross a process or language boundary — same
   columnar layout, no serialization tax, schema-discoverable via
   `GetSchema`.

What this means for Phase 0 *now*:

- Output schemas are designed so pattern #3 doesn't require a rewrite.
  Specifically: `per_trade_bar_path` must be cheaply sortable by `t`
  across all trades, which the column order
  `(trade_id, security_id, entry_date, t, periods_held)` already
  supports via a secondary Parquet index on `t`.
- The schema-version field in file metadata (§4) lets Phase 1
  consumers refuse to load a Phase 0 file whose schema has drifted
  past their compatibility range.
- We do not build the Flight endpoint, the replay iterator, or any
  agentic surface in Phase 0. We just don't *foreclose* them.

---

## 11. Optimizations & API budget

Cache aggressively, and avoid REST when a flat file or a derived
artifact will do. Each optimization below is ordered by impact.

### 11.1 Flat files instead of per-ticker REST (biggest win, ~100×)

Without flat files, the original plan was ~10k tickers × 1–2 REST calls
each = ~15–20k requests just for historical bars, repeated whenever we
re-pull. With flat files:

- **~2,750 daily files** for 2015→present, regardless of universe size.
  Universe growth is free.
- Each file is one HTTP/S3 GET of a gzipped CSV. The S3 path is the
  "API call" — pulling 10 years of full-market 1-min bars is 1 transfer
  per trading day, parallelizable.
- Universe expansion (new tickers, delisted recoveries) adds **zero
  marginal API cost** because all tickers are already in every file.
- **1-min rows land in Parquet unchanged.** No ingest-time aggregation;
  the engine rolls up to 10-min at read time (§6.2). This keeps the
  raw 1-min on disk for free Phase 1 optionality (e.g. a 5-min
  variant, or intra-10-min giveback studies) — no S3 re-pull needed.

### 11.2 Immutable raw + derived adjusted + sibling deltas (idempotent reruns)

The split between `bars_1m_raw/` (immutable, per-ticker) and
`bars_1m/` (derived adjusted) plus the sibling `bars_1m_deltas/`
scheme collapses re-run cost to near-zero in common cases. Parquet is
write-once, so we explicitly **never append** to per-ticker partitions
day-by-day — that would force ~10k file rewrites every trading day.
Instead:

- **New trading day** (the common case): download one flat file
  (~100 MB gz), write a single `bars_1m_deltas/YYYY-MM-DD.parquet`
  — one file, all tickers, 1-min rows passed through unchanged. The
  bar reader's view UNIONs it in automatically (§5.1 step 4). The
  adjustment kernel recomputes only `bars_1m/T` for tickers whose
  values changed. No history re-downloaded, no per-ticker rewrites.
- **Monthly compaction:** fold accumulated daily deltas back into
  per-ticker partitions in one batched pass (10k rewrites once a
  month, not 10k rewrites per day). Deterministic, idempotent, safe
  to interrupt.
- **New split or dividend on ticker T:** recompute *only* `bars_1m/T`
  from `bars_1m_raw/T` + the updated snapshot. CPU only, no network.
- **Backtest engine rerun (the most common operation):** zero ingest
  cost, ever — engine is a pure function of the cache.

### 11.3 Reference-data cadence

- `/v3/reference/tickers` (universe): refresh **daily**, not per-run.
  Cache the response with the as-of date. The "what was the universe
  on day D" lookup is a join against the cached table, not an API
  call.
- `/v3/reference/tickers/{T}` (per-ticker metadata): only re-fetch for
  tickers whose `last_updated_utc` field changed, or that newly
  appeared in the universe.
- `/stocks/v1/splits` and `/stocks/v1/dividends`: refresh daily but
  fetch only events with `execution_date > last_seen_max`. Both
  endpoints are append-only by date, so the delta query is small.
- `/vX/reference/tickers/{id}/events`: only for tickers that appear in
  the rename chain or that have changed primary listing. Skip the rest.

### 11.4 Wire- and disk-level

- **HTTP gzip** on every REST call (`Accept-Encoding: gzip`). Free ~10×
  bandwidth for JSON.
- **Parquet compression:** Zstd level 3 for raw bars (better ratio
  than Snappy by ~25%, decode still fast). Snappy for the path
  join-index (tiny per row; decode speed matters more than ratio).
- **Row-group sizing:** **128k rows per group** for `bars_1m_raw/{T}`.
  Math: 10y × 252 trading days × ~390 one-minute bars = ~980k rows
  per ticker (10× more than the old 10-min plan). 128k rows gives
  ~7–8 row groups per ticker file, so a query for a specific 252-day
  window (~98k rows) hits ~1 row group and skips the rest via min/max
  stats on `t`. Going smaller than 128k at this row volume hurts
  compression more than it helps pruning; 128k is the sweet spot.
- **Sort within partition:** every per-ticker Parquet sorted by `t`
  before write. Min/max stats become tight; predicate pushdown on
  `t` ranges becomes O(log row-groups), not full scan.
- **Secondary t-index for the path join:** the `per_trade_bar_path`
  parquet sorts by `(t, trade_id)` so the synchronized-clock replay
  (§10) is also a row-group-skip-friendly scan.

### 11.5 What we deliberately don't optimize

- **Per-call retry caching of REST responses on disk** — the REST
  surface is now too small for this to matter. SQLite cursor + idempotent
  fetch covers the rerun case.
- **Parallel adjustment kernel** — single-threaded is fast enough on
  raw flat-file data; we only adjust on snapshot change. Premature.
- **Custom CSV parser for flat files** — Arrow's CSV reader is already
  vectorized and within 2× of hand-tuned. Not worth the bug surface.

### 11.6 Audit property tests (paid for by these optimizations)

The flat-file + client-side adjustment split lets us test invariants
that catch entire classes of bugs cheaply:

- **Price roundtrip:** `adjusted_price ÷ split_factor_cum == raw_price`
  to float64 epsilon.
- **Volume roundtrip:** `adjusted_volume × split_factor_cum ==
  raw_volume` to float64 epsilon.
- **Dividend non-application:** `adjusted_close` on an ex-dividend
  date is approximately `prev_close − dividend × split_factor_cum`
  (i.e., the drop is *visible*, not adjusted away). If the kernel
  silently dividend-adjusted bars, this property would fail — that's
  the catch.
- **Per-file snapshot coherence:** for every `bars_1m/{T}.parquet`
  and every split with execution date `E` affecting ticker `T`, assert
  `file.splits_snapshot_date >= E`. (Per §5.3, files no longer need to
  share a single snapshot date; the invariant is that each file is at
  or ahead of every split it should already reflect.) Cheap pass at
  engine startup.
- **Flat-file coverage continuity:** the SPY ticker rows in
  `bars_1m_raw/SPY` cover every trading day between
  `min(date)` and `max(date)` with no gaps (the U.S. equity calendar
  has no internal gaps; any gap = a missed flat-file download).

### 11.7 Field parity: flat files vs REST aggregates

Flat files ship **fewer columns** than the REST aggregates endpoint.
Cross-reference:

| Field | REST `/v2/aggs/...` | Flat files | Phase 0 needs it? |
|---|---|---|---|
| `open, high, low, close, volume` | yes (`o,h,l,c,v`) | yes (`open,high,low,close,volume`) | **yes** |
| Transaction count | `n` | `transactions` | no (stored as bonus) |
| Timestamp | `t` (Unix ms) | `window_start` (Unix **ns** — more precise) | yes |
| **VWAP** | `vw` | **absent** | **no** |
| **OTC flag** | `otc` (omitted when false) | **absent** | no (OTC tickers excluded at the universe level — §9) |

The missing fields don't cost us anything for Phase 0:

- The spec's required bar columns (`timestamp, open, high, low, close,
  volume`) are all present in flat files.
- Nothing in the signal, exit, path recording, or any §Metrics list
  uses VWAP. If a future phase wants VWAP, derive it from the
  `/stocks/trades` flat files (same S3 tree, same auth) or, if narrowly
  scoped, hit REST aggregates for just the bars that need it. Don't
  rebuild the whole pipeline around a column we don't yet use.
- The OTC flag is moot because OTC tickers are excluded at universe
  construction time; an OTC bar would never make it into `bars_1m_raw/`.

The OHLCV values reconcile to the cent between REST and flat files for
the same window — both are built from the same upstream
conditions-qualified trade tape. Verifying that reconciliation on a
handful of bars is a useful M2 sanity check.
