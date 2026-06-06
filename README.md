# Momentum Hold — Phase 0 Build Plan

This repo implements the experiment specified in
[`momentum-hold-phase0-frozen-v2.md`](./momentum-hold-phase0-frozen-v2.md).

Phase 0 is **not** a trading verdict — it builds a reusable trade-path dataset
so we can later evaluate giveback/trailing/profit-target rules without
re-running the simulation.

Stack: **C++20**, Massive (Polygon) REST API, Parquet/DuckDB for storage.

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
     252-day horizon, stored as a thin join-index into `bars_10m` — see §6.5)
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
| **Historical 1-minute OHLCV bars, market-wide** | **S3 Flat Files** — `/stocks/minute-aggregates`, daily files | **Primary historical ingest path.** Each daily file contains *every* ticker's 1-min bars for that day. ~2,750 files for the 2015→present window vs. ~15–20k REST calls per-ticker — two orders of magnitude fewer requests and effectively zero "API budget." We aggregate 1-min → 10-min locally. Flat files are unadjusted raw tape; adjustment is applied client-side per §5.3. See §11. |
| Same window, last 1–2 days only | `GET /v2/aggs/ticker/{ticker}/range/1/minute/{from}/{to}` | **Fallback only**, for the lagging window before today's flat file publishes (typically ~T+1). 1-min raw; locally aggregated to 10-min via the same kernel as flat-file ingest (keeps the adjustment pipeline single-codepath). |
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

## 3. Repository layout (proposed)

```
sb-v0/
├── README.md                       # this file
├── momentum-hold-phase0-frozen-v2.md   # frozen spec (do not edit)
├── CMakeLists.txt
├── cmake/                          # FindXxx.cmake, toolchain bits
├── third_party/                    # vendored or FetchContent'd deps
├── include/momentum/               # public headers
│   ├── api/         # massive client
│   ├── calendar/    # trading-day + ET timezone
│   ├── store/       # parquet/duckdb wrappers
│   ├── universe/    # eligibility filter
│   ├── signal/      # 10:00 AM signal
│   ├── engine/      # backtest loop, position book, path recorder
│   └── io/          # output table writers
├── src/                            # implementations matching include/
├── apps/
│   ├── ingest.cpp                  # CLI: pull/refresh cache
│   ├── backtest.cpp                # CLI: run Phase 0 over cache
│   └── inspect.cpp                 # CLI: quick DuckDB queries on output
├── tests/                          # unit + golden tests
├── data/                           # gitignored cache root
│   ├── reference/                  # tickers, splits, dividends, events
│   ├── bars_10m/                   # partitioned by ticker (or date)
│   └── output/                     # per_trade_bar_path, per_trade_summary
└── scripts/                        # one-off helpers (parquet schema, etc.)
```

---

## 4. Dependencies (C++)

| Lib | Why |
|---|---|
| **libcurl** | HTTP. Mature, easy. |
| **simdjson** *or* **nlohmann::json** | JSON parse. simdjson for ingest hot path; nlohmann for config / one-off. |
| **Apache Arrow + Parquet C++** | Columnar bar storage. Compresses 10× vs CSV; mmap-friendly. **Install via vcpkg (`vcpkg install arrow[parquet]`) or system package manager — do NOT `FetchContent` it.** Arrow's transitive deps (Thrift, Protobuf, jemalloc, gRPC) make a from-source build a multi-hour exercise that breaks on local toolchain quirks. 30 min of vcpkg setup saves ~3 days of M0 toolchain wrestling. |
| **DuckDB (C++ API)** | Embedded SQL over Parquet. Drop in the **single-file `duckdb.hpp` + `duckdb.cpp` amalgamation** — no CMake integration, no transitive deps, builds in one TU. |
| **Howard Hinnant `date.h` / `<chrono>` + tz** | **Critical** — every signal/exit boundary is in America/New_York. Do not use naive UTC math; DST will silently shift bars. |
| **fmt** | Logging/formatting. |
| **spdlog** | Logging. |
| **CLI11** | Argument parsing for the three CLI apps. |
| **SQLite (sqlite_modern_cpp)** | Persistent ingest-cursor state. Cheaper resumption than recomputing deltas from the JSON manifest on every reboot (see §5.2). |
| **Catch2** *or* **GoogleTest** | Unit tests. |

Build via **CMake + FetchContent** (or vcpkg if preferred). Target C++20.

**Schema discipline (cross-language contract).** The Parquet/Arrow output
files are the only contract downstream tooling sees — and downstream is
overwhelmingly going to be Python/PyArrow (ML pipelines, agentic-sim
work). Hold one canonical Arrow schema per output table in
`include/momentum/io/schema.hpp` and write it to Parquet metadata. Type
choices must round-trip cleanly to PyArrow with **zero implicit casting**:
- timestamps as `timestamp[ns, tz="UTC"]` (never `int64` epoch millis);
- monetary values as `float64` (not `decimal` — DuckDB/PyArrow handling
  diverges on edge cases and Phase 0 doesn't need decimal precision);
- enums (`phase0_exit_reason`, `path_end_reason`, `terminal_value_source`)
  as Arrow `dictionary<string, int32>`, not raw strings;
- categorical IDs (`trade_id`, `security_id`) as `string`, sortable lexically.

A schema change bumps a version field in the file metadata; consumers
check the version before loading.

**Identifier discipline — `security_id` is identity, `symbol` is
decorative.** Stock tickers rename (FB→META), get reused (defunct
ticker → new company later), and change share class (GOOG/GOOGL).
**Treat the ticker symbol as a display attribute, not a key.** Every
output table keys on:

- `security_id` = composite FIGI (or share-class FIGI fallback). Stable
  across renames; this is the canonical join key.
- `display_symbol` = the ticker as it appeared at a given timestamp.
  Stored as a column alongside `security_id`; useful for human
  readouts, not joins.

`per_trade_summary` therefore has both `security_id` and
`entry_display_symbol`; `bars_10m/` files are still partitioned by
*current* display symbol for fast file-level lookup, but every row
carries `security_id` as the durable identity. Engine joins use
`security_id`. The rename chain (from
`/vX/reference/tickers/{id}/events`) maps `(display_symbol, t) →
security_id` and is the only place the symbol↔identity translation
lives.

**No file is rewritten on rename.** Pre-rename bars stay in their
original `bars_10m/{old_symbol}.parquet`; post-rename bars accumulate
in `{new_symbol}.parquet`. The bar reader takes `(security_id,
t_range)`, resolves the affected display symbols via the rename chain
in `figi_map.parquet`, and UNIONs the matching files. The only
operation that rewrites a `bars_10m/` file is split-snapshot
recomputation (§5.3); renames never trigger one.

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
   batched map-reduce, not a per-day write loop.

   - **Stage 1 — bulk download.** Download every flat file in
     `[2015-01-01, T−1]` into `data/_staging/flat_files/YYYY/MM/YYYY-MM-DD.csv.gz`.
     Concurrent S3 transfers (§5.2). The SQLite ingest cursor (§5.2)
     keys on `(date, status)` since one file == one day.
   - **Stage 2 — batched ticker partitioning.** In yearly batches
     (~252 files each), use Arrow/DuckDB to:
     - Stream-read each day's gzipped CSV into Arrow record batches.
     - Aggregate 1-min → 10-min in one pass: group by
       `(ticker, floor(window_start_ns / 600_000_000_000))`,
       `open=first`, `close=last`, `high=max`, `low=min`, `volume=sum`,
       `transactions=sum`.
     - Re-group the year by ticker.
     - Write `data/bars_10m_raw/{ticker}.parquet` **once per year per
       ticker**. For year 2, read the existing per-ticker file +
       new year's rows together and rewrite. Yes that's a rewrite, but
       it happens once per ticker per year of history during the
       initial bulk load (~10 rewrites per ticker over 10y, ~5 seconds
       each on a laptop — minutes per ticker, not days for the whole
       universe). Acceptable one-shot cost.
   - **Stage 3 — purge staging.** Once a year's Parquet partition writes
     successfully, drop its raw CSV.gz files from `_staging/` — they're
     reproducible from S3 if needed and they're ~50 GB/year uncompressed.

   - Flat files are **unadjusted raw tape**. Adjustment runs as a
     deterministic post-step (§5.3); raw is never modified.

4. **Daily incremental (T onward) via `daily_deltas/`**

   Once the historical bulk load is done, *new* trading days do **not**
   get rewritten into per-ticker partitions on arrival — that would
   force ~10k rewrites per day. Instead:

   - Each new trading day's flat file is aggregated 1-min → 10-min and
     written *whole* to `data/bars_10m_raw/_daily_deltas/YYYY-MM-DD.parquet`
     — one file per day, all tickers, sorted by `(ticker, t)`.
   - The bar reader exposes a DuckDB view that UNIONs the per-ticker
     historical partitions with all of `_daily_deltas/*.parquet`:
     ```sql
     CREATE VIEW bars_10m_raw_v AS
       SELECT * FROM read_parquet('bars_10m_raw/*.parquet')
       UNION ALL
       SELECT * FROM read_parquet('bars_10m_raw/_daily_deltas/*.parquet');
     ```
     Predicates on `(security_id, t)` push down into both halves
     (every row carries `security_id` per §4, so engine joins go
     through that key, not the display symbol).
   - **Monthly compaction:** fold accumulated deltas back into per-ticker
     partitions (one rewrite per ticker), then clear `_daily_deltas/`.
     Deterministic, idempotent, safe to interrupt and resume.

5. **Tail-window bars via REST** (flat files lag by ~T+1)

   For the most-recent ~1 trading day before the flat file publishes,
   hit `GET /v2/aggs/ticker/{T}/range/1/minute/{from}/{to}?adjusted=false&limit=50000`
   for tickers in the eligible universe. Write to a separate
   `bars_10m_raw/_rest_tail.parquet` that the bar reader's view also
   UNIONs in; clear it once the next flat file lands and replaces the
   data. This is the only REST call in the bar-ingest path.

   **Timestamp precision.** REST aggregates ship `t` as Unix **ms**;
   flat files ship `window_start` as Unix **ns**. At write time, cast
   `t * 1_000_000` and store the column as
   `timestamp[ns, tz="UTC"]` so `_rest_tail.parquet` is bit-compatible
   with the per-ticker and `_daily_deltas/` partitions before the
   UNION view sees it. Without this cast, a `(security_id, t)` join
   against flat-file bars silently drops or duplicates rows at the
   boundary day.

### 5.2 Rate-limit + concurrency

- **REST surface is now narrow** (reference data + tail-window
  aggregates only) so concurrency tuning matters less than it did when
  REST was the bulk path. Still: start at ~10 inflight, measure, ramp.
- **Always request `Accept-Encoding: gzip`** on REST calls — JSON
  responses compress ~10×; libcurl decompresses transparently when
  `CURLOPT_ACCEPT_ENCODING` is set. Free bandwidth.
- Retry on 429/5xx with exponential backoff + jitter. A sustained 429
  storm trips a circuit breaker that halves the inflight cap and pauses
  for 60s before resuming.
- **S3 flat-file downloads** use a separate, larger concurrency pool
  (start at ~16 transfers) since the bottleneck there is bandwidth,
  not API quota.
- Cap memory: stream JSON → Arrow record batch → Parquet writer; never
  hold the whole response in a `std::string`. Same rule for flat-file
  ingest: stream gunzip → CSV parser → 10-min aggregator → Parquet
  writer; never materialize a full day's market-wide CSV in RAM.
- **Persistent ingest cursor (SQLite).** Recomputing what's missing
  from the JSON manifest on every restart is O(universe) and gets slow
  on partial failures. Maintain a `data/_ingest_state.sqlite` with
  `(ticker, last_completed_window_to, status, last_error, attempt_count)`,
  written transactionally after each successful Parquet flush. Reboot
  resumes from the cursor in milliseconds; the JSON manifest remains
  the human-readable summary of what's on disk.

### 5.3 Split-adjusted bars for engine mechanics; dividends stored separately (frozen)

Flat files ship **unadjusted raw tape**. We apply **split adjustment
only** to the engine-facing bars; cash dividends are stored as events
and not folded into the price line. Earlier drafts of this section froze
"split *and* dividend adjustment together" — that was wrong, and is
reversed here. The reasoning:

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
bars_10m_raw/{T}.parquet  (canonical, from flat files, immutable)
        + splits.parquet  (snapshot, stamped)
        ↓
        split-adjustment kernel (deterministic)
        ↓
bars_10m/{T}.parquet  (split-adjusted; what the engine reads)
        + columns: o, h, l, c, v, split_factor_cum
        + parquet metadata: splits_snapshot_date

dividends.parquet  (snapshot, stamped — joined alongside paths in analytics, never into bars)
```

**Adjustment math:**

```
adjusted_price  =  raw_price  ×  split_factor_cum
adjusted_volume =  raw_volume ÷  split_factor_cum
```

- A 2-for-1 split halves price *and* doubles share count → both raw
  price and raw volume need restating onto the current-share basis.
- Dividends do not enter either equation.

Rules:

- **Splits applied to bars; dividends not applied to bars.**
  Both data sets are pulled, snapshotted, and stamped; only splits feed
  the bar-adjustment kernel.
- **Each `bars_10m/{T}.parquet` carries its own `splits_snapshot_date`
  in metadata.** Per-ticker incremental rebuild is legal and expected;
  the corpus is not required to share a single snapshot date. (An
  earlier draft required "one snapshot per build, mixed = build error,"
  which is incompatible with the per-ticker incremental rebuild rule
  below. The per-file stamp resolves the contradiction.)
- When a new split appears in a later snapshot, **only the affected
  ticker's adjusted file is recomputed from raw** — no re-download —
  and its `splits_snapshot_date` is bumped to the new snapshot's date.
- The eligibility comparison `bar.low <= entry_price` operates on
  split-adjusted bars from a single snapshot. Phantom-split-gap risk
  is eliminated by construction; ex-dividend drops are *not*
  eliminated and that is the intended behavior.
- Audit roundtrip (tested as property §8):
  - `adjusted_price ÷ split_factor_cum == raw_price`
  - `adjusted_volume × split_factor_cum == raw_volume`
  - Both within float64 epsilon.

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
data/bars_10m_raw/{ticker}.parquet          # unadjusted, immutable, source of truth
data/bars_10m_raw/_daily_deltas/YYYY-MM-DD.parquet  # incremental days (§5.1 step 4)
data/bars_10m_raw/_rest_tail.parquet        # REST tail-window only (§5.1 step 5)
data/bars_10m/{ticker}.parquet              # split-adjusted, derived; engine reads this
data/reference/tickers.parquet
data/reference/splits.parquet               # stamped with snapshot date
data/reference/dividends.parquet            # stamped with snapshot date; joined to paths as events (§5.3.1), not folded into bars
data/reference/ticker_events.parquet        # rename chain: (display_symbol, t) → security_id
data/reference/figi_map.parquet             # (security_id, display_symbol, valid_from, valid_to) — sourced from ticker_events
data/output/eligibility_log.parquet         # (date, security_id, eligible, reason) — persisted per-day decisions
data/_staging/flat_files/YYYY/MM/...        # transient bulk-download staging (purged after partitioning)
data/_manifest.json                         # human-readable summary of coverage
data/_ingest_state.sqlite                   # resume cursor (§5.2)
```

SPY no longer needs its own file — it's just one ticker among many in
the daily flat files, so it lives in `bars_10m_raw/SPY.parquet` like
everything else. The trading-day calendar is derived from
`SELECT DISTINCT date FROM bars_10m_raw/SPY` at engine startup.

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

Take the set of distinct ET-dates appearing in `spy_10m.parquet`. Those
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

### 6.3 Daily loop (pseudocode)

```cpp
for (auto day : trading_days) {
    // 1. Universe at this date — yields (security_id, display_symbol_at_day) tuples.
    //    Common stocks not yet delisted as of `day`.
    auto eligible = universe.snapshot(day);

    // 2. Score every eligible security once at 10:00 ET. Key everything on
    //    security_id; display_symbol is carried only for the entry record.
    for (auto& [sid, disp] : eligible) {
        auto bars = bar_store.session_bars(sid, day);   // ≤39 bars (half-day aware, §6.8)
        if (!bars.has(0930) || !bars.has(1000_close) || !bars.has(1010_open))
            continue;
        double sig = bars.close_at(1000) / bars.open_at(0930) - 1.0;
        if (sig > 0 && !book.is_open(sid)) {
            book.enter(sid, disp, day, bars.open_at(1010), sig,
                       spy_return_0930_to_1000(day));
        }
    }

    // 3. Advance every open position by one trading day; record path; check exit
    book.advance_to(day);
}
```

**Symbol continuity across renames.** Because the engine keys
positions, the path index, and `bars_10m` joins on `security_id`
(composite FIGI), a mid-path rename (FB→META, share-class rejigs,
etc.) is invisible to the daily loop: `bar_store.session_bars(sid,
day)` resolves the affected display symbols via the rename chain
(§4) and UNIONs the matching `bars_10m/{sym}.parquet` files. If the
engine ever keyed on `display_symbol` instead, every path would
silently go empty after a rename date and the trade would look
terminated when it wasn't. Golden-tested on FB→META (§8).

### 6.4 Exit logic (verbatim from spec — pin it in a single function)

```cpp
// Called once per 10-min bar AFTER the entry bar.
std::optional<Fill> Position::check_breakeven_stop(const Bar& b) const {
    if (b.low <= entry_price) {
        return Fill{ .price = std::min(entry_price, b.open),
                     .when  = b.t };
    }
    return std::nullopt;
}
```

`phase0_exit_date` is recorded as a **marker**; the path continues for
252 trading days regardless, per spec §Trade Path Tracking.

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
`bars_10m` is already source of truth (and the spec says path metrics are
"recomputable from" the raw bars), store the path as a thin index that
references it:

```
per_trade_bar_path columns:
  trade_id, security_id, entry_date, t, periods_held
  -- (optional) halted_flag, split_event_id, dividend_event_id for context
```

OHLCV is recovered by joining `(security_id, t)` back to `bars_10m`
(via the rename chain at file-resolution time — §4). `entry_price`
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
    `bars_10m`.
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

### 6.7 Portfolio layer — Python notebook downstream, not C++

Trade-level returns ignore allocation entirely; the C++ engine emits
`per_trade_summary` and stops there. Portfolio aggregation is a pure
tabular operation on that table — implementing it in C++ would mean
re-doing date math and accumulator logic that Polars/Pandas do in one
groupby. So **the portfolio module lives in `notebooks/portfolio.ipynb`**
(or `scripts/portfolio.py`), reads `per_trade_summary.parquet` via
DuckDB, and produces:

- Equal-allocation of available cash across all entries on each signal
  day (per spec §Portfolio Accounting).
- Cash freed at each exit becomes available on subsequent signal days.
- No rebalancing of existing positions.
- Output: equity curve, CAGR, max drawdown, concurrent-positions chart.

This is M8 (§7). Outputs land in `data/output/portfolio/` as Parquet +
PNG so the C++ side and downstream consumers see one consistent
artifact tree.

### 6.8 Half-day sessions (early closes)

The U.S. equity market closes early at 13:00 ET on roughly
~6 days/year (day after Thanksgiving, Christmas Eve, Independence Day
eve, etc.). On these days there are fewer than 39 ten-minute bars in
the session. **Do not hardcode "39 bars" or "16:00 close" anywhere.**

- The trading-day calendar (derived from SPY in `bars_10m_raw/SPY`)
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
| M0 | CMake skeleton + libcurl + nlohmann + S3 client hello-world; pull one ticker JSON and one flat file | both work end-to-end | 1 day |
| M1 | Universe snapshot end-to-end (active + delisted, paginated, Parquet) | `tickers.parquet` | 1–2 days |
| M2 | Flat-file ingest for a single recent day, aggregate 1-min → 10-min, partition by ticker | `bars_10m_raw/*.parquet` for one date | 1 day |
| M3 | Bulk flat-file ingest at scale: yearly batched map-reduce (download → 1-min → 10-min → per-ticker rewrite, year by year) + SQLite cursor + REST tail-filler. No daily per-ticker append. | `bars_10m_raw/*.parquet` for full 2015→T window | 2–3 days |
| M4 | Splits / dividends / ticker-events ingest **+ split-adjustment kernel (one `split_factor_cum`, applied to price *and* volume) producing `bars_10m/` from `bars_10m_raw/`. Dividends pulled but NOT folded into bars — joined to paths as events per §5.3.1.** | reference parquets + split-adjusted bars + dividend-event annotations + audit roundtrip passing | 2 days |
| M5 | Backtest engine v0: signal + entry only, no exit logic | `per_trade_summary.parquet` with entry-only rows | 2 days |
| M6 | Exit logic + path recorder (10-min bar resolution, full 252-day horizon, half-day calendar aware) | full `per_trade_bar_path.parquet` | 2–3 days |
| M7 | Terminal accounting (delist/merge/halt/horizon) | `path_end_reason` populated | 1–2 days |
| M8 | **Portfolio module — Python notebook over DuckDB**, not C++. `notebooks/portfolio.ipynb` reads `per_trade_summary.parquet` and produces equity curve, CAGR, max drawdown, concurrent-positions chart. | notebook outputs (PNG + CSV) | **0.5 day in Python (vs ~2 days in C++)** |
| M9 | Golden tests (handful of hand-verified trades, including a half-day session, a rename, and a delisting) + DuckDB inspection CLI | confidence | 2 days |

**Estimate calibration.** Best case ~2.5–3 weeks of focused work; the
realistic-expected case is **5–6 weeks of part-time work** once you
account for API auth setup, S3 access provisioning, a few inevitable
calendar / corporate-action surprises, and the first round of golden
tests catching bugs you have to chase. The 2.5–3 week number is the
floor if nothing surprises you; treat 5–6 weeks as the planning
budget. Flat-file path still makes M2/M3 noticeably faster than the
original REST-per-ticker plan, the rerun cost is near zero (§11), and
moving M8 out of C++ reclaims ~1.5 days regardless of which estimate
holds.

**Why Python for M8:** The spec deliberately separates "trade-level
returns (sizing-independent)" from "portfolio-level metrics
(allocation-dependent)" — the trade-level outputs are what the C++
engine produces, and portfolio aggregation is a pure tabular operation
over `per_trade_summary` that takes 100 lines of Polars or Pandas. The
spec's stated outputs (equity curve, CAGR, max drawdown, concurrent
positions over time) are all one-liners against the summary table.
Implementing them in C++ buys nothing except the chance to write more
date math.

---

## 8. Testing strategy

- **Unit**: bar boundary math, ET/DST conversions, exit rule (intrabar
  touch vs gap-through), eligibility filter.
- **Golden**: pick 5–10 well-known historical trades (e.g. NVDA on a
  known gap-up day, a known split day, a known delisting). Hand-compute
  the path. Engine output must match to the cent.
- **Property**: `peak_price >= entry_price` always (equivalently
  `peak_return >= 0` measured from entry) — note the naive
  `peak_return >= signal_return` is *wrong*: `signal_return` is measured
  09:30→10:00 (pre-entry) and `peak_return` is measured from the
  10:00–10:10 open (post-entry), so they're in different reference
  frames and the inequality doesn't hold (a stock that tops out exactly
  at entry would fail it spuriously); `terminal_return` consistent with
  the last raw bar joined from `bars_10m`; `phase0_exit_fill_price <=
  entry_price` always.
- **Survivorship coverage** (catches the failure mode where the universe
  pull succeeds but the bar pull silently filtered to active names):
  pick a known pre-2020 bankruptcy and assert it has both a row in
  `tickers.parquet` *and* non-empty rows in `bars_10m`. A trade opened
  in that name terminates with `path_end_reason = delisted` and a
  non-null, finite `terminal_return`.
- **Rename continuity**: a trade entered under `FB` before the META
  rename has a path that continues unbroken through and past the rename
  date, with bars resolved from the post-rename rows. (Any well-known
  rename works — FB→META, GOOG/GOOGL share-class quirks, etc.)
- **Adjustment-baseline mismatch**: deliberately construct a path
  whose `bars_10m` partition was pulled on a different
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
- **Timestamp-precision invariant**: every Parquet file under
  `bars_10m_raw/` — per-ticker partitions, `_daily_deltas/*.parquet`,
  and `_rest_tail.parquet` — has column `t` typed exactly
  `timestamp[ns, tz="UTC"]`. Property reads each file's Arrow schema
  via PyArrow and asserts. Catches a REST-tail writer that forgot to
  cast Unix-ms to ns (§5.1 step 5) the moment it ships, before the
  UNION view silently drops boundary-day rows.
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
| **OTC tickers** | **Excluded** | Pink-sheet liquidity invalidates the resting breakeven-stop fill assumptions (the spec's exit rule assumes a stop fill at the touched level; OTC books don't guarantee that). Filter by `primary_exchange ∈ {NYSE, NASDAQ, AMEX/NYSE American}`. |
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

Phase 0 ships two Parquet outputs and the `bars_10m/` cache. That's it
for Phase 0. **But the schema decisions made now lock in how cheap or
expensive downstream agentic simulation work will be**, so we sketch the
intended consumption shape here and use it to validate the schemas
before they're written.

Three downstream access patterns to design for:

1. **Static analytical queries** (notebooks, ad-hoc research). DuckDB
   over the Parquet files. No new code needed — works the day Phase 0
   finishes if the schemas are clean. PyArrow zero-copy read is the
   acceptance test.

2. **Trade-path replay iterator** (per-trade or per-cohort). A thin
   library API that yields `RecordBatch`es of `(trade_id, t, bar)` in
   chronological order across the join. Same query, just packaged. C++
   side ships as a header-only `PathReplay` over DuckDB; Python side
   wraps via `pyarrow`.

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
- Aggregating 1-min → 10-min in-process is cheap (linear scan, one
  pass) and means we keep the raw 1-min only if we want to (Phase 0
  doesn't, but Phase 1 might — e.g. a 5-min variant).

### 11.2 Immutable raw + derived adjusted + daily-deltas (idempotent reruns)

The split between `bars_10m_raw/` (immutable, per-ticker) and
`bars_10m/` (derived adjusted) plus the `_daily_deltas/` scheme
collapses re-run cost to near-zero in common cases. Parquet is
write-once, so we explicitly **never append** to per-ticker partitions
day-by-day — that would force ~10k file rewrites every trading day.
Instead:

- **New trading day** (the common case): download one flat file
  (~100 MB gz), aggregate to 10-min, write a single
  `bars_10m_raw/_daily_deltas/YYYY-MM-DD.parquet` — one file, all
  tickers. The bar reader's view UNIONs it in automatically (§5.1
  step 4). The adjustment kernel recomputes only `bars_10m/T` for
  tickers whose values changed. No history re-downloaded, no
  per-ticker rewrites.
- **Monthly compaction:** fold accumulated daily deltas back into
  per-ticker partitions in one batched pass (10k rewrites once a
  month, not 10k rewrites per day). Deterministic, idempotent, safe
  to interrupt.
- **New split or dividend on ticker T:** recompute *only* `bars_10m/T`
  from `bars_10m_raw/T` + the updated snapshot. CPU only, no network.
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
- **Row-group sizing:** **32k rows per group** for `bars_10m_raw/{T}`.
  Math: 10y × 252 trading days × 39 ten-minute bars = ~98k rows per
  ticker. The earlier draft said "128k rows per group, ~3 groups per
  ticker" — that's contradictory; at 128k rows the whole ticker file
  is *one* row group and DuckDB can only skip at the file level, not
  inside it. 32k rows gives ~3 row groups per ticker file, so a query
  for a specific 252-day window (~1 row group's worth) can skip the
  other ~2 via min/max stats on `t`. Compression hit from going
  smaller-than-default is negligible at this scale.
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
- **Per-file snapshot coherence:** for every `bars_10m/{T}.parquet`
  and every split with execution date `E` affecting ticker `T`, assert
  `file.splits_snapshot_date >= E`. (Per §5.3, files no longer need to
  share a single snapshot date; the invariant is that each file is at
  or ahead of every split it should already reflect.) Cheap pass at
  engine startup.
- **Flat-file coverage continuity:** the SPY ticker rows in
  `bars_10m_raw/SPY` cover every trading day between
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
  construction time; an OTC bar would never make it into `bars_10m_raw/`.

The OHLCV values reconcile to the cent between REST and flat files for
the same window — both are built from the same upstream
conditions-qualified trade tape. Verifying that reconciliation on a
handful of bars is a useful M2 sanity check.
