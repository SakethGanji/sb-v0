# Momentum Hold Experiment — Phase 0 (Final Frozen Version, rev 2)

## Goal

Test a simple idea:

Find stocks that are already showing strength early in the trading day.
Buy them.
Hold them until they stop making money.
Observe how trades evolve.

The purpose of Phase 0 is not to prove an edge.

The purpose is to build a reusable trade-path dataset and understand:

* how long winning trades last,
* how far winners run,
* how deep pullbacks become,
* how often trades recover after retracements,
* how much of the result comes from a small number of large winners,
* whether the strategy behaves like genuine momentum or accidental buy-and-hold.

The output is a description of trade behavior, not a final trading verdict. **No go/no-go trading decision is made from Phase 0** — its only outputs are the trade-behavior distributions below.

---

## Data

Historical intraday stock data. Minimum fields: `timestamp, open, high, low, close, volume`.

Requirements:

* Use adjusted prices whenever possible.
* Include delisted stocks whenever available. If unavailable, clearly label results as survivorship-biased.
* Store the raw bar data used to generate every signal and every trade-path observation.
* **Also store SPY (or a market-proxy ETF) bars over the same timestamps.** SPY is *not* used in the Phase 0 entry decision; it is stored only to record `market_return_at_signal` so the relative-vs-absolute-strength question can be answered later without rerunning.

**Corporate actions (frozen):**

* If intraday bars are **unadjusted, apply split adjustment at minimum.** Dividend adjustment is documented separately (note whether it was applied).
* **Delisting while a position is open:** the path ends with `path_end_reason = delisted` and `terminal_return` uses the **best available delisting / liquidation price**; if none exists, the **last tradable close**. The same rule books `halted` and `merged` terminal values (reopen/cash terms / last reliable price) — symmetrically, so cash-buyout upside and bankruptcy downside are both captured, never NULLed.

---

## Universe (eligibility filter, frozen)

Eligible universe = **common stocks** with a valid 09:30–10:00 bar **and** a valid next-bar (10:00–10:10) open.

* **Exclude** ETFs, preferreds, warrants, rights, and units.
* ADRs: excluded by default (document if included).

Without this filter, illiquid or structurally odd securities can dominate the trade set.

---

## Bar Size & Observation Cadence

**BAR_SIZE = 10 minutes** (regular session), fixed throughout Phase 0.

On 10-minute bars, the 10:00 AM mark is the **close of the 09:50–10:00 bar**; the 09:30 session open is the **open of the 09:30–09:40 bar**.

**OBSERVATION_TIME = 10:00 AM ET.** **Each trading day, evaluate every eligible symbol once, at 10:00 AM ET.** One signal evaluation per symbol per day — no other observation times.

---

## Entry Rule

At the observation time:

```
signal_return = price_at_10am / session_open_price - 1
```

where `price_at_10am` = close of the 09:50–10:00 bar, `session_open_price` = open of the 09:30–09:40 bar.

If `signal_return > 0`, the stock is green.

For every green stock that is **not currently held**:

**Buy at the open of the next bar after the signal — the open of the 10:00–10:10 bar.** Do *not* fill at the 10:00 price used to compute the signal (that transacts at the trigger price — a look-ahead).

Store at entry: signal timestamp, signal return, signal price, and `market_return_at_signal` (SPY 09:30→10:00, identical window).

---

## Exit Rule (Phase 0 Strategy) — precise breach + fill

The Phase 0 exit is a **resting breakeven stop**. The breach and fill are defined by exactly one rule (this is the ambiguity most likely to move results, so it is pinned):

```
While a position is open, on each 10-minute bar AFTER the entry bar:
    if bar.low <= entry_price:
        EXIT this bar, fill = min(entry_price, bar.open)
    else:
        hold
```

* **Intrabar touch** (`bar.open > entry_price`, `bar.low <= entry_price`): fill at `entry_price` (the resting stop level).
* **Gap-through** (`bar.open <= entry_price`): fill at `bar.open` (below entry — the gap; never assume a fill at entry after a gap).
* **Same-bar execution** (a resting stop order), not next-bar.
* **No ordering ambiguity:** the stop level is fixed at entry (not trailing), so any bar whose low reaches entry fills at the stop regardless of intrabar high/low order.
* **Entry-bar guard (frozen):** the stop is **live only from the bar *after* the entry bar.** The entry bar cannot trigger it — `entry_price` equals that bar's open, so the bar's low would otherwise force a spurious same-bar stop-out on essentially every trade.

Examples (both branches of the rule, since the low alone does not determine the fill):

```
Entry = 100 (open of the 10:00–10:10 bar)
next bars: 105 → hold, 120 → hold, 108 → hold

Case A — intrabar touch (bar opens above entry, dips below):
  later bar opens 102, prints low 98 → fill = min(100, 102) = 100

Case B — gap-through (bar opens at or below entry):
  later bar opens 95, prints low 92  → fill = min(100, 95)  = 95
```

---

## Unrealized Positions

Positions that never breach breakeven remain open. At the end of the dataset:

* Mark all open positions to market.
* Report realized and unrealized P&L separately (avoids the open-winner illusion).

**Terminal-mark source:** path recording stops at the path horizon (252 days), but a position still open beyond it remains open until end-of-dataset. Its end-of-dataset mark-to-market price is taken from the **raw bar table**, not the path table, so a multi-year runner's terminal value may come from a bar later than its last recorded path row. The two tables reconcile through the raw bars.

---

## Portfolio Accounting (frozen)

Two layers, deliberately separated:

* **Trade-level returns are position-level and independent of portfolio sizing.** The trade-path dataset and all trade-level metrics ignore allocation entirely.
* **Portfolio-level metrics** use: **equal allocation of available cash across all new entries on each signal day, with no rebalancing of existing positions.** Cash freed when a position exits becomes available for allocation on subsequent signal days. (Simple, deterministic, avoids continuous-rebalancing complexity.)

---

## Trade Path Tracking

The Phase 0 strategy exits at breakeven, but **path recording is independent of the exit.**

For every entry:

* Continue recording the trade path **even after the breakeven exit would have occurred.**
* Store the breakeven exit as a **marker** on the path (`phase0_exit_date`), not as the end of the path.

This lets future exit rules be evaluated without rerunning the simulation.

**Path horizon:** record for `PATH_HORIZON = 252 trading days`, or until delisting / merger / halt / end of dataset, whichever comes first.

**Measurement basis:** the complete raw bar (`open, high, low, close, volume`) is stored per trade-day and is the source of truth; all derived metrics are recomputable from it.

---

## Per-Trade-Day Path Table

One row per trade per observation period.

Required: `trade_id, symbol, entry_date, timestamp, periods_held, open, high, low, close, volume, entry_price`.

Convenience (derivable from raw bars + entry_price): `current_return, peak_price, peak_return, trough_price, trough_return, drawdown_from_peak, giveback_pct_of_peak`.

---

## Per-Trade Summary Table

Required: `trade_id, symbol, entry_date, entry_price`.

Additional:

* `signal_timestamp`, `signal_return`, `signal_price`
* `market_return_at_signal` — SPY 09:30→10:00, same window (stored, not used for entry; lets you later ask "relative strength or just an up market?" without rerunning)
* `days_to_first_profit` — periods until the first profitable observation
* `peak_date`, `peak_return`
* `trough_date`, `trough_return`
* `phase0_exit_date` — when the resting breakeven stop would have exited
* **`phase0_exit_reason` ∈ { `breakeven_rule`, `open_at_end` }** — `open_at_end` = never breached, marked-to-market at end of data
* `path_length` — count of `per_trade_bar_path` rows recorded for this trade (≤ `PATH_HORIZON × bars_per_session`; shorter if the path ended early via delisting, merger, halt, or end of dataset)
* **`path_end_reason` ∈ { `horizon_reached`, `delisted`, `halted`, `merged`, `end_of_data` }** — *note:* `breakeven_rule` is **not** a path-end reason; the breakeven exit is a marker, and the path continues past it
* `terminal_return`

---

## Design Principle

Do not hard-code profit targets, trailing stops, giveback exits, optimization rules, or ranking systems.

Instead: **store the complete path once**, then use the dataset later to evaluate giveback thresholds, profit-taking rules, trailing exits, stronger-vs-weaker signal levels, and alternative holding rules.

---

## Metrics To Record (descriptive only)

Separated by layer for clean implementation.

**Trade-level (position-level, sizing-independent):**

* peak return
* trough return
* terminal return
* time to breakeven exit (`phase0_exit_date − entry`)
* time to peak (`peak_date − entry`)
* giveback distribution
* holding-period distribution

**Portfolio-level (uses the allocation rule above):**

* equity curve
* CAGR
* maximum drawdown
* realized vs. unrealized P&L
* concurrent positions over time

**Most important outputs:** realized vs. unrealized P&L, holding-period distribution, peak-return distribution, drawdown/giveback distribution. These reveal how the strategy behaves and where profits actually come from.

---

## Explicitly Deferred

SPY comparisons (SPY is *stored* via `market_return_at_signal`, but no comparison is computed), random-entry placebo, relative-strength ranking, sector adjustments, profit targets, trailing stops, giveback exits, parameter tuning, statistical significance tests, optimization, short selling, options, inverse ETFs. These belong to later phases if the trade-path dataset proves interesting.

---

## Phase 0 Question

If we buy stocks that are already green at 10:00 AM and hold them until they return to breakeven, what does the resulting trade distribution look like?

Nothing else.

Build the data.
Generate the trades.
Record the paths.
Then let the data tell us what to investigate next.

*Not financial advice. Treat all live capital as fully losable.*
