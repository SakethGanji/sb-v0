"""Golden-day fixtures for the Phase 0 forward tables (B6).

Hand-verified corporate-action stress cases the 2016-H2 smoke window never
exercises (split, ex-dividend, delisting, market-wide LULD halt, ticker
rename). Every fixture is grounded in the real reference data
(`splits.parquet`, `ticker_events.parquet`, `dividends.parquet`,
`tickers_enriched.parquet`) and in the raw 1m tape; the `anchor` numbers are
EXTERNAL TRUTH frozen here (computed from the raw tape on 2026-06-14, and
cross-checked against the public record) so a check can fail independently of
the recompute code it would otherwise share with the engine.

Why a separate output tree
--------------------------
These days are NOT in the smoke-window outputs under `data/outputs/`. Sweep
them into a SEPARATE tree (`GOLDEN_OUT`) so they never perturb the
smoke-window battery's cross-day cumulative state (the collision-purge set and
the signal-concentration history both walk `data/outputs/` in calendar order;
injecting non-contiguous 2020/2022 days there would corrupt those).

Warmup
------
`write-phase0` needs ~1 month of lead to warm the SHORT trailing windows
(ATR-14, ADV-21) that the forward pass consumes for its ATR-threshold columns.
The DEEP-trailing entry-context columns (52w high/low, beta, 252d windows) are
at warmup-edge in these short sweeps and are therefore NOT asserted by the
golden checks — those are already covered by the smoke-window L2 D-section
full-history exact recompute. The forward pass itself needs no warmup: it reads
the raw bars D..D+252 directly.

Run the sweeps in `SWEEP_RECIPE`, then:  scripts/validate_phase0_outputs.py --golden
"""

from datetime import date

# Output tree for golden sweeps (relative to repo root). Kept apart from
# data/outputs/ on purpose — see module docstring.
GOLDEN_OUT = "data/outputs_golden"


# Each fixture:
#   name          stable slug
#   event_type    split | dividend | delisting | halt | rename
#   entry_date    the forward_outcomes entry day (must be a swept entry day)
#   entry_offset  which of the 17 ENTRY_OFFSETS_V1 rows to assert on
#   security_id   the FIGI the row is keyed by (continuity anchor)
#   symbol        display symbol on the entry day
#   event_date    the corporate-action date
#   anchor        external-truth values frozen here
#   tests         one-line statement of the invariant this day defends
GOLDEN_DAYS = [
    {
        "name": "aapl-4to1-split",
        "event_type": "split",
        "entry_date": date(2020, 8, 28),   # Fri before; the ret_1d window straddles the split
        "entry_offset": "1000",
        "security_id": "BBG000B9XRY4",
        "symbol": "AAPL",
        "event_date": date(2020, 8, 31),   # 1:4 forward split executes
        "anchor": {
            "split_from": 1.0,
            "split_to": 4.0,
            "ratio": 4.0,
            # frozen from the raw tape (uv recompute 2026-06-14):
            "naive_raw_ret_1d": -0.7429,        # the BUG signature if the basis is inconsistent
            "sane_adj_ret_1d_approx": 0.0284,   # pin-adjusted, consistent basis
        },
        "tests": "A ret_1d window straddling a 4:1 split must be taken on a single "
                 "consistent (pin-adjusted) basis: ~+3%, NOT the -74% a cross-basis "
                 "read shows and NOT a double-adjusted value.",
    },
    {
        "name": "aapl-ex-dividend",
        "event_type": "dividend",
        "entry_date": date(2020, 8, 6),    # day before the ex-date
        "entry_offset": "1000",
        "security_id": "BBG000B9XRY4",
        "symbol": "AAPL",
        "event_date": date(2020, 8, 7),    # ex-dividend date
        "anchor": {
            "cash_amount": 0.82,                 # raw, pre-split basis
            "split_adjusted_cash_amount": 0.205, # pin basis (0.82 / 4)
        },
        "tests": "dividend_ex_date_within_1d must fire when the ex-date falls in the "
                 "1d horizon, and ret_total - ret_price must equal the pin-adjusted "
                 "dividend over the entry price.",
    },
    {
        "name": "logm-delisting",
        "event_type": "delisting",
        "entry_date": date(2020, 8, 28),   # last day LOGM trades
        "entry_offset": "1000",
        "security_id": "BBG009DVP6J6",
        "symbol": "LOGM",
        "event_date": date(2020, 9, 1),    # tickers_enriched.delisted_utc
        "anchor": {
            "terminal_event_type": "delisted_unknown",  # no delisting-reason feed on disk
            "terminal_event_confidence": "high",
            "last_trade_date": date(2020, 8, 28),        # zero bars on/after 2020-08-31
        },
        "tests": "A security that vanishes from the tape right after entry must surface "
                 "a delisted_unknown terminal event dated to the vendor delist date, "
                 "never a fabricated reason.",
    },
    {
        "name": "covid-luld-halt",
        "event_type": "halt",
        "entry_date": date(2020, 3, 9),    # Level-1 market-wide circuit breaker
        "entry_offset": "0935",            # entry minute lands INSIDE the halt window
        "security_id": "BBG000B9XRY4",
        "symbol": "AAPL",
        "event_date": date(2020, 3, 9),
        "anchor": {
            "halt_start_et": "09:34",   # last bar before the halt
            "resume_et": "09:49",       # first bar after — entry must fill here
        },
        "tests": "An entry offset that lands in a market-wide LULD halt must set "
                 "is_halted_at_entry and fill at the resumption bar (09:49), not "
                 "silently skip or fabricate a fill.",
    },
    {
        "name": "fb-to-meta-rename",
        "event_type": "rename",
        "entry_date": date(2022, 6, 8),    # last FB day; forward window crosses into META
        "entry_offset": "1000",
        "security_id": "BBG000MM2P62",
        "symbol": "FB",
        "event_date": date(2022, 6, 9),    # ticker_events ticker_change -> META
        "anchor": {
            "old_symbol": "FB",
            "new_symbol": "META",
            "new_symbol_date": date(2022, 6, 9),
        },
        "tests": "A forward window must follow the security_id across a ticker rename: "
                 "the entry resolves under FB, the forward bars (under META) must still "
                 "be found, so ret_1d is non-null and continuous.",
        # Companion entry the day OF the rename, to assert the symbol flip itself.
        "companion_entry": date(2022, 6, 9),
        "companion_symbol": "META",
    },
]


# Sweep recipe — three clusters cover all five fixtures. Run from the repo
# root; --release; long sweeps in the background. Each cluster warms write-phase0
# (~1 month lead) then runs the forward pass over the entry days.
SWEEP_RECIPE = r"""
# Cluster A — 2020-H2: AAPL split (entry 08-28), AAPL ex-div (entry 08-06),
#                      LOGM delisting (entry 08-28).
cargo run --release --bin write-phase0 -- \
    --from 2020-07-01 --to 2020-09-04 \
    --out data/outputs_golden --cursor data/_golden_state.sqlite --force
cargo run --release --bin write-forward-outcomes -- \
    --from 2020-08-05 --to 2020-08-31 \
    --out data/outputs_golden --cursor data/_golden_state.sqlite --force

# Cluster B — 2020-03-09 market-wide LULD halt (entry 03-09 @0935).
cargo run --release --bin write-phase0 -- \
    --from 2020-02-03 --to 2020-03-10 \
    --out data/outputs_golden --cursor data/_golden_state.sqlite --force
cargo run --release --bin write-forward-outcomes -- \
    --from 2020-03-09 --to 2020-03-09 \
    --out data/outputs_golden --cursor data/_golden_state.sqlite --force

# Cluster C — 2022-06-09 FB -> META rename (entries 06-08 FB, 06-09 META).
cargo run --release --bin write-phase0 -- \
    --from 2022-05-02 --to 2022-06-13 \
    --out data/outputs_golden --cursor data/_golden_state.sqlite --force
cargo run --release --bin write-forward-outcomes -- \
    --from 2022-06-07 --to 2022-06-10 \
    --out data/outputs_golden --cursor data/_golden_state.sqlite --force
""".strip()
