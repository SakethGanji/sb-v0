# Phase 7 A2 pre-registration — orthogonal microstructure invariants (property tests)

**Frozen:** 2026-07-06, before the profile table was built or any persistence number
seen. Per `phase7-implementation-plan.md` Step 2. These are PROPERTY tests — does the
cross-section contain a persistent invariant beyond vol and liquidity — **not**
strategies. Cell claim: none (pure science). **Pre-committed monetization map:** a
PASS routes ONLY to a Branch B extension (can our tail/concentration forecast beat
option-implied skew/smile?) — it cannot open any delta-one branch (closure theorem)
and cannot reopen Channel E under any narrative. A FAIL closes the question: beyond
volatility, price/volume microstructure carries no discovered persistent invariant.

**Amendment at freeze (before any build):** a third property test A2c is added — the
`bars_1m_raw.transactions` field (per-minute trade counts) never entered the 657-column
feature set; average trade size is the classic retail-vs-institutional participation
footprint and belongs in this sprint. Same PASS bar, same monetization map.

**Window/universe:** train era 2016-06-08→2020-12-31; Phase 1 train universe (CS,
mega/large/mid × highly_liquid/liquid/normal) at each observation date. Eras for
sign-stability = calendar years 2016..2020 (5).

## The build (one pass over bars_1m_raw, RTH 09:30–16:00 NY)

`volume_profile_daily` per (security_id, day): volume HHI over **30 equal-width price
bins** spanning [day low, day high] (bar volume assigned to its close-price bin —
frozen approximation), share-in-modal-bin, total volume, dollar volume, transaction
count, `avg_trade_size` = volume/transactions, `avg_dollar_trade` =
dollar_volume/transactions, and per-tail top-40 1-min log returns (Hill inputs;
monthly top-100 pooled from daily top-40s — frozen approximation, valid unless >40 of
a month's top-100 land on one day). Written to
`data/phase1_analysis/volume_profile_daily/` (engine output #9 if adopted).

## The three property tests

Common design: per observation date, cross-sectionally regress the measure on the
**vol block (8 STATIC_VOL features) + liquidity block (adv_20d, addv_20d)**, all as
per-date ranks; take residual ranks. Property = rank persistence of the residual.
**PASS floor (frozen, per test):** residual rank autocorrelation ≥ **0.10** at the
shortest horizon, day-clustered (or month-clustered) 95% CI excluding 0, sign-stable
5/5 eras. Below floor = decorative, FAIL.

- **A2a — liquidity-concentration persistence:** residualized volume-profile HHI (and
  modal share, reported); horizons t+1 / t+5 / t+21 trading days.
- **A2b — tail-shape persistence:** per (security_id, month) Hill index on each tail
  (top-100 pooled |1-min log returns|, k=100); Hill is scale-free but the vol block is
  partialled out anyway; horizon = month-over-month. **Power gate first at (stock,
  month):** MDE95 for monthly residual rank-AC from ~54 months × cross-section size;
  if MDE95 > 0.10 the verdict is UNANSWERABLE at this granularity — coarsen to
  quarter only if the gate demands, and say so.
- **A2c — trade-size persistence:** residualized avg_dollar_trade (primary) and
  avg_trade_size (reported); horizons t+1 / t+5 / t+21.

Registered priors: A2a weak-positive (microstructure habits are sticky but ADV may
absorb it — only the residual counts); A2b weak/none (equity tail indices are noisy,
roughly universal); A2c positive (trade-size is a participant fingerprint — this is
the one expected PASS, and it still monetizes nowhere except Branch B).

**Deliverable:** `phase7-findings.md` §A2. No re-tuning of bins, k, horizons, floors,
or universe after results; any variant is a new registration.
