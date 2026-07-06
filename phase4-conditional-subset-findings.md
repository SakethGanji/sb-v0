# Phase 4 — The Conditional-Subset Hypothesis: review, protocol, and result

**Date:** 2026-07-06 · Script: `scripts/phase4_prim_subgroup.py` ·
Output: `data/phase1_analysis/prim_subgroup_results.parquet`

The question (user, sharpened): *"Global prediction is null. But could a very small subset of
trades — 1% of opportunities — carry a dramatically higher conditional win probability that
global-loss ML would smooth away? Is searching for it mathematically distinct from what was
already done, or an illusion of progress? Design the rigorous test or dismantle the idea."*

---

## 1. Statistical review: is this genuinely a different search?

**Partially yes — it is not fully subsumed. Three concrete gaps in the prior work:**

1. **Loss dilution is real arithmetic, not hand-waving.** A region holding 1% of rows with a
   +25pp win-rate edge improves global log-loss by ~0.25% relative. Our joint probe's GBM used
   `min_samples_leaf=200`, `l2_regularization=1.0`, and early stopping on *global* validation
   loss — a leaf whose global contribution is inside noise gets pruned or never granted. The
   probe was tuned to find *broad* signal, and correctly reported there is none.
2. **Evaluation dilution.** Every prior verdict metric (rank-IC, decile spread, AUC) averages
   over the full distribution or over deciles (10% bins). A 1%-support pocket is diluted 10:1
   before it can register. Nobody ever looked at the extreme tail of a *region search*.
3. **Different objective class.** PRIM/subgroup discovery optimizes `sup over boxes of
   quality(box)` subject to a support floor — a sup-norm objective with a patient (5%-peel)
   search bias. GBM leaves are boxes too, but greedy axis-aligned splitting under global L2
   loss explores a very different trajectory through the same hypothesis space.

**What IS already covered, honestly:** the factor scan (98 BY hits) was a coarse-cell subgroup
search; the metalabel top-decile economics, the config search (408 configs), and Phase 3's
Bayesian evidence-stacking were all conditional-subset analyses of specific kinds. Phase 3 is
the damning precedent: it *found* the user's hypothetical — P(win) climbing 50%→85% with
stacked conditions — and the remaining-expectancy was ~0 net. High conditional P(win) has
already appeared in this data and been an artifact of banked gains + market drift both times.

**The multiple-testing objection (why this is dangerous):** the space of ≤4-constraint boxes
over 97 features is astronomically larger than 408 configs, and (name,day) rows are heavily
dependent (cross-sectional correlation, overlapping 5d horizons, regime clustering). Naive
"75% win in a 1% pocket" findings are *expected* under the null. Any run must charge the
search, not the box: the null distribution must be of the **entire pipeline's best OOS box**
under label permutation, not a t-test on the reported box.

## 2. Market-mechanics review: could such a pocket even exist?

For a durable 75%-win / 1%-support pocket in *pure price/volume states of liquid US mid+
caps* to exist in 2016–2020 and still be there OOS, it must have survived: every stat-arb
shop scanning exactly these axis-aligned conditionals with better data, faster clocks, and
lower costs, continuously, for decades. Grossman–Stiglitz logic says public-data edges persist
only where a wall blocks the capital: capacity (micro-caps — found, wall #1), unhedgeable
crisis risk (found, wall #4), shorting frictions (found, wall #2), or information genuinely
absent from price/volume (earnings — found, and Phase 4 showed even that is priced within a
day now). A liquid-universe price/volume pocket has **no wall to hide behind**. The realistic
best case for a "real" discovered box is therefore: a crisis-state risk premium (conditional
mean positive because it's compensation for occasionally catastrophic left tails) — tradeable
only by someone willing to be short vol in disguise.

**Prior: strongly null. But the test costs one session and the data exists — and the value of
running it is that "no tradeable subsets" becomes a measured statement with a null
distribution attached, instead of an inference from global-model results.**

## 3. Pre-registered protocol (frozen before the run)

- **Data:** deployable universe (CS, mega/large/mid, HL/L/N liq), entry 10:00, target
  `ret_5d_excess_spy`, the 97 leak-audited joint-probe features. 2016–2020 only.
- **Split:** search 2016–2018, confirm 2019–2020. (2021-22 validation untouched unless a box
  passes; 2023+ holdout sealed.)
- **Two criteria** (run separately): NETEXP = mean excess − 20bp; WINRATE = P(excess>0) —
  the user's exact framing.
- **Search:** PRIM, 5% peel, support ≥0.5% of search rows, ≥100 train days, ≥50 names,
  quality good in ≥2/3 train years; K=5 boxes by cover-and-remove; search on a seeded 400k
  subsample, all evaluation on full data.
- **Multiplicity:** full-pipeline permutation null — labels shuffled within train day, the
  *entire* search re-run, its best box scored on real 2019–20 outcomes; N=25 reruns. The real
  box must beat the null's 95th percentile.
- **Verdict rule:** a box is REAL only if OOS net CI>0 AND beats the permutation null AND
  win-rate cliff <10pp AND crisis-share <50% AND ≥40 OOS days / ≥30 names. Else the door
  closes with evidence.

## 4. Results

### 4a. Score-tail check (`phase4_score_tail.py` / `_verify.py`) — the model-side answer

Before searching feature space, we asked: does the joint GBM's own OOS ranking hide a
high-P(win) pocket at the extreme tail that decile evaluation diluted? (identical
leakage-hardened walk-forward as phase1_joint_probe, pooled OOS 2018-2020, 690k rows):

| tail (per day) | n | gross bp/5d | net@20 | win% |
|---|---|---|---|---|
| top 5% | 34,545 | +13.5 | −6.5 | 49.3 |
| top 1% | 6,860 | +25.4 | +5.4 | 49.0 |
| top 0.5% | 3,554 | +31.1 | +11.1 | 49.3 |
| top 0.1% | 756 | +53.4 | +33.4 | **50.0** |

The nominal net expectancy grows toward the tail — but the verification battery kills it:

- **Win rate is a coin flip at every tail.** The user's "Condition A → 75%" does not appear
  even among the model's 756 favorite trades in 3 years. The entire expectancy is **payoff
  skew**: avg win +808.8bp vs avg loss −702.0bp (ratio 1.15) on vol-percentile-0.76 names.
- **Day-clustered CIs include 0 by a mile**: 0.1% tail net CI[−107, +189]; 0.5% CI[−59, +93].
  (The naive shuffle-null it "beat" was variance-mismatched: random picks are low-vol.)
- **Not year-stable**: 0.5% tail was −34.9bp gross in 2018; 0.1% swings −0.6/+101.5/+59.1.
- Name-concentrated (top-5 names = 31.7% of 0.1%-tail picks), median cap $5.6B (small-mid).

**Verdict: vol mirage #5.** The model's extreme tail is 50/50 lottery tickets whose skew
asymmetry cannot be distinguished from zero. This is the *same* magnitude structure found in
every prior phase, now confirmed at the finest selection granularity the data supports.

### 4b. PRIM subgroup search (`phase4_prim_subgroup.py`) — the feature-space answer

Smoke run (2 perms) already showed the canonical pattern; the full 25-perm run's numbers:

*(deep = unconstrained sup-search; shallow = best ≤4-condition box, the human-scale variant)*

**NETEXP criterion** (5 deep boxes found, 31–45 conditions each):

| box | train net@20 (2016-18) | train win | OOS net@20 (2019-20) | OOS win | OOS rows |
|---|---|---|---|---|---|
| 1 | +101.8 CI[+61,+149] | 62.2% | −21.3 CI[−114,+77] | 45.7% | 173 |
| 2 | +63.5 CI[+27,+108] | 59.8% | −31.6 CI[−121,+69] | 42.7% | 567 |
| 3 | +48.6 CI[+21,+77] | 60.7% | **−66.1 CI[−135,−4]** | 46.8% | 233 |
| 4 | +105.4 CI[+72,+137] | 64.8% | +11.3 CI[−96,+111] | 57.6% | 33 |
| 5 | +47.6 CI[+15,+83] | 64.0% | **−129.6 CI[−209,−48]** | 41.3% | 75 |

Best *valid* OOS box (≥40 days/≥30 names): **−31.6bp net**. Permutation-null best-OOS: mean
−16.9, **95th pct +12.7** → the real search result is *inside* the null (and below its mean).

**WINRATE criterion**: train win rates 58.4–65.0% collapsed to 40.8–52.3% OOS — an average
cliff of ~14pp; **no box met the OOS validity minimums** (9–20 unique OOS names). Null 95th
pct of best-OOS win rate: 53% — nothing to compare because nothing valid survived.

**The shallow (≤4-condition) result is the sharpest finding**: across the real run AND all
50 permutation pipelines, **not one ≤4-condition box ever qualified in-sample** — with four
price/volume conditions you cannot even carve a net-positive, era-stable region of the
*training* set at ≥0.5% support. The "Condition A" of the hypothesis requires 30–50
conditions to exist in-sample, and at that depth it is pure memorization: OOS support
collapses 10–100× (train 3,731 rows → OOS 173) because the non-stationary market-state
features drift out of their 2016-18 ranges, and what remains performs at or below noise.
*(Footnote: PRIM's greedy peel is not an exhaustive ≤4-dim search; a dedicated shallow
search could do somewhat better in-sample — but it would face the same null-charged OOS bar.)*

Every deep box, on both criteria, is the same creature: elevated stock vol + elevated market
vol/dispersion + mid-cap — the known magnitude structure (walls #1/#3/#4) rediscovered, as
§2 predicted.

## 5. Verdict

**The conditional-subset door is now closed with evidence, on both sides:**

1. **Model side (score tail):** the strongest global model's own top-0.1% picks are 50/50
   coin flips with lottery skew — no high-P(win) pocket exists in the model's ranking.
2. **Feature side (PRIM):** a direct sup-objective search with a full-pipeline permutation
   null finds train pockets of 62–65% win that are *inside the null* OOS. High train
   conditional win rates are exactly as abundant as multiple-testing theory predicts under
   H0, and no more.

Answering the user's three questions precisely:
1. **Yes**, subgroup search is mathematically distinct from global-loss ML (sup vs L2
   objective) — that's why the test was run rather than dismissed.
2. **Yes**, global objectives can in principle hide a tiny region (loss dilution is real
   arithmetic) — but here the region they'd be hiding measurably does not exist.
3. **PRIM did not rediscover *new* structure** — it rediscovered the vol/dispersion
   magnitude corners, at higher train quality and with total OOS collapse, exactly the
   known-walls-in-disguise outcome the market-mechanics prior predicted.

The "Condition A → 75%" world is statistically indistinguishable from the "every condition
→ ~50%" world in this data — and the *one* mechanism that genuinely produced high
conditional directional probability anywhere in this project (earnings surprise) was shown
the same day to be priced within 24 hours (phase4-findings.md). The pursuit is not an
illusion of *reasoning* — the hypothesis was legitimate and is now measured — but continuing
to search price/volume conditionals after this result would be an illusion of progress.
