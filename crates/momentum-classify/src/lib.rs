//! `momentum-classify` — the v1 behavioral-tag rules from
//! `phase0-observation-pivot-rfc.md` §11.6.3.
//!
//! ## Why this crate exists separately
//!
//! Per RFC §18.3 the tag rules are a single shared function with version
//! stamping. Isolating them in their own crate makes three things explicit:
//!
//! 1. The crate version pin (`SECURITY_CLASSIFICATION_VERSION` in
//!    `momentum-core::phase0_outputs`) is meaningful — bumping the rules
//!    means bumping that constant and re-running every downstream join.
//! 2. The snapshot tests at the bottom of this file are the CI guard
//!    against accidental rule edits. If the rules change without a
//!    version bump, the snapshot test fails loudly.
//! 3. Phase 0 ingest tools and the (future) engine pull from one place;
//!    no copy-paste of the heuristic thresholds.
//!
//! ## What's in here
//!
//! - `ClassificationInputs` — the per-(day, security_id) input bundle the
//!   rule engine consumes. Bars-derivable fields are computed by the
//!   engine in Phase 0-D; reference-data fields come from
//!   `tickers_classified.parquet` (see momentum-core::schema docs on
//!   the `tickers_enriched` naming clarification).
//! - `BehavioralTags` — the bool-bundle the rule engine produces.
//!   Matches the `is_*` columns in
//!   `security_classification_daily.parquet` exactly.
//! - `classify` — the pure function that maps inputs → tags.
//!
//! ## Stability contract
//!
//! These rules are pinned to `v1`. **Do not edit any threshold without
//! bumping `SECURITY_CLASSIFICATION_VERSION` in momentum-core and
//! re-running the snapshot test with `cargo insta review`** (or, here,
//! updating the inline expected values, since we don't depend on `insta`).
//!
//! The rules are **heuristic proxies**, not ground truth (RFC §13.8).
//! `is_meme_candidate` and `is_low_float_candidate` (without float data)
//! will produce false positives by design.

use momentum_core::phase0_outputs::SECURITY_CLASSIFICATION_VERSION;

/// Per-(day, security_id) inputs to the v1 behavioral-tag rules.
///
/// **Convention:** every rolling field is point-in-time over `[D-N, D-1]`
/// per RFC §6.4. Engine-side computation enforces that via a shared
/// rolling-window helper; this struct is a pure dumb bag of f64s/bools.
///
/// `None` is informational — it means "we don't know on this date" and a
/// rule that depends on the missing input is **not asserted**. Tag rules
/// short-circuit to `false` on missing critical inputs rather than fake
/// an answer.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClassificationInputs<'a> {
    // ---- Structure (reference data) ----
    pub ticker_type: Option<&'a str>,
    pub is_adr: Option<bool>,
    pub country_of_origin: Option<&'a str>,

    // ---- Buckets (precomputed by the engine before this fn is called) ----
    pub market_cap_bucket: Option<MarketCapBucket>,

    // ---- Sector / industry (from tickers_classified) ----
    pub sector: Option<&'a str>,
    pub industry: Option<&'a str>,
    pub sub_industry: Option<&'a str>,

    // ---- Rolling / point-in-time stats ([D-N, D-1]) ----
    pub addv_20d_rank_today: Option<i32>,
    pub realized_vol_21d_rank_today_percentile: Option<f64>, // 0..=100
    pub realized_vol_21d_percentile_jump_vs_prior_30d: Option<f64>,
    pub addv_20d_percentile_today: Option<f64>, // 0..=100
    pub intraday_dollar_volume_0930_to_1000_rank_today: Option<i32>,
    pub atr_14d_over_prior_close: Option<f64>,
    pub prior_close_price: Option<f64>,
    pub beta_qqq_60d: Option<f64>,
    pub float_shares: Option<f64>,
    pub days_since_ipo_or_first_bar: Option<i32>,
}

/// Output of the v1 tag rules. Field names mirror the `is_*` columns in
/// `security_classification_daily.parquet`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BehavioralTags {
    pub is_mega_cap_tech: bool,
    pub is_large_cap_tech: bool,
    pub is_semiconductor: bool,
    pub is_biotech: bool,
    pub is_regional_bank: bool,
    pub is_energy: bool,
    pub is_china_adr: bool,
    pub is_low_float_candidate: bool,
    pub is_meme_candidate: bool,
    pub is_recent_ipo: bool,
}

/// Market-cap bucket. Engine pre-buckets the raw `market_cap` per
/// `security_classification_daily`'s `market_cap_bucket` column convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketCapBucket {
    Micro,
    Small,
    Mid,
    Large,
    Mega,
}

impl MarketCapBucket {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Micro => "micro",
            Self::Small => "small",
            Self::Mid => "mid",
            Self::Large => "large",
            Self::Mega => "mega",
        }
    }
}

/// RFC §11.6.3 thresholds, exported so downstream queries can see exactly
/// which constants the v1 rules used.
pub mod thresholds {
    /// `is_mega_cap_tech` requires addv_20d_rank ≤ this.
    pub const MEGA_CAP_TECH_ADDV_RANK_MAX: i32 = 50;
    /// `is_mega_cap_tech` requires beta_qqq_60d > this.
    pub const MEGA_CAP_TECH_BETA_QQQ_MIN: f64 = 0.9;
    /// `is_large_cap_tech` requires addv_20d_rank ≤ this.
    pub const LARGE_CAP_TECH_ADDV_RANK_MAX: i32 = 500;
    /// `is_low_float_candidate` (with float data): float < this.
    pub const LOW_FLOAT_SHARES_MAX: f64 = 50_000_000.0;
    /// `is_low_float_candidate` (fallback proxy): ATR/close > this.
    pub const LOW_FLOAT_PROXY_ATR_RATIO_MIN: f64 = 0.05;
    /// `is_low_float_candidate` (fallback proxy): prior_close > this.
    pub const LOW_FLOAT_PROXY_PRIOR_CLOSE_MIN: f64 = 1.0;
    /// `is_low_float_candidate` (fallback proxy): addv_rank > this
    /// (note: HIGH numeric rank = LOW liquidity, per the engine's
    /// "rank 1 = top dollar volume" convention).
    pub const LOW_FLOAT_PROXY_ADDV_RANK_MIN: i32 = 5_000;
    /// `is_meme_candidate`: realized_vol_21d percentile ≥ this.
    pub const MEME_VOL_PERCENTILE_MIN: f64 = 95.0;
    /// `is_meme_candidate` arm A: intraday_dollar_volume rank ≤ this.
    pub const MEME_DOLLAR_VOLUME_RANK_MAX: i32 = 50;
    /// `is_meme_candidate` arm B: addv percentile-vs-prior-30d jump ≥ this.
    pub const MEME_ADDV_PERCENTILE_JUMP_MIN: f64 = 90.0;
    /// `is_recent_ipo`: days_since_ipo_or_first_bar < this.
    pub const RECENT_IPO_DAYS_MAX: i32 = 60;
}

const TECH_SECTORS_MEGA: &[&str] = &[
    "Technology",
    "Communication Services",
    "Consumer Discretionary",
];

const TECH_SECTORS_LARGE: &[&str] = &["Technology", "Communication Services"];

const SEMICONDUCTOR_NAMES: &[&str] = &["Semiconductors"];
const BIOTECH_NAMES: &[&str] = &["Biotechnology", "Pharmaceuticals"];
const REGIONAL_BANK_NAMES: &[&str] = &["Regional Banks"];
const CHINA_HK_LOCALES: &[&str] = &["CN", "HK"];

fn contains_str(haystack: Option<&str>, needles: &[&str]) -> bool {
    match haystack {
        Some(h) => needles.contains(&h),
        None => false,
    }
}

/// Map `ClassificationInputs` to `BehavioralTags` per RFC §11.6.3 v1.
///
/// A rule short-circuits to `false` when any critical input is `None`.
/// This is the **single** place these rules live; downstream readers
/// should never reimplement them.
pub fn classify(inputs: &ClassificationInputs<'_>) -> BehavioralTags {
    let mut t = BehavioralTags::default();

    // is_mega_cap_tech
    if let (Some(MarketCapBucket::Mega), Some(rank), Some(beta)) = (
        inputs.market_cap_bucket,
        inputs.addv_20d_rank_today,
        inputs.beta_qqq_60d,
    ) {
        let sector_ok = contains_str(inputs.sector, TECH_SECTORS_MEGA);
        t.is_mega_cap_tech = sector_ok
            && rank <= thresholds::MEGA_CAP_TECH_ADDV_RANK_MAX
            && beta > thresholds::MEGA_CAP_TECH_BETA_QQQ_MIN;
    }

    // is_large_cap_tech
    if let (Some(cap), Some(rank)) = (inputs.market_cap_bucket, inputs.addv_20d_rank_today) {
        let cap_ok = matches!(cap, MarketCapBucket::Large | MarketCapBucket::Mega);
        let sector_ok = contains_str(inputs.sector, TECH_SECTORS_LARGE);
        t.is_large_cap_tech =
            cap_ok && sector_ok && rank <= thresholds::LARGE_CAP_TECH_ADDV_RANK_MAX;
    }

    // is_semiconductor
    t.is_semiconductor = contains_str(inputs.industry, SEMICONDUCTOR_NAMES)
        || contains_str(inputs.sub_industry, SEMICONDUCTOR_NAMES);

    // is_biotech
    t.is_biotech =
        contains_str(inputs.industry, BIOTECH_NAMES) || contains_str(inputs.sub_industry, BIOTECH_NAMES);

    // is_regional_bank
    t.is_regional_bank = contains_str(inputs.industry, REGIONAL_BANK_NAMES)
        || contains_str(inputs.sub_industry, REGIONAL_BANK_NAMES);

    // is_energy
    t.is_energy = inputs.sector == Some("Energy");

    // is_china_adr
    t.is_china_adr =
        inputs.is_adr == Some(true) && contains_str(inputs.country_of_origin, CHINA_HK_LOCALES);

    // is_low_float_candidate
    // Primary: float < 50M, when float data is available.
    // Fallback proxy (no float): low liquidity + high ATR ratio + above-penny.
    if let Some(float) = inputs.float_shares {
        t.is_low_float_candidate = float < thresholds::LOW_FLOAT_SHARES_MAX;
    } else if let (Some(addv_rank), Some(atr_ratio), Some(price)) = (
        inputs.addv_20d_rank_today,
        inputs.atr_14d_over_prior_close,
        inputs.prior_close_price,
    ) {
        t.is_low_float_candidate = addv_rank > thresholds::LOW_FLOAT_PROXY_ADDV_RANK_MIN
            && atr_ratio > thresholds::LOW_FLOAT_PROXY_ATR_RATIO_MIN
            && price > thresholds::LOW_FLOAT_PROXY_PRIOR_CLOSE_MIN;
    }

    // is_meme_candidate
    // "Excess volume + excess vol + cross-sectional attention spike."
    // Two arms — either the intraday dollar-vol rank is in the top, OR the
    // ADDV percentile jumped vs the prior 30 days.
    if let Some(vol_pct) = inputs.realized_vol_21d_rank_today_percentile
        && vol_pct >= thresholds::MEME_VOL_PERCENTILE_MIN
    {
        let arm_a = inputs
            .intraday_dollar_volume_0930_to_1000_rank_today
            .is_some_and(|r| r <= thresholds::MEME_DOLLAR_VOLUME_RANK_MAX);
        let arm_b = inputs
            .realized_vol_21d_percentile_jump_vs_prior_30d
            .is_some_and(|p| p >= thresholds::MEME_ADDV_PERCENTILE_JUMP_MIN);
        t.is_meme_candidate = arm_a || arm_b;
    }

    // is_recent_ipo
    t.is_recent_ipo = inputs
        .days_since_ipo_or_first_bar
        .is_some_and(|d| d < thresholds::RECENT_IPO_DAYS_MAX);

    t
}

/// Return the version stamp for the rules in this module. Mirrors
/// `momentum-core::phase0_outputs::SECURITY_CLASSIFICATION_VERSION`.
pub fn rules_version() -> &'static str {
    SECURITY_CLASSIFICATION_VERSION
}

// =============================================================================
// CI snapshot tests — pin v1 rule logic.
//
// Per RFC §18.3: "Rule logic lives in `crates/momentum-classify`; a CI test
// snapshots the v1 rules and fails on accidental edits without version bump."
//
// We don't depend on `insta` here — the snapshot is the inline expected
// `BehavioralTags`. If a rule changes without intent, the assertion fails;
// if a rule changes WITH intent, bump SECURITY_CLASSIFICATION_VERSION and
// update the expected values below in the same commit.
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn nvda_mega_cap_2024() -> ClassificationInputs<'static> {
        ClassificationInputs {
            ticker_type: Some("CS"),
            is_adr: Some(false),
            country_of_origin: Some("US"),
            market_cap_bucket: Some(MarketCapBucket::Mega),
            sector: Some("Technology"),
            industry: Some("Semiconductors"),
            sub_industry: Some("Semiconductors"),
            addv_20d_rank_today: Some(1),
            realized_vol_21d_rank_today_percentile: Some(80.0),
            realized_vol_21d_percentile_jump_vs_prior_30d: Some(50.0),
            addv_20d_percentile_today: Some(99.9),
            intraday_dollar_volume_0930_to_1000_rank_today: Some(2),
            atr_14d_over_prior_close: Some(0.03),
            prior_close_price: Some(900.0),
            beta_qqq_60d: Some(1.5),
            float_shares: Some(2_500_000_000.0),
            days_since_ipo_or_first_bar: Some(10_000),
        }
    }

    fn biotech_gapper_low_float_2021() -> ClassificationInputs<'static> {
        ClassificationInputs {
            ticker_type: Some("CS"),
            is_adr: Some(false),
            country_of_origin: Some("US"),
            market_cap_bucket: Some(MarketCapBucket::Micro),
            sector: Some("Healthcare"),
            industry: Some("Biotechnology"),
            sub_industry: Some("Biotechnology"),
            addv_20d_rank_today: Some(6_000),
            realized_vol_21d_rank_today_percentile: Some(97.0),
            realized_vol_21d_percentile_jump_vs_prior_30d: Some(92.0),
            addv_20d_percentile_today: Some(40.0),
            intraday_dollar_volume_0930_to_1000_rank_today: Some(120),
            atr_14d_over_prior_close: Some(0.08),
            prior_close_price: Some(4.5),
            beta_qqq_60d: Some(0.3),
            // float data missing — exercise the fallback-proxy arm.
            float_shares: None,
            days_since_ipo_or_first_bar: Some(200),
        }
    }

    fn baba_china_adr_2020() -> ClassificationInputs<'static> {
        ClassificationInputs {
            ticker_type: Some("ADRC"),
            is_adr: Some(true),
            country_of_origin: Some("CN"),
            market_cap_bucket: Some(MarketCapBucket::Mega),
            sector: Some("Consumer Discretionary"),
            industry: Some("Internet Retail"),
            sub_industry: None,
            addv_20d_rank_today: Some(40),
            realized_vol_21d_rank_today_percentile: Some(60.0),
            realized_vol_21d_percentile_jump_vs_prior_30d: Some(20.0),
            addv_20d_percentile_today: Some(99.0),
            intraday_dollar_volume_0930_to_1000_rank_today: Some(35),
            atr_14d_over_prior_close: Some(0.025),
            prior_close_price: Some(220.0),
            beta_qqq_60d: Some(0.8),
            float_shares: Some(2_000_000_000.0),
            days_since_ipo_or_first_bar: Some(2_000),
        }
    }

    fn first_solar_ipo_2007() -> ClassificationInputs<'static> {
        ClassificationInputs {
            ticker_type: Some("CS"),
            is_adr: Some(false),
            country_of_origin: Some("US"),
            market_cap_bucket: Some(MarketCapBucket::Small),
            sector: Some("Energy"),
            industry: Some("Renewable Energy Equipment"),
            sub_industry: None,
            addv_20d_rank_today: Some(800),
            realized_vol_21d_rank_today_percentile: Some(50.0),
            realized_vol_21d_percentile_jump_vs_prior_30d: Some(10.0),
            addv_20d_percentile_today: Some(70.0),
            intraday_dollar_volume_0930_to_1000_rank_today: Some(900),
            atr_14d_over_prior_close: Some(0.04),
            prior_close_price: Some(35.0),
            beta_qqq_60d: Some(1.1),
            float_shares: Some(80_000_000.0),
            days_since_ipo_or_first_bar: Some(20),
        }
    }

    fn regional_bank_normal() -> ClassificationInputs<'static> {
        ClassificationInputs {
            ticker_type: Some("CS"),
            is_adr: Some(false),
            country_of_origin: Some("US"),
            market_cap_bucket: Some(MarketCapBucket::Mid),
            sector: Some("Financials"),
            industry: Some("Regional Banks"),
            sub_industry: Some("Regional Banks"),
            addv_20d_rank_today: Some(500),
            realized_vol_21d_rank_today_percentile: Some(40.0),
            realized_vol_21d_percentile_jump_vs_prior_30d: Some(5.0),
            addv_20d_percentile_today: Some(60.0),
            intraday_dollar_volume_0930_to_1000_rank_today: Some(700),
            atr_14d_over_prior_close: Some(0.02),
            prior_close_price: Some(45.0),
            beta_qqq_60d: Some(0.5),
            float_shares: Some(120_000_000.0),
            days_since_ipo_or_first_bar: Some(5_000),
        }
    }

    fn missing_everything() -> ClassificationInputs<'static> {
        ClassificationInputs::default()
    }

    /// Snapshot: NVDA-shape. Should fire mega-cap-tech, large-cap-tech,
    /// and semiconductor. Should NOT fire biotech, regional bank, ADR
    /// arms, low-float, or meme.
    #[test]
    fn snapshot_nvda_mega_cap_2024() {
        let got = classify(&nvda_mega_cap_2024());
        assert_eq!(
            got,
            BehavioralTags {
                is_mega_cap_tech: true,
                is_large_cap_tech: true,
                is_semiconductor: true,
                is_biotech: false,
                is_regional_bank: false,
                is_energy: false,
                is_china_adr: false,
                is_low_float_candidate: false,
                is_meme_candidate: false,
                is_recent_ipo: false,
            }
        );
    }

    /// Snapshot: biotech low-float gapper, no float data, high vol. Should
    /// fire biotech + low-float (via proxy) + meme (arm B). Should NOT
    /// fire mega-cap-tech, regional bank, ADR arms.
    #[test]
    fn snapshot_biotech_gapper_low_float() {
        let got = classify(&biotech_gapper_low_float_2021());
        assert_eq!(
            got,
            BehavioralTags {
                is_mega_cap_tech: false,
                is_large_cap_tech: false,
                is_semiconductor: false,
                is_biotech: true,
                is_regional_bank: false,
                is_energy: false,
                is_china_adr: false,
                is_low_float_candidate: true,
                is_meme_candidate: true,
                is_recent_ipo: false,
            }
        );
    }

    /// Snapshot: BABA-shape China ADR, mega cap. Should fire
    /// china_adr only. Should NOT fire mega_cap_tech (beta_qqq < 0.9).
    #[test]
    fn snapshot_baba_china_adr() {
        let got = classify(&baba_china_adr_2020());
        assert_eq!(
            got,
            BehavioralTags {
                is_mega_cap_tech: false,
                is_large_cap_tech: false,
                is_semiconductor: false,
                is_biotech: false,
                is_regional_bank: false,
                is_energy: false,
                is_china_adr: true,
                is_low_float_candidate: false,
                is_meme_candidate: false,
                is_recent_ipo: false,
            }
        );
    }

    /// Snapshot: First-Solar-shape, IPO'd 20 days ago, Energy sector. Should
    /// fire energy + recent_ipo. Should NOT fire low-float (float ≥ 50M).
    #[test]
    fn snapshot_first_solar_recent_ipo() {
        let got = classify(&first_solar_ipo_2007());
        assert_eq!(
            got,
            BehavioralTags {
                is_mega_cap_tech: false,
                is_large_cap_tech: false,
                is_semiconductor: false,
                is_biotech: false,
                is_regional_bank: false,
                is_energy: true,
                is_china_adr: false,
                is_low_float_candidate: false,
                is_meme_candidate: false,
                is_recent_ipo: true,
            }
        );
    }

    /// Snapshot: a regional bank. Should fire regional_bank only.
    #[test]
    fn snapshot_regional_bank() {
        let got = classify(&regional_bank_normal());
        assert_eq!(
            got,
            BehavioralTags {
                is_mega_cap_tech: false,
                is_large_cap_tech: false,
                is_semiconductor: false,
                is_biotech: false,
                is_regional_bank: true,
                is_energy: false,
                is_china_adr: false,
                is_low_float_candidate: false,
                is_meme_candidate: false,
                is_recent_ipo: false,
            }
        );
    }

    /// Missing-everything inputs short-circuit to all-false. The whole point
    /// of the `is_some_and` pattern is "informational NULL = no claim."
    #[test]
    fn snapshot_missing_inputs_yields_all_false() {
        let got = classify(&missing_everything());
        assert_eq!(got, BehavioralTags::default());
        // ...and the default IS all-false.
        assert_eq!(got, BehavioralTags {
            is_mega_cap_tech: false,
            is_large_cap_tech: false,
            is_semiconductor: false,
            is_biotech: false,
            is_regional_bank: false,
            is_energy: false,
            is_china_adr: false,
            is_low_float_candidate: false,
            is_meme_candidate: false,
            is_recent_ipo: false,
        });
    }

    /// Snapshot: pin the v1 threshold constants. If any of these change
    /// the security_classification_version must move off "v1" first.
    #[test]
    fn snapshot_v1_thresholds() {
        use thresholds::*;
        assert_eq!(MEGA_CAP_TECH_ADDV_RANK_MAX, 50);
        assert_eq!(MEGA_CAP_TECH_BETA_QQQ_MIN, 0.9);
        assert_eq!(LARGE_CAP_TECH_ADDV_RANK_MAX, 500);
        assert_eq!(LOW_FLOAT_SHARES_MAX, 50_000_000.0);
        assert_eq!(LOW_FLOAT_PROXY_ATR_RATIO_MIN, 0.05);
        assert_eq!(LOW_FLOAT_PROXY_PRIOR_CLOSE_MIN, 1.0);
        assert_eq!(LOW_FLOAT_PROXY_ADDV_RANK_MIN, 5_000);
        assert_eq!(MEME_VOL_PERCENTILE_MIN, 95.0);
        assert_eq!(MEME_DOLLAR_VOLUME_RANK_MAX, 50);
        assert_eq!(MEME_ADDV_PERCENTILE_JUMP_MIN, 90.0);
        assert_eq!(RECENT_IPO_DAYS_MAX, 60);
    }

    #[test]
    fn rules_version_matches_core_constant() {
        assert_eq!(rules_version(), "v1");
        assert_eq!(rules_version(), SECURITY_CLASSIFICATION_VERSION);
    }
}
