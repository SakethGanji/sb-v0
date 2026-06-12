//! `security_classification_daily` builder (RFC §11.6).
//!
//! Point-in-time per-(day, security) tags so downstream never blindly
//! pools mega-cap tech with low-float biotech gappers, leveraged ETFs,
//! ADRs, SPACs, or warrants. Behavioral tags come from the pinned v1
//! rules in `momentum-classify`; everything here only assembles their
//! inputs and the bucket columns.
//!
//! ## Documented v1 proxies (all heuristic, RFC §13.8)
//!
//! - `market_cap`: primary = point-in-time shares from the latest SEC
//!   filing as-of D (basic_average_shares) × UNADJUSTED prior close —
//!   both on D's basis, so splits cancel and issuance/buybacks are
//!   tracked to the quarter. No filings as-of D (funds, foreign 20-F
//!   issuers, pre-first-filing IPOs) → null cap + null bucket; a
//!   snapshot-shares fallback was rejected as dilution-inconsistent
//!   (the independent spot check caught TOPS at $2.8e17 and a $5T
//!   2016 Seadrill under a static guard).
//! - Bucket boundaries (v1, absolute):
//!   market_cap: micro <300M / small <2B / mid <10B / large <200B / mega;
//!   liquidity (addv_20d $): illiquid <1M / thin <10M / normal <100M /
//!   liquid <1B / highly_liquid;
//!   price (prior close): sub_1 / 1_to_5 / 5_to_20 / 20_to_100 / above_100;
//!   volatility (realized_vol_21d annualized): low <0.25 / medium <0.50 /
//!   high <1.0 / extreme;
//!   style (beta_spy_60d): defensive <0.7 / market ≤1.3 / aggressive.
//! - `is_leveraged_etf` / `is_inverse_etf`: beta heuristic (no leverage
//!   factor on disk): ETF with |beta_spy| ≥ 1.8 → leveraged; ETF with
//!   beta_spy ≤ −0.5 → inverse.
//! - `is_spac`: SIC 6770 (blank checks).
//! - `country_of_origin` is NOT on disk (Polygon locale is us/global) →
//!   `is_china_adr` is structurally false in v1; documented gap.
//! - `realized_vol_21d_percentile_jump_vs_prior_30d` input is None in v1
//!   (needs a percentile history) → the meme tag fires via arm A only.
//! - `listing_status` = "trading" (the row exists because bars exist —
//!   the only point-in-time-true statement available);
//!   `listing_status_date` stays null until a status feed exists.
//! - `days_since_ipo_or_first_bar`: calendar days since `list_date` when
//!   known; else since the security's first bar in the dataset, but only
//!   when that first bar is later than the dataset start + 5 trading
//!   days (otherwise unknowable → null, never a fake "recent IPO").

use crate::aggregates::DailyAgg;
use crate::daily_observation::DayContext;
use crate::rolling::RollingState;
use crate::sic;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, RecordBatch, StringArray,
    StringDictionaryBuilder, new_null_array,
};
use arrow::datatypes::Int32Type;
use arrow::error::ArrowError;
use chrono::NaiveDate;
use momentum_classify::{BehavioralTags, ClassificationInputs, MarketCapBucket, classify};
use momentum_core::phase0_outputs::security_classification_daily_schema;
use momentum_store::bar_reader::DaySession;
use momentum_store::tickers_classified::ClassifiedLite;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ClassificationRow<'a> {
    pub session: &'a DaySession,
    pub agg: Option<DailyAgg>,
    /// Σ(close×volume) over 09:30–10:00, for the meme-tag rank input.
    pub dvol_0930_1000: Option<f64>,
    pub reference: Option<&'a ClassifiedLite>,
    /// Point-in-time share count from the latest filing as-of D.
    pub shares_asof: Option<f64>,
}

/// Point-in-time share counts: sid → (filing_date, basic_average_shares),
/// sorted. Availability date = filing_date (shares are public once filed).
pub struct SharesLookup {
    map: HashMap<String, Vec<(NaiveDate, f64)>>,
}

impl SharesLookup {
    pub fn from_filings(filings: &[momentum_store::financials::FilingLite]) -> Self {
        let mut map: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
        for f in filings {
            if let Some(sh) = f.basic_average_shares.filter(|s| *s > 0.0) {
                let sid = f.security_id.clone().unwrap_or_else(|| f.ticker.clone());
                map.entry(sid).or_default().push((f.filing_date, sh));
            }
        }
        for v in map.values_mut() {
            // Deterministic tie-break for same-day filings (quarterly +
            // annual often share a filing date with different share
            // counts): date, then share count. Vendor file order is not
            // stable across re-ingests.
            v.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then(a.1.partial_cmp(&b.1).expect("finite shares"))
            });
        }
        Self { map }
    }

    /// Latest filed share count with filing_date <= day.
    pub fn as_of(&self, sid: &str, day: NaiveDate) -> Option<f64> {
        let v = self.map.get(sid)?;
        let idx = v.partition_point(|(d, _)| *d <= day);
        (idx > 0).then(|| v[idx - 1].1)
    }

    pub fn securities(&self) -> usize {
        self.map.len()
    }
}

fn market_cap_bucket(cap: f64) -> (&'static str, MarketCapBucket) {
    if cap < 300e6 {
        ("micro", MarketCapBucket::Micro)
    } else if cap < 2e9 {
        ("small", MarketCapBucket::Small)
    } else if cap < 10e9 {
        ("mid", MarketCapBucket::Mid)
    } else if cap < 200e9 {
        ("large", MarketCapBucket::Large)
    } else {
        ("mega", MarketCapBucket::Mega)
    }
}

fn liquidity_bucket(addv: f64) -> &'static str {
    if addv < 1e6 {
        "illiquid"
    } else if addv < 10e6 {
        "thin"
    } else if addv < 100e6 {
        "normal"
    } else if addv < 1e9 {
        "liquid"
    } else {
        "highly_liquid"
    }
}

fn price_bucket(px: f64) -> &'static str {
    if px < 1.0 {
        "sub_1"
    } else if px < 5.0 {
        "1_to_5"
    } else if px < 20.0 {
        "5_to_20"
    } else if px < 100.0 {
        "20_to_100"
    } else {
        "above_100"
    }
}

fn volatility_bucket(rv: f64) -> &'static str {
    if rv < 0.25 {
        "low"
    } else if rv < 0.50 {
        "medium"
    } else if rv < 1.0 {
        "high"
    } else {
        "extreme"
    }
}

fn style_bucket(beta_spy: f64) -> &'static str {
    if beta_spy < 0.7 {
        "defensive"
    } else if beta_spy <= 1.3 {
        "market"
    } else {
        "aggressive"
    }
}

/// Descending ranks + percentiles (rank 1 = largest; percentile = share
/// of valid cross-section strictly below, rank-based). Same convention
/// as daily_observation's helper.
fn ranks_desc(vals: &[Option<f64>]) -> (Vec<Option<i32>>, Vec<Option<f64>>) {
    let mut idx: Vec<usize> = (0..vals.len())
        .filter(|&i| vals[i].is_some_and(|v| v.is_finite()))
        .collect();
    idx.sort_by(|&a, &b| vals[b].partial_cmp(&vals[a]).expect("finite"));
    let m = idx.len();
    let mut rank = vec![None; vals.len()];
    let mut pct = vec![None; vals.len()];
    for (r0, &i) in idx.iter().enumerate() {
        rank[i] = Some((r0 + 1) as i32);
        pct[i] = Some((m - 1 - r0) as f64 / m as f64);
    }
    (rank, pct)
}

pub fn build(
    day: NaiveDate,
    rows: &[ClassificationRow<'_>],
    rolling: &RollingState,
    ctx: &DayContext<'_>,
    dataset_start: NaiveDate,
) -> Result<RecordBatch, ArrowError> {
    let schema = security_classification_daily_schema();
    let n = rows.len();

    // Pass 1: per-row scalars that feed cross-sectional ranks.
    let mut market_cap: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut addv20: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut rvol21: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut dvol_today: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut dvol_1000: Vec<Option<f64>> = Vec::with_capacity(n);
    for r in rows {
        let hist = rolling.get(r.session.security_id.as_str());
        let prior = hist.and_then(|h| h.prior());
        // Primary: filed shares × unadjusted prior close (same basis at D).
        // Fallback: snapshot shares × adjusted prior close (see module doc).
        // Filed shares × unadjusted prior close, both on D's basis —
        // or NULL. A snapshot-shares fallback was tried and rejected:
        // it is split-consistent but not dilution-consistent, and no
        // static absurdity guard is era-correct (it produced $5T
        // Seadrill in 2016). Foreign 20-F issuers and funds therefore
        // carry null caps — honest absence, never fabricated data.
        //
        // Filed-shares plausibility: a filing row with a unit error
        // (thousands vs shares — caught as a $4.3T 2016 CNX) is rejected
        // by cross-checking against snapshot shares rebased to D's split
        // basis; >30× disagreement → null. No snapshot → filed accepted.
        let snap_rebased = r
            .reference
            .and_then(|c| c.weighted_shares_outstanding_snapshot)
            .zip(prior)
            .map(|(snap, p)| {
                let f = if p.rth_close_unadjusted > 0.0 {
                    p.rth_close / p.rth_close_unadjusted
                } else {
                    1.0
                };
                snap * f
            })
            .filter(|s| *s > 0.0);
        let cap = r
            .shares_asof
            .filter(|sh| {
                snap_rebased.is_none_or(|ps| {
                    let ratio = sh / ps;
                    (1.0 / 30.0..=30.0).contains(&ratio)
                })
            })
            .zip(prior.map(|p| p.rth_close_unadjusted))
            .map(|(sh, px)| sh * px);
        market_cap.push(cap);
        addv20.push(hist.and_then(|h| h.addv(20)));
        rvol21.push(hist.and_then(|h| h.realized_vol(21)));
        dvol_today.push(r.agg.map(|a| a.rth_dollar_volume));
        dvol_1000.push(r.dvol_0930_1000);
    }
    let (cap_rank, cap_pct) = ranks_desc(&market_cap);
    let (addv_rank, addv_pct) = ranks_desc(&addv20);
    let (rvol_rank_unused, rvol_pct) = ranks_desc(&rvol21);
    let _ = rvol_rank_unused;
    let (dvol_rank, _) = ranks_desc(&dvol_today);
    let (dvol1000_rank, _) = ranks_desc(&dvol_1000);

    // Pass 2: everything else + the v1 behavioral tags.
    let mut col_str: HashMap<&str, Vec<Option<String>>> = HashMap::new();
    let mut col_f64: HashMap<&str, Vec<Option<f64>>> = HashMap::new();
    let mut col_i32: HashMap<&str, Vec<Option<i32>>> = HashMap::new();
    let mut col_bool: HashMap<&str, Vec<Option<bool>>> = HashMap::new();
    let mut tags: Vec<BehavioralTags> = Vec::with_capacity(n);

    for (i, r) in rows.iter().enumerate() {
        let hist = rolling.get(r.session.security_id.as_str());
        let prior_close = hist.and_then(|h| h.prior()).map(|p| p.rth_close);
        let c = r.reference;
        let ttype = c.and_then(|c| c.ticker_type.clone());
        let t = ttype.as_deref();
        let is_etf = matches!(t, Some("ETF") | Some("ETV") | Some("ETS") | Some("FUND"));
        let is_adr = t.is_some_and(|t| t.starts_with("ADR"));
        let beta_spy = hist.and_then(|h| h.beta(ctx.index_cumlog[0], 60));
        let beta_qqq = hist.and_then(|h| h.beta(ctx.index_cumlog[1], 60));
        let beta_iwm = hist.and_then(|h| h.beta(ctx.index_cumlog[2], 60));
        let (sector, industry) = c
            .and_then(|c| c.sic_code.as_deref())
            .map(sic::sector_industry)
            .unwrap_or((None, None));
        let sub_industry = c.and_then(|c| c.sic_description.clone());
        let atr14 = hist.and_then(|h| h.atr(14));
        let days_since_ipo: Option<i32> = match c.and_then(|c| c.list_date) {
            Some(ld) => Some((day - ld).num_days() as i32),
            None => hist
                .map(|h| h.first_bar_day)
                .filter(|fb| (*fb - dataset_start).num_days() > 7)
                .map(|fb| (day - fb).num_days() as i32),
        };

        col_str.entry("ticker_type").or_default().push(ttype.clone());
        col_bool.entry("is_common_stock").or_default().push(t.map(|t| t == "CS"));
        col_bool.entry("is_etf").or_default().push(t.map(|_| is_etf));
        col_bool
            .entry("is_leveraged_etf")
            .or_default()
            .push(Some(is_etf && beta_spy.is_some_and(|b| b.abs() >= 1.8)));
        col_bool
            .entry("is_inverse_etf")
            .or_default()
            .push(Some(is_etf && beta_spy.is_some_and(|b| b <= -0.5)));
        col_bool.entry("is_etn").or_default().push(t.map(|t| t == "ETN"));
        col_bool.entry("is_adr").or_default().push(t.map(|_| is_adr));
        col_bool
            .entry("is_spac")
            .or_default()
            .push(Some(c.and_then(|c| c.sic_code.as_deref()) == Some("6770")));
        col_bool.entry("is_warrant").or_default().push(t.map(|t| t == "WARRANT"));
        col_bool.entry("is_preferred").or_default().push(t.map(|t| t == "PFD"));
        col_bool.entry("is_unit").or_default().push(t.map(|t| t == "UNIT"));
        col_str
            .entry("primary_exchange")
            .or_default()
            .push(c.and_then(|c| c.primary_exchange.clone()));
        col_str
            .entry("listing_status")
            .or_default()
            .push(Some("trading".into()));
        col_i32.entry("days_since_ipo_or_first_bar").or_default().push(days_since_ipo);
        col_str.entry("sector").or_default().push(sector.map(String::from));
        col_str.entry("industry").or_default().push(industry.map(String::from));
        col_str.entry("sub_industry").or_default().push(sub_industry.clone());
        col_f64.entry("market_cap").or_default().push(market_cap[i]);
        let cap_b = market_cap[i].map(market_cap_bucket);
        col_str
            .entry("market_cap_bucket")
            .or_default()
            .push(cap_b.map(|(s, _)| s.to_string()));
        col_i32.entry("market_cap_rank_today").or_default().push(cap_rank[i]);
        col_f64.entry("market_cap_percentile_today").or_default().push(cap_pct[i]);
        col_f64.entry("adv_20d").or_default().push(hist.and_then(|h| h.adv(20)));
        col_f64.entry("adv_60d").or_default().push(hist.and_then(|h| h.adv(60)));
        col_f64.entry("addv_20d").or_default().push(addv20[i]);
        col_f64.entry("addv_60d").or_default().push(hist.and_then(|h| h.addv(60)));
        col_str
            .entry("liquidity_bucket")
            .or_default()
            .push(addv20[i].map(|a| liquidity_bucket(a).to_string()));
        col_i32.entry("addv_rank_today").or_default().push(addv_rank[i]);
        col_i32.entry("dollar_volume_rank_today").or_default().push(dvol_rank[i]);
        col_f64.entry("prior_close_price").or_default().push(prior_close);
        col_str
            .entry("price_bucket")
            .or_default()
            .push(prior_close.map(|p| price_bucket(p).to_string()));
        col_f64.entry("atr_14d").or_default().push(atr14);
        col_f64.entry("realized_vol_21d").or_default().push(rvol21[i]);
        col_f64
            .entry("yang_zhang_vol_21d")
            .or_default()
            .push(hist.and_then(|h| h.yang_zhang_vol(21)));
        col_str
            .entry("volatility_bucket")
            .or_default()
            .push(rvol21[i].map(|v| volatility_bucket(v).to_string()));
        col_f64.entry("volatility_percentile_today").or_default().push(rvol_pct[i]);
        col_f64.entry("beta_spy_60d").or_default().push(beta_spy);
        col_f64.entry("beta_qqq_60d").or_default().push(beta_qqq);
        col_f64.entry("beta_iwm_60d").or_default().push(beta_iwm);
        col_str
            .entry("style_bucket")
            .or_default()
            .push(beta_spy.map(|b| style_bucket(b).to_string()));

        tags.push(classify(&ClassificationInputs {
            ticker_type: t,
            is_adr: t.map(|_| is_adr),
            country_of_origin: None, // not on disk — documented v1 gap
            market_cap_bucket: cap_b.map(|(_, b)| b),
            sector,
            industry,
            sub_industry: sub_industry.as_deref(),
            addv_20d_rank_today: addv_rank[i],
            realized_vol_21d_rank_today_percentile: rvol_pct[i].map(|p| p * 100.0),
            realized_vol_21d_percentile_jump_vs_prior_30d: None, // v1 gap
            addv_20d_percentile_today: addv_pct[i].map(|p| p * 100.0),
            intraday_dollar_volume_0930_to_1000_rank_today: dvol1000_rank[i],
            atr_14d_over_prior_close: atr14.zip(prior_close).map(|(a, p)| a / p),
            prior_close_price: prior_close,
            beta_qqq_60d: beta_qqq,
            float_shares: None, // deferred ingest
            days_since_ipo_or_first_bar: days_since_ipo,
        }));
    }

    // Assemble against the schema.
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let day32 = (day - epoch).num_days() as i32;
    let dict = |v: &[Option<String>]| -> ArrayRef {
        let mut b = StringDictionaryBuilder::<Int32Type>::new();
        for x in v {
            b.append_option(x.as_deref());
        }
        Arc::new(b.finish())
    };

    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| -> ArrayRef {
            let name = f.name().as_str();
            if let Some(v) = col_str.get(name) {
                return dict(v);
            }
            if let Some(v) = col_f64.get(name) {
                return Arc::new(Float64Array::from(v.clone()));
            }
            if let Some(v) = col_i32.get(name) {
                return Arc::new(Int32Array::from(v.clone()));
            }
            if let Some(v) = col_bool.get(name) {
                return Arc::new(BooleanArray::from(v.clone()));
            }
            match name {
                "day" => Arc::new(Date32Array::from(vec![day32; n])),
                "security_id" => Arc::new(StringArray::from(
                    rows.iter().map(|r| r.session.security_id.as_str()).collect::<Vec<_>>(),
                )),
                "display_symbol_on_day" => Arc::new(StringArray::from(
                    rows.iter().map(|r| r.session.display_symbol.as_str()).collect::<Vec<_>>(),
                )),
                "is_mega_cap_tech" => Arc::new(BooleanArray::from(
                    tags.iter().map(|t| Some(t.is_mega_cap_tech)).collect::<Vec<_>>(),
                )),
                "is_large_cap_tech" => Arc::new(BooleanArray::from(
                    tags.iter().map(|t| Some(t.is_large_cap_tech)).collect::<Vec<_>>(),
                )),
                "is_semiconductor" => Arc::new(BooleanArray::from(
                    tags.iter().map(|t| Some(t.is_semiconductor)).collect::<Vec<_>>(),
                )),
                "is_biotech" => Arc::new(BooleanArray::from(
                    tags.iter().map(|t| Some(t.is_biotech)).collect::<Vec<_>>(),
                )),
                "is_regional_bank" => Arc::new(BooleanArray::from(
                    tags.iter().map(|t| Some(t.is_regional_bank)).collect::<Vec<_>>(),
                )),
                "is_energy" => Arc::new(BooleanArray::from(
                    tags.iter().map(|t| Some(t.is_energy)).collect::<Vec<_>>(),
                )),
                "is_china_adr" => Arc::new(BooleanArray::from(
                    tags.iter().map(|t| Some(t.is_china_adr)).collect::<Vec<_>>(),
                )),
                "is_low_float_candidate" => Arc::new(BooleanArray::from(
                    tags.iter().map(|t| Some(t.is_low_float_candidate)).collect::<Vec<_>>(),
                )),
                "is_meme_candidate" => Arc::new(BooleanArray::from(
                    tags.iter().map(|t| Some(t.is_meme_candidate)).collect::<Vec<_>>(),
                )),
                "is_recent_ipo" => Arc::new(BooleanArray::from(
                    tags.iter().map(|t| Some(t.is_recent_ipo)).collect::<Vec<_>>(),
                )),
                _ => new_null_array(f.data_type(), n),
            }
        })
        .collect();
    RecordBatch::try_new(schema, columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slices::et;
    use arrow::array::{Array, AsArray};
    use momentum_core::bar::Bar;
    use momentum_core::ids::SecurityId;
    use momentum_core::store::Session;

    fn mk(day: NaiveDate, sid: &str, sym: &str, px: f64) -> DaySession {
        DaySession {
            security_id: SecurityId::new(sid),
            display_symbol: sym.to_string(),
            session: Session::new(
                day,
                vec![Bar { t: et(day, 9, 30), open: px, high: px, low: px, close: px, volume: 100.0 }],
            ),
        }
    }

    fn lite(sid: &str, sym: &str, ttype: &str, sic: Option<&str>, shares: Option<f64>) -> ClassifiedLite {
        ClassifiedLite {
            security_id: Some(sid.into()),
            display_symbol: sym.into(),
            ticker_type: Some(ttype.into()),
            primary_exchange: Some("XNAS".into()),
            locale: Some("us".into()),
            list_date: Some(NaiveDate::from_ymd_opt(2010, 1, 4).unwrap()),
            sic_code: sic.map(String::from),
            sic_description: sic.map(|_| "desc".to_string()),
            market_cap_snapshot: None,
            weighted_shares_outstanding_snapshot: shares,
        }
    }

    #[test]
    fn buckets_flags_and_schema() {
        let day = NaiveDate::from_ymd_opt(2021, 3, 15).unwrap();
        let prev = NaiveDate::from_ymd_opt(2021, 3, 12).unwrap();
        let mut rolling = RollingState::new();
        // Prior close 50 for the CS; gives market_cap = 1e9 * ... shares 100M → 5e9 (mid).
        let s_prev = mk(prev, "BBGCS", "CS1", 50.0);
        rolling.update(
            "BBGCS",
            prev,
            crate::aggregates::compute(&s_prev.session, prev, et(prev, 9, 30), 1.0).unwrap(),
        );

        let sessions = [
            mk(day, "BBGCS", "CS1", 50.0),
            mk(day, "BBGETF", "LETF", 10.0),
            mk(day, "BBGW", "WTS", 1.5),
        ];
        let refs = [
            lite("BBGCS", "CS1", "CS", Some("3674"), Some(100e6)),
            lite("BBGETF", "LETF", "ETF", None, None),
            lite("BBGW", "WTS", "WARRANT", None, None),
        ];
        let rows: Vec<ClassificationRow<'_>> = sessions
            .iter()
            .zip(refs.iter())
            .map(|(s, c)| ClassificationRow {
                session: s,
                agg: crate::aggregates::compute(&s.session, day, et(day, 9, 30), 1.0),
                dvol_0930_1000: Some(1000.0),
                reference: Some(c),
                shares_asof: c.weighted_shares_outstanding_snapshot, // CS row: 100M filed
            })
            .collect();
        let empty: HashMap<NaiveDate, f64> = HashMap::new();
        let ctx = DayContext { index_cumlog: [&empty, &empty, &empty], signal_share_history: &[] };
        let batch = build(day, &rows, &rolling, &ctx, NaiveDate::from_ymd_opt(2016, 6, 8).unwrap()).unwrap();

        assert_eq!(batch.schema(), security_classification_daily_schema());
        assert_eq!(batch.num_rows(), 3);
        let b = |name: &str| {
            batch.column_by_name(name).unwrap().as_boolean().clone()
        };
        assert!(b("is_common_stock").value(0));
        assert!(!b("is_etf").value(0));
        assert!(b("is_etf").value(1));
        assert!(b("is_warrant").value(2));
        assert!(b("is_semiconductor").value(0), "SIC 3674 → Semiconductors tag");

        // market_cap = 100M filed shares × unadjusted prior close 50 = 5e9 → mid.
        let cap = batch.column_by_name("market_cap").unwrap();
        let cap = cap.as_primitive::<arrow::datatypes::Float64Type>();
        assert_eq!(cap.value(0), 5e9);
        let bucket = arrow::compute::cast(
            batch.column_by_name("market_cap_bucket").unwrap(),
            &arrow::datatypes::DataType::Utf8,
        )
        .unwrap();
        assert_eq!(bucket.as_string::<i32>().value(0), "mid");
        // ETF row has no shares → null cap, null bucket.
        assert!(cap.is_null(1));

        // days_since_ipo from list_date (calendar days 2010-01-04 → 2021-03-15).
        let dsi = batch.column_by_name("days_since_ipo_or_first_bar").unwrap();
        let dsi = dsi.as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(dsi.value(0), 4088);

        let status = arrow::compute::cast(
            batch.column_by_name("listing_status").unwrap(),
            &arrow::datatypes::DataType::Utf8,
        )
        .unwrap();
        assert_eq!(status.as_string::<i32>().value(0), "trading");
    }
}
