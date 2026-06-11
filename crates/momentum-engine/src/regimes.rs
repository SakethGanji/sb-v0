//! `regime_definitions` derivation (RFC §10) — post-pass over the
//! written `market_context_daily` files.
//!
//! Five v1 taxonomies, long format (day, taxonomy, regime_id):
//! - `era` — fixed calendar labels (strategy doc §2.5).
//! - `spy_trend` — trailing 63-trading-day SPY close return, tertiled.
//! - `vix_level`, `breadth` (A/D eod), `liquidity` (universe median
//!   ADDV) — tertiled.
//!
//! **Leakage discipline:** tertile boundaries are computed ONLY on the
//! exploration window (2016-06-08 → 2020-12-31, frozen in
//! `phase1-research-strategy.md` §2.5/§7.7.1) and stamped into file
//! metadata (`regime_thresholds_<taxonomy>` + the window) so every
//! assignment is reproducible. Days without the underlying metric
//! (e.g. the first 63 days for spy_trend) simply have no row for that
//! taxonomy — absence is honest, never a fake bucket.

use arrow::array::{ArrayRef, Date32Array, Int32Array, RecordBatch, StringArray};
use arrow::error::ArrowError;
use chrono::NaiveDate;
use momentum_core::phase0_outputs::{regime_definitions_schema, regime_thresholds_key};
use std::sync::Arc;

pub const EXPLORATION_START: &str = "2016-06-08";
pub const EXPLORATION_END: &str = "2020-12-31";
pub const TAXONOMY_VERSION: &str = "v1";

#[derive(Debug, Clone)]
pub struct DayMetrics {
    pub day: NaiveDate,
    pub spy_close: Option<f64>,
    pub vix_close: Option<f64>,
    pub breadth_ad_eod: Option<f64>,
    pub median_addv: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RegimeAssignment {
    pub day: NaiveDate,
    pub taxonomy: &'static str,
    pub regime_id: i32,
    pub regime_name: &'static str,
}

fn era_of(day: NaiveDate) -> (i32, &'static str) {
    let d = |y, m, dd| NaiveDate::from_ymd_opt(y, m, dd).expect("valid");
    if day <= d(2019, 12, 31) {
        (1, "zirp_bull")
    } else if day <= d(2022, 6, 30) {
        (2, "covid_meme")
    } else if day <= d(2024, 12, 31) {
        (3, "rate_shock_ai")
    } else {
        (4, "recent")
    }
}

/// Tertile boundaries (33.3 / 66.7 percentiles, linear interpolation)
/// over the exploration-window values of one metric.
fn tertile_bounds(mut vals: Vec<f64>) -> Option<(f64, f64)> {
    if vals.len() < 9 {
        return None; // too few days to define a regime taxonomy
    }
    vals.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let q = |p: f64| {
        let pos = p * (vals.len() - 1) as f64;
        let lo = pos.floor() as usize;
        let frac = pos - lo as f64;
        if lo + 1 < vals.len() {
            vals[lo] * (1.0 - frac) + vals[lo + 1] * frac
        } else {
            vals[lo]
        }
    };
    Some((q(1.0 / 3.0), q(2.0 / 3.0)))
}

fn bucket3(v: f64, bounds: (f64, f64)) -> usize {
    if v <= bounds.0 {
        0
    } else if v <= bounds.1 {
        1
    } else {
        2
    }
}

/// Derive all assignments + the threshold metadata stamps.
/// `metrics` must be sorted by day (the spy_trend lookback indexes it).
pub fn derive(metrics: &[DayMetrics]) -> (Vec<RegimeAssignment>, Vec<(String, String)>) {
    let expl_start = NaiveDate::parse_from_str(EXPLORATION_START, "%Y-%m-%d").expect("const");
    let expl_end = NaiveDate::parse_from_str(EXPLORATION_END, "%Y-%m-%d").expect("const");
    let in_expl = |d: NaiveDate| d >= expl_start && d <= expl_end;

    // Trailing 63-trading-day SPY return per day.
    let spy_ret63: Vec<Option<f64>> = (0..metrics.len())
        .map(|i| {
            if i < 63 {
                return None;
            }
            metrics[i]
                .spy_close
                .zip(metrics[i - 63].spy_close)
                .filter(|(_, base)| *base > 0.0)
                .map(|(now, base)| now / base - 1.0)
        })
        .collect();

    struct Tax {
        name: &'static str,
        names: [&'static str; 3],
        vals: Vec<Option<f64>>,
    }
    let taxes = [
        Tax {
            name: "spy_trend",
            names: ["downtrend", "sideways", "uptrend"],
            vals: spy_ret63,
        },
        Tax {
            name: "vix_level",
            names: ["low", "medium", "high"],
            vals: metrics.iter().map(|m| m.vix_close).collect(),
        },
        Tax {
            name: "breadth",
            names: ["narrow", "mixed", "broad"],
            vals: metrics.iter().map(|m| m.breadth_ad_eod).collect(),
        },
        Tax {
            name: "liquidity",
            names: ["low", "medium", "high"],
            vals: metrics.iter().map(|m| m.median_addv).collect(),
        },
    ];

    let mut out = Vec::new();
    let mut stamps = vec![
        (
            "regime_thresholds_window".to_string(),
            format!("{EXPLORATION_START}..{EXPLORATION_END}"),
        ),
        ("regime_taxonomy_version".to_string(), TAXONOMY_VERSION.to_string()),
    ];

    for m in metrics {
        let (id, name) = era_of(m.day);
        out.push(RegimeAssignment { day: m.day, taxonomy: "era", regime_id: id, regime_name: name });
    }
    stamps.push((regime_thresholds_key("era"), "calendar:2019-12-31,2022-06-30,2024-12-31".into()));

    for tax in taxes {
        let expl_vals: Vec<f64> = metrics
            .iter()
            .zip(&tax.vals)
            .filter(|(m, v)| in_expl(m.day) && v.is_some())
            .map(|(_, v)| v.expect("filtered"))
            .collect();
        let Some(bounds) = tertile_bounds(expl_vals) else {
            stamps.push((regime_thresholds_key(tax.name), "insufficient-data".into()));
            continue;
        };
        stamps.push((
            regime_thresholds_key(tax.name),
            format!("tertiles:{:.6},{:.6}", bounds.0, bounds.1),
        ));
        for (m, v) in metrics.iter().zip(&tax.vals) {
            if let Some(v) = v {
                let b = bucket3(*v, bounds);
                out.push(RegimeAssignment {
                    day: m.day,
                    taxonomy: tax.name,
                    regime_id: (b + 1) as i32,
                    regime_name: tax.names[b],
                });
            }
        }
    }
    (out, stamps)
}

pub fn to_batch(rows: &[RegimeAssignment]) -> Result<RecordBatch, ArrowError> {
    use arrow::array::StringDictionaryBuilder;
    use arrow::datatypes::Int32Type;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let mut tax = StringDictionaryBuilder::<Int32Type>::new();
    for r in rows {
        tax.append_value(r.taxonomy);
    }
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(
            rows.iter().map(|r| (r.day - epoch).num_days() as i32).collect::<Vec<_>>(),
        )),
        Arc::new(tax.finish()),
        Arc::new(StringArray::from(vec![TAXONOMY_VERSION; rows.len()])),
        Arc::new(Int32Array::from(rows.iter().map(|r| r.regime_id).collect::<Vec<_>>())),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.regime_name).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(regime_definitions_schema(), cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(n: i64) -> NaiveDate {
        NaiveDate::from_ymd_opt(2016, 6, 8).unwrap() + chrono::Duration::days(n)
    }

    #[test]
    fn eras_follow_the_frozen_calendar() {
        assert_eq!(era_of(NaiveDate::from_ymd_opt(2018, 5, 1).unwrap()), (1, "zirp_bull"));
        assert_eq!(era_of(NaiveDate::from_ymd_opt(2021, 1, 27).unwrap()), (2, "covid_meme"));
        assert_eq!(era_of(NaiveDate::from_ymd_opt(2023, 3, 1).unwrap()), (3, "rate_shock_ai"));
        assert_eq!(era_of(NaiveDate::from_ymd_opt(2026, 6, 11).unwrap()), (4, "recent"));
    }

    #[test]
    fn tertiles_split_vix_into_thirds_and_stamp() {
        // 90 days: vix ramps 10 → 99 linearly → clean tertiles.
        let metrics: Vec<DayMetrics> = (0..90)
            .map(|i| DayMetrics {
                day: d(i),
                spy_close: Some(100.0),
                vix_close: Some(10.0 + i as f64),
                breadth_ad_eod: None,
                median_addv: None,
            })
            .collect();
        let (rows, stamps) = derive(&metrics);
        let vix_rows: Vec<_> = rows.iter().filter(|r| r.taxonomy == "vix_level").collect();
        assert_eq!(vix_rows.len(), 90);
        assert_eq!(vix_rows[0].regime_name, "low");
        assert_eq!(vix_rows[89].regime_name, "high");
        let counts = [1, 2, 3].map(|id| vix_rows.iter().filter(|r| r.regime_id == id).count());
        assert!(counts.iter().all(|&c| (29..=31).contains(&c)), "{counts:?}");
        assert!(stamps.iter().any(|(k, v)| k == "regime_thresholds_vix_level" && v.starts_with("tertiles:")));
        // No breadth metric anywhere → no breadth rows, stamped as such.
        assert!(!rows.iter().any(|r| r.taxonomy == "breadth"));
        assert!(stamps.iter().any(|(k, v)| k == "regime_thresholds_breadth" && v == "insufficient-data"));
    }

    #[test]
    fn spy_trend_needs_63_days_of_history() {
        let metrics: Vec<DayMetrics> = (0..100)
            .map(|i| DayMetrics {
                day: d(i),
                spy_close: Some(100.0 * (1.0 + 0.001 * i as f64)),
                vix_close: None,
                breadth_ad_eod: None,
                median_addv: None,
            })
            .collect();
        let (rows, _) = derive(&metrics);
        let trend: Vec<_> = rows.iter().filter(|r| r.taxonomy == "spy_trend").collect();
        assert_eq!(trend.len(), 100 - 63, "first 63 days have no trailing return");
    }

    #[test]
    fn batch_matches_schema() {
        let rows = vec![RegimeAssignment {
            day: d(0),
            taxonomy: "era",
            regime_id: 1,
            regime_name: "zirp_bull",
        }];
        let b = to_batch(&rows).unwrap();
        assert_eq!(b.schema(), regime_definitions_schema());
        assert_eq!(b.num_rows(), 1);
    }
}
