//! `sector_aggregates_daily` builder (RFC §11.5).
//!
//! One row per (day, sector). Sector identity comes from the SIC →
//! sector mapping ([`crate::sic`]) over `tickers_classified.parquet` —
//! a *snapshot* taxonomy (no historical sector membership on disk;
//! documented proxy, RFC §13.8). Securities with no resolvable sector
//! aggregate under `"Unknown"` rather than being dropped — no silent
//! universe filtering (frozen amendment).
//!
//! Definitions:
//! - `sector_ret_*` = EQUAL-WEIGHTED mean of constituent returns over
//!   constituents with a valid value (cap-weighting needs point-in-time
//!   market cap, which is snapshot-only on disk — same proxy caveat).
//! - `sector_pct_green_at_*` = share of valid constituents with ret > 0.
//! - `sector_ret_0930_to_1000_rank_today` = rank of the sector's mean
//!   ret@1000 across that day's sectors, 1 = best.

use arrow::array::{ArrayRef, Date32Array, Float64Array, Int32Array, RecordBatch};
use arrow::error::ArrowError;
use chrono::NaiveDate;
use momentum_core::phase0_outputs::sector_aggregates_daily_schema;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Per-security inputs for one day's sector aggregation.
pub struct SectorRowInput<'a> {
    /// Mapped sector; None aggregates under "Unknown".
    pub sector: Option<&'a str>,
    pub ret_0950: Option<f64>,
    pub ret_1000: Option<f64>,
    pub ret_1030: Option<f64>,
    /// Intraday day return (rth_close / rth_open − 1). [RES]
    pub eod_ret: Option<f64>,
}

#[derive(Default)]
struct Acc {
    n: i32,
    sums: [f64; 4],
    counts: [i32; 4],
    green_1000: i32,
    green_1030: i32,
}

pub fn build(day: NaiveDate, rows: &[SectorRowInput<'_>]) -> Result<RecordBatch, ArrowError> {
    let mut by_sector: BTreeMap<&str, Acc> = BTreeMap::new();
    for r in rows {
        let acc = by_sector.entry(r.sector.unwrap_or("Unknown")).or_default();
        acc.n += 1;
        for (k, v) in [r.ret_0950, r.ret_1000, r.ret_1030, r.eod_ret].into_iter().enumerate() {
            if let Some(v) = v {
                acc.sums[k] += v;
                acc.counts[k] += 1;
                if v > 0.0 {
                    if k == 1 {
                        acc.green_1000 += 1;
                    } else if k == 2 {
                        acc.green_1030 += 1;
                    }
                }
            }
        }
    }

    let mean = |a: &Acc, k: usize| (a.counts[k] > 0).then(|| a.sums[k] / a.counts[k] as f64);
    let sectors: Vec<&str> = by_sector.keys().copied().collect();
    let n = sectors.len();

    // Rank sectors by mean ret@1000 (1 = best; sectors with no valid
    // constituents at 10:00 get a null rank).
    let mean_1000: Vec<Option<f64>> = by_sector.values().map(|a| mean(a, 1)).collect();
    let mut order: Vec<usize> = (0..n).filter(|&i| mean_1000[i].is_some()).collect();
    order.sort_by(|&a, &b| mean_1000[b].partial_cmp(&mean_1000[a]).expect("finite"));
    let mut rank = vec![None; n];
    for (r0, &i) in order.iter().enumerate() {
        rank[i] = Some((r0 + 1) as i32);
    }

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let day32 = (day - epoch).num_days() as i32;

    let mut sector_ids = arrow::array::StringDictionaryBuilder::<arrow::datatypes::Int32Type>::new();
    for s in &sectors {
        sector_ids.append_value(s);
    }
    let accs: Vec<&Acc> = by_sector.values().collect();
    let pct = |g: i32, c: i32| (c > 0).then(|| g as f64 / c as f64);

    let cols: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(vec![day32; n])),
        Arc::new(sector_ids.finish()),
        Arc::new(Int32Array::from(accs.iter().map(|a| a.n).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(accs.iter().map(|a| mean(a, 0)).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(accs.iter().map(|a| mean(a, 1)).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(accs.iter().map(|a| mean(a, 2)).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(accs.iter().map(|a| mean(a, 3)).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(
            accs.iter().map(|a| pct(a.green_1000, a.counts[1])).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            accs.iter().map(|a| pct(a.green_1030, a.counts[2])).collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(rank)),
    ];
    RecordBatch::try_new(sector_aggregates_daily_schema(), cols)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};

    fn row(sector: Option<&'static str>, r1000: Option<f64>) -> SectorRowInput<'static> {
        SectorRowInput {
            sector,
            ret_0950: r1000,
            ret_1000: r1000,
            ret_1030: r1000,
            eod_ret: r1000.map(|x| x * 2.0),
        }
    }

    #[test]
    fn aggregates_rank_and_unknown_bucket() {
        let day = NaiveDate::from_ymd_opt(2021, 3, 15).unwrap();
        let rows = vec![
            row(Some("Technology"), Some(0.02)),
            row(Some("Technology"), Some(-0.01)),
            row(Some("Energy"), Some(0.03)),
            row(None, Some(0.006)),
            row(Some("Technology"), None), // counted in n, not in means
        ];
        let batch = build(day, &rows).unwrap();
        assert_eq!(batch.schema(), sector_aggregates_daily_schema());
        assert_eq!(batch.num_rows(), 3); // Energy, Technology, Unknown

        let sectors = arrow::compute::cast(batch.column_by_name("sector_id").unwrap(), &arrow::datatypes::DataType::Utf8).unwrap();
        let sectors = sectors.as_string::<i32>();
        let idx_of = |name: &str| (0..sectors.len()).find(|&i| sectors.value(i) == name).unwrap();
        let tech = idx_of("Technology");
        let energy = idx_of("Energy");
        let unknown = idx_of("Unknown");

        let count = batch.column_by_name("sector_constituent_count_with_bars").unwrap();
        let count = count.as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(count.value(tech), 3);
        assert_eq!(count.value(unknown), 1);

        let r1000 = batch.column_by_name("sector_ret_0930_to_1000").unwrap();
        let r1000 = r1000.as_primitive::<arrow::datatypes::Float64Type>();
        assert!((r1000.value(tech) - 0.005).abs() < 1e-12, "(0.02 - 0.01)/2");
        assert!((r1000.value(energy) - 0.03).abs() < 1e-12);

        let green = batch.column_by_name("sector_pct_green_at_1000").unwrap();
        let green = green.as_primitive::<arrow::datatypes::Float64Type>();
        assert!((green.value(tech) - 0.5).abs() < 1e-12, "1 of 2 valid");

        let rank = batch.column_by_name("sector_ret_0930_to_1000_rank_today").unwrap();
        let rank = rank.as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(rank.value(energy), 1, "Energy 3% is the day's best");
        assert_eq!(rank.value(unknown), 2, "Unknown 0.6%");
        assert_eq!(rank.value(tech), 3, "Tech mean 0.5%");
    }

    #[test]
    fn empty_universe_builds_empty_batch() {
        let day = NaiveDate::from_ymd_opt(2021, 3, 15).unwrap();
        let batch = build(day, &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }
}
