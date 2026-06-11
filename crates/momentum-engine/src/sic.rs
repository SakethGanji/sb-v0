//! SIC code → (sector, industry) mapping, v1.
//!
//! Heuristic range mapping from 4-digit SIC codes (the only industry
//! taxonomy on disk, via `tickers_classified.parquet`) to GICS-style
//! sector names. The label set is pinned by `momentum-classify`'s rule
//! constants — `Technology` / `Communication Services` / `Consumer
//! Discretionary` / `Energy` sectors and `Semiconductors` /
//! `Biotechnology` / `Pharmaceuticals` / `Regional Banks` industries
//! must match those rules byte-for-byte.
//!
//! This is a *proxy* taxonomy (RFC §13.8): SIC is coarser and staler
//! than GICS. Bumping any range here is a classification-version event
//! (`SECURITY_CLASSIFICATION_VERSION`), same contract as the tag rules.

/// Map a 4-digit SIC code string to `(sector, industry)`. Unknown /
/// unparseable codes → `(None, None)`.
pub fn sector_industry(sic: &str) -> (Option<&'static str>, Option<&'static str>) {
    let Ok(code) = sic.trim().parse::<u16>() else {
        return (None, None);
    };
    let (s, i) = match code {
        100..=999 => ("Consumer Staples", "Agriculture"),
        1000..=1299 => ("Materials", "Metals & Mining"),
        1300..=1399 => ("Energy", "Oil & Gas"),
        1400..=1499 => ("Materials", "Mining & Quarrying"),
        1500..=1799 => ("Industrials", "Construction"),
        2000..=2199 => ("Consumer Staples", "Food, Beverage & Tobacco"),
        2200..=2399 => ("Consumer Discretionary", "Textiles & Apparel"),
        2400..=2499 => ("Materials", "Wood Products"),
        2500..=2599 => ("Consumer Discretionary", "Furniture"),
        2600..=2699 => ("Materials", "Paper"),
        2700..=2799 => ("Communication Services", "Publishing"),
        2834..=2835 => ("Healthcare", "Pharmaceuticals"),
        2836 => ("Healthcare", "Biotechnology"),
        2800..=2899 => ("Materials", "Chemicals"),
        2900..=2999 => ("Energy", "Petroleum Refining"),
        3000..=3199 => ("Consumer Discretionary", "Rubber, Plastics & Leather"),
        3200..=3299 => ("Materials", "Stone, Clay & Glass"),
        3300..=3499 => ("Materials", "Metals"),
        3570..=3579 => ("Technology", "Computer Hardware"),
        3500..=3599 => ("Industrials", "Machinery"),
        3674 => ("Technology", "Semiconductors"),
        3600..=3699 => ("Technology", "Electronics"),
        3711..=3716 => ("Consumer Discretionary", "Automobiles"),
        3700..=3799 => ("Industrials", "Transportation Equipment"),
        3841..=3851 => ("Healthcare", "Medical Devices"),
        3800..=3839 => ("Industrials", "Instruments"),
        3860..=3999 => ("Consumer Discretionary", "Misc Manufacturing"),
        4000..=4799 => ("Industrials", "Transportation"),
        4800..=4899 => ("Communication Services", "Telecommunications"),
        4900..=4999 => ("Utilities", "Utilities"),
        5000..=5199 => ("Consumer Discretionary", "Wholesale"),
        5400..=5499 => ("Consumer Staples", "Food Retail"),
        5200..=5999 => ("Consumer Discretionary", "Retail"),
        // 6022 = state commercial banks — the closest SIC gets to
        // "regional bank"; 6020/6021 (national/major) stay plain Banks.
        6022 => ("Financials", "Regional Banks"),
        6000..=6099 => ("Financials", "Banks"),
        6100..=6199 => ("Financials", "Credit Services"),
        6200..=6299 => ("Financials", "Capital Markets"),
        6300..=6499 => ("Financials", "Insurance"),
        6500..=6599 => ("Real Estate", "Real Estate"),
        6798 => ("Real Estate", "REITs"),
        // 6770 blank checks (SPAC shells) land here deliberately.
        6600..=6799 => ("Financials", "Investment Offices"),
        7000..=7299 => ("Consumer Discretionary", "Consumer Services"),
        7370..=7379 => ("Technology", "Software & IT Services"),
        7300..=7399 => ("Industrials", "Business Services"),
        7400..=7799 => ("Consumer Discretionary", "Consumer Services"),
        7800..=7899 => ("Communication Services", "Media & Entertainment"),
        7900..=7999 => ("Consumer Discretionary", "Entertainment & Recreation"),
        8000..=8099 => ("Healthcare", "Healthcare Services"),
        // 8731 = commercial physical & biological research — where most
        // pre-revenue biotechs live.
        8731 => ("Healthcare", "Biotechnology"),
        8100..=8999 => ("Industrials", "Professional Services"),
        _ => return (None, None),
    };
    (Some(s), Some(i))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_match_classify_rule_constants() {
        // These exact strings are matched by momentum-classify v1 rules.
        assert_eq!(sector_industry("3674"), (Some("Technology"), Some("Semiconductors")));
        assert_eq!(sector_industry("2836"), (Some("Healthcare"), Some("Biotechnology")));
        assert_eq!(sector_industry("8731"), (Some("Healthcare"), Some("Biotechnology")));
        assert_eq!(sector_industry("2834"), (Some("Healthcare"), Some("Pharmaceuticals")));
        assert_eq!(sector_industry("6022"), (Some("Financials"), Some("Regional Banks")));
        assert_eq!(sector_industry("1311").0, Some("Energy"));
        assert_eq!(sector_industry("7372"), (Some("Technology"), Some("Software & IT Services")));
    }

    #[test]
    fn range_boundaries_and_unknowns() {
        assert_eq!(sector_industry("6798"), (Some("Real Estate"), Some("REITs")));
        assert_eq!(sector_industry("6770"), (Some("Financials"), Some("Investment Offices")));
        assert_eq!(sector_industry("4911").0, Some("Utilities"));
        assert_eq!(sector_industry("3711").0, Some("Consumer Discretionary"));
        assert_eq!(sector_industry("3841").0, Some("Healthcare"));
        assert_eq!(sector_industry(""), (None, None));
        assert_eq!(sector_industry("9999"), (None, None));
        assert_eq!(sector_industry("garbage"), (None, None));
    }
}
