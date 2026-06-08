use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{Duration, sleep};

#[derive(Debug, thiserror::Error)]
pub enum RestError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("non-success status {status} from {url}: {body}")]
    Status { status: u16, url: String, body: String },
    #[error("pagination exceeded safety cap of {cap} pages")]
    PaginationOverflow { cap: usize },
}

#[derive(Debug, Deserialize, Clone)]
pub struct TickerDetails {
    pub ticker: String,
    pub name: Option<String>,
    pub market: Option<String>,
    pub locale: Option<String>,
    pub primary_exchange: Option<String>,
    #[serde(rename = "type")]
    pub ticker_type: Option<String>,
    pub active: Option<bool>,
    pub currency_name: Option<String>,
    pub cik: Option<String>,
    pub composite_figi: Option<String>,
    pub share_class_figi: Option<String>,
    pub last_updated_utc: Option<String>,
    pub delisted_utc: Option<String>,

    // ---- Classification fields (build-ticker-details) ----
    pub market_cap: Option<f64>,
    pub sic_code: Option<String>,
    pub sic_description: Option<String>,
    pub ticker_root: Option<String>,
    pub total_employees: Option<i64>,
    pub list_date: Option<String>, // yyyy-mm-dd
    pub share_class_shares_outstanding: Option<f64>,
    pub weighted_shares_outstanding: Option<f64>,
    pub round_lot: Option<i32>,
    pub description: Option<String>,
    pub address: Option<TickerAddress>,
    pub homepage_url: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TickerAddress {
    pub city: Option<String>,
    pub state: Option<String>,
    pub postal_code: Option<String>,
    pub address1: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TickerDetailsResponse {
    results: TickerDetails,
    status: String,
    request_id: String,
}

pub struct RestClient {
    base_url: String,
    api_key: String,
    http: reqwest::Client,
}

impl RestClient {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        let http = reqwest::Client::builder()
            .gzip(true)
            .build()
            .expect("reqwest client build");
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
            http,
        }
    }

    pub async fn ticker_details(&self, ticker: &str) -> Result<TickerDetails, RestError> {
        let url = format!("{}/v3/reference/tickers/{}", self.base_url, ticker);
        let resp = self
            .http
            .get(&url)
            .query(&[("apiKey", &self.api_key)])
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(RestError::Status {
                status: status.as_u16(),
                url,
                body,
            });
        }
        let parsed: TickerDetailsResponse = resp.json().await?;
        tracing::debug!(request_id = %parsed.request_id, status = %parsed.status, "ticker details");
        Ok(parsed.results)
    }

    pub async fn list_tickers(&self, active: bool) -> Result<Vec<TickerListItem>, RestError> {
        const PAGE_CAP: usize = 1024;
        let mut all: Vec<TickerListItem> = Vec::new();
        let mut next: Option<String> = None;
        for page in 0..=PAGE_CAP {
            if page == PAGE_CAP {
                return Err(RestError::PaginationOverflow { cap: PAGE_CAP });
            }
            let url = next
                .clone()
                .unwrap_or_else(|| format!("{}/v3/reference/tickers", self.base_url));
            let mut req = self.http.get(&url).query(&[("apiKey", &self.api_key)]);
            if next.is_none() {
                req = req.query(&[
                    ("type", "CS"),
                    ("market", "stocks"),
                    ("active", if active { "true" } else { "false" }),
                    ("limit", "1000"),
                ]);
            }
            let resp = req.send().await?;
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(RestError::Status {
                    status: status.as_u16(),
                    url,
                    body,
                });
            }
            let parsed: TickerListResponse = resp.json().await?;
            tracing::debug!(
                page,
                got = parsed.results.as_ref().map(|r| r.len()).unwrap_or(0),
                "ticker list page"
            );
            if let Some(results) = parsed.results {
                all.extend(results);
            }
            match parsed.next_url {
                Some(u) if !u.is_empty() => next = Some(u),
                _ => break,
            }
        }
        Ok(all)
    }
}

// =============================================================================
// Splits — `GET /stocks/v1/splits` (corporate-actions, used at engine read
// time per §5.3 amended 2026-06-06). Paginated like list_tickers.
// =============================================================================

#[derive(Debug, Deserialize, Clone)]
pub struct Split {
    pub id: String,
    pub ticker: String,
    pub execution_date: String, // yyyy-mm-dd
    pub split_from: f64,
    pub split_to: f64,
    pub adjustment_type: Option<String>,
    pub historical_adjustment_factor: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct SplitsResponse {
    results: Option<Vec<Split>>,
    next_url: Option<String>,
    #[allow(dead_code)]
    status: Option<String>,
    #[allow(dead_code)]
    request_id: Option<String>,
}

impl RestClient {
    /// Paginate `/stocks/v1/splits` returning every split row Massive
    /// publishes. No ticker filter — we pull the whole universe and join
    /// to our `tickers.parquet` at write time to attach `security_id`.
    pub async fn list_splits(&self) -> Result<Vec<Split>, RestError> {
        const PAGE_CAP: usize = 4096;
        let mut all: Vec<Split> = Vec::new();
        let mut next: Option<String> = None;
        for page in 0..=PAGE_CAP {
            if page == PAGE_CAP {
                return Err(RestError::PaginationOverflow { cap: PAGE_CAP });
            }
            let url = next
                .clone()
                .unwrap_or_else(|| format!("{}/stocks/v1/splits", self.base_url));
            let mut req = self.http.get(&url).query(&[("apiKey", &self.api_key)]);
            if next.is_none() {
                req = req.query(&[("limit", "5000")]);
            }
            let resp = req.send().await?;
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(RestError::Status {
                    status: status.as_u16(),
                    url,
                    body,
                });
            }
            let parsed: SplitsResponse = resp.json().await?;
            tracing::debug!(
                page,
                got = parsed.results.as_ref().map(|r| r.len()).unwrap_or(0),
                "splits page"
            );
            if let Some(rs) = parsed.results {
                all.extend(rs);
            }
            match parsed.next_url {
                Some(u) if !u.is_empty() => next = Some(u),
                _ => break,
            }
        }
        Ok(all)
    }
}

// =============================================================================
// Short interest — `GET /stocks/v1/short-interest`. FINRA bi-weekly
// settlement. The endpoint serves the WHOLE universe per call (no
// ticker filter required); a single global pagination pulls everything
// in ~120 pages of 10k rows each.
// =============================================================================

#[derive(Debug, Deserialize, Clone)]
pub struct ShortInterest {
    pub ticker: String,
    pub settlement_date: String, // yyyy-mm-dd
    pub short_interest: f64,
    pub avg_daily_volume: Option<f64>,
    pub days_to_cover: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct ShortInterestResponse {
    results: Option<Vec<ShortInterest>>,
    next_url: Option<String>,
    #[allow(dead_code)]
    status: Option<String>,
    #[allow(dead_code)]
    request_id: Option<String>,
}

impl RestClient {
    /// Paginate `/stocks/v1/short-interest` returning every row Massive
    /// publishes (whole universe × all bi-weekly settlement dates).
    /// `progress` is called after each page with the running total.
    pub async fn list_short_interest<F>(
        &self,
        mut progress: F,
    ) -> Result<Vec<ShortInterest>, RestError>
    where
        F: FnMut(usize, usize),
    {
        const PAGE_CAP: usize = 8192;
        let mut all: Vec<ShortInterest> = Vec::new();
        let mut next: Option<String> = None;
        for page in 0..=PAGE_CAP {
            if page == PAGE_CAP {
                return Err(RestError::PaginationOverflow { cap: PAGE_CAP });
            }
            let url = next
                .clone()
                .unwrap_or_else(|| format!("{}/stocks/v1/short-interest", self.base_url));
            let mut req = self.http.get(&url).query(&[("apiKey", &self.api_key)]);
            if next.is_none() {
                req = req.query(&[("limit", "10000")]);
            }
            let resp = req.send().await?;
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(RestError::Status {
                    status: status.as_u16(),
                    url,
                    body,
                });
            }
            let parsed: ShortInterestResponse = resp.json().await?;
            let got = parsed.results.as_ref().map(|r| r.len()).unwrap_or(0);
            if let Some(rs) = parsed.results {
                all.extend(rs);
            }
            progress(page + 1, all.len());
            match parsed.next_url {
                Some(u) if !u.is_empty() => next = Some(u),
                _ => {
                    let _ = got;
                    break;
                }
            }
        }
        Ok(all)
    }
}

// =============================================================================
// Financials — `GET /vX/reference/financials`. SEC filings (10-Q, 10-K)
// with `acceptance_datetime` as the point-in-time stamp. Max page size
// is 100 (confirmed by probe — limit=1000 returns 0 rows). Whole
// universe pulled via a single global pagination; the bin filters out
// TTM rollups (no filing_date) and joins per-ticker to the FIGI map.
// =============================================================================

/// Raw envelope shape returned by Massive — only the top-level fields
/// the writer extracts as typed columns are deserialized here; the rest
/// stays in `financials_raw` for JSON pass-through.
#[derive(Debug, Deserialize, Clone)]
pub struct Financial {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub filing_date: Option<String>,
    pub acceptance_datetime: Option<String>,
    pub timeframe: Option<String>,
    pub fiscal_period: Option<String>,
    pub fiscal_year: Option<String>,
    pub cik: Option<String>,
    pub sic: Option<String>,
    pub tickers: Option<Vec<String>>,
    pub company_name: Option<String>,
    pub source_filing_url: Option<String>,
    /// Keeps the original `financials` object as serde_json::Value so the
    /// writer can both extract a handful of native fields and persist the
    /// whole struct as a JSON string.
    #[serde(default)]
    pub financials: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct FinancialsResponse {
    results: Option<Vec<Financial>>,
    next_url: Option<String>,
    #[allow(dead_code)]
    status: Option<String>,
    #[allow(dead_code)]
    request_id: Option<String>,
}

impl RestClient {
    /// Paginate `/vX/reference/financials` (no ticker filter). Returns
    /// every filing Massive publishes. The bin should filter out TTM
    /// rollups (`filing_date is None`).
    pub async fn list_financials<F>(&self, mut progress: F) -> Result<Vec<Financial>, RestError>
    where
        F: FnMut(usize, usize),
    {
        const PAGE_CAP: usize = 16384;
        let mut all: Vec<Financial> = Vec::new();
        let mut next: Option<String> = None;
        for page in 0..=PAGE_CAP {
            if page == PAGE_CAP {
                return Err(RestError::PaginationOverflow { cap: PAGE_CAP });
            }
            let url = next
                .clone()
                .unwrap_or_else(|| format!("{}/vX/reference/financials", self.base_url));
            let mut req = self.http.get(&url).query(&[("apiKey", &self.api_key)]);
            if next.is_none() {
                req = req.query(&[("limit", "100")]);
            }
            let resp = req.send().await?;
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(RestError::Status {
                    status: status.as_u16(),
                    url,
                    body,
                });
            }
            let parsed: FinancialsResponse = resp.json().await?;
            if let Some(rs) = parsed.results {
                all.extend(rs);
            }
            progress(page + 1, all.len());
            match parsed.next_url {
                Some(u) if !u.is_empty() => next = Some(u),
                _ => break,
            }
        }
        Ok(all)
    }
}

// =============================================================================
// Dividends — `GET /stocks/v1/dividends` (corporate-actions, stored as
// events; §5.3.1). NOT folded into bars.
// =============================================================================

#[derive(Debug, Deserialize, Clone)]
pub struct Dividend {
    pub id: String,
    pub ticker: String,
    pub ex_dividend_date: String,           // yyyy-mm-dd, required
    pub pay_date: Option<String>,
    pub record_date: Option<String>,
    pub declaration_date: Option<String>,
    pub cash_amount: f64,
    pub split_adjusted_cash_amount: Option<f64>,
    pub historical_adjustment_factor: Option<f64>,
    pub currency: Option<String>,
    pub distribution_type: Option<String>,
    pub frequency: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct DividendsResponse {
    results: Option<Vec<Dividend>>,
    next_url: Option<String>,
    #[allow(dead_code)]
    status: Option<String>,
    #[allow(dead_code)]
    request_id: Option<String>,
}

impl RestClient {
    /// Paginate `/stocks/v1/dividends` returning every dividend row. No
    /// ticker filter — pull the whole universe, attach `security_id` at
    /// write time via the FIGI lookup.
    pub async fn list_dividends(&self) -> Result<Vec<Dividend>, RestError> {
        const PAGE_CAP: usize = 8192;
        let mut all: Vec<Dividend> = Vec::new();
        let mut next: Option<String> = None;
        for page in 0..=PAGE_CAP {
            if page == PAGE_CAP {
                return Err(RestError::PaginationOverflow { cap: PAGE_CAP });
            }
            let url = next
                .clone()
                .unwrap_or_else(|| format!("{}/stocks/v1/dividends", self.base_url));
            let mut req = self.http.get(&url).query(&[("apiKey", &self.api_key)]);
            if next.is_none() {
                req = req.query(&[("limit", "5000")]);
            }
            let resp = req.send().await?;
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(RestError::Status {
                    status: status.as_u16(),
                    url,
                    body,
                });
            }
            let parsed: DividendsResponse = resp.json().await?;
            tracing::debug!(
                page,
                got = parsed.results.as_ref().map(|r| r.len()).unwrap_or(0),
                "dividends page"
            );
            if let Some(rs) = parsed.results {
                all.extend(rs);
            }
            match parsed.next_url {
                Some(u) if !u.is_empty() => next = Some(u),
                _ => break,
            }
        }
        Ok(all)
    }
}

// =============================================================================
// Ticker events — `GET /vX/reference/tickers/{id}/events`. Per-ticker call
// (not paginated). Used to derive the rename chain (FB → META, etc.) so
// the engine can resolve `(display_symbol, t) → security_id`.
// =============================================================================

/// One rename event from the response. `new_ticker` is the symbol that
/// emerged on `date`. Currently the only `type` Massive returns is
/// `ticker_change`.
#[derive(Debug, Clone)]
pub struct TickerEvent {
    pub date: String, // yyyy-mm-dd
    pub event_type: String,
    pub new_ticker: String,
}

/// Aggregate response for one ticker. `events` may be empty (no
/// renames). `name` is the current company name.
#[derive(Debug, Clone)]
pub struct TickerEventsResult {
    pub queried_id: String,
    pub name: Option<String>,
    pub events: Vec<TickerEvent>,
}

#[derive(Debug, Deserialize)]
struct RawTickerEventsResponse {
    results: Option<RawTickerEventsResults>,
    #[allow(dead_code)]
    status: Option<String>,
    #[allow(dead_code)]
    request_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawTickerEventsResults {
    name: Option<String>,
    #[serde(default)]
    events: Vec<RawTickerEvent>,
}

#[derive(Debug, Deserialize)]
struct RawTickerEvent {
    date: String,
    #[serde(rename = "type")]
    event_type: String,
    ticker_change: Option<RawTickerChange>,
}

#[derive(Debug, Deserialize)]
struct RawTickerChange {
    ticker: String,
}

/// Diagnostic counts for `fetch_ticker_events_concurrent`.
///
/// `err_404` is a common — and expected — outcome here. Many composite
/// FIGIs don't have a corresponding events record on Massive's side
/// (especially for delisted historical symbols).
#[derive(Debug, Clone, Copy, Default)]
pub struct TickerEventsStats {
    pub queried: usize,
    pub ok: usize,
    pub ok_with_events: usize,
    pub err: usize,
    pub err_404: usize,
    pub err_429: usize,
    pub err_5xx: usize,
    pub err_other: usize,
}

impl RestClient {
    /// Single-call fetch for `/vX/reference/tickers/{id}/events`. `id`
    /// can be a display ticker, CUSIP, or composite FIGI; for the
    /// rename-chain use case the canonical input is composite FIGI.
    pub async fn ticker_events(&self, id: &str) -> Result<TickerEventsResult, RestError> {
        let url = format!("{}/vX/reference/tickers/{}/events", self.base_url, id);
        let resp = self
            .http
            .get(&url)
            .query(&[("apiKey", &self.api_key)])
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(RestError::Status {
                status: status.as_u16(),
                url,
                body,
            });
        }
        let parsed: RawTickerEventsResponse = resp.json().await?;
        let r = parsed.results.unwrap_or(RawTickerEventsResults {
            name: None,
            events: Vec::new(),
        });
        let events = r
            .events
            .into_iter()
            .filter_map(|e| {
                e.ticker_change.map(|tc| TickerEvent {
                    date: e.date,
                    event_type: e.event_type,
                    new_ticker: tc.ticker,
                })
            })
            .collect();
        Ok(TickerEventsResult {
            queried_id: id.to_string(),
            name: r.name,
            events,
        })
    }
}

/// Bounded-concurrency bulk fetch — same retry shape as
/// `enrich_universe_gap`. Failures are logged and the ID is skipped;
/// the caller decides if partial coverage is acceptable.
pub async fn fetch_ticker_events_concurrent(
    client: Arc<RestClient>,
    ids: Vec<String>,
    config: EnrichConfig,
) -> Result<(Vec<TickerEventsResult>, TickerEventsStats), RestError> {
    let sem = Arc::new(Semaphore::new(config.concurrency));
    let mut joinset: tokio::task::JoinSet<(String, Result<TickerEventsResult, RestError>)> =
        tokio::task::JoinSet::new();
    let queried = ids.len();
    for id in ids {
        let client = Arc::clone(&client);
        let sem = Arc::clone(&sem);
        joinset.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            let mut attempt: u32 = 0;
            loop {
                match client.ticker_events(&id).await {
                    Ok(r) => return (id, Ok(r)),
                    Err(e) => {
                        attempt += 1;
                        if attempt >= config.max_attempts || !is_retryable(&e) {
                            return (id, Err(e));
                        }
                        let exp = (config.base_backoff_ms.saturating_mul(1u64 << attempt))
                            .min(config.max_backoff_ms);
                        let jitter = fastrand::u64(0..=(exp / 2 + 1));
                        sleep(Duration::from_millis(exp + jitter)).await;
                    }
                }
            }
        });
    }
    let mut out: Vec<TickerEventsResult> = Vec::new();
    let mut stats = TickerEventsStats { queried, ..Default::default() };
    while let Some(j) = joinset.join_next().await {
        let (id, res) = j.expect("ticker_events task panicked");
        match res {
            Ok(r) => {
                stats.ok += 1;
                if !r.events.is_empty() {
                    stats.ok_with_events += 1;
                }
                out.push(r);
            }
            Err(e) => {
                tracing::warn!(%id, error = %e, "ticker_events failed");
                stats.err += 1;
                match &e {
                    RestError::Status { status: 404, .. } => stats.err_404 += 1,
                    RestError::Status { status: 429, .. } => stats.err_429 += 1,
                    RestError::Status { status, .. } if *status >= 500 => {
                        stats.err_5xx += 1
                    }
                    _ => stats.err_other += 1,
                }
            }
        }
    }
    Ok((out, stats))
}

#[derive(Debug, Deserialize, Clone)]
pub struct TickerListItem {
    pub ticker: String,
    pub name: Option<String>,
    pub market: Option<String>,
    pub locale: Option<String>,
    pub primary_exchange: Option<String>,
    #[serde(rename = "type")]
    pub ticker_type: Option<String>,
    pub active: bool,
    pub currency_name: Option<String>,
    pub cik: Option<String>,
    pub composite_figi: Option<String>,
    pub share_class_figi: Option<String>,
    pub last_updated_utc: Option<String>,
    pub delisted_utc: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TickerListResponse {
    results: Option<Vec<TickerListItem>>,
    next_url: Option<String>,
    #[allow(dead_code)]
    status: Option<String>,
    #[allow(dead_code)]
    request_id: Option<String>,
}

// =============================================================================
// Universe enrichment — fill the gap left by the list endpoint.
//
// Measured against live Massive on 2026-06-06: the list endpoint returns
// composite_figi for only ~59% of US common-stock rows and delisted_utc for
// ~96% of inactive rows. The frozen design keys engine joins on
// `security_id` (composite FIGI per decision #12), so rows without FIGI fall
// back to display_symbol and silently miss post-rename data. To fill the
// gap we hit `/v3/reference/tickers/{T}` per-ticker — but only for rows
// that actually need it (not blanket).
//
// Concurrency: bounded by a Semaphore (~10 inflight matches the frozen
// "starting point against Massive"). Retry: exponential backoff + jitter on
// 429/5xx/transport errors. Failures are logged and the row is left as-is;
// the caller decides whether partial enrichment is acceptable.
// =============================================================================

/// Knobs for the enrichment pass. Defaults match the frozen-decisions
/// concurrency starting point.
#[derive(Debug, Clone, Copy)]
pub struct EnrichConfig {
    /// Max simultaneous in-flight requests against `/v3/reference/tickers/{T}`.
    pub concurrency: usize,
    /// Per-ticker retry ceiling. After this many attempts the row is left
    /// unenriched (logged at WARN) and the pass continues.
    pub max_attempts: u32,
    /// Initial backoff for the exponential schedule. Each attempt doubles.
    /// Jitter of up to `base_backoff_ms / 2` is added per attempt.
    pub base_backoff_ms: u64,
    /// Hard ceiling on backoff so a string of 5xx doesn't wedge for hours.
    pub max_backoff_ms: u64,
}

impl Default for EnrichConfig {
    fn default() -> Self {
        Self {
            concurrency: 10,
            max_attempts: 5,
            base_backoff_ms: 200,
            max_backoff_ms: 30_000,
        }
    }
}

/// Diagnostic counts returned alongside the enriched list.
///
/// Error subcounts let the caller distinguish "Massive doesn't have this
/// ticker at all" (typically 404 — common for old delisted symbols) from
/// transient infrastructure issues (429/5xx — should retry or scale back).
#[derive(Debug, Clone, Copy, Default)]
pub struct EnrichStats {
    /// Rows the enrichment pass identified as needing a per-ticker call.
    pub gap: usize,
    /// Per-ticker calls that succeeded (the row was patched).
    pub ok: usize,
    /// Per-ticker calls that failed after max_attempts (row left as-is).
    pub err: usize,
    /// Subcount: 404s. Almost always "ticker not carried by detail endpoint."
    pub err_404: usize,
    /// Subcount: 429s. Indicates we're over our rate-limit budget — drop
    /// concurrency.
    pub err_429: usize,
    /// Subcount: 5xx. Server-side; not actionable from our end.
    pub err_5xx: usize,
    /// Subcount: everything else (transport errors, non-2xx 4xx other than
    /// 404, parse failures).
    pub err_other: usize,
}

/// Returns true if a row is missing any field the downstream pipeline needs
/// from per-ticker enrichment. Specifically:
/// - `composite_figi` is None — engine identity key, mandatory.
/// - `(!active) && delisted_utc.is_none()` — needed by the eligibility
///   filter to know when the name became ineligible.
///
/// Pure function; safe to call before fan-out so the gap can be sized
/// without spawning tasks.
pub fn needs_enrichment(item: &TickerListItem) -> bool {
    item.composite_figi.is_none() || (!item.active && item.delisted_utc.is_none())
}

fn is_retryable(e: &RestError) -> bool {
    match e {
        RestError::Status { status, .. } => *status == 429 || *status >= 500,
        RestError::Http(_) => true,
        RestError::PaginationOverflow { .. } => false,
    }
}

fn merge_into(item: &mut TickerListItem, d: TickerDetails) {
    if item.composite_figi.is_none() {
        item.composite_figi = d.composite_figi;
    }
    if item.share_class_figi.is_none() {
        item.share_class_figi = d.share_class_figi;
    }
    if item.delisted_utc.is_none() {
        item.delisted_utc = d.delisted_utc;
    }
    if item.cik.is_none() {
        item.cik = d.cik;
    }
    if item.primary_exchange.is_none() {
        item.primary_exchange = d.primary_exchange;
    }
    if item.ticker_type.is_none() {
        item.ticker_type = d.ticker_type;
    }
    if item.name.is_none() {
        item.name = d.name;
    }
    if item.locale.is_none() {
        item.locale = d.locale;
    }
    if item.market.is_none() {
        item.market = d.market;
    }
    if item.currency_name.is_none() {
        item.currency_name = d.currency_name;
    }
    if item.last_updated_utc.is_none() {
        item.last_updated_utc = d.last_updated_utc;
    }
}

/// Enrich only the rows where `needs_enrichment` returns true. The returned
/// vector preserves the input order; rows not needing enrichment are
/// untouched. Rows whose per-ticker call failed after `max_attempts` are
/// also returned untouched and counted in `stats.err`.
///
/// Concurrency is bounded by `config.concurrency`; the caller does not
/// need to wrap the function call in additional rate-limiting.
pub async fn enrich_universe_gap(
    client: Arc<RestClient>,
    mut items: Vec<TickerListItem>,
    config: EnrichConfig,
) -> Result<(Vec<TickerListItem>, EnrichStats), RestError> {
    let gap_indices: Vec<usize> = items
        .iter()
        .enumerate()
        .filter_map(|(i, it)| needs_enrichment(it).then_some(i))
        .collect();
    let gap = gap_indices.len();

    let sem = Arc::new(Semaphore::new(config.concurrency));
    let mut joinset: tokio::task::JoinSet<(usize, Result<TickerDetails, RestError>)> =
        tokio::task::JoinSet::new();

    for i in gap_indices {
        let client = Arc::clone(&client);
        let sem = Arc::clone(&sem);
        let ticker = items[i].ticker.clone();
        joinset.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            let mut attempt: u32 = 0;
            loop {
                match client.ticker_details(&ticker).await {
                    Ok(details) => return (i, Ok(details)),
                    Err(e) => {
                        attempt += 1;
                        if attempt >= config.max_attempts || !is_retryable(&e) {
                            return (i, Err(e));
                        }
                        let exp = (config.base_backoff_ms.saturating_mul(1u64 << attempt))
                            .min(config.max_backoff_ms);
                        let jitter = fastrand::u64(0..=(exp / 2 + 1));
                        sleep(Duration::from_millis(exp + jitter)).await;
                    }
                }
            }
        });
    }

    let mut stats = EnrichStats { gap, ..Default::default() };
    while let Some(join_res) = joinset.join_next().await {
        let (i, res) = join_res.expect("enrichment task panicked");
        match res {
            Ok(d) => {
                merge_into(&mut items[i], d);
                stats.ok += 1;
            }
            Err(e) => {
                tracing::warn!(ticker = %items[i].ticker, error = %e, "enrichment failed");
                stats.err += 1;
                match &e {
                    RestError::Status { status: 404, .. } => stats.err_404 += 1,
                    RestError::Status { status: 429, .. } => stats.err_429 += 1,
                    RestError::Status { status, .. } if *status >= 500 => {
                        stats.err_5xx += 1
                    }
                    _ => stats.err_other += 1,
                }
            }
        }
    }
    Ok((items, stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn item(ticker: &str, active: bool) -> serde_json::Value {
        serde_json::json!({
            "ticker": ticker,
            "name": format!("{ticker} Inc."),
            "market": "stocks",
            "locale": "us",
            "primary_exchange": "XNYS",
            "type": "CS",
            "active": active,
            "currency_name": "usd",
            "composite_figi": format!("BBG{ticker:0>9}"),
            "share_class_figi": format!("SCF{ticker:0>9}"),
            "last_updated_utc": "2025-01-02T03:04:05Z",
        })
    }

    #[tokio::test]
    async fn list_tickers_follows_next_url() {
        let server = MockServer::start().await;
        let next_url = format!("{}/v3/reference/tickers?cursor=PAGE2", server.uri());

        Mock::given(method("GET"))
            .and(path("/v3/reference/tickers"))
            .and(query_param("active", "true"))
            .and(query_param("apiKey", "k"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [item("AAPL", true), item("MSFT", true)],
                "next_url": next_url,
                "status": "OK",
                "request_id": "p1",
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/v3/reference/tickers"))
            .and(query_param("cursor", "PAGE2"))
            .and(query_param("apiKey", "k"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [item("GOOG", true)],
                "status": "OK",
                "request_id": "p2",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = RestClient::new(server.uri(), "k");
        let got = client.list_tickers(true).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].ticker, "AAPL");
        assert_eq!(got[2].ticker, "GOOG");
    }

    #[tokio::test]
    async fn list_tickers_propagates_http_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/reference/tickers"))
            .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
            .mount(&server)
            .await;
        let client = RestClient::new(server.uri(), "k");
        let err = client.list_tickers(true).await.unwrap_err();
        match err {
            RestError::Status { status, .. } => assert_eq!(status, 429),
            other => panic!("expected Status, got {other:?}"),
        }
    }

    fn raw_item(ticker: &str, active: bool, figi: Option<&str>, delisted: Option<&str>)
        -> TickerListItem
    {
        TickerListItem {
            ticker: ticker.into(),
            name: Some(format!("{ticker} Inc.")),
            market: Some("stocks".into()),
            locale: Some("us".into()),
            primary_exchange: Some("XNYS".into()),
            ticker_type: Some("CS".into()),
            active,
            currency_name: Some("usd".into()),
            cik: None,
            composite_figi: figi.map(String::from),
            share_class_figi: figi.map(String::from),
            last_updated_utc: Some("2025-01-02T03:04:05Z".into()),
            delisted_utc: delisted.map(String::from),
        }
    }

    #[test]
    fn needs_enrichment_flags_missing_figi_and_missing_delist() {
        // Active + has FIGI → already complete.
        assert!(!needs_enrichment(&raw_item("AAPL", true, Some("BBG0AAPL"), None)));
        // Active but no FIGI → needs enrichment for engine identity key.
        assert!(needs_enrichment(&raw_item("NEW", true, None, None)));
        // Inactive with delisted_utc set → already complete.
        assert!(!needs_enrichment(&raw_item(
            "DEAD", false, Some("BBG0DEAD"), Some("2022-06-15T00:00:00Z")
        )));
        // Inactive but no delisted_utc → needs enrichment for eligibility filter.
        assert!(needs_enrichment(&raw_item("DEAD", false, Some("BBG0DEAD"), None)));
        // Inactive AND missing FIGI → needs enrichment (either condition).
        assert!(needs_enrichment(&raw_item("DEAD", false, None, None)));
    }

    fn details_body(
        ticker: &str,
        figi: Option<&str>,
        delisted: Option<&str>,
    ) -> serde_json::Value {
        let mut results = serde_json::json!({
            "ticker": ticker,
            "name": format!("{ticker} Inc."),
            "market": "stocks",
            "locale": "us",
            "primary_exchange": "XNYS",
            "type": "CS",
            "active": true,
            "currency_name": "usd",
            "cik": "0001234567",
            "last_updated_utc": "2025-01-02T03:04:05Z",
        });
        if let Some(f) = figi {
            results["composite_figi"] = serde_json::Value::String(f.into());
            results["share_class_figi"] = serde_json::Value::String(f.into());
        }
        if let Some(d) = delisted {
            results["delisted_utc"] = serde_json::Value::String(d.into());
        }
        serde_json::json!({
            "results": results,
            "status": "OK",
            "request_id": format!("d-{ticker}"),
        })
    }

    #[tokio::test]
    async fn enrich_universe_gap_patches_only_gap_rows() {
        let server = MockServer::start().await;

        // Per-ticker detail mocks for the two gap rows. NOTOK (the row that
        // needs FIGI) gets BBG-filled; DEAD (inactive, no delisted_utc) gets
        // a delisted_utc filled.
        Mock::given(method("GET"))
            .and(path("/v3/reference/tickers/NOTOK"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                details_body("NOTOK", Some("BBG0NOTOK"), None),
            ))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/reference/tickers/DEAD"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                details_body("DEAD", Some("BBG0DEAD"), Some("2022-06-15T00:00:00Z")),
            ))
            .expect(1)
            .mount(&server)
            .await;
        // AAPL is already complete; the enrichment pass must NOT call its
        // detail endpoint. wiremock fails the test if an unmounted endpoint
        // is hit, so the absence of an AAPL mock is itself a guard.

        let client = Arc::new(RestClient::new(server.uri(), "k"));
        let input = vec![
            raw_item("AAPL", true, Some("BBG0AAPL"), None), // complete
            raw_item("NOTOK", true, None, None),             // needs FIGI
            raw_item("DEAD", false, Some("BBG0DEAD"), None), // needs delisted_utc
        ];

        let (out, stats) = enrich_universe_gap(
            client,
            input,
            EnrichConfig {
                concurrency: 4,
                max_attempts: 2,
                base_backoff_ms: 1,
                max_backoff_ms: 5,
            },
        )
        .await
        .unwrap();

        assert_eq!(stats.gap, 2);
        assert_eq!(stats.ok, 2);
        assert_eq!(stats.err, 0);

        assert_eq!(out[0].composite_figi.as_deref(), Some("BBG0AAPL")); // untouched
        assert_eq!(out[1].composite_figi.as_deref(), Some("BBG0NOTOK")); // filled
        assert_eq!(
            out[2].delisted_utc.as_deref(),
            Some("2022-06-15T00:00:00Z")
        ); // filled
        // cik backfill via enrichment, since list endpoint doesn't return it.
        assert_eq!(out[1].cik.as_deref(), Some("0001234567"));
    }

    fn split_json(id: &str, ticker: &str, date: &str, from: f64, to: f64, kind: &str)
        -> serde_json::Value
    {
        serde_json::json!({
            "id": id,
            "ticker": ticker,
            "execution_date": date,
            "split_from": from,
            "split_to": to,
            "adjustment_type": kind,
            "historical_adjustment_factor": from / to,
        })
    }

    #[tokio::test]
    async fn list_splits_paginates_and_decodes() {
        let server = MockServer::start().await;
        let next_url = format!("{}/stocks/v1/splits?cursor=PAGE2", server.uri());

        Mock::given(method("GET"))
            .and(path("/stocks/v1/splits"))
            .and(query_param("limit", "5000"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [
                    split_json("S1", "AAPL", "2020-08-31", 1.0, 4.0, "forward_split"),
                    split_json("S2", "NVDA", "2021-07-20", 1.0, 4.0, "forward_split"),
                ],
                "next_url": next_url,
                "status": "OK",
                "request_id": "s-p1",
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/stocks/v1/splits"))
            .and(query_param("cursor", "PAGE2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [split_json("S3", "TSLA", "2022-08-25", 1.0, 3.0, "forward_split")],
                "status": "OK",
                "request_id": "s-p2",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = RestClient::new(server.uri(), "k");
        let got = client.list_splits().await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].ticker, "AAPL");
        assert_eq!(got[0].split_to, 4.0);
        assert_eq!(got[2].ticker, "TSLA");
        assert_eq!(got[2].adjustment_type.as_deref(), Some("forward_split"));
    }

    fn dividend_json(id: &str, ticker: &str, ex: &str, cash: f64) -> serde_json::Value {
        serde_json::json!({
            "id": id, "ticker": ticker, "ex_dividend_date": ex, "pay_date": ex,
            "record_date": ex, "declaration_date": ex, "cash_amount": cash,
            "split_adjusted_cash_amount": cash, "currency": "USD",
            "distribution_type": "recurring", "frequency": 4,
            "historical_adjustment_factor": 1.0,
        })
    }

    #[tokio::test]
    async fn list_dividends_paginates_and_decodes() {
        let server = MockServer::start().await;
        let next_url = format!("{}/stocks/v1/dividends?cursor=PAGE2", server.uri());
        Mock::given(method("GET"))
            .and(path("/stocks/v1/dividends"))
            .and(query_param("limit", "5000"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [
                    dividend_json("D1", "AAPL", "2025-08-11", 0.26),
                    dividend_json("D2", "MSFT", "2025-08-21", 0.83),
                ],
                "next_url": next_url, "status": "OK", "request_id": "d-p1",
            })))
            .expect(1).mount(&server).await;
        Mock::given(method("GET"))
            .and(path("/stocks/v1/dividends"))
            .and(query_param("cursor", "PAGE2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [dividend_json("D3", "AAPL", "2025-11-10", 0.27)],
                "status": "OK", "request_id": "d-p2",
            })))
            .expect(1).mount(&server).await;

        let client = RestClient::new(server.uri(), "k");
        let got = client.list_dividends().await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].ticker, "AAPL");
        assert_eq!(got[0].cash_amount, 0.26);
        assert_eq!(got[2].cash_amount, 0.27);
        assert_eq!(got[1].currency.as_deref(), Some("USD"));
    }

    #[tokio::test]
    async fn ticker_events_decodes_and_skips_non_ticker_change() {
        let server = MockServer::start().await;
        // FB → META rename, mirrored from the docs example response.
        Mock::given(method("GET"))
            .and(path("/vX/reference/tickers/BBG000MM2P62/events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "request_id": "abc",
                "results": {
                    "events": [
                        {"date": "2022-06-09", "type": "ticker_change",
                         "ticker_change": {"ticker": "META"}},
                        {"date": "2012-05-18", "type": "ticker_change",
                         "ticker_change": {"ticker": "FB"}},
                        {"date": "2099-01-01", "type": "future_type"},  // no ticker_change → skipped
                    ],
                    "name": "Meta Platforms, Inc. Class A Common Stock"
                },
                "status": "OK"
            })))
            .mount(&server).await;

        let client = RestClient::new(server.uri(), "k");
        let r = client.ticker_events("BBG000MM2P62").await.unwrap();
        assert_eq!(r.queried_id, "BBG000MM2P62");
        assert_eq!(r.name.as_deref(), Some("Meta Platforms, Inc. Class A Common Stock"));
        assert_eq!(r.events.len(), 2);
        assert_eq!(r.events[0].new_ticker, "META");
        assert_eq!(r.events[0].date, "2022-06-09");
        assert_eq!(r.events[1].new_ticker, "FB");
    }

    #[tokio::test]
    async fn fetch_ticker_events_concurrent_counts_404_separately() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/vX/reference/tickers/BBG000HAS/events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "request_id": "ok", "status": "OK",
                "results": {"events": [], "name": "Has Co."}
            })))
            .expect(1).mount(&server).await;
        Mock::given(method("GET"))
            .and(path("/vX/reference/tickers/BBG000NOT/events"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .expect(1).mount(&server).await;

        let client = Arc::new(RestClient::new(server.uri(), "k"));
        let ids = vec!["BBG000HAS".to_string(), "BBG000NOT".to_string()];
        let (results, stats) = fetch_ticker_events_concurrent(
            client,
            ids,
            EnrichConfig {
                concurrency: 2,
                max_attempts: 1,
                base_backoff_ms: 1,
                max_backoff_ms: 5,
            },
        )
        .await
        .unwrap();

        assert_eq!(stats.queried, 2);
        assert_eq!(stats.ok, 1);
        assert_eq!(stats.ok_with_events, 0);
        assert_eq!(stats.err, 1);
        assert_eq!(stats.err_404, 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].queried_id, "BBG000HAS");
    }

    #[tokio::test]
    async fn enrich_universe_gap_logs_and_continues_on_persistent_4xx() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/reference/tickers/UNKNOWN"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .mount(&server)
            .await;

        let client = Arc::new(RestClient::new(server.uri(), "k"));
        let input = vec![raw_item("UNKNOWN", true, None, None)];
        let (out, stats) = enrich_universe_gap(
            client,
            input,
            EnrichConfig {
                concurrency: 1,
                max_attempts: 2,
                base_backoff_ms: 1,
                max_backoff_ms: 5,
            },
        )
        .await
        .unwrap();

        // 404 is not retryable → single attempt, single err, row left as-is.
        assert_eq!(stats.gap, 1);
        assert_eq!(stats.ok, 0);
        assert_eq!(stats.err, 1);
        assert!(out[0].composite_figi.is_none());
    }
}
