use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum RestError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("non-success status {status} from {url}: {body}")]
    Status { status: u16, url: String, body: String },
}

#[derive(Debug, Deserialize)]
pub struct TickerDetails {
    pub ticker: String,
    pub name: Option<String>,
    pub market: Option<String>,
    pub locale: Option<String>,
    pub primary_exchange: Option<String>,
    #[serde(rename = "type")]
    pub ticker_type: Option<String>,
    pub active: Option<bool>,
    pub composite_figi: Option<String>,
    pub share_class_figi: Option<String>,
    pub delisted_utc: Option<String>,
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
}
