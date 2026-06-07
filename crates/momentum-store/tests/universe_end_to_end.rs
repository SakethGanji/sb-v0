use arrow::array::{Array, AsArray, RecordBatch};
use chrono::{DateTime, NaiveDate, Utc};
use momentum_api::rest::{RestClient, TickerListItem};
use momentum_core::schema::tickers_schema;
use momentum_store::tickers::{TickerRow, write_tickers};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use tempfile::tempdir;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn list_item_to_row(it: &TickerListItem) -> TickerRow {
    let parse_ts = |s: &Option<String>| -> Option<DateTime<Utc>> {
        s.as_ref()
            .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
            .map(|d| d.with_timezone(&Utc))
    };
    TickerRow {
        display_symbol: it.ticker.clone(),
        name: it.name.clone(),
        market: it.market.clone(),
        locale: it.locale.clone(),
        primary_exchange: it.primary_exchange.clone(),
        ticker_type: it.ticker_type.clone(),
        active: it.active,
        currency_name: it.currency_name.clone(),
        cik: it.cik.clone(),
        composite_figi: it.composite_figi.clone(),
        share_class_figi: it.share_class_figi.clone(),
        last_updated_utc: parse_ts(&it.last_updated_utc),
        delisted_utc: parse_ts(&it.delisted_utc),
    }
}

fn item(ticker: &str, active: bool, delisted: Option<&str>) -> serde_json::Value {
    let mut v = serde_json::json!({
        "ticker": ticker,
        "name": format!("{ticker} Inc."),
        "market": "stocks",
        "locale": "us",
        "primary_exchange": "XNYS",
        "type": "CS",
        "active": active,
        "currency_name": "usd",
        "composite_figi": format!("BBG{ticker}"),
        "share_class_figi": format!("SCF{ticker}"),
        "last_updated_utc": "2025-01-02T03:04:05Z",
    });
    if let Some(d) = delisted {
        v["delisted_utc"] = serde_json::Value::String(d.into());
    }
    v
}

#[tokio::test]
async fn snapshot_active_and_delisted_round_trip_to_parquet() {
    let server = MockServer::start().await;

    // active page
    Mock::given(method("GET"))
        .and(path("/v3/reference/tickers"))
        .and(query_param("active", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "results": [item("AAPL", true, None), item("MSFT", true, None)],
            "status": "OK",
            "request_id": "a1",
        })))
        .mount(&server)
        .await;

    // delisted page
    Mock::given(method("GET"))
        .and(path("/v3/reference/tickers"))
        .and(query_param("active", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "results": [item("DEAD", false, Some("2022-06-15T00:00:00Z"))],
            "status": "OK",
            "request_id": "d1",
        })))
        .mount(&server)
        .await;

    let client = RestClient::new(server.uri(), "k");
    let active = client.list_tickers(true).await.unwrap();
    let delisted = client.list_tickers(false).await.unwrap();
    assert_eq!(active.len(), 2);
    assert_eq!(delisted.len(), 1);

    let rows: Vec<TickerRow> = active
        .iter()
        .chain(delisted.iter())
        .map(list_item_to_row)
        .collect();

    let dir = tempdir().unwrap();
    let out = dir.path().join("tickers.parquet");
    let as_of = NaiveDate::from_ymd_opt(2026, 6, 6).unwrap();
    write_tickers(&out, &rows, as_of).unwrap();

    let file = File::open(&out).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
    let b = &batches[0];
    assert_eq!(b.num_rows(), 3);
    assert_eq!(b.schema(), tickers_schema());

    let active_col = b
        .column_by_name("active")
        .unwrap()
        .as_boolean();
    assert!(active_col.value(0));
    assert!(active_col.value(1));
    assert!(!active_col.value(2));

    let delisted_col = b
        .column_by_name("delisted_utc")
        .unwrap()
        .as_primitive::<arrow::datatypes::TimestampNanosecondType>();
    assert!(delisted_col.is_null(0));
    assert!(delisted_col.is_null(1));
    assert!(!delisted_col.is_null(2));
}
