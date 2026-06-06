use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use chrono::{Datelike, NaiveDate};
use std::io::Read;

#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    #[error("s3 get_object failed: {0}")]
    Get(#[from] aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>),
    #[error("s3 list_objects_v2 failed: {0}")]
    List(#[from] aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error>),
    #[error("byte stream collect failed: {0}")]
    Stream(#[from] aws_sdk_s3::primitives::ByteStreamError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub struct FlatFileClient {
    bucket: String,
    s3: Client,
}

impl FlatFileClient {
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        key_id: impl Into<String>,
        secret: impl Into<String>,
    ) -> Self {
        let creds = Credentials::new(key_id, secret, None, None, "massive-static");
        let conf = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .region(Region::new("us-east-1"))
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .force_path_style(true)
            .build();
        Self {
            bucket: bucket.into(),
            s3: Client::from_conf(conf),
        }
    }

    pub fn minute_aggs_key(date: NaiveDate) -> String {
        format!(
            "us_stocks_sip/minute_aggs_v1/{:04}/{:02}/{}.csv.gz",
            date.year(),
            date.month(),
            date.format("%Y-%m-%d")
        )
    }

    pub async fn list_prefix(&self, prefix: &str, max_keys: i32) -> Result<Vec<String>, S3Error> {
        let resp = self
            .s3
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .max_keys(max_keys)
            .send()
            .await?;
        Ok(resp
            .contents()
            .iter()
            .filter_map(|o| o.key().map(str::to_owned))
            .collect())
    }

    pub async fn get_object_bytes(&self, key: &str) -> Result<Vec<u8>, S3Error> {
        let resp = self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        let bytes = resp.body.collect().await?;
        Ok(bytes.to_vec())
    }

    pub fn gunzip_first_lines(gz: &[u8], n: usize) -> Result<Vec<String>, S3Error> {
        let mut decoder = flate2::read::GzDecoder::new(gz);
        let mut buf = String::new();
        let _ = decoder.read_to_string(&mut buf)?;
        Ok(buf.lines().take(n).map(str::to_owned).collect())
    }
}

