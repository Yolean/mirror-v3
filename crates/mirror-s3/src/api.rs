//! Signed S3 requests that object_store does not make: a PUT that sets a
//! retention lock, reading an object's retention back, the bucket's
//! object lock configuration and versioning, and listing and deleting
//! object versions.
//!
//! object_store cannot send `x-amz-object-lock-*` headers: its put
//! attributes are a fixed set plus user metadata, and a header added
//! after signing goes out unsigned, which S3 refuses for `x-amz-*`. The
//! requests here are signed with object_store's own SigV4
//! ([`AwsAuthorizer`]) and sent through its HTTP client, so TLS and
//! proxies behave as for its own requests.

use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use md5::{Digest, Md5};
use mirror_fs::floor::{parse_utc_rfc3339, utc_rfc3339};
use object_store::aws::{AwsAuthorizer, AwsCredential};
use object_store::client::{HttpClient, HttpConnector, HttpRequestBody, ReqwestConnector};
use object_store::ClientOptions;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde::Deserialize;

/// What stays unencoded in a URL: S3's unreserved characters.
const UNRESERVED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');
/// The same, and `/` between the parts of a key.
const KEY: &AsciiSet = &UNRESERVED.remove(b'/');

/// Attempts at a request that failed to connect or got a 5xx.
const ATTEMPTS: u32 = 3;

/// The headers a store takes a retention lock in. S3's are the only ones
/// today; Google Cloud Storage's XML API names them `x-goog-object-lock-*`
/// and is not supported yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockDialect {
    Amz,
}

impl LockDialect {
    fn mode_header(self) -> &'static str {
        match self {
            LockDialect::Amz => "x-amz-object-lock-mode",
        }
    }

    fn retain_until_header(self) -> &'static str {
        match self {
            LockDialect::Amz => "x-amz-object-lock-retain-until-date",
        }
    }
}

/// A failed request: the HTTP status and S3 error code where the store
/// answered with an error, the transport error where it did not answer,
/// and what was wrong where its answer was not what S3 sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub status: Option<u16>,
    pub code: Option<String>,
    pub message: String,
    /// The request did not reach the store, or its answer was cut off.
    pub transport: bool,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.status, &self.code) {
            (Some(status), Some(code)) => write!(f, "HTTP {status} {code}: {}", self.message),
            (Some(status), None) => write!(f, "HTTP {status}: {}", self.message),
            (None, _) => write!(f, "{}", self.message),
        }
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    /// The store did not answer, or answered with a server error: trying
    /// again later can succeed. An answer that is not what S3 sends (a
    /// store without the feature) cannot.
    pub fn is_transient(&self) -> bool {
        self.transport || self.status.is_some_and(|s| s >= 500)
    }

    fn other(message: String) -> Self {
        Self {
            status: None,
            code: None,
            message,
            transport: false,
        }
    }

    fn transport(message: String) -> Self {
        Self {
            transport: true,
            ..Self::other(message)
        }
    }
}

/// An object's retention lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retention {
    pub mode: String,
    /// Unix seconds.
    pub retain_until: u64,
}

impl std::fmt::Display for Retention {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} until {}", self.mode, utc_rfc3339(self.retain_until))
    }
}

/// A bucket's default retention: what an object written without a lock
/// of its own gets, counted from its write time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultRetention {
    pub mode: String,
    pub days: Option<u32>,
    pub years: Option<u32>,
}

impl std::fmt::Display for DefaultRetention {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.mode)?;
        if let Some(days) = self.days {
            write!(f, " {days} days")?;
        }
        if let Some(years) = self.years {
            write!(f, " {years} years")?;
        }
        Ok(())
    }
}

/// What GetObjectLockConfiguration says about a bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketLock {
    Enabled {
        default: Option<DefaultRetention>,
    },
    /// Why not: the store's error code, or what its answer lacked.
    NotEnabled {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Versioning {
    /// Never enabled: a DELETE removes the data.
    Off,
    Enabled,
    /// Was enabled: versions written then are still kept.
    Suspended,
}

/// One entry of ListObjectVersions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyVersion {
    pub key: String,
    pub version_id: String,
    pub delete_marker: bool,
}

/// One bucket, reached with one identity.
pub struct S3Api {
    /// `<endpoint>/<bucket>`: path-style, as object_store addresses it.
    bucket_url: String,
    bucket: String,
    region: String,
    credential: AwsCredential,
    client: HttpClient,
    dialect: LockDialect,
}

/// Leaves the credential out.
impl std::fmt::Debug for S3Api {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Api")
            .field("bucket_url", &self.bucket_url)
            .field("region", &self.region)
            .field("key_id", &self.credential.key_id)
            .finish_non_exhaustive()
    }
}

impl S3Api {
    /// `endpoint` as in the destination's config: `None` for AWS's
    /// regional endpoint.
    pub fn new(
        endpoint: Option<&str>,
        region: &str,
        bucket: &str,
        access_key_id: String,
        secret_access_key: String,
    ) -> Result<Self, ApiError> {
        let endpoint = match endpoint {
            Some(e) => e.trim_end_matches('/').to_string(),
            None => format!("https://s3.{region}.amazonaws.com"),
        };
        let options = ClientOptions::new().with_allow_http(endpoint.starts_with("http://"));
        let client = ReqwestConnector::default()
            .connect(&options)
            .map_err(|e| ApiError::other(format!("building the HTTP client: {e}")))?;
        Ok(Self {
            bucket_url: format!("{endpoint}/{}", utf8_percent_encode(bucket, UNRESERVED)),
            bucket: bucket.to_string(),
            region: region.to_string(),
            credential: AwsCredential {
                key_id: access_key_id,
                secret_key: secret_access_key,
                token: None,
            },
            client,
            dialect: LockDialect::Amz,
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    fn key_url(&self, key: &str, query: &str) -> String {
        let mut url = format!("{}/{}", self.bucket_url, utf8_percent_encode(key, KEY));
        if !query.is_empty() {
            url.push('?');
            url.push_str(query);
        }
        url
    }

    fn bucket_query_url(&self, query: &str) -> String {
        format!("{}?{query}", self.bucket_url)
    }

    /// Write `body` as `key`, failing if `key` exists (`If-None-Match:
    /// *`, an error with status 412), with its user metadata and a
    /// COMPLIANCE retention lock until `retain_until` (unix seconds), all
    /// in the one request that creates it.
    pub async fn put_object_locked(
        &self,
        key: &str,
        body: Bytes,
        metadata: &[(&'static str, String)],
        retain_until: u64,
    ) -> Result<(), ApiError> {
        let mut headers = HeaderMap::new();
        insert(&mut headers, "if-none-match", "*")?;
        let md5 = base64::engine::general_purpose::STANDARD.encode(Md5::digest(&body));
        insert(&mut headers, "content-md5", &md5)?;
        for (name, value) in metadata {
            insert(&mut headers, &format!("x-amz-meta-{name}"), value)?;
        }
        insert(&mut headers, self.dialect.mode_header(), "COMPLIANCE")?;
        insert(
            &mut headers,
            self.dialect.retain_until_header(),
            &utc_rfc3339(retain_until),
        )?;
        let (status, body) = self
            .send(Method::PUT, self.key_url(key, ""), headers, body)
            .await?;
        expect_success(status, &body)
    }

    /// The retention lock of `key` (GetObjectRetention): `None` when it
    /// has none.
    pub async fn object_retention(&self, key: &str) -> Result<Option<Retention>, ApiError> {
        let (status, body) = self
            .send(
                Method::GET,
                self.key_url(key, "retention"),
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        // By the code alone: VersityGW answers it with 400, not 404.
        if !status.is_success()
            && error_code(&body).as_deref() == Some("NoSuchObjectLockConfiguration")
        {
            return Ok(None);
        }
        expect_success(status, &body)?;
        // AWS answers `<Retention>`, VersityGW `<ObjectLockRetention>`;
        // the fields are the same.
        let doc: RetentionDoc = parse_xml(&body, "GetObjectRetention")?;
        let mode = doc.mode.filter(|m| !m.is_empty());
        let until = doc.retain_until_date.filter(|d| !d.is_empty());
        match (mode, until) {
            (None, None) => Ok(None),
            (Some(mode), Some(until)) => {
                let retain_until = parse_utc_rfc3339(&until).ok_or_else(|| {
                    ApiError::other(format!("GetObjectRetention: RetainUntilDate {until:?}"))
                })?;
                Ok(Some(Retention { mode, retain_until }))
            }
            (mode, until) => Err(ApiError::other(format!(
                "GetObjectRetention: Mode {mode:?} with RetainUntilDate {until:?}"
            ))),
        }
    }

    /// GetObjectLockConfiguration, read strictly: a store that ignores
    /// the `?object-lock` subresource and answers with something else (a
    /// bucket listing) is an error, not a bucket with object lock.
    pub async fn bucket_object_lock(&self) -> Result<BucketLock, ApiError> {
        let (status, body) = self
            .send(
                Method::GET,
                self.bucket_query_url("object-lock"),
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        if status == StatusCode::NOT_FOUND {
            if let Some(code) =
                error_code(&body).filter(|c| c == "ObjectLockConfigurationNotFoundError")
            {
                return Ok(BucketLock::NotEnabled { reason: code });
            }
        }
        expect_success(status, &body)?;
        match root_element(&body).as_deref() {
            Some("ObjectLockConfiguration") => {}
            other => {
                return Err(ApiError::other(format!(
                    "the store answered GetObjectLockConfiguration with {}, not an \
                     ObjectLockConfiguration: it does not support object lock",
                    other.map_or_else(|| "no XML document".to_string(), |r| format!("<{r}>"))
                )))
            }
        }
        let doc: LockConfigurationDoc = parse_xml(&body, "GetObjectLockConfiguration")?;
        if doc.object_lock_enabled.as_deref() != Some("Enabled") {
            return Ok(BucketLock::NotEnabled {
                reason: format!(
                    "ObjectLockEnabled is {:?}, not Enabled",
                    doc.object_lock_enabled.unwrap_or_default()
                ),
            });
        }
        let default = doc.rule.and_then(|r| r.default_retention).and_then(|d| {
            match d.mode.filter(|m| !m.is_empty()) {
                Some(mode) => Some(DefaultRetention {
                    mode,
                    days: d.days,
                    years: d.years,
                }),
                None => None,
            }
        });
        Ok(BucketLock::Enabled { default })
    }

    /// GetBucketVersioning.
    pub async fn bucket_versioning(&self) -> Result<Versioning, ApiError> {
        let (status, body) = self
            .send(
                Method::GET,
                self.bucket_query_url("versioning"),
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        expect_success(status, &body)?;
        match root_element(&body).as_deref() {
            Some("VersioningConfiguration") => {}
            other => {
                return Err(ApiError::other(format!(
                    "the store answered GetBucketVersioning with {}, not a \
                     VersioningConfiguration",
                    other.map_or_else(|| "no XML document".to_string(), |r| format!("<{r}>"))
                )))
            }
        }
        let doc: VersioningDoc = parse_xml(&body, "GetBucketVersioning")?;
        match doc.status.as_deref() {
            None | Some("") => Ok(Versioning::Off),
            Some("Enabled") => Ok(Versioning::Enabled),
            Some("Suspended") => Ok(Versioning::Suspended),
            Some(other) => Err(ApiError::other(format!(
                "GetBucketVersioning: Status {other:?}"
            ))),
        }
    }

    /// Every version and delete marker of every key under `prefix`
    /// (ListObjectVersions, all pages).
    pub async fn list_object_versions(&self, prefix: &str) -> Result<Vec<KeyVersion>, ApiError> {
        let mut out = Vec::new();
        let mut markers: Option<(String, String)> = None;
        loop {
            let mut query = format!(
                "versions&prefix={}",
                utf8_percent_encode(prefix, UNRESERVED)
            );
            if let Some((key, version)) = &markers {
                query.push_str(&format!(
                    "&key-marker={}&version-id-marker={}",
                    utf8_percent_encode(key, UNRESERVED),
                    utf8_percent_encode(version, UNRESERVED)
                ));
            }
            let (status, body) = self
                .send(
                    Method::GET,
                    self.bucket_query_url(&query),
                    HeaderMap::new(),
                    Bytes::new(),
                )
                .await?;
            expect_success(status, &body)?;
            let doc: ListVersionsDoc = parse_xml(&body, "ListObjectVersions")?;
            let next = next_versions_page(&doc)?;
            for (entries, delete_marker) in [(doc.versions, false), (doc.delete_markers, true)] {
                out.extend(entries.into_iter().map(|v| KeyVersion {
                    key: v.key,
                    version_id: v.version_id,
                    delete_marker,
                }));
            }
            let Some(next) = next else {
                return Ok(out);
            };
            if markers.as_ref() == Some(&next) {
                return Err(ApiError::other(format!(
                    "ListObjectVersions repeated the page after key {:?}",
                    next.0
                )));
            }
            markers = Some(next);
        }
    }

    /// DeleteObject with a version id: removes that version, or delete
    /// marker, for good. One that does not exist is not an error.
    pub async fn delete_object_version(&self, key: &str, version_id: &str) -> Result<(), ApiError> {
        let query = format!("versionId={}", utf8_percent_encode(version_id, UNRESERVED));
        let (status, body) = self
            .send(
                Method::DELETE,
                self.key_url(key, &query),
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        if status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        expect_success(status, &body)
    }

    async fn send(
        &self,
        method: Method,
        url: String,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<(StatusCode, Bytes), ApiError> {
        let mut attempt = 1;
        loop {
            let mut request = http::Request::builder()
                .method(method.clone())
                .uri(&url)
                .body(HttpRequestBody::from(body.clone()))
                .map_err(|e| ApiError::other(format!("{method} {url}: {e}")))?;
            *request.headers_mut() = headers.clone();
            AwsAuthorizer::new(&self.credential, "s3", &self.region).authorize(&mut request, None);
            let result = match self.client.execute(request).await {
                Ok(response) => {
                    let status = response.status();
                    match response.into_body().bytes().await {
                        Ok(bytes) => Ok((status, bytes)),
                        Err(e) => Err(format!("{method} {url}: reading the answer: {e}")),
                    }
                }
                Err(e) => Err(format!("{method} {url}: {e}")),
            };
            let retry = match &result {
                Ok((status, _)) => status.is_server_error(),
                Err(_) => true,
            };
            if !retry || attempt >= ATTEMPTS {
                return result.map_err(ApiError::transport);
            }
            tokio::time::sleep(Duration::from_millis(200 * 4u64.pow(attempt - 1))).await;
            attempt += 1;
        }
    }
}

fn insert(headers: &mut HeaderMap, name: &str, value: &str) -> Result<(), ApiError> {
    let name = HeaderName::try_from(name)
        .map_err(|e| ApiError::other(format!("header name {name:?}: {e}")))?;
    let value = HeaderValue::try_from(value)
        .map_err(|e| ApiError::other(format!("header {name} value {value:?}: {e}")))?;
    headers.insert(name, value);
    Ok(())
}

fn expect_success(status: StatusCode, body: &[u8]) -> Result<(), ApiError> {
    if status.is_success() {
        return Ok(());
    }
    let doc: ErrorDoc = quick_xml::de::from_reader(body).unwrap_or_default();
    Err(ApiError {
        status: Some(status.as_u16()),
        code: doc.code.filter(|c| !c.is_empty()),
        message: doc
            .message
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| status.canonical_reason().unwrap_or("").to_string()),
        transport: false,
    })
}

fn error_code(body: &[u8]) -> Option<String> {
    quick_xml::de::from_reader::<_, ErrorDoc>(body)
        .ok()
        .and_then(|d| d.code)
}

fn parse_xml<T: for<'de> Deserialize<'de>>(body: &[u8], what: &str) -> Result<T, ApiError> {
    quick_xml::de::from_reader(body).map_err(|e| ApiError::other(format!("{what}: {e}")))
}

/// The name of the document's root element.
fn root_element(body: &[u8]) -> Option<String> {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(body);
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf).ok()? {
            Event::Start(e) | Event::Empty(e) => {
                return Some(String::from_utf8_lossy(e.local_name().as_ref()).into_owned())
            }
            Event::Eof => return None,
            _ => buf.clear(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ErrorDoc {
    code: Option<String>,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RetentionDoc {
    mode: Option<String>,
    retain_until_date: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LockConfigurationDoc {
    object_lock_enabled: Option<String>,
    rule: Option<RuleDoc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RuleDoc {
    default_retention: Option<DefaultRetentionDoc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DefaultRetentionDoc {
    mode: Option<String>,
    days: Option<u32>,
    years: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct VersioningDoc {
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListVersionsDoc {
    is_truncated: Option<bool>,
    next_key_marker: Option<String>,
    next_version_id_marker: Option<String>,
    #[serde(rename = "Version", default)]
    versions: Vec<VersionDoc>,
    #[serde(rename = "DeleteMarker", default)]
    delete_markers: Vec<VersionDoc>,
}

/// The key and version id markers that ask for the page after `doc`,
/// or `None` when `doc` is the last page. A truncated page without a
/// next key marker cannot be continued, and is an error rather than
/// the end of the listing.
fn next_versions_page(doc: &ListVersionsDoc) -> Result<Option<(String, String)>, ApiError> {
    if !doc.is_truncated.unwrap_or(false) {
        return Ok(None);
    }
    match doc.next_key_marker.as_deref() {
        None | Some("") => Err(ApiError::other(
            "ListObjectVersions answered IsTruncated without NextKeyMarker".to_string(),
        )),
        Some(key) => Ok(Some((
            key.to_string(),
            doc.next_version_id_marker.clone().unwrap_or_default(),
        ))),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct VersionDoc {
    key: String,
    version_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_what_versitygw_and_aws_answer() {
        let lock = br#"<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Days>30</Days><Mode>COMPLIANCE</Mode></DefaultRetention></Rule></ObjectLockConfiguration>"#;
        assert_eq!(
            root_element(lock).as_deref(),
            Some("ObjectLockConfiguration")
        );
        let doc: LockConfigurationDoc = parse_xml(lock, "t").unwrap();
        let d = doc.rule.unwrap().default_retention.unwrap();
        assert_eq!((d.mode.as_deref(), d.days), (Some("COMPLIANCE"), Some(30)));

        let listing =
            br#"<?xml version="1.0"?><ListBucketResult><Name>b</Name></ListBucketResult>"#;
        assert_eq!(root_element(listing).as_deref(), Some("ListBucketResult"));

        let versions = br#"<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><DeleteMarker><IsLatest>true</IsLatest><Key>p/o</Key><VersionId>m1</VersionId></DeleteMarker><IsTruncated>false</IsTruncated><NextKeyMarker></NextKeyMarker><Version><IsLatest>false</IsLatest><Key>p/o</Key><Size>3</Size><VersionId>v1</VersionId></Version><Version><Key>p/q</Key><VersionId>v2</VersionId></Version></ListVersionsResult>"#;
        let doc: ListVersionsDoc = parse_xml(versions, "t").unwrap();
        assert_eq!(doc.versions.len(), 2);
        assert_eq!(doc.delete_markers[0].version_id, "m1");
        assert_eq!(doc.is_truncated, Some(false));

        let err = br#"<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>Access Denied.</Message><RequestId>X</RequestId></Error>"#;
        let e = expect_success(StatusCode::FORBIDDEN, err).unwrap_err();
        assert_eq!(e.to_string(), "HTTP 403 AccessDenied: Access Denied.");
        assert!(!e.is_transient());
    }

    #[test]
    fn a_truncated_version_listing_needs_a_next_key_marker() {
        let page = |xml: &str| -> ListVersionsDoc {
            parse_xml(
                format!("<ListVersionsResult>{xml}</ListVersionsResult>").as_bytes(),
                "t",
            )
            .unwrap()
        };
        assert_eq!(
            next_versions_page(&page("<IsTruncated>false</IsTruncated>")).unwrap(),
            None
        );
        assert_eq!(next_versions_page(&page("")).unwrap(), None);
        assert_eq!(
            next_versions_page(&page(
                "<IsTruncated>true</IsTruncated><NextKeyMarker>p/o</NextKeyMarker>\
                 <NextVersionIdMarker>v1</NextVersionIdMarker>"
            ))
            .unwrap(),
            Some(("p/o".to_string(), "v1".to_string()))
        );
        for cut_short in [
            "<IsTruncated>true</IsTruncated>",
            "<IsTruncated>true</IsTruncated><NextKeyMarker></NextKeyMarker>",
        ] {
            let e = next_versions_page(&page(cut_short)).unwrap_err();
            assert!(e.to_string().contains("without NextKeyMarker"), "{e}");
            assert!(!e.is_transient());
        }
    }

    #[test]
    fn keys_and_queries_are_encoded_once() {
        let api = S3Api::new(Some("http://gw:7070/"), "r", "b", "k".into(), "s".into()).unwrap();
        assert_eq!(
            api.key_url("sites/a b/0/00-01.parquet", "retention"),
            "http://gw:7070/b/sites/a%20b/0/00-01.parquet?retention"
        );
        assert_eq!(
            api.bucket_query_url("object-lock"),
            "http://gw:7070/b?object-lock"
        );
        let aws = S3Api::new(None, "eu-north-1", "b", "k".into(), "s".into()).unwrap();
        assert_eq!(
            aws.bucket_query_url("versioning"),
            "https://s3.eu-north-1.amazonaws.com/b?versioning"
        );
    }
}
