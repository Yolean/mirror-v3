//! The retention lock against a fake S3 endpoint: the locked PUT and its
//! read-back, the start check, a store that drops the lock, and the
//! versions trim deletes on a versioned bucket. The fake keeps object
//! content in an `InMemory` store that the sink lists and reads through
//! object_store, as it would the bucket.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use mirror_core::{Record, Sink, SinkError, TimestampType};
use mirror_envelope::{Format, ParquetCompression};
use mirror_fs::blob::{BlobError, BlobStore, NewObject, ObjectVersion};
use mirror_s3::{FlushTriggers, S3Api, S3Sink, S3SinkConfig, S3Store, SinkLock};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{Attribute, Attributes, ObjectStore, PutOptions};

const NOW: u64 = 1_800_000_000;
const DAY: u64 = 86_400;

#[derive(Clone, Copy, PartialEq, Eq)]
enum LockAnswer {
    Enabled,
    EnabledWithDefault,
    NotFound,
    Listing,
    Denied,
}

struct Fake {
    objects: Arc<InMemory>,
    lock_answer: Mutex<LockAnswer>,
    /// A store or proxy that takes the lock headers and drops them.
    drop_lock: Mutex<bool>,
    versioning: Mutex<&'static str>,
    versions: Mutex<Vec<(String, String, bool)>>,
    retention: Mutex<HashMap<String, (String, String)>>,
    /// `<METHOD> <key>?<query>`, in order.
    requests: Mutex<Vec<String>>,
}

impl Fake {
    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

fn xml(status: StatusCode, body: String) -> Response {
    (status, body).into_response()
}

fn error(status: StatusCode, code: &str) -> Response {
    xml(
        status,
        format!(
            "<?xml version=\"1.0\"?><Error><Code>{code}</Code><Message>{code}</Message></Error>"
        ),
    )
}

async fn handle(
    State(fake): State<Arc<Fake>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().trim_start_matches('/');
    let key = path.split_once('/').map_or("", |(_, k)| k).to_string();
    let query = uri.query().unwrap_or("").to_string();
    fake.requests
        .lock()
        .unwrap()
        .push(format!("{method} {key}?{query}"));
    if !headers.contains_key("authorization") {
        return error(StatusCode::FORBIDDEN, "MissingSecurityHeader");
    }
    match (method.as_str(), key.is_empty(), query.as_str()) {
        ("GET", true, "object-lock") => match *fake.lock_answer.lock().unwrap() {
            LockAnswer::Enabled => xml(
                StatusCode::OK,
                "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule></Rule></ObjectLockConfiguration>".into(),
            ),
            LockAnswer::EnabledWithDefault => xml(
                StatusCode::OK,
                "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>".into(),
            ),
            LockAnswer::NotFound => error(StatusCode::NOT_FOUND, "ObjectLockConfigurationNotFoundError"),
            LockAnswer::Listing => xml(
                StatusCode::OK,
                "<ListBucketResult><Name>b</Name></ListBucketResult>".into(),
            ),
            LockAnswer::Denied => error(StatusCode::FORBIDDEN, "AccessDenied"),
        },
        ("GET", true, "versioning") => xml(
            StatusCode::OK,
            match *fake.versioning.lock().unwrap() {
                "" => "<VersioningConfiguration></VersioningConfiguration>".into(),
                status => format!("<VersioningConfiguration><Status>{status}</Status></VersioningConfiguration>"),
            },
        ),
        ("GET", true, q) if q.starts_with("versions") => {
            let entries: String = fake
                .versions
                .lock()
                .unwrap()
                .iter()
                .map(|(key, id, marker)| {
                    let tag = if *marker { "DeleteMarker" } else { "Version" };
                    format!("<{tag}><Key>{key}</Key><VersionId>{id}</VersionId></{tag}>")
                })
                .collect();
            xml(
                StatusCode::OK,
                format!("<ListVersionsResult><IsTruncated>false</IsTruncated>{entries}</ListVersionsResult>"),
            )
        }
        ("PUT", false, "") => {
            let md5 = base64::engine::general_purpose::STANDARD
                .encode(<md5::Md5 as md5::Digest>::digest(&body));
            if headers.get("content-md5").and_then(|v| v.to_str().ok()) != Some(md5.as_str()) {
                return error(StatusCode::BAD_REQUEST, "BadDigest");
            }
            let path = Path::from(key.as_str());
            if headers.get("if-none-match").is_some() && fake.objects.head(&path).await.is_ok() {
                return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
            }
            let mode = headers.get("x-amz-object-lock-mode").map(|v| v.to_str().unwrap().to_string());
            let until = headers
                .get("x-amz-object-lock-retain-until-date")
                .map(|v| v.to_str().unwrap().to_string());
            if mode.is_some()
                && !matches!(
                    *fake.lock_answer.lock().unwrap(),
                    LockAnswer::Enabled | LockAnswer::EnabledWithDefault
                )
            {
                return error(StatusCode::BAD_REQUEST, "InvalidRequest");
            }
            let mut attributes = Attributes::new();
            for (name, value) in &headers {
                if let Some(meta) = name.as_str().strip_prefix("x-amz-meta-") {
                    attributes.insert(
                        Attribute::Metadata(meta.to_string().into()),
                        value.to_str().unwrap().to_string().into(),
                    );
                }
            }
            fake.objects
                .put_opts(
                    &path,
                    body.to_vec().into(),
                    PutOptions {
                        attributes,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            if let (Some(mode), Some(until), false) = (mode, until, *fake.drop_lock.lock().unwrap()) {
                fake.retention.lock().unwrap().insert(key, (mode, until));
            }
            StatusCode::OK.into_response()
        }
        ("GET", false, "retention") => match fake.retention.lock().unwrap().get(&key) {
            Some((mode, until)) => xml(
                StatusCode::OK,
                format!("<Retention><Mode>{mode}</Mode><RetainUntilDate>{until}</RetainUntilDate></Retention>"),
            ),
            None => error(StatusCode::NOT_FOUND, "NoSuchObjectLockConfiguration"),
        },
        ("DELETE", false, q) if q.starts_with("versionId=") => StatusCode::NO_CONTENT.into_response(),
        _ => error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
    }
}

async fn fake() -> (Arc<Fake>, Arc<S3Api>) {
    let fake = Arc::new(Fake {
        objects: Arc::new(InMemory::new()),
        lock_answer: Mutex::new(LockAnswer::Enabled),
        drop_lock: Mutex::new(false),
        versioning: Mutex::new(""),
        versions: Mutex::new(Vec::new()),
        retention: Mutex::new(HashMap::new()),
        requests: Mutex::new(Vec::new()),
    });
    let app = axum::Router::new()
        .fallback(handle)
        .with_state(Arc::clone(&fake));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let api = S3Api::new(
        Some(&format!("http://{addr}")),
        "example-region",
        "b",
        "AKID".into(),
        "secret".into(),
    )
    .unwrap();
    (fake, Arc::new(api))
}

fn cfg(fake: &Fake, api: &Arc<S3Api>, lock_days: u32) -> S3SinkConfig {
    let store: Arc<dyn ObjectStore> = fake.objects.clone();
    S3SinkConfig {
        read_store: Arc::clone(&store),
        write_store: store,
        prefix: Some(Path::from("archive")),
        destination_name: "ops".into(),
        partition: 0,
        format: Format::Parquet,
        compression: ParquetCompression::Zstd1,
        keys: mirror_envelope::ColumnType::Utf8,
        values: mirror_envelope::ColumnType::Utf8,
        compaction: None,
        encryption: None,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: 2,
            daily_at_utc_seconds: None,
        },
        lock: Some(SinkLock {
            days: lock_days,
            api: Arc::clone(api),
        }),
    }
}

fn rec(offset: u64, timestamp_ms: Option<i64>) -> Record {
    Record {
        topic: "t".into(),
        partition: 0,
        source_offset: offset,
        timestamp_ms,
        timestamp_type: TimestampType::CreateTime,
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(b"v".to_vec()),
        headers: vec![],
    }
}

async fn open(cfg: S3SinkConfig) -> Result<S3Sink, BlobError> {
    S3Sink::open_with_clock(cfg, Arc::new(|| NOW)).await
}

fn rfc(secs: u64) -> String {
    mirror_fs::floor::utc_rfc3339(secs)
}

#[tokio::test]
async fn each_new_object_is_locked_until_its_newest_record_plus_lock_days() {
    let (fake, api) = fake().await;
    let mut sink = open(cfg(&fake, &api, 396)).await.unwrap();
    let newest_ms = (NOW - 10 * DAY) as i64 * 1000;
    sink.write(rec(0, Some(newest_ms - 5000))).await.unwrap();
    sink.write(rec(1, Some(newest_ms))).await.unwrap();

    let key = "archive/ops/0/00000000000000000000-00000000000000000001.parquet";
    assert_eq!(
        fake.retention.lock().unwrap().get(key),
        Some(&("COMPLIANCE".to_string(), rfc(NOW - 10 * DAY + 396 * DAY)))
    );
    // Open asks about the bucket; the flush puts the new object and reads
    // its lock back. Nothing else is asked of any object.
    assert_eq!(
        fake.requests(),
        [
            "GET ?object-lock".to_string(),
            format!("PUT {key}?"),
            format!("GET {key}?retention"),
        ]
    );
    // The record times are in its metadata, as without a lock.
    let store = S3Store::read_only(fake.objects.clone(), Path::from("archive/ops/0"));
    let times = store
        .record_times("00000000000000000000-00000000000000000001.parquet")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(times.max_ms, Some(newest_ms as u64));
    assert_eq!(sink.next_expected_offset().await.unwrap(), 2);
}

#[tokio::test]
async fn a_backfill_older_than_lock_days_is_written_without_a_lock() {
    let (fake, api) = fake().await;
    let mut sink = open(cfg(&fake, &api, 396)).await.unwrap();
    let old_ms = (NOW - 397 * DAY) as i64 * 1000;
    sink.write(rec(0, Some(old_ms))).await.unwrap();
    sink.write(rec(1, Some(old_ms + 1))).await.unwrap();
    assert!(fake.retention.lock().unwrap().is_empty());
    assert_eq!(
        fake.requests(),
        ["GET ?object-lock"],
        "through object_store"
    );
    assert_eq!(fake.objects.list(None).collect::<Vec<_>>().await.len(), 1);
}

#[tokio::test]
async fn untrusted_record_times_lock_from_the_write() {
    let (fake, api) = fake().await;
    let mut sink = open(cfg(&fake, &api, 396)).await.unwrap();
    sink.write(rec(0, None)).await.unwrap();
    sink.write(rec(1, Some((NOW - DAY) as i64 * 1000)))
        .await
        .unwrap();
    let future_ms = (NOW + 2 * 3600) as i64 * 1000;
    sink.write(rec(2, Some(future_ms))).await.unwrap();
    sink.write(rec(3, Some(future_ms))).await.unwrap();
    let retention = fake.retention.lock().unwrap();
    let write_lock = ("COMPLIANCE".to_string(), rfc(NOW + 396 * DAY));
    for name in [
        "00000000000000000000-00000000000000000001",
        "00000000000000000002-00000000000000000003",
    ] {
        assert_eq!(
            retention.get(&format!("archive/ops/0/{name}.parquet")),
            Some(&write_lock),
            "{name}"
        );
    }
}

#[tokio::test]
async fn open_refuses_a_bucket_that_cannot_hold_the_lock_and_says_why() {
    for (answer, want) in [
        (
            LockAnswer::NotFound,
            "has no object lock (ObjectLockConfigurationNotFoundError)",
        ),
        (LockAnswer::Listing, "does not support object lock"),
        (LockAnswer::Denied, "s3:GetBucketObjectLockConfiguration"),
    ] {
        let (fake, api) = fake().await;
        *fake.lock_answer.lock().unwrap() = answer;
        let err = open(cfg(&fake, &api, 396)).await.err().expect("refused");
        assert!(matches!(err, BlobError::Lock(_)), "{err:?}");
        assert!(err.to_string().contains(want), "{err}");
    }
    let (fake, api) = fake().await;
    *fake.lock_answer.lock().unwrap() = LockAnswer::EnabledWithDefault;
    open(cfg(&fake, &api, 396))
        .await
        .expect("a default retention is a warning");
}

#[tokio::test]
async fn a_store_that_drops_the_lock_stops_the_mirror() {
    let (fake, api) = fake().await;
    *fake.drop_lock.lock().unwrap() = true;
    let mut sink = open(cfg(&fake, &api, 396)).await.unwrap();
    sink.write(rec(0, Some((NOW - DAY) as i64 * 1000)))
        .await
        .unwrap();
    let err = sink
        .write(rec(1, Some((NOW - DAY) as i64 * 1000)))
        .await
        .unwrap_err();
    assert!(matches!(err, SinkError::Inconsistent(_)), "{err:?}");
    assert!(
        err.to_string().contains("the store did not apply the lock"),
        "{err}"
    );
    assert!(!err.is_transient());
}

#[tokio::test]
async fn a_lock_put_over_an_existing_name_is_a_second_writer() {
    let (fake, api) = fake().await;
    let store = S3Store::new(
        fake.objects.clone(),
        fake.objects.clone(),
        Path::from("archive/ops/0"),
    )
    .with_api(Arc::clone(&api))
    .with_lock(396);
    let marker = NewObject::Marker { written_at: NOW };
    assert_eq!(
        store
            .put_new("floor-x.json", b"{}".to_vec(), &marker)
            .await
            .unwrap(),
        Some(NOW + 396 * DAY)
    );
    let err = store
        .put_new("floor-x.json", b"{}".to_vec(), &marker)
        .await
        .unwrap_err();
    assert!(matches!(err, BlobError::AlreadyExists(_)), "{err:?}");
}

#[tokio::test]
async fn versions_are_listed_and_deleted_by_id_on_a_versioned_bucket() {
    let (fake, api) = fake().await;
    let store = S3Store::new(
        fake.objects.clone(),
        fake.objects.clone(),
        Path::from("archive/ops/0"),
    )
    .with_api(api);
    assert_eq!(
        store.versions().await.unwrap(),
        None,
        "versioning never enabled"
    );
    *fake.versioning.lock().unwrap() = "Enabled";
    *fake.versions.lock().unwrap() = vec![
        ("archive/ops/0/a.parquet".into(), "v1".into(), false),
        ("archive/ops/0/a.parquet".into(), "m1".into(), true),
    ];
    assert_eq!(
        store.versions().await.unwrap(),
        Some(vec![
            ObjectVersion {
                name: "a.parquet".into(),
                version_id: "v1".into(),
                delete_marker: false
            },
            ObjectVersion {
                name: "a.parquet".into(),
                version_id: "m1".into(),
                delete_marker: true
            },
        ])
    );
    store.delete_version("a.parquet", "v1").await.unwrap();
    assert!(fake
        .requests()
        .contains(&"DELETE archive/ops/0/a.parquet?versionId=v1".to_string()));
}

use futures::StreamExt;
