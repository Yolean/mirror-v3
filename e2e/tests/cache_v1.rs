//! E2e for the KKV drop-in surface (`/cache/v1`).
//!
//! Topology:
//!   produce → kafka-native broker → mirror-v3 (filesystem sink, append
//!   mode, `http-access: { api: cache-v1 }`) → axum HTTP server on a
//!   random port → reqwest client.
//!
//! Covers:
//!   - 503 before the mirror has caught up to its startup high-watermark
//!   - 200 + value bytes after catch-up
//!   - latest value wins for repeated keys
//!   - tombstone (null value) removes a key (404)
//!   - `/keys` and `/values` return the merged view
//!   - `/offset/{topic}/{partition}` decimal text
//!   - `x-kkv-last-seen-offsets` header on read responses
//!   - `/openapi.json` is served and round-trips JSON

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mirror_cache::KKV_OFFSETS_HEADER;
use mirror_core::CacheState;
use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{
    create_topic, produce_records, produce_records_with_nullable_values,
};
use mirror_e2e::mirror_runner::{spawn_kafka_to_filesystem, FsMirrorSpec};
use mirror_e2e::{ProvisionedStack, Provisioner};
use mirror_fs::FlushTriggers;

const TOPIC: &str = "mirror-e2e-cache-v1";

fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

async fn poll_until<F, Fut>(deadline: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let stop = Instant::now() + deadline;
    while Instant::now() < stop {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cache_v1_serves_latest_per_key_and_honours_tombstones() {
    install_tracing();

    let stack = DockerProvisioner.provision().await.expect("provision");
    let source = stack.source_bootstrap();
    let root = tempfile::tempdir().expect("tempdir");
    create_topic(&source, TOPIC, 1).await.expect("topic");

    // Seed three keys; later, overwrite k1 and tombstone k2. The
    // cache should reflect: k1 → v1-updated, k3 → v3, k2 absent.
    produce_records(
        &source,
        TOPIC,
        0,
        &[
            ("k1".into(), "v1".into()),
            ("k2".into(), "v2".into()),
            ("k3".into(), "v3".into()),
            ("k1".into(), "v1-updated".into()),
        ],
    )
    .await
    .expect("seed");
    produce_records_with_nullable_values(&source, TOPIC, 0, &[("k2".into(), None)])
        .await
        .expect("tombstone");

    // Capture the bootstrap high-watermark *before* starting the mirror.
    // The cache shouldn't be ready until we've consumed at least this
    // many records.
    let bootstrap_hwm = {
        let bootstrap = source.clone();
        tokio::task::spawn_blocking(move || {
            mirror_kafka::fetch_high_watermark(&bootstrap, TOPIC, 0, Duration::from_secs(5))
        })
        .await
        .unwrap()
        .expect("hwm") as u64
    };
    assert!(
        bootstrap_hwm >= 5,
        "expected >=5 records, got {bootstrap_hwm}"
    );

    // Build CacheState and register the mirror against the captured
    // watermark.
    let cache_state = Arc::new(CacheState::new());
    cache_state.register_mirror("cache-mirror", bootstrap_hwm);
    let binding = mirror_fs::CacheBinding {
        state: Arc::clone(&cache_state),
        mirror_name: "cache-mirror".into(),
    };

    // BEFORE we start the mirror, the cache is not ready. Sanity-
    // check that by spinning up the HTTP server now and asking.
    let port = portpicker::pick_unused_port().expect("port");
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_state = Arc::clone(&cache_state);
    let server_handle = tokio::spawn(async move {
        let signal = async move {
            let _ = server_shutdown_rx.await;
        };
        mirror_cache::serve(addr, server_state, signal)
            .await
            .expect("serve");
    });
    // Give the server a beat to bind.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let client = reqwest::Client::new();
    let early = client
        .get(format!("http://{addr}/cache/v1/raw/k1"))
        .send()
        .await
        .expect("early GET");
    assert_eq!(
        early.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "must be 503 before catch-up"
    );

    // Start the mirror — append mode with cache-v1 enabled.
    let flush = FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: u64::MAX,
        daily_at_utc_seconds: None,
    };
    let mirror = spawn_kafka_to_filesystem(FsMirrorSpec {
        source_bootstrap: source.clone(),
        source_topic: TOPIC.into(),
        partition: 0,
        group_id: "mirror-e2e-cache-v1".into(),
        root: root.path().to_path_buf(),
        destination_name: "cache-mirror".into(),
        format: mirror_envelope::Format::Parquet,
        compression: mirror_envelope::ParquetCompression::Zstd1,
        keys: mirror_envelope::ColumnType::Utf8,
        values: mirror_envelope::ColumnType::Utf8,
        compaction: None, // append mode + cache-v1: the in-memory view is the cache
        cache: Some(binding),
        flush,
    })
    .expect("spawn mirror");

    // Wait until the cache reports caught up.
    let ready = poll_until(Duration::from_secs(30), || {
        let url = format!("http://{addr}/cache/v1/raw/k1");
        let c = client.clone();
        async move {
            let r = c.get(&url).send().await.ok();
            r.map(|r| r.status() == reqwest::StatusCode::OK)
                .unwrap_or(false)
        }
    })
    .await;
    assert!(ready, "cache never reached ready state");

    // Latest value wins for k1.
    let resp = client
        .get(format!("http://{addr}/cache/v1/raw/k1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let headers = resp.headers().clone();
    assert!(
        headers.contains_key(KKV_OFFSETS_HEADER),
        "every cache read must carry the offsets header"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), b"v1-updated");

    // k3 still there.
    let resp = client
        .get(format!("http://{addr}/cache/v1/raw/k3"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"v3");

    // k2 tombstoned → 404.
    let resp = client
        .get(format!("http://{addr}/cache/v1/raw/k2"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // Unknown key → 404.
    let resp = client
        .get(format!("http://{addr}/cache/v1/raw/never-existed"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // /keys lists what we expect (k1, k3) in lex order.
    let resp = client
        .get(format!("http://{addr}/cache/v1/keys"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await.unwrap();
    // Insertion order: k1 (offset 0) first, k3 (offset 2) second.
    // Trailing newline per the /cache/v1/keys contract.
    assert_eq!(body, "k1\nk3\n", "got: {body:?}");

    // /values mirrors the order.
    let resp = client
        .get(format!("http://{addr}/cache/v1/values"))
        .send()
        .await
        .unwrap();
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), b"v1-updated\nv3\n");

    // /offset returns a decimal string ≥ bootstrap_hwm - 1.
    let resp = client
        .get(format!("http://{addr}/cache/v1/offset/{TOPIC}/0"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await.unwrap();
    let offset: u64 = body.parse().expect("decimal offset");
    assert!(
        offset + 1 >= bootstrap_hwm,
        "offset {offset} should be at or past bootstrap_hwm-1 ({})",
        bootstrap_hwm - 1
    );

    // OpenAPI 3.1 is served and parseable.
    let resp = client
        .get(format!("http://{addr}/openapi.json"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let spec: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(spec["openapi"], "3.1.0");
    assert!(spec["paths"]["/cache/v1/raw/{key}"].is_object());

    mirror.abort();
    let _ = server_shutdown_tx.send(());
    let _ = server_handle.await;
}
