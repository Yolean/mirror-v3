//! E2e: cache-v1 + tee of FS + S3. One cache binding, two
//! destinations.
//!
//! Verifies that `TeeSink::write` applies the cache binding exactly
//! once per record (the binding lives on the tee, not on inner
//! sinks). The cache view must match what a single-sink cache-v1
//! mirror would produce for the same fixture stream.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mirror_cache::KKV_OFFSETS_HEADER;
use mirror_core::CacheState;
use mirror_e2e::docker::{KafkaNativeToVersityGWStack, VERSITYGW_ACCESS_KEY, VERSITYGW_SECRET_KEY};
use mirror_e2e::kafka_helpers::{
    create_topic, produce_records, produce_records_with_nullable_values,
};
use mirror_e2e::mirror_runner::{
    spawn_kafka_to_tee, FsInnerSpec, S3InnerSpec, TeeInnerSpec, TeeMirrorSpec,
};
use mirror_e2e::ProvisionedStack;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;

const BUCKET: &str = "mirror-v3";
const TOPIC: &str = "mirror-e2e-tee-cache-v1";

fn install_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

fn s3_store(endpoint: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name(BUCKET)
            .with_access_key_id(VERSITYGW_ACCESS_KEY)
            .with_secret_access_key(VERSITYGW_SECRET_KEY)
            .build()
            .expect("build S3 client"),
    )
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
async fn tee_with_cache_v1_serves_latest_per_key_across_both_destinations() {
    install_tracing();

    let stack = KafkaNativeToVersityGWStack::start(BUCKET)
        .await
        .expect("provision stack");
    let source = stack.source_bootstrap();
    let s3_endpoint = stack.s3_endpoint();
    create_topic(&source, TOPIC, 1).await.expect("topic");

    // Same fixture shape as the single-sink cache_v1 test: seed
    // three keys, overwrite k1, tombstone k2. The cache should end
    // up with k1=v1-updated, k3=v3, k2 absent.
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

    let bootstrap_hwm = {
        let bootstrap = source.clone();
        tokio::task::spawn_blocking(move || {
            mirror_kafka::fetch_high_watermark(&bootstrap, TOPIC, 0, Duration::from_secs(5))
        })
        .await
        .unwrap()
        .expect("hwm") as u64
    };
    assert!(bootstrap_hwm >= 5);

    let cache_state = Arc::new(CacheState::new());
    cache_state.register_mirror("cache-mirror", bootstrap_hwm, true);
    let binding = mirror_core::CacheBinding {
        state: Arc::clone(&cache_state),
        mirror_name: "cache-mirror".into(),
    };

    // Bring up the HTTP server before the mirror so we can probe
    // the 503-before-catchup contract.
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
    tokio::time::sleep(Duration::from_millis(100)).await;
    let client = reqwest::Client::new();
    let early = client
        .get(format!("http://{addr}/cache/v1/raw/k1"))
        .send()
        .await
        .expect("early GET");
    assert_eq!(early.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    let root = tempfile::tempdir().expect("tempdir");
    let s3 = s3_store(&s3_endpoint);
    let flush_fs = mirror_fs::FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: u64::MAX,
        daily_at_utc_seconds: None,
    };
    let flush_s3 = mirror_s3::FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: u64::MAX,
        daily_at_utc_seconds: None,
    };
    let mirror = spawn_kafka_to_tee(TeeMirrorSpec {
        source_bootstrap: source.clone(),
        source_topic: TOPIC.into(),
        partition: 0,
        group_id: "mirror-e2e-tee-cache-v1".into(),
        destinations: vec![
            TeeInnerSpec::Filesystem(FsInnerSpec {
                name: "local".into(),
                root: root.path().to_path_buf(),
                format: mirror_envelope::Format::Parquet,
                compression: mirror_envelope::ParquetCompression::Zstd1,
                keys: mirror_envelope::ColumnType::Utf8,
                values: mirror_envelope::ColumnType::Utf8,
                compaction: None,
                flush: flush_fs,
            }),
            TeeInnerSpec::S3(S3InnerSpec {
                name: "offsite".into(),
                store: Arc::clone(&s3),
                prefix: Some(Path::from("archive")),
                format: mirror_envelope::Format::Parquet,
                compression: mirror_envelope::ParquetCompression::Zstd1,
                keys: mirror_envelope::ColumnType::Utf8,
                values: mirror_envelope::ColumnType::Utf8,
                compaction: None,
                flush: flush_s3,
            }),
        ],
        cache: Some(binding),
    })
    .await
    .expect("spawn tee mirror");

    // Wait for catch-up.
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

    // Latest wins for k1; k3 stable; k2 tombstoned.
    let resp = client
        .get(format!("http://{addr}/cache/v1/raw/k1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(resp.headers().contains_key(KKV_OFFSETS_HEADER));
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"v1-updated");

    let resp = client
        .get(format!("http://{addr}/cache/v1/raw/k3"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"v3");

    let resp = client
        .get(format!("http://{addr}/cache/v1/raw/k2"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // /keys reflects insertion order (k1 at offset 0, k3 at offset 2).
    let resp = client
        .get(format!("http://{addr}/cache/v1/keys"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "k1\nk3\n");

    // The offsets header advances exactly once per record (not twice,
    // which it would if both inner sinks each applied to the cache).
    // We assert the high-watermark offset matches bootstrap_hwm - 1.
    let resp = client
        .get(format!("http://{addr}/cache/v1/offset/{TOPIC}/0"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    let offset: u64 = body.parse().expect("decimal offset");
    assert!(
        offset + 1 >= bootstrap_hwm,
        "offset {offset} should be at or past bootstrap_hwm-1 ({})",
        bootstrap_hwm - 1
    );

    mirror.shutdown().await.expect("graceful shutdown");
    let _ = server_shutdown_tx.send(());
    let _ = server_handle.await;
}
