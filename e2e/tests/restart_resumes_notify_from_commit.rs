//! Dev2 symptom reproducer for the between-pods notify gap.
//!
//! Production symptom in `dev2` (checkit, June 2026): a consumer pod
//! polled `mirror-v3-worker`'s `/cache/v1` and got a 200 with stale
//! values after the worker pod restarted. Trace: mirror-v3 had
//! `enable.auto.commit=false` and never called a commit, so the
//! group had no broker-side state. The bootstrap-suppression PR
//! (`5ef7c9e`) reseeded suppression at every restart from the
//! broker high-watermark, so records produced between the previous
//! shutdown and the new startup got silently suppressed instead of
//! firing the consumer-invalidation webhook.
//!
//! Fix shape (commits 1-6 of the delivery-semantics PR):
//!   * `Source::commit_through` + `commit_pending` write the
//!     consumer's progress back to the broker.
//!   * `KkvV1Notifier` accepts an `AckSink` and notes the high
//!     offset of every successful drain.
//!   * `register_mirror_with_topic` takes
//!     `last_committed_offset: Option<u64>` and computes
//!     `suppression_threshold = max(last_committed, bootstrap_hwm)`.
//!     On a returning deploy with a previous commit, records in
//!     `[last_committed, bootstrap_hwm)` are no longer suppressed —
//!     the between-pods gap fires the webhook.
//!
//! This test exercises the whole flow against a real Kafka broker:
//!
//!   1. Produce 5 records. Run a mirror with `KkvV1Notifier`
//!      pointing at an in-process webhook receiver. Wait for the
//!      webhook to capture all 5 keys. Commit through offset 5.
//!   2. Stop the mirror. Produce 5 more records (offsets 5-9).
//!   3. Start a *new* mirror with the same `group.id`. Its
//!      `register_mirror_with_topic` is fed
//!      `last_committed_offset = fetch_committed_offset()`.
//!      Assert the webhook now captures offsets 5-9 (the gap is
//!      closed) and does NOT replay offsets 0-4 (the suppression
//!      threshold blocks records below the committed value).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use mirror_config::{
    FanOut, Notify, NotifyApi, NotifyDebounce, NotifyOutcomes, NotifyRetry, NotifyTarget,
    NotifyTrigger, TriggerOn,
};
use mirror_core::{run_mirror_with_notifier, CacheBinding, CacheState, Sink, Source, TeeSink};
use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{create_topic, produce_records};
use mirror_e2e::webhook_receiver::WebhookReceiver;
use mirror_e2e::{ProvisionedStack, Provisioner};
use mirror_envelope::{ColumnType, Format, ParquetCompression};
use mirror_fs::{FilesystemSink, FilesystemSinkConfig, FlushTriggers};
use mirror_kafka::{KafkaSource, KafkaSourceConfig};

const TOPIC: &str = "mirror-e2e-restart-resumes-notify";

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

fn notify_pointing_at(addr: std::net::SocketAddr) -> Notify {
    Notify {
        api: NotifyApi::KkvV1,
        targets: vec![NotifyTarget {
            url: format!("http://{addr}"),
            path: None,
            fan_out: FanOut::None,
        }],
        trigger: NotifyTrigger {
            on: TriggerOn::SourceConsume,
            debounce: Some(NotifyDebounce {
                max_records: 100,
                // Tight enough that 5 records drain before the
                // wait_for() timeout, slack enough that the dispatcher
                // batches them in one or two POSTs (not five).
                max_time_ms: 200,
            }),
        },
        timeout_ms: 2000,
        retry: NotifyRetry {
            max_attempts: 3,
            backoff_ms: 50,
        },
        outcomes: NotifyOutcomes::default(),
    }
}

fn fs_spec(root: &std::path::Path) -> FilesystemSinkConfig {
    FilesystemSinkConfig {
        root: root.to_path_buf(),
        destination_name: "notify".into(),
        partition: 0,
        format: Format::Ndjson,
        compression: ParquetCompression::Zstd1,
        keys: ColumnType::Utf8,
        values: ColumnType::Utf8,
        compaction: None,
        cache: None,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets: u64::MAX,
            daily_at_utc_seconds: None,
        },
    }
}

/// Extract the `updates` map keys from a kkv-v1 notify body. The
/// notifier POSTs JSON of shape `{"v":"v1","topic":..., "offsets":
/// {...}, "updates": {"<key>": "<base64>"}}`.
fn keys_in_body(body: &[u8]) -> HashSet<String> {
    let v: serde_json::Value = serde_json::from_slice(body).expect("notify body is JSON");
    v.get("updates")
        .and_then(|u| u.as_object())
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn webhooks_resume_at_committed_offset_after_restart() {
    init_tracing();
    let stack = DockerProvisioner.provision().await.expect("provision");
    let source_bootstrap = stack.source_bootstrap();
    let root = tempfile::tempdir().expect("tempdir");
    create_topic(&source_bootstrap, TOPIC, 1)
        .await
        .expect("topic");

    let group_id = format!("mirror-e2e-restart-resumes-notify-{}", uuid::Uuid::new_v4());
    let receiver = WebhookReceiver::start().await;
    let notify = notify_pointing_at(receiver.addr);

    // Stage 1: 5 records to source, run mirror, wait for webhook,
    // commit through offset 5.
    let pairs_a: Vec<(String, String)> = (0..5)
        .map(|i| (format!("k{i:03}"), format!("v{i:03}")))
        .collect();
    produce_records(&source_bootstrap, TOPIC, 0, &pairs_a)
        .await
        .expect("produce stage A");

    {
        let cache = Arc::new(CacheState::new());
        cache.register_mirror_with_topic("notify", 0, None, false, TOPIC, 0);
        let cache_binding = CacheBinding {
            state: Arc::clone(&cache),
            mirror_name: "notify".into(),
        };

        let source = KafkaSource::open(KafkaSourceConfig::new(
            source_bootstrap.clone(),
            group_id.clone(),
            TOPIC,
            0,
        ))
        .expect("open source A");
        let commit_handle = source.commit_handle();

        let fs_cfg = FilesystemSinkConfig {
            cache: Some(mirror_fs::CacheBinding {
                state: Arc::clone(&cache),
                mirror_name: "notify".into(),
            }),
            ..fs_spec(root.path())
        };
        let sink: Box<dyn Sink> = Box::new(FilesystemSink::open(fs_cfg).expect("open fs sink A"));
        let tee = TeeSink::open(vec![("notify".into(), sink)], Some(cache_binding))
            .await
            .expect("tee A");

        let notifier = mirror_notify_kkv::KkvV1Notifier::from_config(
            &notify,
            TOPIC.into(),
            0,
            Arc::clone(&cache),
            "notify".into(),
        )
        .expect("notifier A");

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let signal = async move {
            let _ = shutdown_rx.changed().await;
        };
        let handle = tokio::spawn(async move {
            run_mirror_with_notifier(
                source,
                tee,
                notifier,
                signal,
                mirror_core::DEFAULT_HEARTBEAT_INTERVAL,
            )
            .await
        });

        // Webhook receives every key we produced.
        let captured = receiver.wait_for(1, Duration::from_secs(15)).await;
        let mut got: HashSet<String> = HashSet::new();
        for req in &captured {
            got.extend(keys_in_body(&req.body));
        }
        for i in 0..5 {
            let want = format!("k{i:03}");
            assert!(
                got.contains(&want),
                "stage A webhooks must include {want}; got {got:?}"
            );
        }

        // Shut the mirror down, then write the consumer's progress
        // back to the broker. In production the supervisor's periodic
        // commit task does this on a schedule; here we drive it once
        // by hand to keep the test deterministic.
        let _ = shutdown_tx.send(true);
        handle.await.expect("join A").expect("mirror A ok");

        // The notifier's drain already advanced the in-memory ack
        // state; persist offset 5 to the broker so the next pod sees
        // it as the group's committed offset.
        commit_handle
            .commit_through(5)
            .expect("stage A commit_through");
        commit_handle
            .commit_pending()
            .expect("stage A commit_pending");
    }

    // Verify the broker accepted the commit before producing stage B.
    let observed = poll_for_committed(&source_bootstrap, &group_id, Duration::from_secs(10)).await;
    assert_eq!(
        observed,
        Some(5),
        "broker must report committed offset 5 after stage A"
    );

    // Stage 2: 5 more records to source.
    let pairs_b: Vec<(String, String)> = (5..10)
        .map(|i| (format!("k{i:03}"), format!("v{i:03}")))
        .collect();
    produce_records(&source_bootstrap, TOPIC, 0, &pairs_b)
        .await
        .expect("produce stage B");

    // Stage 2 webhook capture starts from where stage A left off,
    // since the same receiver is reused.
    let baseline = receiver.request_count();

    {
        let bootstrap_hwm = 10u64;
        let last_committed =
            poll_for_committed(&source_bootstrap, &group_id, Duration::from_secs(5))
                .await
                .expect("group must already have a committed offset");
        assert_eq!(last_committed, 5);

        let cache = Arc::new(CacheState::new());
        cache.register_mirror_with_topic(
            "notify",
            bootstrap_hwm,
            Some(last_committed),
            false,
            TOPIC,
            0,
        );
        let cache_binding = CacheBinding {
            state: Arc::clone(&cache),
            mirror_name: "notify".into(),
        };

        let source = KafkaSource::open(KafkaSourceConfig::new(
            source_bootstrap.clone(),
            group_id.clone(),
            TOPIC,
            0,
        ))
        .expect("open source B");

        let fs_cfg = FilesystemSinkConfig {
            cache: Some(mirror_fs::CacheBinding {
                state: Arc::clone(&cache),
                mirror_name: "notify".into(),
            }),
            ..fs_spec(root.path())
        };
        let sink: Box<dyn Sink> = Box::new(FilesystemSink::open(fs_cfg).expect("open fs sink B"));
        let tee = TeeSink::open(vec![("notify".into(), sink)], Some(cache_binding))
            .await
            .expect("tee B");

        let notifier = mirror_notify_kkv::KkvV1Notifier::from_config(
            &notify,
            TOPIC.into(),
            0,
            Arc::clone(&cache),
            "notify".into(),
        )
        .expect("notifier B");

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let signal = async move {
            let _ = shutdown_rx.changed().await;
        };
        let handle = tokio::spawn(async move {
            run_mirror_with_notifier(
                source,
                tee,
                notifier,
                signal,
                mirror_core::DEFAULT_HEARTBEAT_INTERVAL,
            )
            .await
        });

        // Stage B records (offsets 5-9) must fire the webhook. Wait
        // until at least one new POST has arrived since baseline,
        // then collect every captured key from stage B.
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if receiver.request_count() > baseline {
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("stage B: webhook receiver got no new POSTs");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let all_captured = receiver.captured().await;
        let mut stage_b_keys: HashSet<String> = HashSet::new();
        for req in &all_captured[baseline..] {
            stage_b_keys.extend(keys_in_body(&req.body));
        }
        for i in 5..10 {
            let want = format!("k{i:03}");
            assert!(
                stage_b_keys.contains(&want),
                "stage B webhooks must include {want} (between-pods gap); \
                 got stage-B keys {stage_b_keys:?}"
            );
        }
        // Stage A records must NOT be replayed: the suppression
        // threshold (committed offset 5) blocks notifies for records
        // 0..5. The mirror's source.seek() also doesn't go below the
        // group's committed offset, but the cache-side suppression
        // gate is the load-bearing check.
        for i in 0..5 {
            let unwanted = format!("k{i:03}");
            assert!(
                !stage_b_keys.contains(&unwanted),
                "stage A key {unwanted} must NOT replay on the new pod; \
                 got stage-B keys {stage_b_keys:?}"
            );
        }

        let _ = shutdown_tx.send(true);
        handle.await.expect("join B").expect("mirror B ok");
    }
}

async fn poll_for_committed(bootstrap: &str, group: &str, timeout: Duration) -> Option<u64> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let cfg = KafkaSourceConfig::new(bootstrap.to_string(), group.to_string(), TOPIC, 0);
        let mut s = KafkaSource::open(cfg).expect("re-open");
        if let Ok(Some(off)) = s.fetch_committed_offset().await {
            return Some(off);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

const TOPIC_UNACKED: &str = "mirror-e2e-unacked-window-replays";

/// The other half of the restart contract: records that are already
/// durable on the destination but whose notify batch was never acked
/// (committed) must re-fire after a restart. The destination state
/// alone would resume above them; the supervisor closes the gap by
/// setting the tee's resume floor to the committed offset, which
/// re-reads `[committed, durable)` from the source, skips the
/// destination writes and lets the suppression threshold (==
/// committed) admit exactly the un-acked records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unacked_notify_window_replays_after_restart() {
    init_tracing();
    let stack = DockerProvisioner.provision().await.expect("provision");
    let source_bootstrap = stack.source_bootstrap();
    let root = tempfile::tempdir().expect("tempdir");
    create_topic(&source_bootstrap, TOPIC_UNACKED, 1)
        .await
        .expect("topic");

    let group_id = format!("mirror-e2e-unacked-window-{}", uuid::Uuid::new_v4());
    let receiver = WebhookReceiver::start().await;
    let mut notify = notify_pointing_at(receiver.addr);
    notify.targets[0].url = format!("http://{}", receiver.addr);

    // Stage A: 5 records consumed and flushed durably, but the
    // broker commit only reaches offset 3 - as if the receiver died
    // after acking the first batch and the un-acked batch (offsets
    // 3-4) never advanced the commit.
    let pairs: Vec<(String, String)> = (0..5)
        .map(|i| (format!("k{i:03}"), format!("v{i:03}")))
        .collect();
    produce_records(&source_bootstrap, TOPIC_UNACKED, 0, &pairs)
        .await
        .expect("produce stage A");

    {
        let cache = Arc::new(CacheState::new());
        cache.register_mirror_with_topic("notify", 0, None, false, TOPIC_UNACKED, 0);
        let cache_binding = CacheBinding {
            state: Arc::clone(&cache),
            mirror_name: "notify".into(),
        };
        let source = KafkaSource::open(KafkaSourceConfig::new(
            source_bootstrap.clone(),
            group_id.clone(),
            TOPIC_UNACKED,
            0,
        ))
        .expect("open source A");
        let commit_handle = source.commit_handle();
        let fs_cfg = FilesystemSinkConfig {
            cache: Some(mirror_fs::CacheBinding {
                state: Arc::clone(&cache),
                mirror_name: "notify".into(),
            }),
            destination_name: "notify".into(),
            ..fs_spec(root.path())
        };
        let sink: Box<dyn Sink> = Box::new(FilesystemSink::open(fs_cfg).expect("open fs sink A"));
        let tee = TeeSink::open(vec![("notify".into(), sink)], Some(cache_binding))
            .await
            .expect("tee A");
        let notifier = mirror_notify_kkv::KkvV1Notifier::from_config(
            &notify,
            TOPIC_UNACKED.into(),
            0,
            Arc::clone(&cache),
            "notify".into(),
        )
        .expect("notifier A");

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let signal = async move {
            let _ = shutdown_rx.changed().await;
        };
        let handle = tokio::spawn(async move {
            run_mirror_with_notifier(
                source,
                tee,
                notifier,
                signal,
                mirror_core::DEFAULT_HEARTBEAT_INTERVAL,
            )
            .await
        });

        receiver.wait_for(1, Duration::from_secs(15)).await;
        let _ = shutdown_tx.send(true);
        handle.await.expect("join A").expect("mirror A ok");
        // The graceful shutdown flushed offsets 0-4 to the fs chain
        // (durable head = 5); commit only through 3.
        commit_handle.commit_through(3).expect("commit_through");
        commit_handle.commit_pending().expect("commit_pending");
    }

    let observed = poll_for_committed_on(
        &source_bootstrap,
        &group_id,
        TOPIC_UNACKED,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(observed, Some(3), "broker must report committed offset 3");

    let baseline = receiver.request_count();

    // Stage B: restart against the same fs root and group. Replicates
    // the supervisor's startup recipe: threshold = committed,
    // resume floor = max(committed, low watermark).
    {
        let cache = Arc::new(CacheState::new());
        cache.register_mirror_with_topic("notify", 5, Some(3), false, TOPIC_UNACKED, 0);
        let cache_binding = CacheBinding {
            state: Arc::clone(&cache),
            mirror_name: "notify".into(),
        };
        let source = KafkaSource::open(KafkaSourceConfig::new(
            source_bootstrap.clone(),
            group_id.clone(),
            TOPIC_UNACKED,
            0,
        ))
        .expect("open source B");
        let fs_cfg = FilesystemSinkConfig {
            cache: Some(mirror_fs::CacheBinding {
                state: Arc::clone(&cache),
                mirror_name: "notify".into(),
            }),
            destination_name: "notify".into(),
            ..fs_spec(root.path())
        };
        let sink: Box<dyn Sink> = Box::new(FilesystemSink::open(fs_cfg).expect("open fs sink B"));
        let mut tee = TeeSink::open(vec![("notify".into(), sink)], Some(cache_binding))
            .await
            .expect("tee B");
        let durable_head = tee.heads().iter().map(|(_, h)| *h).min().unwrap();
        assert_eq!(durable_head, 5, "stage A must have flushed 0-4 durably");
        tee.set_resume_floor(3);

        let notifier = mirror_notify_kkv::KkvV1Notifier::from_config(
            &notify,
            TOPIC_UNACKED.into(),
            0,
            Arc::clone(&cache),
            "notify".into(),
        )
        .expect("notifier B");

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let signal = async move {
            let _ = shutdown_rx.changed().await;
        };
        let handle = tokio::spawn(async move {
            run_mirror_with_notifier(
                source,
                tee,
                notifier,
                signal,
                mirror_core::DEFAULT_HEARTBEAT_INTERVAL,
            )
            .await
        });

        // No new records are produced: everything that arrives at the
        // receiver is the replayed un-acked window.
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if receiver.request_count() > baseline {
                tokio::time::sleep(Duration::from_millis(300)).await;
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("stage B: no webhook fired for the un-acked window");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let all_captured = receiver.captured().await;
        let mut replay_keys: HashSet<String> = HashSet::new();
        for req in &all_captured[baseline..] {
            replay_keys.extend(keys_in_body(&req.body));
        }
        for i in 3..5 {
            let want = format!("k{i:03}");
            assert!(
                replay_keys.contains(&want),
                "un-acked window key {want} must re-fire; got {replay_keys:?}"
            );
        }
        for i in 0..3 {
            let unwanted = format!("k{i:03}");
            assert!(
                !replay_keys.contains(&unwanted),
                "committed key {unwanted} must stay suppressed; got {replay_keys:?}"
            );
        }

        let _ = shutdown_tx.send(true);
        handle.await.expect("join B").expect("mirror B ok");
    }

    // The replay must not have duplicated destination data: the
    // chain still ends at offset 4.
    let sink_check = FilesystemSink::open(fs_spec(root.path())).expect("re-open for check");
    let mut boxed: Box<dyn Sink> = Box::new(sink_check);
    let head = boxed.next_expected_offset().await.expect("head check");
    assert_eq!(head, 5, "destination chain must be unchanged by the replay");
}

async fn poll_for_committed_on(
    bootstrap: &str,
    group: &str,
    topic: &str,
    timeout: Duration,
) -> Option<u64> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let cfg = KafkaSourceConfig::new(bootstrap.to_string(), group.to_string(), topic, 0);
        let mut s = KafkaSource::open(cfg).expect("re-open");
        if let Ok(Some(off)) = s.fetch_committed_offset().await {
            return Some(off);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
