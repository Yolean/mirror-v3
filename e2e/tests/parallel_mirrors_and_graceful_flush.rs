//! Two mirrors of distinct partitions in one test, both writing to the
//! filesystem. Validates two things at once:
//!
//! 1. **Parallel mirrors:** independent (topic, partition) loops can
//!    run side by side and converge on independent destination
//!    directories.
//! 2. **Graceful shutdown / flush:** producing fewer records than the
//!    flush trigger leaves them buffered; calling `shutdown()` on the
//!    mirror handle must flush before returning, so the records land
//!    on disk before the test asserts.

use std::time::Duration;

use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{create_topic, produce_records};
use mirror_e2e::mirror_runner::{spawn_kafka_to_filesystem, FsMirrorSpec};
use mirror_e2e::{ProvisionedStack, Provisioner};
use mirror_fs::{read_all_records, FlushTriggers};

const TOPIC: &str = "mirror-e2e-parallel";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_mirrors_run_in_parallel_and_flush_on_shutdown() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    let stack = DockerProvisioner
        .provision()
        .await
        .expect("provision stack");
    let source = stack.source_bootstrap();
    let root = tempfile::tempdir().expect("tempdir");

    create_topic(&source, TOPIC, 2).await.expect("topic");

    // Send 7 records to partition 0 and 3 to partition 1. Both are
    // BELOW max-offsets=10 so the count trigger should not fire; only
    // graceful shutdown will flush them.
    let fixtures_p0: Vec<(String, String)> = (0..7)
        .map(|i| (format!("p0k{i}"), format!("p0v{i}")))
        .collect();
    let fixtures_p1: Vec<(String, String)> = (0..3)
        .map(|i| (format!("p1k{i}"), format!("p1v{i}")))
        .collect();
    produce_records(&source, TOPIC, 0, &fixtures_p0)
        .await
        .expect("produce p0");
    produce_records(&source, TOPIC, 1, &fixtures_p1)
        .await
        .expect("produce p1");

    let flush = FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: 10,
    };

    let m0 = spawn_kafka_to_filesystem(FsMirrorSpec {
        source_bootstrap: source.clone(),
        source_topic: TOPIC.into(),
        partition: 0,
        group_id: "mirror-e2e-parallel-0".into(),
        root: root.path().to_path_buf(),
        destination_name: "ops".into(),
        flush,
    })
    .expect("spawn m0");
    let m1 = spawn_kafka_to_filesystem(FsMirrorSpec {
        source_bootstrap: source.clone(),
        source_topic: TOPIC.into(),
        partition: 1,
        group_id: "mirror-e2e-parallel-1".into(),
        root: root.path().to_path_buf(),
        destination_name: "ops".into(),
        flush,
    })
    .expect("spawn m1");

    // Give both mirrors enough time to consume their records into
    // buffers but NOT enough for any flush trigger to fire (we set
    // max_time to 1h above).
    tokio::time::sleep(Duration::from_secs(2)).await;

    // No files yet: the count trigger needs 10 records.
    let p0_dir = root.path().join("ops").join("0");
    let p1_dir = root.path().join("ops").join("1");
    for d in [&p0_dir, &p1_dir] {
        if d.exists() {
            let names: Vec<_> = std::fs::read_dir(d)
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .filter(|n| !n.to_string_lossy().contains(".tmp."))
                .collect();
            assert!(
                names.is_empty(),
                "{}: nothing should be flushed yet, got {names:?}",
                d.display()
            );
        }
    }

    // Graceful shutdown -> flush.
    m0.shutdown().await.expect("m0 shutdown");
    m1.shutdown().await.expect("m1 shutdown");

    let recs_p0 = read_all_records(&p0_dir).expect("read p0");
    assert_eq!(recs_p0.len(), 7, "p0 records");
    for (i, rec) in recs_p0.iter().enumerate() {
        assert_eq!(rec.source_offset, i as u64);
        assert_eq!(rec.key.as_deref(), Some(format!("p0k{i}").as_bytes()));
        assert_eq!(rec.value.as_deref(), Some(format!("p0v{i}").as_bytes()));
    }

    let recs_p1 = read_all_records(&p1_dir).expect("read p1");
    assert_eq!(recs_p1.len(), 3, "p1 records");
    for (i, rec) in recs_p1.iter().enumerate() {
        assert_eq!(rec.source_offset, i as u64);
        assert_eq!(rec.key.as_deref(), Some(format!("p1k{i}").as_bytes()));
        assert_eq!(rec.value.as_deref(), Some(format!("p1v{i}").as_bytes()));
    }
}
