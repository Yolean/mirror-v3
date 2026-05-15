//! E2e: kafka-native → mirror-v3 (Filesystem sink).
//!
//! Drives the same source as kafka_native_to_redpanda but writes the
//! destination to a temp directory. Verifies:
//!   - The expected number of files exist with correct `from-to` names.
//!   - Records inside the files match what was produced, in order.
//!   - On restart (open a new sink), `next_expected_offset` matches
//!     the durable position (i.e. restart correctness from
//!     destination).

use std::time::Duration;

use mirror_core::Sink;
use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{create_topic, produce_records};
use mirror_e2e::mirror_runner::{spawn_kafka_to_filesystem, FsMirrorSpec};
use mirror_e2e::{ProvisionedStack, Provisioner};
use mirror_fs::{read_all_records, FilesystemSink, FilesystemSinkConfig, FlushTriggers};

const TOPIC: &str = "mirror-e2e-fs";
const N: usize = 50;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mirrors_to_filesystem_with_offset_named_files() {
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
    tracing::info!(source = %source, root = %root.path().display(), "stack ready");

    create_topic(&source, TOPIC, 1).await.expect("topic");

    let fixtures: Vec<(String, String)> = (0..N)
        .map(|i| (format!("k{i:04}"), format!("v{i:04}")))
        .collect();
    produce_records(&source, TOPIC, 0, &fixtures)
        .await
        .expect("produce");

    // Flush trigger: every 10 records -> 5 files of 10 records each.
    let flush = FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: 10,
    };
    let mirror = spawn_kafka_to_filesystem(FsMirrorSpec {
        source_bootstrap: source.clone(),
        source_topic: TOPIC.into(),
        partition: 0,
        group_id: "mirror-e2e-fs".into(),
        root: root.path().to_path_buf(),
        destination_name: "ops".into(),
        format: mirror_envelope::Format::Ndjson,
        compression: mirror_envelope::ParquetCompression::Zstd1,
        flush,
    })
    .expect("spawn mirror");

    // Poll the destination directory until N records have landed.
    let dir = root.path().join("ops").join("0");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let records = read_all_records(&dir, mirror_envelope::Format::Ndjson).unwrap_or_default();
        if records.len() >= N {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {N} records; got {}",
            records.len()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    mirror.abort();

    // Assert: 5 files of 10 each, named 0-9, 10-19, ..., 40-49.
    let mut filenames: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| !n.contains(".tmp."))
        .collect();
    filenames.sort();
    let expected_names: Vec<String> = (0..5)
        .map(|i| {
            let from = i * 10;
            let to = from + 9;
            format!("{from:020}-{to:020}.ndjson")
        })
        .collect();
    assert_eq!(filenames, expected_names, "filenames");

    // Assert: byte-identical key/value at each offset.
    let records = read_all_records(&dir, mirror_envelope::Format::Ndjson).expect("read all");
    assert_eq!(records.len(), N);
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.source_offset, i as u64, "offset at {i}");
        assert_eq!(
            rec.key.as_deref(),
            Some(format!("k{i:04}").as_bytes()),
            "key at {i}"
        );
        assert_eq!(
            rec.value.as_deref(),
            Some(format!("v{i:04}").as_bytes()),
            "value at {i}"
        );
    }

    // Restart correctness: a fresh sink against the same dir reports
    // next_expected_offset == N (not 0), proving destination is the
    // truth.
    let mut restarted = FilesystemSink::open(FilesystemSinkConfig {
        root: root.path().to_path_buf(),
        destination_name: "ops".into(),
        partition: 0,
        format: mirror_envelope::Format::Ndjson,
        compression: mirror_envelope::ParquetCompression::Zstd1,
        flush,
    })
    .expect("reopen");
    assert_eq!(restarted.next_expected_offset().await.unwrap(), N as u64);
}
