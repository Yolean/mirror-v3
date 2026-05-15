//! Restart correctness e2e: the load-bearing claim of mirror-v3.
//!
//! "Restart correctness derives from the destination" means: after a
//! crashed mirror process is restarted, the new process must
//!   1. resume at exactly `max(to)+1` of the destination's chain;
//!   2. re-read any source records the old process buffered in memory
//!      but never flushed;
//!   3. produce no duplicates (no offset is written twice);
//!   4. leave no gaps in the destination chain.
//!
//! This test exercises that against the filesystem sink, which is
//! easiest to assert on. The same invariant holds for the Kafka sink
//! (proven by `kafka_native_to_redpanda`'s happy path + the
//! mirror-core mock-based gate tests).

use std::time::Duration;

use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{create_topic, produce_records};
use mirror_e2e::mirror_runner::{spawn_kafka_to_filesystem, FsMirrorSpec};
use mirror_e2e::{ProvisionedStack, Provisioner};
use mirror_fs::{read_all_records, FlushTriggers};

const TOPIC: &str = "mirror-e2e-restart";

fn spec(source: &str, root: &std::path::Path, group: &str, max_offsets: u64) -> FsMirrorSpec {
    FsMirrorSpec {
        source_bootstrap: source.to_string(),
        source_topic: TOPIC.into(),
        partition: 0,
        group_id: group.into(),
        root: root.to_path_buf(),
        destination_name: "ops".into(),
        format: mirror_envelope::Format::Ndjson,
        compression: mirror_envelope::ParquetCompression::Zstd1,
        flush: FlushTriggers {
            max_time: Duration::from_secs(3600),
            max_bytes: u64::MAX,
            max_offsets,
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aborted_mirror_resumes_from_destination_with_no_gaps_or_duplicates() {
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
    create_topic(&source, TOPIC, 1).await.expect("topic");

    // Stage 1: 25 records, mirror with max_offsets=10. Wait for two
    // files to be durable (offsets 0-9 and 10-19) — that means
    // exactly 20 are committed and up to 5 more (20-24) are buffered.
    let fixtures_a: Vec<(String, String)> = (0..25)
        .map(|i| (format!("k{i:03}"), format!("v{i:03}")))
        .collect();
    produce_records(&source, TOPIC, 0, &fixtures_a)
        .await
        .expect("produce a");

    let m1 = spawn_kafka_to_filesystem(spec(&source, root.path(), "mirror-e2e-restart", 10))
        .expect("spawn m1");

    let dir = root.path().join("ops").join("0");
    wait_for_durable_chain(&dir, 2, Duration::from_secs(30)).await;
    // Hard abort: simulates SIGKILL / OOM. Buffered records (20-24)
    // are lost.
    m1.abort();

    // Verify the destination is at offset 20, with no leftovers.
    let mid = read_all_records(&dir, mirror_envelope::Format::Ndjson).expect("mid read");
    assert_eq!(mid.len(), 20, "exactly 20 records durable mid-test");
    for (i, rec) in mid.iter().enumerate() {
        assert_eq!(rec.source_offset, i as u64);
    }

    // Stage 2: produce 10 more records to source (offsets 25-34).
    let fixtures_b: Vec<(String, String)> = (25..35)
        .map(|i| (format!("k{i:03}"), format!("v{i:03}")))
        .collect();
    produce_records(&source, TOPIC, 0, &fixtures_b)
        .await
        .expect("produce b");

    // Restart mirror. It should:
    //   - Compute next_expected_offset = 20 from listing.
    //   - Seek source to offset 20 (NOT to wherever m1 left off in
    //     memory — that state is gone).
    //   - Pick up records 20-34 (15 in total).
    //   - With max_offsets=10, flush once at 20-29, leave 30-34
    //     buffered.
    let m2 = spawn_kafka_to_filesystem(spec(&source, root.path(), "mirror-e2e-restart", 10))
        .expect("spawn m2");

    // Wait for 3 durable files (the new 20-29 lands).
    wait_for_durable_chain(&dir, 3, Duration::from_secs(30)).await;

    // Graceful shutdown forces a flush of the 30-34 buffer.
    m2.shutdown().await.expect("m2 shutdown");

    // Final assertion: 35 records, contiguous offsets 0..34, content
    // byte-identical to the union of fixtures_a and fixtures_b. No
    // duplicates, no gaps.
    let final_records =
        read_all_records(&dir, mirror_envelope::Format::Ndjson).expect("final read");
    assert_eq!(final_records.len(), 35, "35 records total");
    for (i, rec) in final_records.iter().enumerate() {
        assert_eq!(rec.source_offset, i as u64, "offset at index {i}");
        assert_eq!(
            rec.key.as_deref(),
            Some(format!("k{i:03}").as_bytes()),
            "key at offset {i}"
        );
        assert_eq!(
            rec.value.as_deref(),
            Some(format!("v{i:03}").as_bytes()),
            "value at offset {i}"
        );
    }

    // And the file layout proves nothing was re-flushed over an
    // existing range (which would itself be a hard error via the
    // PutMode::Create / rename-EEXIST guard — but we verify
    // structurally just in case).
    let mut filenames: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| !n.contains(".tmp."))
        .collect();
    filenames.sort();
    assert_eq!(
        filenames,
        vec![
            "00000000000000000000-00000000000000000009.ndjson".to_string(),
            "00000000000000000010-00000000000000000019.ndjson".to_string(),
            "00000000000000000020-00000000000000000029.ndjson".to_string(),
            "00000000000000000030-00000000000000000034.ndjson".to_string(),
        ]
    );
}

async fn wait_for_durable_chain(dir: &std::path::Path, expected_files: usize, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let count = match std::fs::read_dir(dir) {
            Ok(rd) => rd
                .filter_map(|e| e.ok())
                .filter(|e| !e.file_name().to_string_lossy().contains(".tmp."))
                .count(),
            Err(_) => 0,
        };
        if count >= expected_files {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "timed out waiting for {expected_files} durable files; got {count} in {}",
                dir.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
