//! Round-trip the new `Source::commit_through` /
//! `fetch_committed_offset` + `KafkaCommitHandle::commit_pending`
//! against a real Kafka broker. Pins:
//!   * a fresh group reports `None`,
//!   * `commit_through` + `commit_pending` then a re-open with the
//!     same group reports the previously-staged value,
//!   * the monotonic guard ignores a regressing `commit_through`.

use std::time::Duration;

use mirror_core::Source;
use mirror_e2e::docker::DockerProvisioner;
use mirror_e2e::kafka_helpers::{create_topic, produce_records};
use mirror_e2e::{ProvisionedStack, Provisioner};
use mirror_kafka::{KafkaSource, KafkaSourceConfig};

const TOPIC: &str = "mirror-e2e-commit-offsets";

fn fresh_group(suffix: &str) -> String {
    // Each test in this file uses a fresh group id so the previous
    // test's commits don't leak. `uuid` is already a workspace dep
    // (used by mirror-fs).
    format!("mirror-e2e-commit-{suffix}-{}", uuid::Uuid::new_v4())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_group_has_no_committed_offset() {
    let stack = DockerProvisioner.provision().await.expect("provision");
    let bootstrap = stack.source_bootstrap();
    create_topic(&bootstrap, TOPIC, 1).await.expect("topic");
    let group = fresh_group("fresh");
    let mut source = KafkaSource::open(KafkaSourceConfig::new(bootstrap.clone(), group, TOPIC, 0))
        .expect("open");
    let got = source.fetch_committed_offset().await.expect("fetch");
    assert_eq!(got, None, "fresh group must report None");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_through_then_commit_pending_round_trips() {
    let stack = DockerProvisioner.provision().await.expect("provision");
    let bootstrap = stack.source_bootstrap();
    create_topic(&bootstrap, TOPIC, 1).await.expect("topic");
    // The broker needs at least one record so the committed offset
    // we stage is a valid one to read back.
    let pairs: Vec<(String, String)> = (0..3).map(|i| (format!("k{i}"), format!("v{i}"))).collect();
    produce_records(&bootstrap, TOPIC, 0, &pairs)
        .await
        .expect("produce");

    let group = fresh_group("rt");
    {
        let mut source = KafkaSource::open(KafkaSourceConfig::new(
            bootstrap.clone(),
            group.clone(),
            TOPIC,
            0,
        ))
        .expect("open");
        // `store_offsets` requires the partition to be in the
        // consumer's assigned set; in production the run loop's
        // `seek` establishes that before the supervisor's periodic
        // commit task fires. Mirror it here.
        source.seek(0).await.expect("seek");
        // Trait method stages; handle flushes. This mirrors the
        // supervisor's periodic-task wiring landing in a later
        // commit.
        source.commit_through(2).await.expect("commit_through");
        let handle = source.commit_handle();
        handle.commit_pending().expect("commit_pending");
    }
    // `commit_consumer_state(Async)` returns immediately; poll a
    // fresh re-open until the broker has acknowledged the write.
    let observed = poll_for_committed(&bootstrap, &group, Duration::from_secs(10)).await;
    assert_eq!(
        observed,
        Some(2),
        "round-trip must observe the staged offset"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_through_is_monotonic() {
    let stack = DockerProvisioner.provision().await.expect("provision");
    let bootstrap = stack.source_bootstrap();
    create_topic(&bootstrap, TOPIC, 1).await.expect("topic");
    let pairs: Vec<(String, String)> = (0..5).map(|i| (format!("k{i}"), format!("v{i}"))).collect();
    produce_records(&bootstrap, TOPIC, 0, &pairs)
        .await
        .expect("produce");

    let group = fresh_group("mono");
    let mut source = KafkaSource::open(KafkaSourceConfig::new(
        bootstrap.clone(),
        group.clone(),
        TOPIC,
        0,
    ))
    .expect("open");
    source.seek(0).await.expect("seek");
    source.commit_through(4).await.expect("first stage");
    // Regress; the guard must drop this silently. No error, no
    // overwrite of the broker's committed value.
    source.commit_through(1).await.expect("regress is no-op");
    source
        .commit_handle()
        .commit_pending()
        .expect("commit_pending");

    let observed = poll_for_committed(&bootstrap, &group, Duration::from_secs(10)).await;
    assert_eq!(
        observed,
        Some(4),
        "regression must be ignored; broker keeps the higher value"
    );
}
