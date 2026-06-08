//! Discoverable contracts for test coverage we know we owe but
//! don't currently have. Each `#[ignore = "TODO: ..."]` test names
//! the gap, the rationale, and a pointer to the strategy document.
//!
//! Why this file exists
//! --------------------
//!
//! Several recent commits in this repo end up with a "the existing
//! e2e doesn't catch this" or "the test was passing for the wrong
//! reason" paragraph in their messages; useful prose, but it sits
//! in `git log` rather than the test suite. The reviewer's smaller
//! observation §1 in `REVIEW_TEST_STRATEGY.md` calls this out and
//! asks us to convert each known gap into a `cargo test --list`-able
//! contract. That's what this file is.
//!
//! Each test:
//!   - is `#[ignore = "TODO: ..."]` with the strategy-doc section
//!     it tracks,
//!   - documents in its body what shape the eventual implementation
//!     should take,
//!   - uses `unimplemented!()` so it doesn't accidentally run if
//!     `cargo test -- --include-ignored` is added to CI before the
//!     body is written.
//!
//! Removing the `#[ignore]` once the test is implemented is the
//! contract closure. `cargo test --list -p mirror-e2e | grep ignored`
//! is the discovery surface for what's left.

#![allow(unreachable_code, clippy::diverging_sub_expression)]

#[tokio::test]
#[ignore = "TODO: REVIEW_TEST_STRATEGY.md §3; needs real-broker compaction (not delete-records)"]
async fn kafka_source_low_watermark_after_pure_compaction_only() {
    //! Broker contract: a topic with `cleanup.policy=compact` (and
    //! *not* `compact,delete`) keeps `LogStartOffset = 0` after
    //! compaction has deduplicated keys; the segment start hasn't
    //! moved. From a consumer's point of view, `fetch_watermarks`
    //! returns `(0, high)` but `seek(0)` produces a record at some
    //! offset > 0 because the earlier records were dropped by
    //! upstream dedup.
    //!
    //! The existing `e2e/tests/compacted_source_with_compaction_log.rs`
    //! claims to cover this case but is using `delete-records` as a
    //! stand-in; that advances `LogStartOffset` and so doesn't
    //! reproduce the contract this test would assert.
    //!
    //! Implementation sketch:
    //!   1. Provision Redpanda (or Apache Kafka) with the topic
    //!      created `cleanup.policy=compact` only, `retention.ms=-1`,
    //!      `min.cleanable.dirty.ratio` = very low (e.g. 0.01),
    //!      `segment.ms` small enough to force segment rolls.
    //!   2. Produce N records over a small key-space (e.g. 1000
    //!      records over 50 keys, looping).
    //!   3. Force a segment roll (e.g. `rpk topic alter-config
    //!      segment.ms=1`, wait, restore).
    //!   4. Poll until the log cleaner runs and the segment on disk
    //!      is smaller than the original record count.
    //!   5. Call `KafkaSource::low_watermark()`; assert it returns
    //!      `0` (the contract this test exists to pin).
    //!   6. Call `consumer.seek(0)` + poll one; assert the first
    //!      delivered offset is > 0 (the gap the mirror has to
    //!      tolerate under `compaction:log`).
    //!
    //! Pairs with `kafka_source_low_watermark_contract.rs`, which
    //! covers the *post-delete-records* case (low watermark advances,
    //! the path 7fa70e7 fixed). Keeping both pinned at the broker-
    //! contract level lets a future librdkafka or Redpanda upgrade
    //! fail loudly here before the mirror-level tests break.
    unimplemented!("see REVIEW_TEST_STRATEGY.md §3 for the harness work this depends on");
}

#[tokio::test]
#[ignore = "TODO: REVIEW_TEST_STRATEGY.md §2; needs multi-broker Apache Kafka stack variant"]
async fn kafka_source_low_watermark_against_realistic_metadata_latency() {
    //! Bug class: `StreamConsumer::fetch_watermarks` on a fresh
    //! consumer that has not yet completed broker connection /
    //! metadata fetch returns `Ok((0, 0))` instead of querying the
    //! broker, against a real multi-broker Kafka cluster. 7fa70e7
    //! fixed this for `KafkaSource::low_watermark` by routing
    //! through a fresh `BaseConsumer` via `spawn_blocking`, but the
    //! local Redpanda harness can't reproduce the original failure
    //! mode because single-broker boot establishes connections
    //! fast enough that the StreamConsumer call also succeeds.
    //!
    //! Implementation options (REVIEW_TEST_STRATEGY.md §2 walks
    //! these in more detail):
    //!   - **Multi-broker Apache Kafka** via testcontainers. Slow
    //!     (~60s cold start) and adds a real CI cost; catches the
    //!     bug class directly.
    //!   - **Single-broker Kafka with injected metadata-fetch
    //!     latency** (e.g. a toxiproxy delay on the broker port).
    //!     Cheaper; catches the same class of bug as long as the
    //!     delay window crosses the consumer's "first call before
    //!     metadata arrived" threshold.
    //!
    //! The test would: open a `KafkaSource`, immediately call
    //! `low_watermark()`, assert the broker's actual value is
    //! returned. A second variant (or a parameterised run) calls
    //! `fetch_watermarks` *directly* on the StreamConsumer and
    //! asserts it returns the broken `(0, 0)`; that becomes the
    //! regression guard so a future commit can't silently revert
    //! to the StreamConsumer path without this test failing.
    unimplemented!(
        "see REVIEW_TEST_STRATEGY.md §2 for the multi-broker / latency-injection choice"
    );
}

#[tokio::test]
#[ignore = "TODO: REVIEW_TEST_STRATEGY.md smaller obs §2; stress fixture, not per-PR CI"]
async fn compaction_log_handles_production_scale_fixture() {
    //! Production reproducer the current 12-record e2e seeds don't
    //! exercise: 1.2M source offsets, multiple keys, real broker-
    //! side compaction work. Catches buffer-pressure issues, flush-
    //! trigger edge cases, and mid-stream-gap density patterns
    //! (compact-heavy topics deliver one gap per surviving key after
    //! upstream dedup; at scale, that's hundreds of thousands of
    //! gaps per restart) that small seeds don't surface.
    //!
    //! Should NOT run on every PR; the data volume is the point.
    //! Gate on a schedule (nightly?), a label, or a manual workflow
    //! dispatch. The strategy document explicitly suggests not
    //! conflating this with bug-catching coverage (that's what the
    //! sink matrix and the contract tests above are for).
    //!
    //! Implementation sketch:
    //!   1. Produce ~100k records over ~5k keys (cycle to force
    //!      compaction work).
    //!   2. Force broker compaction.
    //!   3. Start a `compaction:log` mirror.
    //!   4. Wait for the mirror to catch up.
    //!   5. Assert: no crash, the destination snapshot has ~5k
    //!      keys, the gap-accept counter
    //!      (`mirror_v3_source_offset_gap_records_total`) is in
    //!      the expected ballpark.
    unimplemented!("see REVIEW_TEST_STRATEGY.md smaller obs §2 for sizing + gating discussion");
}

#[tokio::test]
#[ignore = "TODO: REVIEW_TEST_STRATEGY.md §5; restart matrix, builds on §3 harness"]
async fn restart_correctness_across_cleanup_policies() {
    //! The seven-row matrix from REVIEW_TEST_STRATEGY.md §5:
    //!
    //! | Cleanup policy   | Destination state          | Behaviour |
    //! |------------------|---------------------------|-----------|
    //! | `delete`         | empty                      | seek(0)   |
    //! | `delete`         | non-empty                  | seek(next_expected) |
    //! | `compact,delete` | empty, after DeleteRecords | bootstrap-align |
    //! | `compact,delete` | non-empty < broker low     | bootstrap-align |
    //! | `compact,delete` | non-empty ≥ broker low     | no gap    |
    //! | `compact` only   | empty                      | first-delivery gap |
    //! | `compact` only   | non-empty                  | mid-stream gaps |
    //!
    //! The two `compact only` rows are the cells the PR-#1 work
    //! turned from "silently misbehaving" into "correct"; but
    //! there's no e2e test that exercises the full restart cycle
    //! against them. The other five rows are individually covered
    //! by existing tests; encoding them as one table catches "we
    //! added a sixth row and forgot to update the table" later.
    //!
    //! Depends on the real-broker compaction harness from §3 (the
    //! `compact only` rows can't run against a delete-records
    //! stand-in without circularity).
    //!
    //! Implementation: same shape as `restart_correctness.rs` for
    //! one cell, parameterised over the seven rows. Each cell:
    //!   1. Provision the broker with the given cleanup policy.
    //!   2. Seed records + apply the policy-specific advancement
    //!      (DeleteRecords for `*delete`, forced compaction for
    //!      `compact only`, nothing for `delete` empty case).
    //!   3. Optionally pre-populate the destination (the "non-empty"
    //!      rows).
    //!   4. Start the mirror.
    //!   5. Assert it reaches steady state without error, the
    //!      destination matches the broker's deliverable set, no
    //!      duplicates, no gaps that weren't legitimate compaction.
    unimplemented!("see REVIEW_TEST_STRATEGY.md §5; blocked on §3");
}
