# Test coverage / strategy critique

Written after a downstream consumer hit three bugs in a row that all
slipped through the existing suite:

1. `StreamConsumer.fetch_watermarks` returning `(0, 0)` against a
   real multi-broker Kafka cluster (the 7fa70e7 fix).
2. `LogStartOffset = 0` on a `cleanup.policy=compact`-only topic
   producing an apparent gap on first delivery that the c2e64c11
   bootstrap branch didn't anticipate.
3. A follow-up: mid-stream gaps under `compaction: log` exhausting
   `Sink::align_to_source_low_watermark`'s empty-buffer precondition
   on the second delivery (caught in production immediately after
   shipping the fix for #2, see the accompanying PR).

Each of those bugs is a direct consequence of one of three different
ways the harness systematically lies about production conditions.
This document spells those out and proposes specific test additions.
Nothing here requires new infrastructure beyond what's already in the
repo; step 5 is the one that genuinely needs a CI conversation.

## 1. The mock sink layer doesn't enforce real sink invariants

`mirror_core::mock::MockSink` and `WriteInspector::InspectorSink`
under `tests/loop_invariants.rs` track a running position but no
buffer/durable split. The real `FilesystemSink`/`S3Sink` have:

- a `buffer: Vec<Record>` that mutates after every `write`,
- a `durable_position` distinct from buffer head,
- an empty-buffer precondition on `align_to_source_low_watermark`,
- `flush_locked` deriving `(from, to)` from buffer state.

The mid-stream-gap bug never failed `loop_invariants.rs` because
`InspectorSink` had none of that state  `cargo test` was green and
production crashed on the second record. The earlier draft of the
compaction-only fix even called `align_to_source_low_watermark` from
the run loop on every mid-stream gap; the mock accepted it, the real
sink didn't.

**Fix.** Either add a `BufferedMockSink` that models a buffer +
durable split plus the empty-buffer precondition, or  better  bring
the mirror-core loop-invariant tests up against real `FilesystemSink`
instances using `tempfile::TempDir`. Neither needs network. The
latter has the additional benefit that any future invariant change
on the real sinks (e.g. tightening `align`'s preconditions further)
shows up as a `mirror-core` test failure rather than a production
crash.

## 2. Single-broker Redpanda  production multi-broker Kafka

The `kafka_source_low_watermark_contract.rs` test added with 7fa70e7
explicitly notes the harness can't reproduce the bug it set out to
fix; the test exists as a contract guard against future maintainers
silently regressing the fetch path. That's fine as far as it goes 
but it leaves the fix itself untested against any reproducer, and
the e2e suite still can't catch any other multi-broker metadata-race
bug in the same class.

**Fix.** A second e2e stack variant against Apache Kafka with 3+
brokers, run on PR. Each existing e2e test marked
`#[broker_count = single | any]` so the multi-broker pass becomes a
subset rather than a duplicate run. Cost is real (`testcontainers`
with three brokers is slow) but the bug class is uncatchable any
other way.

## 3. Stand-in policies embed wrong premises

The c2e64c11 commit message admits:

> E2e coverage uses delete-records as a deterministic stand-in for
> broker-side compaction (the consumer sees an identical post-trim
> low watermark).

That sentence IS the bug. `cleanup.policy=delete` (or `delete-records`
on a `compact,delete` topic) advances `LogStartOffset`  the broker
reports the new low. `cleanup.policy=compact` alone does not  the
broker reports `low = 0` regardless of how many keys have been
deduplicated, because the segment start hasn't moved. The fix built
on the wrong premise let the next layer of the same bug (gaps mid-
stream) hide too.

**Fix.** Each Kafka cleanup policy gets its own broker-contract
test, named after the policy. No stand-ins. Suggested:

  - `kafka_source_low_watermark_after_delete_records` (rename of
    current `kafka_source_low_watermark_contract.rs` for clarity;
    keep it  it documents the post-trim contract, which is still
    real and correct).
  - `kafka_source_low_watermark_after_compaction_only` (new  uses
    `log_compaction_interval_ms` + small `segment.ms` + forced
    segment roll; asserts `low == 0` after a real compaction pass).
  - `kafka_source_low_watermark_after_compact_and_delete` (new 
    asserts the `compact,delete` semantics, which differ from both
    above).

The "compaction:log accepts gaps" mirror-level tests then sit on top
of these broker-contract tests. If a future librdkafka or Redpanda
upgrade changes broker semantics, the contract tests fail loudly
before the mirror tests do.

## 4. The sink trait surface isn't exercised matrix-style

`allows_compacted_source()` is a mode flag that gates four other
methods (`write`, `next_expected_offset`, `flush_locked`,
`align_to_source_low_watermark`) plus the run-loop's per-record
gate. The test layout treats it as one feature with a handful of
happy-path tests; the actual matrix is:

| mode      | buffer state | event             | outcome                                |
| --------- | ------------ | ----------------- | -------------------------------------- |
| append    | empty        | delivered=exp     | write OK                               |
| append    | empty        | delivered>exp     | SourceGapAboveExpected                 |
| append    | empty        | delivered<exp     | SourceWentBackwards                    |
| append    | non-empty    | delivered=exp     | write OK                               |
| append    | non-empty    | delivered>exp     | SourceGapAboveExpected                 |
| append    | non-empty    | delivered<exp     | SourceWentBackwards                    |
| log       | empty        | delivered=exp     | write OK                               |
| log       | empty        | delivered>exp     | write OK (bootstrap-time gap)          |
| log       | empty        | delivered<exp     | SourceWentBackwards                    |
| log       | non-empty    | delivered=exp     | write OK                               |
| log       | non-empty    | delivered>exp     | write OK (compaction gap mid-stream)   |
| log       | non-empty    | delivered<exp     | SourceWentBackwards                    |
| log       | empty        | bootstrap align   | align OK                               |
| log       | non-empty    | bootstrap align   | empty-buffer precondition trips        |
| log       | any          | flush             | filename `<from>-<max(offsets)>.<ext>` |
| append    | any          | flush             | filename `<from>-<dur+len-1>.<ext>`    |

The mid-stream gap was the `log  non-empty  delivered>exp` cell 
new code, no test before the fix. A table-driven test in
`mirror-fs/tests/` and `mirror-s3/tests/` walking those rows against
real sinks would have caught the buffer-state mismatch before the
ship, and protects against future regressions where a mode change
touches one of the gated paths but not the others.

`proptest` is overkill; a `#[rstest]`-style or hand-rolled
table-driven test is fine. Rust enums + match on outcome keep the
table compile-checked.

## 5. Restart correctness has its own small matrix

The "destination is the source of truth" invariant is load-bearing
for every fix in this area. There's currently one e2e
(`compacted_source_with_compaction_log.rs`) that exercises one
corner of it. The full matrix is small enough to enumerate:

| Cleanup policy   | Destination state          | Expected behaviour                       |
| ---------------- | -------------------------- | ---------------------------------------- |
| `delete`         | empty                      | seek(0), no gap                          |
| `delete`         | non-empty                  | seek(next_expected), no gap              |
| `compact,delete` | empty, after DeleteRecords | seek(0), bootstrap-align to broker low   |
| `compact,delete` | non-empty < broker low     | seek(next_expected), bootstrap-align     |
| `compact,delete` | non-empty  broker low     | seek(next_expected), no gap              |
| `compact` only   | empty                      | seek(0), gap on first delivery           |
| `compact` only   | non-empty                  | seek(next_expected), gap mid-stream      |

Seven rows. Two of them (`compact only`) silently misbehaved until
the current fix landed. A table-driven e2e walking these would
catch any variant of this bug class going forward.

## Smaller observations

- **Commit messages doing their own testing.** Several recent
  commits ("Why the existing e2e didn't catch this", "the test was
  passing for the wrong reason") flag known coverage gaps in prose.
  That's good engineering culture, but the gaps then sit in
  `git log` instead of the test suite. Convert each such note into
  an `#[ignore = ""]` test with a TODO contract so it's
  discoverable from `cargo test --list`, not just from `git blame`.

- **Bench against bigger fixtures.** The production HWM the bug
  surfaced against (1.2M offsets, real compaction work) is orders
  of magnitude larger than the 12-record seeds in the existing e2e.
  A medium-sized stress fixture (10k100k records, multiple keys,
  forced compaction) catches buffer-pressure issues and flush-
  trigger edge cases that small seeds don't. Doesn't have to run
  on every PR  keep it gated on a label or schedule.

- **Don't conflate `delete-records` and "compaction" in test
  names.** The `compacted_source_*` e2e tests today are about
  `delete-records`, not real compaction. Renaming makes the gap
  visible at the file-listing level instead of buried in the
  comments.

## Order of operations if I were the maintainer

1. **Real-compaction repro in the existing single-broker harness.**
   Cheap, low risk; just `log_compaction_interval_ms` + `segment.ms`
   + forced roll. Unblocks all the renamed/split tests in 3.

2. **Convert the prose "we don't cover X" notes into ignored
   tests.** Five-minute hygiene with high payoff: each known gap
   becomes a discoverable contract.

3. **Sink-trait matrix table (4).** All in-process, no broker. Use
   real `FilesystemSink` instances. Catches the next mode  buffer-
   state regression for free.

4. **Restart matrix table (5).** Builds on 1. Touches the
   destination/restart story which is the load-bearing invariant
   for everything else in this area.

5. **Multi-broker Apache Kafka stack variant.** Expensive but
   recovers the missing third lie. Worth doing once the 14 work
   has caught everything cheap to catch.

Steps 1-4 are all in-repo, no new infrastructure or CI changes.
Step 5 is the one that genuinely needs a CI conversation.

