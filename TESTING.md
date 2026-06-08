# Testing strategy for mirror-v3

This is the entrypoint for "I need to test a spec change; where does
my test go?" The answer is almost always one of the seven layers
below. Pick the cheapest one that can actually exercise the
invariant.

The palette is sorted from cheapest (in-process, no I/O) to most
expensive (Docker, multi-broker). Each layer lists what kind of
spec change belongs there, what's already in it, and the
testability primitives available.

## TL;DR by spec-change shape

| Spec change touches… | Layer | Cost |
|---|---|---|
| Pure data (envelopes, config parsing, validation rules) | **L1** unit | ms |
| The `run_mirror` loop's invariants (offset gates, source events) | **L2** loop_invariants | ms |
| A sink's internal invariants (buffer/durable split, filename, align) | **L3** sink matrix | ms |
| The loop + sink combination (mock-vs-real divergence guard) | **L4** loop_invariants_with_real_sink | ~tens of ms |
| HTTP handler / OpenAPI / cache view | **L5** in-process http (tower::oneshot) | ms |
| Real Kafka semantics (broker contracts, librdkafka) | **L6** Docker e2e | seconds |
| Things we know we owe but haven't built yet | **L7** known_coverage_gaps.rs | n/a (placeholder) |

## L1; Per-crate unit tests (in-source `#[cfg(test)] mod tests`)

**Where:** `crates/*/src/*.rs` inline `mod tests {…}` blocks.

**Use when:** the spec is about a pure function: parsing YAML, validating a config rule, encoding/decoding an envelope, computing a file path, expanding env interpolation. No async, no I/O, no traits.

**Existing examples:**
- `mirror-config/src/envsubst.rs`; `${VAR}` / `${VAR:-default}` expansion algorithm.
- `mirror-config/src/lib.rs` (daily_tests); `at_utc: "HH:MM:SS"` parsing.
- `mirror-core/src/cache.rs`; monotonic CacheState, insertion-order keys, tombstone semantics.
- `mirror-core/src/tee.rs` (tests module); TeeSink's per-sink head logic against in-process mock inner sinks.

**Testability primitives available:** all of `std`, `serde_json::Value` for AST-style assertions, no special harness needed.

## L2; Loop invariants against `MockSink` (`mirror-core/tests/loop_invariants.rs`)

**Where:** `crates/mirror-core/tests/loop_invariants.rs`.

**Use when:** the spec is about `run_mirror`'s decision-making; when it errors, what error variant, how it advances `expected`, what it does on idle. The invariant under test should hold *regardless* of which concrete sink is plugged in, so a mock sink is appropriate.

**Existing examples:**
- `errors_on_source_offset_gap_in_append_mode`; append mode rejects forward gaps.
- `errors_on_source_going_backwards`; backwards is always fatal.
- `compaction_log_accepts_repeated_gaps_mid_stream`; the production-bug repro.
- `errors_on_destination_drift_during_idle`; idle re-check catches out-of-band writes.

**Testability primitives available:**
- `mirror_core::mock::MockSource`; script `Record`, `Idle`, `Error`, `Hang` events.
- `MockSource::with_low_watermark(u64)`; broker low watermark for the bootstrap branch.
- `MockSource::with_high_watermark(u64)`; broker high watermark, for spec changes that introduce a "sink can't exceed source HWM" gate.
- `mirror_core::mock::MockSink`; scripted `next_expected_offset`, write-error injection, recorded writes.
- `MockSink::with_allows_compacted_source(bool)`; gate for compaction-log behaviour.
- `mirror_core::testing::BlanketMockSink`; closure-per-method Sink for TDD-style spec tests where the existing `MockSink` builder doesn't express what you need. Each method is an `FnMut`, so the closure can capture mutable test state (counters, scripted sequences). All trait-method invocations are recorded in `BlanketMockSink::calls()` for post-hoc assertions. See the `tests` module in `crates/mirror-core/src/testing.rs` for usage shapes.
- Metric assertions: not yet; emit-side assertion is in [`L7` known_coverage_gaps](#l7--documented-coverage-gaps-e2etestsknown_coverage_gapsrs) until a spec change actually needs it. The typical workaround today is to assert on the visible side-effect (logged message, written record) instead of the metric itself.

**When to escalate to L4:** if the spec touches the sink's *internal* state machine (buffer/durable split, view, filename). MockSink doesn't model those. Promote to L3 if the spec is *about* the sink, or L4 if it's about the loop+sink combination.

## L3; Sink matrix (`mirror-{fs,s3}/tests/sink_matrix.rs`)

**Where:** `crates/mirror-fs/tests/sink_matrix.rs` and `crates/mirror-s3/tests/sink_matrix.rs`.

**Use when:** the spec is about a sink's per-record state machine; what `write` accepts under which mode and buffer state, what `next_expected_offset` returns, what `align_to_source_low_watermark` requires, what filename `flush` produces. The cells are `(compaction-mode × buffer-state × action)`.

**Existing structure:** a `MATRIX: Vec<Case>` with named cells (e.g. `log/non_empty/write_above_expected/ok_midstream_gap`). Each cell:
- `preload: &[u64]`; records to write before the action.
- `buffer_state: Empty | NonEmpty`; flush after preload or not.
- `action: Write | Flush | Align | NextExpected`.
- `expected: Ok | NextExpectedIs(u64) | UnexpectedPosition{...} | TransportContains("...")`.

**To add a spec test:** append one `Case` to `matrix_cases()`. Pick the cell coordinates (mode, state, action), name it `<mode>/<state>/<action>/<outcome>`. Mirror it row-for-row in the S3 file unless the contract genuinely diverges between backends.

**Testability primitives available:**
- `tempfile::TempDir` for FS isolation; `object_store::memory::InMemory` for S3 isolation.
- The `Outcome` enum is exhaustive across the trait surface; extend it if a new spec introduces a new observable outcome.

**When to escalate to L4:** the spec is about how the *run loop* reacts to the sink's state (e.g. "loop must crash if sink rejects in compaction mode"). The matrix is sink-only; the loop interaction belongs in L4.

## L4; Loop + real sink (`mirror-fs/tests/loop_invariants_with_real_sink.rs`)

**Where:** `crates/mirror-fs/tests/loop_invariants_with_real_sink.rs`.

**Use when:** the spec change spans the loop ↔ sink boundary, and either:
- a similar mock-only test in L2 wouldn't catch a real-sink invariant mismatch, or
- the spec is "the loop's behaviour AND the sink's behaviour together produce X observable state on disk."

**Existing examples:**
- `compaction_log_real_sink_accepts_repeated_midstream_gaps`; the production repro (loop accepts forward gaps + sink buffers them + flush emits a `0-470.parquet` with 2 deduplicated keys).
- `append_mode_real_sink_rejects_source_gap`; loop's `SourceGapAboveExpected` is observable from the test, no disk write.

**Testability primitives available:**
- `drive_real_fs(compaction, events, grace_duration)` helper drives `run_mirror` against a real FilesystemSink and a scripted MockSource. The shutdown future is a timer (`tokio::time::sleep(grace)`) so the loop has a window to process events before graceful shutdown.
- All L2 primitives (MockSource, BlanketMock* via mirror_core::testing).

**When to escalate to L6:** real librdkafka, real broker semantics (compaction policy, transactional offsets, metadata-fetch latency), or anything that requires a network address.

## L5; In-process HTTP (`mirror-cache/tests/handlers.rs`)

**Where:** `crates/mirror-cache/tests/handlers.rs`.

**Use when:** the spec is about the `/cache/v1/*` HTTP surface (routing, status codes, headers, response bodies). Uses `tower::ServiceExt::oneshot` against the `axum::Router`; no socket, no port allocation, no flakes.

**Pattern:**
```rust
let app = build_router(state, shutdown_tx);
let resp = app.oneshot(Request::get("/cache/v1/raw/k0").body(Body::empty())?).await?;
assert_eq!(resp.status(), StatusCode::OK);
```

**When to escalate to L6:** the spec involves real network behaviour (TLS, concurrent clients, real backpressure).

## L6; Docker e2e (`e2e/tests/*.rs`)

**Where:** `e2e/tests/*.rs`. Provisioned via `mirror_e2e::docker::DockerProvisioner` (Redpanda + VersityGW + Toxiproxy as needed).

**Use when:** the spec is about a broker contract you can't honestly fake (cleanup policies, low/high watermark behaviour, librdkafka client lifecycle), or about a multi-component scenario (mirror + cache + HTTP server, crash + restart with real durable state on disk, fault injection via Toxiproxy).

**Cost:** seconds per test, sequenced via `--test-threads=1` because tests share Docker resources.

**Existing patterns:**
- `kafka_helpers::create_topic`, `produce_records`, `drain_partition`; Kafka fixture utilities.
- `mirror_runner::spawn_kafka_to_filesystem`, `spawn_kafka_to_s3`, `spawn_kafka_to_tee`; start a mirror in-process against the provisioned source/sink.
- `stack.source_bootstrap()`, `stack.target_kafka_bootstrap()`, `stack.s3_endpoint()`, `stack.target_down()`; environment handles.

**When to escalate to L7:** the spec needs a broker behaviour we don't yet have a harness for (real compaction, multi-broker metadata race, large-scale fixtures).

## L7; Documented coverage gaps (`e2e/tests/known_coverage_gaps.rs`)

**Where:** `e2e/tests/known_coverage_gaps.rs`.

**Use when:** the test infrastructure for a spec doesn't exist yet, but the contract is real and should be visible. Each entry is an `#[ignore = "TODO: ..."]` test with `unimplemented!()` body and a doc-comment naming the contract and the layer it would belong in once implementable.

**Discovery:** `cargo test --list -p mirror-e2e | grep ignored`.

**Pattern:** add a stub with the ignore reason pointing at `REVIEW_TEST_STRATEGY.md §X`. When the harness arrives, drop `#[ignore]` and fill in the body.

## Adding a new layer

If a spec's natural test wouldn't fit anywhere above; for example, a property-based test against the gate semantics, or a CPU-bench fixture; add a new file at the appropriate crate level and document it here. Resist the temptation to overload an existing layer with a new responsibility; the catalogue is most useful when each layer has one clear charter.

## Quick reference: writing a test for a brand-new invariant

Example spec: *"The mirror must crash with a specific error variant if `sink.next_expected_offset()` ever exceeds `source.high_watermark()`. This catches destination chains that have somehow advanced past the broker (out-of-band writes, restored from a too-recent backup)."*

1. **Pick the layer.** The check belongs in `run_mirror`'s startup or idle path, so the test belongs in **L2** (`loop_invariants.rs`).
2. **Write the test first.** Using the existing palette:
   ```rust
   #[test]
   fn errors_when_sink_is_ahead_of_source_high_watermark() {
       let source = MockSource::new([MockSourceEvent::Hang])
           .with_high_watermark(100);
       let sink = MockSink::starting_at(150); // sink is ahead!
       let result = drive(run_mirror(source, sink, never()));
       match result {
           Err(MirrorError::SinkAheadOfSource { sink_offset, source_hwm }) => {
               assert_eq!(sink_offset, 150);
               assert_eq!(source_hwm, 100);
           }
           other => panic!("expected SinkAheadOfSource, got {other:?}"),
       }
   }
   ```
3. **Run it.** It fails to compile (`SinkAheadOfSource` doesn't exist yet); that's the red part of red-green-refactor.
4. **Add the variant** to `MirrorError`, **add the check** in `run_mirror_with_heartbeat` (`Source::high_watermark` already exists with a u64::MAX default that won't trip existing tests), run again; green.
5. **No mock infrastructure changes needed.** `with_high_watermark` is already a builder method on `MockSource`. That's the point of the palette.

If the same spec applied to the sink's internal state (e.g. "sink rejects align if its durable position exceeds the requested low_watermark") the test would land in **L3** (`sink_matrix.rs`) instead, by adding a row to `matrix_cases()`. Same flow: write the row, watch it fail, implement the check, watch it pass.
