# Delivery semantics revisit

Re-derives the per-mirror delivery contract after auditing kkv,
Quarkus mirror-v3-worker, and our current Rust binary. Scoped to the
checkit deployment (`mirror-v3-worker` Pod, two mirrors) but the
recommended changes are general.

## What we removed and why it was wrong

When we rewrote mirror-v3 in Rust we set `enable.auto.commit=false`
on the source consumer and never call `commit*()`. The `group.id` is
still set (defaults to `mirror-v3-<name>`) but is cosmetic  manual
`assign()` doesn't use it for partition coordination and nothing
ever writes to `__consumer_offsets`.

We thought this was safe because each sink has its own authoritative
"next expected offset":

- Filesystem/S3: derived from the latest written file's offset.
- Kafka: derived from the destination topic's end offset (or its
  own consumer group if running as a tee).
- `NotifyOnlySink`: in-memory `position`, reset to source
  low-watermark on every restart.

For sinks with **durable destination state** (the first two) this is
correct and gives exactly-once via destination idempotence. For the
notify path it isn't  there is no durable destination state, so a
restart re-seeks to low-watermark and the suppression PR (`5ef7c9e`)
silently drops any notify for records between previous-shutdown and
new-startup.

Both kkv and the Quarkus mirror-v3-worker avoided this gap by
letting SmallRye Reactive Messaging commit periodically (default:
throttled, 5 s) in the background. `enable.auto.commit=false` on
the Kafka client doesn't disable SmallRye's framework-level commits.
On restart `consumer.position()` returns the last committed offset
and processing resumes there. kkv's HWM-at-startup suppression then
fires only on the very first deployment of a fresh group.

Our suppression PR replicates the *fresh-group* behavior on every
restart, because we never advance the persisted high-water mark.

## checkit mirrors and what each one needs

`mirror-v3-worker-config.yaml` runs two mirrors against the
`kafka-v2-read-bootstrap` cluster, partition 0:

| Mirror      | Destinations              | `http-access`     | `notify`     | Required delivery semantics                                 |
|-------------|---------------------------|-------------------|--------------|-------------------------------------------------------------|
| operations  | kafka-v3, filesystem (GCS) | none              | none today   | Exactly-once on each destination (already correct)          |
| userstate   | filesystem (GCS)          | `cache-v1` (+main once we migrate) | kkv-v1 to `kkv-userstate` consumers | Exactly-once on GCS; **at-least-once on notify**            |

The production symptom in dev2 (boards-v1 stuck on stale
org-membership after a mirror restart) is the `userstate` notify
path missing its delivery contract.

## Recommended changes, in order

### 1. Commit source-consumer offsets from mirror-v3

Add a `commit_through(offset: u64)` method to the `Source` trait.
For `KafkaSource` it calls `store_offsets` (per-partition) and a
periodic `commit_consumer_state(CommitMode::Async)` from a
background task (interval: 5 s, override via
`MIRROR_V3_OFFSET_COMMIT_INTERVAL_MS`).

`enable.auto.commit=false` stays  we keep manual control over what
gets committed. We are *not* switching to `subscribe()`; pod churn
is bounded by `replicas: 1 + strategy: Recreate`, so manual
`assign()` is still correct.

### 2. Commit only after a sink/notify has accepted the offset

The supervisor tracks one committed offset per mirror:

- A mirror with a `notify` block: commit only after the notify
  dispatcher's drain (source-consume) or flush-event POST
  (destination-flush) has returned success. Commit at the highest
  offset whose batch was accepted.
- A notify-only mirror: same  the in-memory `NotifyOnlySink` is
  not authoritative; the *notify dispatch* is.
- A mirror without notify (today: `operations`): commit at the
  destination's flushed offset. This is observability /
  external-lag-monitoring only; resume position still comes from
  destination state.

Result for `userstate`: a notify that hasn't been ACK'd by the
target consumer pod stays uncommitted; a restart resumes from the
last successfully-delivered batch and re-fires from there. That is
at-least-once.

### 3. Replace HWM-at-startup suppression with committed-offset suppression

Today (`CacheState::MirrorSlot.bootstrap_hwm`): captured fresh on
every restart, used as a threshold below which `KkvV1Notifier`
suppresses.

After this change:
```rust
suppression_threshold = max(
    last_committed_offset,        // from the broker, on startup
    bootstrap_hwm_if_no_commit,   // fallback for first-ever deploy
)
```

- Existing deployments: `last_committed_offset` wins, and notify
  resumes exactly where the previous pod left off. The
  between-pods gap is closed.
- Fresh deployments: no commit yet, fall back to HWM-at-startup 
  matches kkv's "first deployment doesn't backfill historical
  records" behavior.

`cache-v1` view rebuild is unaffected  the source consumer still
seeks to `low_watermark` (or to a `compaction: log` snapshot
offset) for any mirror with `http_access.cache_v1`. The committed
offset is only consulted to decide *what to suppress*, not where to
seek.

### 4. Readiness driven by committed-offset lag

Replace the current "captured HWM at startup" gauge with a per-mirror
freshness signal:

```
lag(mirror) = current_end_offset - last_committed_offset
ready(mirror) = lag(mirror) <= MIRROR_V3_READINESS_LAG (default 0)
```

The supervisor polls broker end-offsets every few seconds (cheap;
single `fetch_watermarks` per mirror). For mirrors with no notify
(commits are observability-only) we use the destination's flushed
offset instead of the committed group offset.

`is_mirror_ready()` flips on the lag condition becoming true and
stays sticky-true (for now  the dev2 thread also flagged wanting
re-degradation on source-state regressions, but that is a separate
PR).

The kkv-compat `/q/health/ready` continues to AND together every
mirror's per-mirror flag.

### 5. Use the same gate for `/cache/v1/{mirror}/...`

Already aligned after the `cache-v1` rework (`0905f9d`): the
per-mirror cache routes already call `is_mirror_ready(mirror)`.
After (3) and (4), that flag is the lag-based gate and consumers
get a meaningful "still warming up" 503 across restarts.

## What stays from the current suppression PR

- `CacheState::MirrorSlot` and the per-mirror layout.
- The notify-side gate in `KkvV1Notifier::on_record` and
  `FlushDispatcher::on_flushed`.
- The `mirror_v3_notify_suppressed_records_total` counter, but now
  it fires for "below last-committed" rather than "below HWM at
  startup"  operator-visible difference: the counter only ticks
  during fresh deployments, not on every restart.

Effectively the suppression PR becomes the *first-deploy fallback*
and the new commit machinery is the *steady-state* mechanism.

## Files we expect to touch

- `crates/mirror-core/src/lib.rs`: `Source::commit_through` trait
  method; `Sink::flushed_through` for the observability-only
  commit path on no-notify mirrors.
- `crates/mirror-kafka/src/lib.rs`: implement
  `commit_through` (store_offsets + periodic
  `commit_consumer_state`); add a startup
  `fetch_committed_offset()` helper.
- `crates/mirror-core/src/cache.rs`: add
  `last_committed_offset: AtomicU64` next to `bootstrap_hwm`;
  expose `record_committed(mirror, offset)` and update the
  readiness predicate to use lag.
- `crates/mirror-notify-kkv/src/lib.rs`: after a successful batch
  / flush dispatch, call `source.commit_through(batch.high_offset)`
  via a handle the supervisor wires in.
- `crates/mirror-bin/src/main.rs`: at startup, per registered
  mirror, `fetch_committed_offset()` and pass into
  `register_mirror`. Spawn the periodic end-offset poller used by
  readiness.
- `crates/mirror-cache/src/lib.rs`: no surface change; the
  per-mirror routes already gate on `is_mirror_ready`.

## Scope decisions

In scope for the first PR after this revisit:

- (1)  (4) above, behind no feature flag (always on; fallback
  behaves correctly on fresh groups).
- Tests: restart simulation against an in-process Kafka harness
  showing zero dropped notify between pods.

Out of scope (separate work):

- Per-mirror, per-capability JSON on `/q/health/ready`.
- Re-degradation of the ready flag on source-state regressions.
- Multi-pod / rebalance handling  we stay on `assign()` while
  the worker is `replicas: 1 + strategy: Recreate`.

## Rollout for checkit

1. Land the change behind no flag (a fresh group has the same
   semantics as today on first deploy).
2. Cut a `dev2` deploy. The first restart still drops between-pod
   notifies (group has no committed offset yet); from the second
   restart on, the gap is closed.
3. Verify in dev2: invite a user to the example org, kill the
   mirror-v3-worker pod, confirm boards-v1 receives the invalidate
   on the new pod within one commit interval after re-fetch.
4. Promote to `prod` once dev2 stays healthy through a deliberate
   pod-restart cycle.

The webhooks branch in `Yolean/mirror-v3` already contains the
prerequisites (per-mirror cache surface, suppression scaffolding);
the commit work is additive on top of that.

