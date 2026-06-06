# Proposal: opt-in HTTP notify for mirror-v3

A minimal, configurable outbound webhook surface so mirror-v3 can
replace `Yolean/kafka-keyvalue` (kkv) end-to-end, not just on the
read side. The existing `http-access: { api: cache-v1 }` block
covers the GET surface; this proposal adds the symmetric
*you-need-to-re-read* push that legacy consumers depend on.

## Background

Legacy kkv was push-based by design. When a source-topic record
landed, kkv POST'd to each pod backing a `TARGET_SERVICE_NAME`
headless Kubernetes Service (discovered via the K8s Endpoints API),
telling the consumer "these keys have changed; re-read them via
`/cache/v1/raw/<key>`". The downstream client library
(`@yolean/kafka-keyvalue` for Node) invalidates its in-process cache
on receipt and re-fetches lazily.

mirror-v3's cache-v1 is pull-only. Consumers' in-process caches
therefore never refresh after their initial replay. In production
this manifests as records produced *after* a consumer service
started up never reaching that service's local view: the source
topic has the new record, mirror-v3's cache-v1 in-memory map sees
it, but the consumer's own in-process cache is stuck on the value
it snapshotted at startup; because nothing tells it to invalidate.

This proposal adds the missing push side as a per-mirror opt-in,
without resurrecting any of kkv's other behaviour.

## Goals and non-goals

Goals:

- Cover every current kkv deployment shape with one mirror-v3
  feature (see "Use cases" for the shape catalogue).
- Match kkv's wire contract exactly so the existing
  `@yolean/kafka-keyvalue` client (`getOnUpdateRoute()`,
  `ON_UPDATE_DEFAULT_PATH = "/kafka-keyvalue/v1/updates"`) works
  unmodified against mirror-v3.
- Stay K8s-API-free in the binary itself: no `Endpoints` watch, no
  Kubernetes SDK dependency, no in-cluster RBAC requirement on the
  mirror's own ServiceAccount.
- Keep the existing destinations / cache-v1 / compaction:log
  contracts unchanged. This is additive.

Non-goals (out of scope, deferable):

- Auth on the outbound request (mTLS, bearer, signing). MVP assumes
  in-cluster targets behind a trusted network boundary; the
  legacy kkv had the same assumption.
- Per-key or per-prefix subscription filters. Today all keys go to
  all targets.
- Per-target circuit breakers. MVP: any retry-exhausted target
  failure crashes the mirror task (consistent with mirror-v3's
  "unrecoverable error exits the process" model).
- Push-only mode (no cache-v1, just notify). The kkv contract
  assumes consumers re-fetch via cache-v1 on receipt; require
  `http-access: { api: cache-v1 }` to coexist for now.

## Use cases this needs to cover

The deployment shape used by every observed kkv instance:

| dimension                | shape                                                        |
|--------------------------|--------------------------------------------------------------|
| One mirror per…          | (source topic, partition); same as mirror-v3 already         |
| Target discovery         | A Kubernetes *headless* Service named after the role          |
| Target replica count     | 1–N consumer pods behind that Service                         |
| Target route             | `POST /kafka-keyvalue/v1/updates` on each pod, port 8080      |
| Consumer client library  | `@yolean/kafka-keyvalue` (Node); mounts the route as-is      |

Consumer-side route mount, identical across every deployment seen:

```js
const { ON_UPDATE_DEFAULT_PATH, getOnUpdateRoute } = require('@yolean/kafka-keyvalue');
app.post(ON_UPDATE_DEFAULT_PATH, getOnUpdateRoute());
```

A single wire format therefore suffices for the entire installed
fleet. Multi-replica targets are the common case (1–N consumer
pods behind a headless Service), so notify must fan out across
the Service's full pod set, not just one pod.

## Proposed config

Per-mirror block, alongside `http-access`:

```yaml
mirrors:
  - name: events
    source: { bootstrap-servers: kafka:9092 }
    topic: events-stream
    partition: 0
    destinations:
      - type: s3
        region: us-east-1
        bucket: my-bucket
    format: parquet
    compression: zstd-1
    compaction: log
    http-access:
      api: cache-v1
    notify:
      api: kkv-v1                              # only variant initially
      targets:
        - url: http://events-cache-target:8080
          fan-out: dns-a                        # resolve to all A records, POST to each
      trigger:
        on: source-consume                      # or destination-flush; see "Trigger" below
        debounce:                               # only meaningful for source-consume
          max-records: 100
          max-time-ms: 250
      timeout-ms: 5000                          # per-request HTTP timeout; independent of retry/outcome
      retry:                                    # shared by every outcome with `retry: true` below
        max-attempts: 5
        backoff-ms: 100                         # exponential, capped
      outcomes:                                 # six independent cases, same shape, different defaults
        timeout:       { retry: true,  final: fail   }
        connrefused:  { retry: true,  final: fail   }
        2xx:           { retry: false, final: accept }
        3xx:           { retry: false, final: fail   }
        4xx:           { retry: false, final: fail   }
        5xx:           { retry: true,  final: fail   }
    flush:
      max-time-ms: 60000
      max-bytes: 67108864
      max-offsets: 10000
```

Field-level notes:

- **`notify.api: kkv-v1`** is explicit so future variants
  (e.g. `notify.api: nats-v1`, or a kkv-v2 with auth) can be added
  without re-shaping the block. Same pattern as
  `http-access.api`.
- **`notify.targets[].url`** is a full URL. The path component
  defaults to `/kafka-keyvalue/v1/updates` for `api: kkv-v1` if
  unset; explicit override is allowed for non-kkv clients.
- **`notify.targets[].fan-out`** decides how the URL's host is
  resolved:
  - `none` (default): standard DNS, single connection. Adequate for
    a single-replica target.
  - `dns-a`: resolve the host to all A/AAAA records and POST to
    every address that comes back. Headless Kubernetes Services
    naturally return one A record per pod, so this gives the same
    fan-out kkv used to do via the Endpoints API; without mirror-v3
    needing K8s API access. Resolutions are cached up to the DNS
    record TTL.
- **`notify.trigger`** decides what internal event causes a POST.
  See the dedicated section below; default is `source-consume` with
  small debounce, matching kkv's "as records arrive" behaviour.
- **`notify.timeout-ms`** is the per-request HTTP timeout; strictly
  about how long to wait for *this* request before declaring it a
  `timeout` outcome. It does not influence retry decisions or
  exhaustion behaviour; those live in `notify.outcomes` and
  `notify.retry`.
- **`notify.retry`** is one shared backoff/exhaust policy used by
  any outcome marked `retry: true`. There is intentionally no
  per-outcome backoff override; heterogeneous retry shapes per
  status class are scope creep for the MVP and can be added later
  if the four-outcome surface proves insufficient.
- **`notify.outcomes`** decides what each of six distinct request
  outcomes means for the mirror. See "Outcomes and retry policy"
  below; defaults match what kkv operators tend to expect.

The block is **forbidden** unless the mirror also has
`http-access: { api: cache-v1 }` (validator rejects otherwise). The
notify body tells consumers "go re-read"; that's only meaningful if
there's somewhere to re-read from.

## Wire contract (`api: kkv-v1`)

Matches the legacy kkv exactly so the upstream Node client works
unmodified.

**Request.**

- Method: `POST`
- Path: `/kafka-keyvalue/v1/updates` (default; override via
  `notify.targets[].path`)
- Content-Type: `application/json`
- Headers:
  - `x-kkv-topic: <source-topic>`
  - `x-kkv-offsets: <json-encoded {partition: offset}>`
- Body:
  ```json
  {
    "topic": "<source-topic>",
    "offsets": { "<partition>": <highest-offset-in-batch> },
    "updates": { "<key>": null }
  }
  ```
  - `topic` matches the header for double-check robustness.
  - `offsets` carries the highest source offset across the batch
    per partition. Single-partition mirrors send `{"0": <max>}`.
  - `updates` is keyed by Kafka record key. Values are `null` -
    consumers re-read via `GET /cache/v1/raw/<key>`. (The legacy
    kkv allowed a payload hint but the upstream client immediately
    re-fetches via `requireOffset: highestOffset` anyway, so the
    hint was never load-bearing.)

**Response.**

- 2xx → success, drop the batch.
- Anything else → retry per `notify.retry`.
- After retry exhaustion → mirror task errors out
  (`MirrorError::NotifyTargetExhausted`); process exits; orchestrator
  restarts the pod; the dropped batch is re-read at startup because
  the underlying source offsets weren't committed yet.

Batches are sent in source-offset order per target. The mirror does
not wait for an ACK on batch *N* before issuing batch *N+1*; missed
intermediate batches are caught up at the consumer level via the
existing `x-kkv-last-seen-offsets` semantics on cache-v1 reads.

## Trigger: source-consume vs. destination-flush

Two natural points to emit a notify exist. Operators should be able
to pick between them per mirror.

### `trigger.on: source-consume` (default)

A POST is queued as soon as the source consumer hands a record to
the mirror loop. The record has already been applied to the
cache-v1 in-memory view (`write()` does that per-record), so a
consumer that re-fetches `/cache/v1/raw/<key>` immediately on
notify sees the just-updated value. Destination flush cadence is
irrelevant; flushes can lag minutes or hours and cache freshness
on the consumer side is unaffected.

This is what kkv did, and what every existing `@yolean/kafka-keyvalue`
consumer expects: sub-second invalidation, decoupled from any
blob-storage flush.

Because per-record HTTP would be wasteful at high record rates,
`source-consume` requires a `debounce` block:

```yaml
trigger:
  on: source-consume
  debounce:
    max-records: 100     # batch up to N record-changes per POST
    max-time-ms: 250      # flush partial batch at most this old
```

A batch is sent when `max-records` is reached OR `max-time-ms`
has elapsed since the first record entered the batch, whichever
comes first. Setting `max-records: 1` yields per-record POSTs;
the higher the value, the better at coalescing bursts (e.g. a
restart catchup) at the cost of a small invalidation delay.

`debounce` interacts with `notify.timeout-ms` and `retry`: an
in-flight batch blocks the next batch from being sent on the same
target, which provides natural backpressure if the receiver is
slow. (The source consume loop itself doesn't pause; new records
land in the next batch's buffer.)

### `trigger.on: destination-flush`

A POST is queued only after the destination(s) durably commit a
batch; i.e. the same moment the `flushed batch` log line fires
in mirror-fs / mirror-s3. The notify body's offset range matches
the flushed snapshot's `from`–`to` exactly. No `debounce` block
applies (the destination's flush triggers ARE the debounce).

Use case: downstream consumers that care about durability rather
than freshness; e.g. an archival sync job that wants "tell me
when a parquet file lands so I can copy it elsewhere". Not the
right fit for cache invalidation, since destination flush cadence
is typically minutes.

For mirror-v3's TeeSink (multiple destinations per mirror), the
notify fires when ALL destinations have committed past the batch's
high-water offset. Single-destination mirrors fire on every flush.
A mirror with no blob destinations (kafka-only) cannot use
`destination-flush`; validator rejects.

### Compatibility / defaults

- Default `trigger.on` is `source-consume` so the kkv replacement
  path works out of the box.
- Default `debounce` is `{ max-records: 100, max-time-ms: 250 }`.
  Operators tune these for their own latency/cost trade-off.
- `trigger` and `notify.on-response` are independent of each other:
  the response policy applies to whichever batch is emitted.

## Outcomes and retry policy

Six distinct request outcomes are recognised. Three of them are
non-HTTP-response cases (no status code came back); the other three
are status-class buckets.

| outcome        | what it means                                                                       |
|----------------|-------------------------------------------------------------------------------------|
| `timeout`      | Request didn't complete within `notify.timeout-ms`.                                  |
| `connrefused` | TCP refused fast (target's port is closed or the host is missing).                  |
| `2xx`          | HTTP 200–299.                                                                        |
| `3xx`          | HTTP 300–399 (redirects; unusual for a webhook).                                    |
| `4xx`          | HTTP 400–499 (target says "your request is wrong").                                  |
| `5xx`          | HTTP 500–599 (target says "I'm broken").                                             |

Each outcome carries the same two-field shape:

```yaml
outcomes:
  <name>:
    retry: <bool>             # if true, retry per notify.retry; if false, jump straight to `final`
    final: accept | skip | fail
```

`final` is the action taken either immediately (if `retry: false`)
or after retry exhaustion (if `retry: true`). Possible values:

| action   | meaning                                                                |
|----------|------------------------------------------------------------------------|
| `accept` | Count the batch as successfully delivered, advance.                    |
| `skip`   | Log a WARN, drop the batch silently, advance. No further action.       |
| `fail`   | Mirror task errors out; orchestrator restarts; mirror replays the batch from durable state. |

The matrix is intentionally orthogonal; every combination of
`retry × final` is valid and meaningful:

| `retry` | `final`  | behaviour                                                          | typical use                                |
|---------|----------|--------------------------------------------------------------------|--------------------------------------------|
| false   | accept   | one attempt, treat as success regardless                           | `2xx` (always)                              |
| false   | skip     | one attempt, log + drop                                            | `4xx: skip` when targets briefly return 410 during rolling restart |
| false   | fail     | one attempt, immediate fatal                                       | `3xx`/`4xx` defaults                        |
| true    | accept   | retry per policy, treat as success on exhaustion                    | best-effort heartbeats (rare)               |
| true    | skip     | retry per policy, log + drop on exhaustion                          | non-critical notify channel                 |
| true    | fail     | retry per policy, fatal on exhaustion                               | `5xx` / `timeout` / `connrefused` defaults |

### Defaults

```yaml
outcomes:
  timeout:       { retry: true,  final: fail   }
  connrefused:  { retry: true,  final: fail   }
  2xx:           { retry: false, final: accept }
  3xx:           { retry: false, final: fail   }
  4xx:           { retry: false, final: fail   }
  5xx:           { retry: true,  final: fail   }
```

Rationale:

- **`timeout` and `connrefused`** are network-level; the target
  may be transiently slow / restarting / being rolled. Retry per
  policy; only exit when the operator's retry budget is exhausted.
- **`2xx`** is the only success case. `accept`, no retry.
- **`3xx`** is almost always a misconfiguration: webhook receivers
  shouldn't be redirecting. Fail loud so the operator notices.
- **`4xx`** indicates the mirror is sending something the target
  doesn't accept; retrying the same payload won't change that.
  Fail loud.
- **`5xx`** is transient server-side trouble; retry per policy, then
  fail if it doesn't clear.

### Operator-facing knobs the matrix unlocks

- **"Targets routinely 404 during rolling restart, don't crash on
  that"** → `4xx: { retry: false, final: skip }`. Downstream cache
  staleness is recovered next time the consumer reads cache-v1 with
  the `x-kkv-last-seen-offsets` header.
- **"Receiver is flaky, never fail the mirror on it"** →
  `5xx: { retry: true, final: skip }`. Pure best-effort notify.
- **"Fail fast on slow receivers instead of waiting through retry"**
  → `timeout: { retry: false, final: fail }`.
- **"Stop tolerating 5xx after this many attempts"** → tune
  `notify.retry.max-attempts` (shared across all retryable
  outcomes).

### Notes

- `timeout-ms`, `retry.max-attempts`, and `retry.backoff-ms` are
  three independent dials. The first bounds a single attempt's
  wall-clock; the other two bound the total attempt count and
  spacing for any outcome with `retry: true`.
- If the operator needs per-status-code overrides in future (e.g.
  `429 → always retry regardless of class default`), a `status` map
  layered ahead of the class buckets is the natural extension. Out
  of scope for MVP; the six-outcome surface already covers every
  current kkv use case.
- `skip` advances the source-offset position (the batch is
  considered delivered for ordering purposes) but logs at WARN so
  operators can grep for dropped batches.

## Notify-only mirrors (zero destinations)

A mirror with `destinations: []` and `notify: { … }` set MUST be
valid. The use case is "consume from source, emit webhooks, don't
keep anything durable"; a pure invalidation feed, or a fan-out of
record-change events into a non-mirror-v3 downstream system.

### Why webhook is not a destination

A destination, in mirror-v3's contract, is a thing that **owns its
own next-expected source offset** and surfaces it via
`next_expected_offset()` on startup. The whole "restart correctness
derives from the destination, never from committed group offsets"
invariant rests on that. Kafka/FS/S3 sinks all satisfy it: they
inspect what's already durable on their side and report a number.

A webhook receiver fundamentally cannot. There's no generic
contract that lets mirror-v3 ask a webhook receiver "what's the
highest source offset you've successfully processed?". Even a
sophisticated receiver that tracked it internally would have no
shared protocol for reporting it back to a generic webhook caller.
The legacy kkv didn't even try; it relied on Kafka consumer-group
offsets, which mirror-v3 explicitly does not use.

So `notify` is a *side-effect* of consuming records, not a place
records are stored. Classifying it as a destination would force
either a fake `next_expected_offset()` (always 0, or always
"current") or a separate "destinations don't have to report
offsets" exception; both of which leak into every sink
implementation. Keeping it on the mirror as a peer to `destinations`
keeps the destination trait clean and lets webhook-only mirrors
exist without distorting the model.

### Restart correctness when there are no destinations

With no durable state, there is no `next_expected_offset` to seek
to. On every startup the source seeks to the broker's *low
watermark*, i.e. the earliest record the source still has. Under
`cleanup.policy=compact` that's effectively offset 0 (or whatever
survived compaction); under `cleanup.policy=delete` it's whatever
retention has kept. The mirror then re-fires webhooks for every
record from that point forward.

For kkv-style cache invalidation this is the *correct* behaviour:
when the mirror restarts, downstream consumers' caches that depend
on it are themselves either restarting or holding stale data, and a
full replay re-syncs them. The legacy kkv had the same shape; it
held nothing durable and replayed on every restart.

Operators should be aware that "notify-only on a busy topic"
produces a burst of webhook traffic per mirror restart. Tuning
`notify.trigger.debounce` upward (larger `max-records`, longer
`max-time-ms`) coalesces the burst. Adding a cheap blob destination
(`type: filesystem` to a small PVC, or `type: s3` to a low-cost
bucket) gives durable resume-from-offset and silences the burst at
the cost of one more sink.

### Validation rules for notify-only

When `destinations` is empty:

- `notify` MUST be set with at least one target.
- `notify.trigger.on` MUST be `source-consume` (no destinations to
  ack, so `destination-flush` is meaningless and the validator
  rejects it).
- `format`, `compression`, `keys`, `values`, `compaction`, `flush`
  are forbidden; they all parameterise destinations that don't
  exist. (`keys`/`values` may stay as a future opt-in for key/value
  validation on the source; out of scope for MVP.)
- `http-access` is forbidden. The cache-v1 contract today requires
  bootstrapping from durable destination state; a notify-only
  mirror has none. (A future "bootstrap cache by replaying from
  broker" mode is conceivable but adds complexity; defer.)

When `destinations` is non-empty AND `notify` is set: no change
from the rules already specified; both `trigger.on` values are
allowed, and `http-access` works as before.

### Side note: combining notify with cache-v1 + destinations

The kkv replacement use case needs all three on the same mirror:
a durable blob destination (parquet to S3 or filesystem), cache-v1
for `GET /cache/v1/raw/<key>`, and notify so consumers know when
to re-read. This proposal keeps that combination as the "full"
shape and notify-only as the minimal one; the schema validator
doesn't need to choose between them.

## Discovery: why DNS-A is enough

Legacy kkv hit the Kubernetes Endpoints API directly (with a
matching Role / RoleBinding for `endpoints` `get,watch,list`) to
enumerate target pods. That ties the mirror to the K8s control
plane and requires per-namespace RBAC.

For the typical kkv deployment topology (every kkv-target is a
*headless* Service), the DNS A record set already contains exactly
the pod IPs kkv was enumerating. A standard resolver returns the
full set on each query; an HTTP fan-out across all returned
addresses is equivalent to kkv's Endpoints walk without any K8s
coupling.

mirror-v3's `fan-out: dns-a` should:

1. Resolve the URL's host on first send. Cache the A/AAAA record set
   up to the DNS TTL (default 30 s if no TTL is published).
2. Open one HTTP/1.1 keep-alive connection per address (kept inside
   a pool, capped at the resolved set size).
3. POST the batch to all addresses concurrently. Aggregate the
   results; if any address returns non-2xx after retry, the whole
   batch is failed.
4. Re-resolve when the cache TTL expires OR when an address fails
   repeatedly (forces an immediate re-resolve to pick up scale-up /
   scale-down).

This handles the rolling-update case: during a Deployment rollout,
the headless Service's A-record set has both old and new pod IPs
for a few seconds; mirror-v3 POSTs to both, the old terminating
pods drain on whatever they got, and the next re-resolve drops
them. Same behaviour kkv had via Endpoints API.

For non-K8s use (a standalone service behind a single hostname),
`fan-out: none` skips all of that and uses a single keep-alive
connection. The choice is per-target so a mirror can mix.

## Interaction with cache-v1

The notify path pushes only after the corresponding records have
already entered the in-memory cache-v1 view. This guarantees that
when a consumer re-fetches `/cache/v1/raw/<key>` in response to a
notify, the value reflects at least the just-notified record. The
legacy kkv had the same ordering by construction (cache write
before HTTP push, both in the same consume thread).

Under the default `trigger.on: source-consume`, the per-record
path is:

1. Apply to the cache-v1 view (`mirror-fs` / `mirror-s3` already
   does this in `write()`).
2. Push to destinations as today.
3. Append to the notify batch buffer (NEW).
4. If `debounce` trips (record count or wall-clock), drain the
   buffer asynchronously.

The notify buffer is independent of the destination flush buffer.
It does NOT depend on `flush.max-time-ms` etc.; consumers want
fresh invalidation; the destinations can buffer for hours if they
want. Cache freshness on the consumer side is bounded by
`notify.trigger.debounce.max-time-ms` (default 250 ms).

Under `trigger.on: destination-flush`, step 4 is replaced by "on
every successful sink flush, post the just-flushed offset range".
Cache freshness is then bounded by `flush.max-time-ms` (typically
seconds-to-minutes), so this mode is wrong for kkv-style cache
invalidation but right for "downstream wants a hint when a parquet
lands".

## Interaction with `compaction: log`

No special handling needed. The notify body's `updates` map only
references keys; under compaction:log the cache-v1 view already
holds the latest-per-key value, so a re-fetch returns that value.
If the same key changes twice within one batch, the batch carries
the key once (set semantics on keys) but the body's `offsets`
field reflects the highest offset, so the consumer's
`requireOffset` constraint pins the read to the post-batch state.

## Failure modes and supervision

| Failure                                | mirror-v3 behaviour                                                         |
|----------------------------------------|-----------------------------------------------------------------------------|
| Target host fails DNS resolution       | per `outcomes.connrefused` (default `{retry: true, final: fail}`)          |
| Target TCP refused                     | per `outcomes.connrefused`                                                  |
| Target slow (no response within timeout-ms) | per `outcomes.timeout` (default `{retry: true, final: fail}`)           |
| Target returns 2xx                     | per `outcomes.2xx` (default `{retry: false, final: accept}`)                |
| Target returns 3xx                     | per `outcomes.3xx` (default `{retry: false, final: fail}`)                  |
| Target returns 4xx                     | per `outcomes.4xx` (default `{retry: false, final: fail}`)                  |
| Target returns 5xx                     | per `outcomes.5xx` (default `{retry: true, final: fail}`)                   |
| `retry: true` exhausts `max-attempts`  | apply that outcome's `final` action                                          |
| One address in a dns-a fan-out fails   | applies per-address; whole batch fails as soon as one address's outcome resolves to `fail` |
| Buffer growth from slow targets        | backpressure: pause the source consume loop until current batch drains; surface as a metric |

Restart correctness is unaffected: notify is best-effort *and*
ordered. If the process crashes mid-batch, the records weren't
committed to the source offset position either, so on restart the
mirror re-consumes from the destination's `next_expected_offset`
and re-issues the lost batch.

## Metrics

Adds, alongside the existing `mirror_v3_destination_*` counters:

| Metric                                          | Type    | Labels                                  | Meaning                                       |
|-------------------------------------------------|---------|------------------------------------------|-----------------------------------------------|
| `mirror_v3_notify_records_total`                | counter | `topic`, `partition`                     | Records appended to a notify batch            |
| `mirror_v3_notify_batches_total`                | counter | `topic`, `partition`, `result=ok\|fail`  | Batches sent                                  |
| `mirror_v3_notify_post_duration_seconds`        | histogram | `topic`, `partition`, `target_host`    | Per-target HTTP latency                       |
| `mirror_v3_notify_inflight_retry`               | gauge   | `topic`, `partition`, `target_host`      | Current retry attempt (1-based, 0 when idle)  |
| `mirror_v3_notify_buffer_records`               | gauge   | `topic`, `partition`                     | Current buffer depth                          |

`target_host` is the resolved host the request went to; for
`fan-out: dns-a` this is the pod IP, so dashboards see per-pod
latency.

## Logging

- One INFO line at startup per notify-enabled mirror:
  `notify start mirror=<name> api=kkv-v1 targets=<host>[,host…] fan-out=<mode>`.
- One INFO line per successful batch:
  `notify sent mirror=<name> batch_records=<n> highest_offset=<o> targets=<n> elapsed_ms=<m>`.
- One WARN per failed attempt with retry remaining:
  `notify retry mirror=<name> target=<addr> attempt=<i>/<max> reason=<err>`.
- One ERROR on retry exhaustion (mirror-task-fatal):
  `notify exhausted mirror=<name> target=<addr> attempts=<n>`.

Per-record DEBUG only; counters cover the operational signal.

## Validation

- `notify` requires `http-access.api: cache-v1` on the same mirror.
- `notify.targets` non-empty.
- `notify.trigger.debounce.max-records >= 1`, `max-time-ms >= 1`
  (when `trigger.on: source-consume`).
- `notify.timeout-ms >= 1`.
- `notify.retry.max-attempts >= 1`, `notify.retry.backoff-ms >= 1`.
- `notify.outcomes` may omit keys; omitted keys fall back to the
  default table above. Listing all six is allowed and
  recommended for production configs so the policy is explicit.
- `final: accept` on `timeout`/`connrefused`/`5xx` with
  `retry: false` is a valid but unusual combination; the validator
  warns (operator probably meant `retry: true, final: accept`).
- **Destinations relaxation** (new in this proposal):
  `destinations` MAY be empty *if and only if* `notify` is set with
  at least one target. See "Notify-only mirrors" above for the full
  matrix of which other fields are then forbidden
  (`format`/`compression`/`compaction`/`flush`/`http-access`) and
  which trigger modes are required (`trigger.on: source-consume`).
- `notify.targets[].url` parses as a valid URL with http:// or https://.
- Each target's resolved host must produce ≥1 address at startup,
  otherwise validation fails (catches typos / missing Services
  before the mirror runs).

## Out-of-scope (future)

- **Authentication.** Bearer tokens / mTLS / HMAC-signed bodies.
- **Selective subscription.** Subscribe to a key prefix or a header.
- **Push-only mode for kkv-style consumers.** Notify *with* zero
  destinations (covered in "Notify-only mirrors") is in scope.
  Notify without cache-v1 *but with destinations*; i.e. the
  consumer is expected to re-read from the durable destination
  rather than from cache-v1; is deferred. Requires a slightly
  different body shape (record-data inline rather than
  null-valued `updates`) and is unrelated to the kkv replacement
  use case driving this proposal.
- **Multi-API targets.** Same mirror notifying both kkv-v1 and a
  future variant.
- **Per-target retry budgets.** Independent failure handling so one
  bad target doesn't crash the mirror.

Each is a small additive change on top of this minimal core.

## Open questions

1. Should `notify` live on the mirror or as a special entry in
   `destinations[]`? Putting it on the mirror keeps the
   destinations-are-durable-storage invariant clean (notify is a
   side-effect, not a sink). Recommendation: on the mirror.
2. Should the `updates` body be allowed to be empty (`{}`) when a
   batch hits `max-records` and the buffered key-set would be large?
   Consumers using `streamValues()` re-fetch everything anyway.
   Saves bytes; matches the kkv behaviour on large bursts. Probably
   worth allowing.
3. Should a failed batch immediately re-resolve DNS, or only after
   the TTL elapses? Re-resolving immediately recovers from
   scale-down faster; staying with the cached set is faster on
   transient single-pod errors. Recommendation: re-resolve on any
   failure (cheap; same DNS query that's already cached after).
4. Should `notify` honour `MIRROR_V3_NOTIFY_DISABLED=true` for ops
   drills (rolling the mirror without invalidating downstream
   caches)? Useful for some debugging workflows; harmless if
   omitted.

---

References:

- `@yolean/kafka-keyvalue` Node client (the receiving side):
  exports `ON_UPDATE_DEFAULT_PATH = "/kafka-keyvalue/v1/updates"`
  and `getOnUpdateRoute()` from `index.js`; the request body
  `{ topic, offsets, updates }` is parsed in `KafkaKeyValue.js`
  and each `key` in `updates` is re-fetched via cache-v1 with the
  `requireOffset: highestOffset` constraint.
- Legacy kkv (Yolean/kafka-keyvalue Quarkus): env vars
  `TARGET_SERVICE_NAME`, `TARGET_SERVICE_PORT`,
  `TARGET_SERVICE_NAMESPACE` resolve a headless Service via the
  Kubernetes Endpoints API; one POST per pod IP per consumed batch.
