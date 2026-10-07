# mirror-v3

Exactly-once Kafka topic+partition mirroring to **Kafka**, **Filesystem**, or **S3**, in one deployment.

> **Status:** feature-complete for the `checkit/mirror-v3` cutover: Kafka source + Kafka/Filesystem/S3 sinks, `/cache/v1` HTTP surface, kkv-v1 notify webhooks, committed-offset delivery semantics and sticky readiness (as kafka-keyvalue). See [AGENTS.md](AGENTS.md) for the development history.

## What this gives you

- One process can run **N parallel mirrors**, each pinned to exactly one source `(topic, partition)`.
- A **shared destination** block configures the sink; each mirror may override the destination name.
- Three destination types:
  - `kafka` — produce to another Kafka-compatible broker (parity with the legacy Java worker).
  - `filesystem` — write atomic, offset-named files to a local directory.
  - `s3` — same model against any S3-compatible endpoint (AWS S3, VersityGW, etc.).
- For `filesystem` / `s3`, **file/blob names encode the source `from`–`to` offset range** and are the source of truth for destination state on restart.
- Configurable **flush triggers** per blob destination: max time, max bytes, max offsets — whichever trips first.

The single non-negotiable: **restart correctness derives from the destination**, not from a local checkpoint. On startup, the mirror inspects the destination, computes the next expected source offset, and seeks the source consumer there.

## Running

```sh
mirror-v3 validate --config config.yaml             # parse-only
mirror-v3 run --config config.yaml                  # start the configured mirrors
mirror-v3 status --config config.yaml               # one-shot health check, table format
mirror-v3 status --config config.yaml --format json # same, machine-readable
mirror-v3 restore --config config.yaml --mirror <name> --offsets preserve|renumber ...  # see Restore
```

`status` queries the source Kafka high watermark and the destination's `next-expected-offset` for every mirror in the config and prints the lag. Exits non-zero if any mirror failed to query (unreachable broker, corrupt destination chain, etc.). Useful as a `kubectl exec` health probe before/during/after an appliance backup, without having to ssh to the node.

All logs go to **stderr** (heartbeat, flush lines, errors). `stdout` is reserved for command-driven output (`status --format json`, the `validate` success line). Standard `1>` / `2>` redirects work as expected.

### `/metrics` (Prometheus)

`mirror-v3 run` starts an HTTP server on `0.0.0.0:9090` that serves Prometheus-format metrics at `/metrics`. Override the port with `MIRROR_V3_METRICS_PORT=<port>`. A bind failure (port in use) is a startup error. Every `MIRROR_V3_*` variable is read once at startup; unset means its default, and a value that does not parse is a startup error naming it.

Every metric carries `topic="<source-topic>"` and `partition="<n>"` labels so they join cleanly with broker-side exporters (`kafka_exporter`, `kafka-lag-exporter`). The metrics every mirror has (`mirror_v3_destination_*`, `mirror_v3_source_*`, `mirror_v3_mirror_restarts_total`, `mirror_v3_cache_*`) also carry `mirror="<name>"`: one process may run two mirrors of one partition, a cache and a backup of the same topic, and their series would otherwise overwrite each other. Joins on `(topic, partition)` keep working (`group_right` below). The `mirror_v3_notify_*` metrics carry `topic` and `partition` only.

| Metric | Type | Description |
|---|---|---|
| `mirror_v3_destination_offset_verified` | gauge | Next source offset the destination would accept; everything below this is durable. Set on startup and advanced by the sink the moment it confirms a commit — `acks=all` produce-delivery for Kafka, `rename(2)` success for Filesystem, `PutObject` success for S3. **This is the load-bearing metric for "how much is safe right now".** |
| `mirror_v3_mirror_restarts_total` | counter | Transient failures of a mirror without cache or notify (its source or a destination could not be reached: S3 down, a broker away), each followed by opening it again in the process after a backoff. A climbing value is the "destination is having problems" signal. |
| `mirror_v3_destination_records_total` | counter | Records that crossed the gate, since process start. |
| `mirror_v3_destination_last_flush_timestamp_seconds` | gauge | Unix timestamp (seconds) of the most recent flush. PromQL `time() - mirror_v3_destination_last_flush_timestamp_seconds` gives "seconds since last flush". Filesystem / S3 only. |
| `mirror_v3_destination_bytes_total` | counter | Cumulative bytes written to the destination by Filesystem / S3 sinks. |
| `mirror_v3_destination_flushes_total` | counter | Number of flushes by Filesystem / S3 sinks. |

Useful PromQL:

```
# A destination keeps failing (its mirror is being reopened in the process)
increase(mirror_v3_mirror_restarts_total[10m]) > 0

# Seconds since last flush — alert if > flush.max-time-ms / 1000 × 2
time() - mirror_v3_destination_last_flush_timestamp_seconds

# End-to-end lag (join with kafka_exporter's source watermark on topic+partition):
kafka_topic_partition_current_offset
  - on(topic, partition) group_right mirror_v3_destination_offset_verified
```

A minimal PodMonitor for the checkit chart points at port 9090; the standard process metrics (`process_cpu_*`, `process_open_fds`, …) are also exposed by the exporter.

`run` spawns one task per mirror, each pinned to one `(topic, partition)`. SIGINT/SIGTERM trigger a graceful shutdown that waits for every mirror to flush its buffered records before exiting zero. A failure of a mirror with a cache (`http-access`) or `notify` ends the whole process with a non-zero exit, and the orchestrator (k8s) restarts it; a mirror with neither is opened again inside the process after a backoff (1 s doubling to 60 s, `mirror_v3_mirror_restarts_total`) when its source or a destination could not be reached, since it holds no state but its destinations, so a destination outage does not take the process's caches down. Any other failure of such a mirror (an offset mismatch, an object it did not write, a corrupt chain, a lost source position, a record or a configuration that cannot work) repeats on every attempt and ends the process too, so it shows as a crash loop.

### `/cache/v1` (drop-in for `Yolean/kafka-keyvalue`)

Per-mirror opt-in via `http-access: { cache-v1: {} }`. When at least one mirror has it set, `mirror-v3 run` starts a second HTTP server on `0.0.0.0:8080` (override with `MIRROR_V3_CACHE_PORT`) that exposes the KKV-shaped surface under each opt-in mirror's name:

```
GET /cache/v1/{mirror}/raw/{key}                  → value bytes (application/octet-stream), 404 if absent
GET /cache/v1/{mirror}/offset/{topic}/{partition} → decimal text
GET /cache/v1/{mirror}/keys                       → newline-separated keys
GET /cache/v1/{mirror}/values                     → newline-separated raw values
```

Each mirror owns its own `key → latest-value` view; a key only shows up under the mirror that consumed it. Reads carry `x-kkv-last-seen-offsets: <JSON>` and return **503** until that mirror has caught up to its source's high watermark at startup, and never again afterwards (sticky, as kafka-keyvalue; see [Readiness](#readiness)), so dependents don't see a partially rebuilt state. The view updates per-record from the consume loop, decoupled from disk flush cadence (set `flush.max-time-ms` high to save bucket ops without sacrificing freshness). Updates are monotonic; if a future feature ever rewinds source consumption, the cache stays at the highest offset seen.

To keep existing kkv consumers working unmodified during a migration, **one** mirror per process may additionally set `cache-v1-main: {}`. That mounts the unprefixed `/cache/v1/...` paths onto that mirror's view (alias-only — same handlers, no separate data path). The validator rejects more than one `cache-v1-main` in the config. Mirror names that collide with the literal path segments `raw | offset | keys | values` are rejected.

Also exposed on the same port:

- `POST /_admin/v1/shutdown` and `POST /_admin/v1/shutdown/{exitcode}` — request graceful exit.
- `GET /openapi.json` and `GET /openapi.yaml` — auto-generated OpenAPI 3.1 spec; the committed copy is at [`schemas/mirror-v3.cache.openapi.json`](./schemas/mirror-v3.cache.openapi.json) (gated by `cargo run -p xtask -- check-openapi`).
- `GET /docs` — Scalar UI rendering the spec.

Bootstrap: a cache is built by reading the source topic from its low watermark, as kafka-keyvalue does, never from a destination. A cache does not need S3 to start and is not stalled by a slow destination when it is the only thing its mirror does, and a blob destination's startup reads object names only. On a mirror that has destinations too, the source is read from the low watermark and the destinations skip what they already hold. Put a cache in its own mirror when its availability must not depend on a destination: a cache mirror's failure ends the process, a destination's failure included. Whether a cache should share a mirror with destinations at all is open; see [Open design questions](#open-design-questions).

## Restore

`mirror-v3 restore` reads one mirror's blob backup (S3 or filesystem) back into a Kafka topic: disaster recovery from the bucket alone, after the cluster that wrote it is gone.

```sh
# Is the backup complete? Reads and decrypts every object, prints a summary, produces nothing.
mirror-v3 restore --config mirror-v3.yaml --mirror operations-backup --offsets preserve --verify-only

# Restore it into an empty topic.
mirror-v3 restore --config mirror-v3.yaml --mirror operations-backup --offsets preserve \
  --bootstrap-servers kafka:9092 --topic <topic>
```

| Flag | |
|---|---|
| `--config`, `--mirror` | The config and mirror that wrote the backup. The backup is that mirror's destination: its directory (`<prefix>/<name>/<partition>/`, or `<root>/<name>/<partition>/`), `format`, `keys`/`values`, encryption `keys-dir` and `credentials.read` are used as they are. The whole config is loaded and validated as for `run`, so its environment variables must be set, and the mirror's `topic` must be the topic the backup was taken of (every record is checked against it). |
| `--destination` | The destination to read, by `name`; required when the mirror has more than one. A Kafka destination or a `compaction: log` mirror (whose objects are snapshots, not the topic's records) is refused. |
| `--offsets` | `preserve` or `renumber`, required: see below. |
| `--chain-start` | The offset the backup starts at, default 0. A chain that starts anywhere else is an error unless this says so, so objects removed from its head are never missed silently. |
| `--verify-only` | Stop after the verify pass. |
| `--bootstrap-servers`, `--topic` | The target: partition `partition` (the mirror's) of an existing, empty topic (with `--follow`, empty or followed before). |
| `--follow` | Keep the target restored as the backup grows, until SIGTERM or SIGINT: see [Continuous restore](#continuous-restore). |
| `--poll-interval-ms` | With `--follow`: how often to list the backup once every object is restored, default 5000. |

Every run first lists the backup and validates the chain of object names (sorted, no gap, no overlap, starting at `--chain-start`, every key id it names present in `keys-dir`), then reads every object and checks it against its name (records in increasing offset order inside `<from>-<to>`, the last at `to`) and the mirror's topic and partition. It prints the summary to stdout:

```
backup: s3://<bucket>/sites/<site>/operations/0 (mirror operations-backup, destination operations)
source: <topic>/0
objects: 3
offsets: 0-24999
records: 25000
holes: 0
```

`holes` are positions of the chain without a record (see [Offset holes](#operational-invariants)): records compaction removed before they were backed up, and transaction markers. Every transaction marker takes an offset, so a topic written in transactions has holes on either broker (Apache Kafka writes a marker after each transaction, Redpanda also one before it, so on Redpanda from offset 0, even after one transaction) and restores only with `renumber`. Any failed check exits non-zero with the reason, and so does `--verify-only` when the backup cannot be restored with the `--offsets` given; `--verify-only` is the scheduled "is the backup complete" check.

Then the records are produced in order, each with its key, value (tombstones included), headers and timestamp, many in flight, every key and value checked against the mirror's `keys`/`values` first as the mirror's Kafka destination does: the producer is idempotent (it retries without duplicating or reordering a record), and the broker must report every record stored at exactly the offset it was sent for, so another writer on the partition, or a record lost or written twice, ends the restore. Before each object is produced it is read again and must be byte for byte the object the verify pass read. The target must be empty (high watermark 0) before the first record, and its high watermark must equal the records produced after the last; otherwise restore exits non-zero. Records sent before a failure may still be stored, so after one the topic is deleted and created again before restoring again. This is not the mirror's per-record high-watermark gate, which a restore does not need: it does not resume, so the gate's guarantee (a restart produces nothing twice) has nothing to protect, and producing fast keeps the window for a failure short. Against a local Redpanda, a million records of about 100 bytes restore from S3 in about 5 s, the verify pass included; with the per-record gate the same would take about two hours.

The two offset modes:

- **`preserve`**: every record at its original offset. For topics whose readers key on offsets, such as an operations topic that a downstream index keys rows on and resumes from `MAX(offset)`. A Kafka topic starts at 0 and cannot be written with holes, so a backup that starts after 0 or has a hole is refused before anything is produced, naming the first hole. After a `preserve` restore the topic continues the backup's chain exactly, so the backup mirror resumes on the same prefix where it stopped.
- **`renumber`**: the records at 0, 1, 2, ... in their original order, for compacted topics (user-states), where holes are expected and keys, values and order are what matter. What changes: every offset from the first hole on (the topic is shorter than the chain by the number of holes), so committed consumer-group offsets and anything else that stores offsets of the old topic do not apply to the new one. Superseded values and tombstones are produced too; the broker compacts the restored topic by its own `cleanup.policy`. The backup's chain now ends past the restored topic's high watermark, so a backup mirror of the restored topic must write a new chain (another destination `name` or `prefix`): on the old one it stops with "the destination is ahead of the source".

In both modes a record's timestamp is produced as its CreateTime, also for a record whose source topic stamped LogAppendTime; a target topic with `message.timestamp.type=LogAppendTime` restamps every record at restore time. A record without a timestamp gets the producer's clock. Time-based retention counts from these timestamps: a target topic whose `retention.ms` is shorter than the age of the oldest restored records deletes them soon after the restore, as their segments roll and expire. Give the target a retention that covers the backup's age (`retention.ms=-1`, or longer than the oldest record) before restoring, and lower it later if it should apply from now on.

Restore does not create the target topic (its partitions, `cleanup.policy` and retention are the operator's decision) and does not resume: a restore that fails part way leaves a topic that the next run refuses as not empty, so delete and create it again and rerun.

### Continuous restore

`--follow` reads the backup as a mirror's source: `run_mirror` with the Kafka destination, its per-record gate (the high watermark read before every produce, no retries, the offset the broker reports checked) and its idle drift check, so nothing else may write the target. It starts at the target's high watermark, and after every object listed so far it lists the backup again every `--poll-interval-ms`, producing the objects the backup's mirror has added, so the target trails the source by about the backup mirror's flush interval: a standby, or a topic moved between clusters through the bucket. It runs until SIGTERM or SIGINT and can be stopped and started again at any time; it is not a replacement for the one-shot restore when the backup is all there is, since the gate makes it slow (about 150 records/s against a local broker), so restore the bulk with the one-shot restore and follow from there.

- **Resume**: a target that is not empty holds the backup's first records, in both offset modes (with `renumber`, the n-th record of the chain is at offset n), so a follower continues at its high watermark without any state of its own. Before it does, it reads the target's last record and requires it to be the backup's record at that position (key, value, headers, timestamp); a topic holding anything else is refused. A one-shot restore and a follower can therefore hand over: follow a topic that a one-shot restore filled, with the same `--offsets`.
- **Checks**: every object is read and checked as in the verify pass before its records are produced, and new objects must continue the chain. There is no verify pass of the whole backup up front, so with `preserve` a hole stops the follower when it is reached, with the records before it restored; `--verify-only` checks a backup before following it. A gap, a hole, a target past the backup's end or another topic's records end the follower with an error that trying again cannot fix; an unreachable store or broker ends it with an error a restart can.

What disaster recovery needs besides the bucket:

- An S3 identity that may list **and get** objects under the prefix, in the variables `credentials.read` names. A least-privilege `read` identity that only lists (the mirror needs no more in append mode) cannot restore. Restore opens the store read-only: the `write` identity's variables need not be set.
- Every Parquet key the objects name (`.k-<key id>.parquet`), in `keys-dir`. A key kept only on the lost machine makes its objects unreadable: keep the keys where the bucket's backups can be restored without that machine.

## Observability

The default INFO-level log stream is operator-oriented:

- One line per mirror at startup with the resolved destination type and source seek.
- A **heartbeat** line every 30 s with `expected_offset` and `progressed` (records since the last heartbeat). Confirms liveness even when the source is idle. Override the interval with `MIRROR_V3_HEARTBEAT_SECS=<seconds>`; set to `0` to disable.
- One line per **flush** for Filesystem and S3 sinks: `path`, `from`, `to`, `count`, `bytes`, `elapsed_ms` (how long this flush took), `interval_ms` (since the previous flush). Kafka sinks don't buffer so they have no flush line — the heartbeat carries the "still alive, here's the offset" signal.

`RUST_LOG=info` is the default; `RUST_LOG=mirror_core=debug,mirror_fs=debug` adds verbose internals.

## Configuration

`mirror-v3 validate --config config.yaml` parses your YAML and exits non-zero on any problem.

A minimal Kafka→Kafka config:

```yaml
# yaml-language-server: $schema=./schemas/mirror-v3.config.schema.json
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
    destinations:
      - type: kafka
        bootstrap-servers: redpanda:9092
```

Each mirror declares `destinations: [...]`. With more than one destination, one source consumer fans every record to all destinations through a tee (`mirror_core::TeeSink`) — the source broker is read once per record, and each destination keeps its own end-offset gate and flush cadence. See [`examples/dual-write-fs-and-s3.yaml`](examples/dual-write-fs-and-s3.yaml) for the dual-write pattern.

An S3 destination names two identities by the environment variables holding their keys, as a least-privilege bucket setup has them: `write` may only PutObject, `read` lists the prefix (and reads the latest snapshot in compaction mode). Nothing else in the environment changes the client (no `AWS_*` discovery), and a missing variable is a startup error:

```yaml
      - type: s3
        endpoint: http://versitygw.observability-s3:7070
        region: example-region
        bucket: mirror-userstate
        credentials:
          write: { access-key-id-env: S3_WRITE_ACCESS_KEY_ID, secret-access-key-env: S3_WRITE_SECRET_ACCESS_KEY }
          read:  { access-key-id-env: S3_READ_ACCESS_KEY_ID,  secret-access-key-env: S3_READ_SECRET_ACCESS_KEY }
        encryption: none
```

`encryption` is required on every S3 destination, so clear text is a written decision: `encryption: none`, or Parquet modular encryption with keys in the observability compactor's layout:

```yaml
        encryption: { keys-dir: /etc/mirror-v3/parquet-keys, key-id: k1 }
    format: parquet   # required, written out
```

`keys-dir` is a directory, normally a mounted Secret, with one file per key: the file name is the key id (`[a-z0-9][a-z0-9-]*`, at most 32 characters), the content the standard base64 of 32 random bytes (`openssl rand -base64 32`); dot entries are skipped, anything else invalid stops the mirror (errors name the file, never its content). New blobs are encrypted with `key-id` (footer and every column, AES-GCM, encrypted footer, so no statistics leak) and named `<from>-<to>.k-<key id>.parquet`; a blob is read with the key its name carries. Rotation: add the new key's file, then change `key-id`; keep old keys while blobs need them. A chain may mix plain and encrypted blobs (turning encryption on). DuckDB reads one key's blobs with:

```sql
PRAGMA add_parquet_key('k1', '<base64 of the key>');
SELECT * FROM read_parquet('s3://bucket/userstate/0/*.k-k1.parquet', encryption_config = {footer_key: 'k1'});
```

More examples: [`examples/`](examples/).

The full schema is committed at [`schemas/mirror-v3.config.schema.json`](schemas/mirror-v3.config.schema.json). Editors with a YAML language server (VS Code's `redhat.vscode-yaml`, Neovim, etc.) pick up the `# yaml-language-server: $schema=…` comment and provide completion + validation as you type.

### Env interpolation

Config values can reference environment variables using the same syntax as [`Yolean/y-cluster`](https://github.com/Yolean/y-cluster)'s envsubst:

```yaml
mirrors:
  - name: ${MIRROR_NAME:-orders}
    source: { bootstrap-servers: ${SOURCE_BROKER} }
    topic: ${TOPIC:-orders}
    partition: 0
    destinations:
      - type: s3
        region: ${AWS_REGION:-us-east-1}
        bucket: ${BUCKET_PREFIX:-yolean-mirror}-${AWS_REGION:-us-east-1}
        credentials: { write: { … }, read: { … } }
```

`${VAR}` is required (fails to start if unset), `${VAR:-default}` falls back to `default`, and `$$` escapes to a literal `$`. Substitution is single-pass — expanded values are not re-scanned. See [`examples/env-interpolation-dual-write.yaml`](examples/env-interpolation-dual-write.yaml) for the DRY pattern across duplicated destinations.

## Building

```sh
cargo build --release
cargo test --workspace
```

### Development on Ubuntu

The toolchain comes from rustup in the user's home, as `rust-toolchain.toml` pins it: install
rustup from <https://rustup.rs> (no system package). rdkafka builds librdkafka from source with
CMake (`cmake-build`), which needs a C and C++ toolchain, and librdkafka 2.12 compiles in libcurl
for OIDC whatever its options say. On Ubuntu 24.04, the build and test packages are the
[`Dockerfile`](Dockerfile)'s builder set, plus zlib for gzip-compressed topics:

```sh
sudo apt-get install -y cmake make g++ pkg-config \
  libcurl4-openssl-dev libssl-dev libsasl2-dev libzstd-dev liblz4-dev zlib1g-dev
```

A container image is built via the multi-stage [`Dockerfile`](Dockerfile) (builder = `rust:1-bookworm`, runtime = `gcr.io/distroless/cc-debian12`):

```sh
docker build -t mirror-v3:dev .
docker run --rm -v "$PWD/examples:/cfg" mirror-v3:dev validate --config /cfg/kafka-to-kafka.yaml
```

## Operational invariants

- **One process owns at most one mirror per `(topic, partition)`.** Run with `replicas: 1` and either `strategy.type: Recreate` or `RollingUpdate` with `maxSurge: 0` and `maxUnavailable: 1` for every mirror-v3 deployment. This is non-negotiable on two counts:
    1. **Destination races.** Two writers will race on destination naming and trip the corrupt-chain detector on the next restart.
    2. **Source-side coordination.** mirror-v3 uses `assign()` instead of `subscribe()` for its Kafka consumer, so there is no consumer-group coordinator deciding which pod owns the partition. Two pods up at once would both consume the same partition and race the consumer-offset commit log.
- **VersityGW specifically:** `If-None-Match: *` is silently ignored (v1.4.1, POSIX backend, verified in e2e), so the deployment guarantee is the *only* atomicity layer for the cross-process race. AWS S3 honors `If-None-Match: *` and gives API-level atomicity on top of the deployment guarantee.
- **An unrecoverable error ends the process.** Restart correctness is the recovery mechanism: the orchestrator restarts the process and every position comes from the destination again. The one exception is a destination-only mirror whose source or destination could not be reached: it is reopened in the process after a backoff, which re-derives its position from the destination the same way.
- **For blob destinations, a `(from, to)` filename/key is the durable "offset"** — atomic rename (FS) or single-shot `PutObject` (S3) makes it visible. The destination listing is the source of truth on startup.
- **Offset holes.** Kafka leaves holes in a partition's offsets where compaction removed records and at transaction markers. A blob object covers consumer positions: `from` is the previous object's `to` + 1 and `to` is the last record it holds, so the chain of names stays contiguous across holes and each record carries its true offset. A Kafka destination cannot reproduce a hole (its next offset is always its high watermark), so a hole in its source ends the mirror with an error saying so: mirror such topics to blobs. A position the broker no longer has (retention deleted records the mirror never read) is an error, never a jump to the earliest offset.

## Readiness

`GET /q/health/ready` returns a structured JSON body in every state:

```json
{
  "ready": "ready" | "warming",
  "mirrors": [
    {
      "name": "userstate",
      "gates_readiness": true,
      "caught_up": true,
      "status": "ready" | "warming" | "lag_behind_source"
              | "source_unassigned" | "destination_lagging",
      "source": {
        "topic": "userstate", "partition": 0, "assigned": true,
        "end_offset": 12345, "last_applied_offset": 12345, "lag": 0
      }
    }
  ],
  "unhealthy": []
}
```

HTTP status is `200` once every mirror that serves `/cache/v1` (`gates_readiness`) has caught up to its source's high watermark at startup, and stays `200` (sticky, as kafka-keyvalue); `503` before. A moment's lag after that is not an outage: it is the `status`/`lag` fields here and the `mirror_v3_source_lag_offsets` metric. Mirrors without `http-access` are listed but do not gate: a blob mirror waiting for S3 does not take the cache out of its Service. The drop-in `@yolean/kafka-keyvalue` Node client only inspects the status code.

`GET /q/health/live` answers `200` while the process serves HTTP.

Per-mirror `/cache/v1/{mirror}/...` routes return the matching `mirrors[i]` element as the `503` body before their mirror has caught up.

Tuning:

- `MIRROR_V3_READINESS_LAG` (default `0`) — offsets of lag tolerated before the body's `status` says `lag_behind_source` (it does not change the HTTP status).
- `MIRROR_V3_READINESS_POLL_MS` (default `2000`) — how often each mirror's broker high-watermark + consumer assignment is re-checked. `0` disables the poller.
- `MIRROR_V3_OFFSET_COMMIT_INTERVAL_MS` (default `5000`) — how often the supervisor commits the consumer's progress back to the broker. `0` disables (the mirror still works but loses the between-pods notify guarantee on the next restart).

Per-destination opt-out:

```yaml
destinations:
  - type: filesystem
    root: /var/lib/mirror-v3
    # affects-readiness: true   # default
  - type: kafka
    bootstrap-servers: ghost-cluster:9092
    affects-readiness: false   # best-effort secondary
```

A destination with `affects-readiness: false` still records its `flushed_through` for observability but is skipped when computing `DestinationLagging`. Use it for observability replicas or archival sinks that must not flip consumer-pod readiness when they fall behind.

## Open design questions

### `http-access` on a mirror with destinations

The config accepts a cache (`http-access`) and destinations in one mirror. Such a mirror reads the source from its low watermark for the cache, and lowers the tee's resume position to it, so every record below each destination's own position is read and skipped for that destination (a *resume floor*; notify re-delivery after a restart uses the same mechanism from the committed offset). What stands out:

- **The destinations no longer set the source position.** Restart correctness still derives from them: each is listed or queried at open, skips what it holds, and only accepts the record at exactly its next offset. But the guard against a destination that is ahead of its source (a recreated or truncated topic) has to look past the floor, at the furthest destination (`Sink::furthest_next_offset`); compared with the floor it would never fire for this shape.
- **The cache and the destinations fail together.** A cache mirror ends the process on any failure, so an S3 outage on its backup takes `/cache/v1` and its notifications down with it, and the pod cannot start while the destination is unreachable. A destination-only mirror is reopened in the process instead.
- **The cache is only as complete as the topic.** It holds what the source still retains, not what the destination archived (before 0309bc8 a cache was bootstrapped from the destination's compaction-mode snapshot instead).

The alternative is the split shape in [`examples/kkv-and-encrypted-backup.yaml`](examples/kkv-and-encrypted-backup.yaml): a cache (and notify) mirror without destinations, and a backup mirror of the same partition with its own consumer group, whose only startup input is its destination. The validator could then reject `http-access` on a mirror with destinations. That is not done, because notify on a mirror with destinations requires `cache-v1-main` on the same mirror, so it would also make `trigger.on: destination-flush` impossible to configure and remove notify re-delivery through the tee, and whether those are wanted is the other half of the question.

### Notify delivery from one replica

kafka-keyvalue ran 2 to 6 replicas per target Service and each pushed every update, so a push one replica lost (a consumer pod restarting, not Ready yet, or answering an error) was usually healed by a sibling's. A kkv-v1 push is one-shot and the consumer acknowledges nothing beyond the HTTP status. mirror-v3 runs one replica, so it has no sibling to heal a lost push.

Delivery today ([WEBHOOKS.md](WEBHOOKS.md)): a batch is POSTed to every resolved address in retry rounds that re-resolve the address set, batches are dispatched one at a time, and the outcome table decides the end: `skip` logs and drops the batch for that address (that consumer serves the key stale until it changes again), `fail` ends the process (the cache with it) after the retry budget, and a slow batch holds back the consume loop on the `max-records` path.

2f3be23 tried "deliver per target address, off the consume loop, until accepted": each address kept its own undelivered keys and got them retried until it accepted or left discovery for 30 s, never blocking the consume loop, with the committed offset at the lowest offset some address had not accepted. It was reverted (40cb69a) for what that cost: retries were pinned to one pod IP, so with `fail` outcomes one consumer pod rolling away could exhaust the budget and end the process, and a pod that answers 4xx for good (one that carries the target label but serves no POST route) held the committed offset back indefinitely.

Open: whether one replica should deliver more reliably (per address, off the consume loop, but re-resolving each attempt and bounding what a missing or refusing address can hold back), or whether the cache and notify mirror should run as more than one replica as kkv did. A mirror without destinations writes nothing, so the single-writer invariant does not bind it, but its replicas would need consumer groups of their own (the committed offset is where notify re-delivery resumes) and the backups would move to a separate single-replica deployment.

