# mirror-v3

Exactly-once Kafka topic+partition mirroring to **Kafka**, **Filesystem**, or **S3**, in one deployment.

> **Status:** Phase 5 — Kafka source + Kafka/Filesystem/S3 sinks; supervisor for parallel mirrors with graceful SIGINT/SIGTERM shutdown that flushes buffered records. Fault-injection tests deferred to Phase 2b; cutover to `checkit/mirror-v3` is a handoff step. See [AGENTS.md](AGENTS.md) for the phase map.

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
```

`status` queries the source Kafka high watermark and the destination's `next-expected-offset` for every mirror in the config and prints the lag. Exits non-zero if any mirror failed to query (unreachable broker, corrupt destination chain, etc.). Useful as a `kubectl exec` health probe before/during/after an appliance backup, without having to ssh to the node.

All logs go to **stderr** (heartbeat, flush lines, errors). `stdout` is reserved for command-driven output (`status --format json`, the `validate` success line). Standard `1>` / `2>` redirects work as expected.

### `/metrics` (Prometheus)

`mirror-v3 run` starts an HTTP server on `0.0.0.0:9090` that serves Prometheus-format metrics at `/metrics`. Override the port with `MIRROR_V3_METRICS_PORT=<port>`; set to `0` to disable the endpoint entirely. A bind failure (port in use) is logged at warn level and is non-fatal — the mirror keeps running, just unmonitored.

Every metric carries `topic="<source-topic>"` and `partition="<n>"` labels so they join cleanly with broker-side exporters (`kafka_exporter`, `kafka-lag-exporter`). The mirror's `name` is logged but is **not** a metric label — it's operator-chosen metadata, not a data-stream dimension.

| Metric | Type | Description |
|---|---|---|
| `mirror_v3_destination_offset_verified` | gauge | Next source offset the destination would accept; everything below this is durable. Set on startup and advanced by the sink the moment it confirms a commit — `acks=all` produce-delivery for Kafka, `rename(2)` success for Filesystem, `PutObject` success for S3. **This is the load-bearing metric for "how much is safe right now".** |
| `mirror_v3_destination_offset_inflight_retry` | gauge | Retry count (zero-based) for the destination write that's in flight. `0` covers both "no write in progress" and "first attempt, no retry yet" — a normal flow stays at `0`. `1` means one retry has happened (currently on the second attempt), `>= 2` means more retries are stacking. Resets to `0` on each successful write. **A non-zero, climbing value is the "destination is having problems" signal**; alert on it. Today this is always `0` in scrapes because mirror-v3 has no retry layer at the sink boundary — any sink error crashes the process. The slot is wired so dashboards can be built ahead of the retry implementation. |
| `mirror_v3_destination_records_total` | counter | Records that crossed the gate, since process start. |
| `mirror_v3_destination_last_flush_timestamp_seconds` | gauge | Unix timestamp (seconds) of the most recent flush. PromQL `time() - mirror_v3_destination_last_flush_timestamp_seconds` gives "seconds since last flush". Filesystem / S3 only. |
| `mirror_v3_destination_bytes_total` | counter | Cumulative bytes written to the destination by Filesystem / S3 sinks. |
| `mirror_v3_destination_flushes_total` | counter | Number of flushes by Filesystem / S3 sinks. |

Useful PromQL:

```
# Destination is currently struggling — any non-zero value is a retry happening
mirror_v3_destination_offset_inflight_retry > 0

# Seconds since last flush — alert if > flush.max-time-ms / 1000 × 2
time() - mirror_v3_destination_last_flush_timestamp_seconds

# End-to-end lag (join with kafka_exporter's source watermark on topic+partition):
kafka_topic_partition_current_offset
  - on(topic, partition) group_right mirror_v3_destination_offset_verified
```

A minimal PodMonitor for the checkit chart points at port 9090; the standard process metrics (`process_cpu_*`, `process_open_fds`, …) are also exposed by the exporter.

`run` spawns one task per mirror, each pinned to one `(topic, partition)`. SIGINT/SIGTERM trigger a graceful shutdown that flushes any buffered records on Filesystem and S3 sinks before exiting zero. Any task failure collapses the whole process with a non-zero exit — the orchestrator (k8s) is expected to restart it.

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
destination:
  type: kafka
  bootstrap-servers: redpanda:9092
mirrors:
  - name: operations
    source:
      bootstrap-servers: kafka-source:9092
    topic: operations-v1
    partition: 0
```

More examples: [`examples/`](examples/).

The full schema is committed at [`schemas/mirror-v3.config.schema.json`](schemas/mirror-v3.config.schema.json). Editors with a YAML language server (VS Code's `redhat.vscode-yaml`, Neovim, etc.) pick up the `# yaml-language-server: $schema=…` comment and provide completion + validation as you type.

### Field reference (Phase 0)

| Path | Type | Required | Notes |
|---|---|---|---|
| `destination.type` | `kafka` \| `filesystem` \| `s3` | yes | Discriminator |
| `destination.bootstrap-servers` | string | kafka only | |
| `destination.root` | path | filesystem only | Absolute path |
| `destination.endpoint` | URL | s3 (optional) | Omit for AWS regional |
| `destination.region` / `.bucket` | string | s3 only | |
| `destination.prefix` | string | s3 optional | |
| `destination.flush.max-time-ms` | u64 | fs/s3 only | ms between forced flushes |
| `destination.flush.max-bytes` | u64 | fs/s3 only | buffered byte cap |
| `destination.flush.max-offsets` | u64 | fs/s3 only | buffered offset cap |
| `mirrors[].name` | string | yes | Logs / metrics label |
| `mirrors[].source.bootstrap-servers` | string | yes | Source Kafka |
| `mirrors[].source.group-id` | string | no | Informational only |
| `mirrors[].topic` | string | yes | Source topic |
| `mirrors[].partition` | u32 | **yes** | Source partition, no default |
| `mirrors[].destination-name-override` | string | no | Per-mirror destination name |

## Building

```sh
cargo build --release
cargo test --workspace
```

A container image is built via the multi-stage [`Dockerfile`](Dockerfile) (builder = `rust:1-bookworm`, runtime = `gcr.io/distroless/cc-debian12`):

```sh
docker build -t mirror-v3:dev .
docker run --rm -v "$PWD/examples:/cfg" mirror-v3:dev validate --config /cfg/kafka-to-kafka.yaml
```

## Operational invariants

- **One process owns at most one mirror per `(topic, partition)`.** Run with `replicas: 1` and `strategy.type: Recreate` in Kubernetes for every mirror-v3 deployment. This is non-negotiable — two writers will race on destination naming and trip the corrupt-chain detector on the next restart.
- **VersityGW specifically:** `If-None-Match: *` is silently ignored (v1.4.1, POSIX backend, verified in e2e), so the deployment guarantee is the *only* atomicity layer for the cross-process race. AWS S3 honors `If-None-Match: *` and gives API-level atomicity on top of the deployment guarantee.
- **Any unrecoverable error in any mirror exits the entire process.** Restart correctness is the recovery mechanism; supervision belongs to the orchestrator.
- **For blob destinations, a `(from, to)` filename/key is the durable "offset"** — atomic rename (FS) or single-shot `PutObject` (S3) makes it visible. The destination listing is the source of truth on startup.
