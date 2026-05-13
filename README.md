# mirror-v3

Exactly-once Kafka topic+partition mirroring to **Kafka**, **Filesystem**, or **S3**, in one deployment.

> **Status:** Phase 3 — Kafka source + Kafka/Filesystem sinks; supervisor for parallel mirrors. S3 sink lands in Phase 4. See [AGENTS.md](AGENTS.md) for the phase map.

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
mirror-v3 validate --config config.yaml   # parse-only
mirror-v3 run --config config.yaml        # start the configured mirrors
```

`run` spawns one task per mirror, each pinned to one `(topic, partition)`. The whole process exits non-zero on the first task failure — the orchestrator (k8s) is expected to restart it. `RUST_LOG=mirror_v3=debug,mirror_core=debug` for verbose tracing.

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

- One process owns at most one mirror per `(topic, partition)`. Run with `replicas: 1` and a `Recreate` strategy in Kubernetes.
- Any unrecoverable error in any mirror exits the entire process. Restart correctness is the recovery mechanism; supervision belongs to the orchestrator.
- For blob destinations, a `(from, to)` filename/key is the durable "offset" — atomic rename (FS) or single-shot `PutObject` (S3) makes it visible.
