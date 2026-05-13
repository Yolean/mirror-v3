# AGENTS.md — notes for agent maintainers

This file is for the next agent (or human) extending mirror-v3. Read this first; it explains the design decisions, invariants you must not violate, and the workflow expected for changes.

## Repo shape

```
.
├── Cargo.toml                workspace, profiles, shared deps
├── rust-toolchain.toml       stable channel + rustfmt + clippy
├── Dockerfile                multi-stage; runtime = distroless/cc-debian12
├── README.md                 end-user docs (config reference)
├── AGENTS.md                 this file
├── crates/
│   ├── mirror-config/        Config structs + serde + schemars
│   ├── mirror-bin/           the `mirror-v3` binary (CLI + main loop)
│   └── xtask/                cargo run -p xtask -- (gen|check)-schema
├── examples/                 YAML configs that double as bin tests
└── schemas/
    └── mirror-v3.config.schema.json   golden file, gated in CI
```

Phase 1 added `mirror-core` and `mirror-kafka`. Phase 2 added the `mirror-e2e` crate at `e2e/` with `testcontainers`-driven Docker stacks and the `Provisioner`/`ProvisionedStack` trait seam for plugging in new test infra. Phase 3 added `mirror-fs` (atomic-rename filesystem sink, `<from>-<to>.ndjson` naming, scan-validate on startup). Phase 4 added `mirror-s3` (same wire format as `mirror-fs`, `object_store`-backed, `PutMode::Create` + scan-validate two-layer atomicity). Phase 5 generalised `run_mirror` to accept a shutdown future, added `Sink::flush` (default no-op; FS/S3 implementations call their `flush_now`), and wired SIGINT/SIGTERM in `mirror-v3 run`.

## The phase plan

Each row is a separate change set / PR. Do not skip phases.

| Phase | Scope | Done when |
|---|---|---|
| 0 | Workspace + config model + JSON Schema gate + CLI stub + Dockerfile | `cargo test --workspace` green, schema committed |
| 1 | `mirror-core` (Source/Sink traits, loop) + `mirror-kafka` source+sink with end-offset gate + `mirror-v3 run` supervisor | Builds + 17 tests green; loop invariants exhaustively unit-tested with mocks |
| 2 | Docker e2e harness (`mirror-e2e` crate) + `kafka-native → redpanda` happy-path test | First real Kafka e2e green: 100 records, byte-identical, offsets preserved |
| 3 | `mirror-fs` sink + flush triggers + scan-validate on startup + e2e | FS sink unit-tested (corrupt-chain, restart, crashed .tmp, flush triggers); kafka→fs e2e green |
| 4 | `mirror-s3` sink via `object_store` + VersityGW e2e + conditional-PUT spike | S3 sink unit-tested against InMemory (incl. `PutMode::Create` enforcement); kafka→VersityGW e2e green; spike answer recorded |
| **5** | Multi-mirror in one process + graceful shutdown + e2e | Loop accepts a shutdown future; `Sink::flush` is wired to `flush_now` on FS/S3; SIGINT/SIGTERM trigger a graceful exit; parallel-mirrors e2e green |
| **2b** (deferred) | Toxiproxy fault injection in the Docker stack | A fault test demonstrates the mirror's crash-and-recover-from-destination behaviour |
| 6 (handoff) | Cutover: replace the Java worker image in `checkit/mirror-v3` | Done by the operator, not this repo |
| 4 | `mirror-s3` sink via `object_store`, `redpanda → versitygw` e2e | Concurrent writer race produces hard exit, never a silently-overlapping blob |
| 5 | Supervisor for N mirrors in one process; per-mirror metrics | Two mirrors run side-by-side under fault injection |
| 6 | Cutover: replace the Java worker image in checkit/mirror-v3 | Dev site running Rust binary; Java module archived |

## Non-negotiable invariants

These are not style preferences. Breaking any of them defeats the whole point of the rewrite.

1. **Restart correctness derives from the destination.** Never persist source position to a local file, lock file, sidecar DB, or consumer-group commit *as the truth*. Group commits are monitoring-only.
2. **Before producing source offset N, verify the destination is exactly at N.** For Kafka sinks: read target `end_offsets` and require it equals N. For blob sinks: derive next-expected offset from the prefix listing, require it equals N. Any mismatch is a hard exit.
3. **Atomic writes only.** Filesystem = same-directory `rename(2)`. S3 = single `PutObject`, ideally with `PutMode::Create` (`If-None-Match: *`). Multi-step writes that could leave the destination in an inconsistent state are unacceptable.
4. **Naming encodes both `from` and `to` source offsets** for blob destinations. Listing → `max(to)+1` is the next-expected offset. Two objects sharing a `from` is a corruption-detection signal — exit and alert.
5. **One process = one writer per `(topic, partition)`.** Deployments run a single replica with `Recreate`. Don't add leader election or coordination; the orchestrator owns singleton-ness.
6. **Correctness > performance, always.** If you have to choose, choose to exit and let k8s restart you.

## Test-driven workflow

Every change should land with a failing test first, then the fix. Three layers:

1. **Unit tests** in `crates/*/src/` (private helpers).
2. **Integration tests** in `crates/*/tests/` (public API of one crate). The `mirror-config` schema golden test (`crates/mirror-config/tests/schema.rs`) is here.
3. **E2e tests** in (Phase 2+) `e2e/` workspace member. These spin Docker containers via `testcontainers-rs`, optionally fronted by Toxiproxy for fault injection. The `TestEnvironment` trait must remain implementable by future provisioners (kind, real cloud).

Run locally:

```sh
# Fast loop (no Docker, ~seconds):
cargo test --workspace --exclude mirror-e2e
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -Dwarnings
cargo run -p xtask -- check-schema   # fails if structs changed without regen

# Full e2e (requires Docker; pulls broker images on first run):
cargo test -p mirror-e2e
```

CI in `.github/workflows/ci.yml` runs all of the above on every PR.

## Changing the config model

The committed JSON Schema at `schemas/mirror-v3.config.schema.json` is **load-bearing**. Editors validate users' YAML against it.

1. Edit structs in `crates/mirror-config/src/lib.rs`.
2. Add tests in `crates/mirror-config/tests/loading.rs` for the new field.
3. `cargo run -p xtask -- gen-schema` to regenerate the committed JSON file.
4. `cargo test --workspace` — the `committed_schema_matches_current_structs` test in `tests/schema.rs` must pass.
5. Update `examples/` if a new field is mandatory.
6. Update the field reference table in `README.md`.

`#[serde(deny_unknown_fields, rename_all = "kebab-case")]` is set on every struct so YAML uses `kebab-case` keys and typos fail loudly. Keep it that way.

## Container image

Base: `gcr.io/distroless/cc-debian12:latest` (glibc + libgcc + libstdc++; enough for librdkafka in Phase 1). Pin by `@sha256:<digest>` once we publish images. Update both stages of the Dockerfile together.

The image runs as `nonroot`. The binary needs no extra capabilities.

## Dependencies of note

- **`rdkafka`** (Phase 1+): librdkafka bindings. Builder image needs `librdkafka-dev` + `libsasl2-dev` apt-installed. Cargo feature default is dynamic linking; do not switch to `cmake-build` static unless you've verified glibc is still available in the runtime image.
- **`object_store`** (Phase 3+): one trait covers `LocalFileSystem` + `AmazonS3`. `PutMode::Create` is the preferred atomicity primitive; fall back to scan-validate on backends that ignore it.
- **`schemars`** v1: derive macro + `schema_for!`. Always re-run `xtask gen-schema` after touching structs.

## Open questions to resolve at their phase

- **Phase 4 (resolved):** Does VersityGW (POSIX backend) honor `If-None-Match: *`? **No.** `e2e/tests/kafka_to_versitygw.rs::versitygw_conditional_put_spike` confirms VersityGW v1.4.1 silently overwrites the second PUT — `PutMode::Create` is a no-op there. AWS S3 honors it (Aug 2024 feature). **Consequence:** when the destination is VersityGW, the *only* protection against the two-writer overlap race is the deployment guarantee (k8s `Recreate`, single replica) plus scan-validate on startup. The S3 sink still issues `PutMode::Create` because the cost is the same and AWS-style backends DO get API-level atomicity from it.
- **Phase 6:** Cutover plan for checkit/mirror-v3 — env-var shim so `mirror-v3-worker-deployment.yaml` doesn't need to change in lockstep.
