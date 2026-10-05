# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the mirror-v3 binary. Builder uses Debian
# bookworm with the Rust toolchain pinned by rust-toolchain.toml.
# Runtime is gcr.io/distroless/cc-debian12, which carries glibc +
# libgcc + libstdc++ — enough for our dynamically-linked binary and,
# in later phases, librdkafka.
#
# Both stages are digest-pinned (tag kept for readability). To
# update: `crane digest <image>:<tag>` and bump both stages
# together.

FROM docker.io/library/rust:1-bookworm@sha256:7d0723df719e7f213b69dc7c8c595985c3f4b060cfbee4f7bc0e347a86fe3b6a AS builder
# rdkafka builds librdkafka from source with cmake (`cmake-build`) and
# links it statically, with zlib (`libz-static`) and zstd (`zstd`,
# built by zstd-sys) for gzip- and zstd-compressed topics; snappy and
# lz4 are librdkafka's own. Without TLS or SASL features the binary
# needs only glibc and libgcc at runtime (distroless/cc). The headers
# below are what librdkafka 2.12's CMake insists on finding (it probes
# libcurl for OIDC whatever WITH_CURL says); cmake drives the build,
# g++/make compile, pkg-config discovers. Keep this list aligned with
# .github/workflows/ci.yaml's LIBRDKAFKA_BUILD_DEPS.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        cmake g++ make pkg-config \
        libcurl4-openssl-dev libssl-dev libsasl2-dev libzstd-dev liblz4-dev && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src

# Cache deps separately from sources for faster incremental builds.
# Note: the workspace's `[workspace.members]` includes `e2e/` even
# though we only build the `mirror-v3` bin from `crates/mirror-bin`,
# so cargo needs to read every member's Cargo.toml during resolve.
# Copying the e2e tree (a few KB) is the simplest fix and doesn't
# affect the runtime stage.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY e2e ./e2e
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin mirror-v3 --locked && \
    cp target/release/mirror-v3 /usr/local/bin/mirror-v3

FROM gcr.io/distroless/cc-debian12:latest@sha256:a90cf0f046efb32466b38b0972fef3a95e7c580e392e79ff1b7ac08c15fed0bc
COPY --from=builder /usr/local/bin/mirror-v3 /usr/local/bin/mirror-v3
# distroless's nonroot, numeric so that runAsNonRoot can verify it
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/mirror-v3"]
CMD ["--help"]
