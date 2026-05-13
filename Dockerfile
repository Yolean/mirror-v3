# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the mirror-v3 binary. Builder uses Debian
# bookworm with the Rust toolchain pinned by rust-toolchain.toml.
# Runtime is gcr.io/distroless/cc-debian12, which carries glibc +
# libgcc + libstdc++ — enough for our dynamically-linked binary and,
# in later phases, librdkafka.
#
# When a new image is cut, replace `:latest` with `@sha256:<digest>`
# and commit the digest. Update both stages together.

FROM docker.io/library/rust:1-bookworm AS builder
# librdkafka 2.12+ unconditionally pulls in libcurl (OIDC support);
# libssl/libsasl2/libzstd/liblz4 give us full feature support. cmake
# drives the build, g++/make do the compiling, pkg-config is for
# library discovery. Keep this list aligned with .github/workflows/ci.yaml's
# LIBRDKAFKA_BUILD_DEPS so local and CI builds match.
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

FROM gcr.io/distroless/cc-debian12:latest
COPY --from=builder /usr/local/bin/mirror-v3 /usr/local/bin/mirror-v3
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/mirror-v3"]
CMD ["--help"]
