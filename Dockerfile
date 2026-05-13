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
WORKDIR /src

# Cache deps separately from sources for faster incremental builds.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin mirror-v3 --locked && \
    cp target/release/mirror-v3 /usr/local/bin/mirror-v3

FROM gcr.io/distroless/cc-debian12:latest
COPY --from=builder /usr/local/bin/mirror-v3 /usr/local/bin/mirror-v3
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/mirror-v3"]
CMD ["--help"]
