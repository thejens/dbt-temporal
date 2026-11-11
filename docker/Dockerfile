FROM rust:1.99-bookworm AS builder
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src/ src/
RUN cargo build --release --features gcs,aws

FROM gcr.io/distroless/cc-debian12 AS base

# libpq for dbt's Postgres driver; see docker/stage-libpq.sh.
FROM debian:bookworm-slim AS libpq
COPY --from=base / /base
COPY docker/stage-libpq.sh /usr/local/bin/
RUN stage-libpq.sh /base /staged

FROM base
COPY --from=libpq /staged/ /
COPY --from=builder /app/target/release/dbt-temporal /usr/local/bin/
ENTRYPOINT ["dbt-temporal"]
