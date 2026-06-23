FROM lukemathwalker/cargo-chef:latest-rust-slim-bookworm AS chef
WORKDIR /build

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
# Build and cache dependencies — this layer is only invalidated when recipe.json changes.
RUN cargo chef cook --release -p hermesmqd --recipe-path recipe.json
COPY . .
RUN cargo build --release -p hermesmqd

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/hermesmqd /usr/local/bin/hermesmqd

ENV HERMESMQ_CLIENT_ADDR=0.0.0.0:7600 \
    HERMESMQ_PEER_ADDR=0.0.0.0:7700 \
    HERMESMQ_METRICS_ADDR=0.0.0.0:9600 \
    HERMESMQ_METRICS_ENABLED=$HERMESMQ_METRICS_ENABLED \
    HERMESMQ_DATA_DIR=/data

VOLUME ["/data"]

EXPOSE 7600 7700 9600

ENTRYPOINT ["hermesmqd"]
