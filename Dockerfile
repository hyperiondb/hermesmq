FROM rust:slim-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p hermesmqd

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/hermesmqd /usr/local/bin/hermesmqd
VOLUME ["/data"]
EXPOSE 7600 7700 9600
ENTRYPOINT ["hermesmqd"]
CMD ["--client-addr", "0.0.0.0:7600", "--peer-addr", "0.0.0.0:7700", "--metrics-addr", "0.0.0.0:9600", "--data-dir", "/data"]
