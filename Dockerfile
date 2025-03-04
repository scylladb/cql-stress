FROM rust:1.85-slim-bookworm AS builder

WORKDIR /usr/src/app

COPY . .

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libssl-dev \
    pkg-config \
    && cargo build --profile dist --all

FROM debian:bookworm-slim AS production

LABEL org.opencontainers.image.source="https://github.com/scylladb/cql-stress"
LABEL org.opencontainers.image.title="ScyllaDB cql-stress"

COPY --from=builder /usr/src/app/target/dist/cql-stress-cassandra-stress /usr/local/bin/cassandra-stress
COPY --from=builder /usr/src/app/target/dist/cql-stress-scylla-bench /usr/local/bin/scylla-bench

RUN --mount=type=cache,target=/var/cache/apt apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y libssl3 \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
