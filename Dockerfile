FROM rust:1.85-slim-bookworm AS builder

WORKDIR /app

ENV RUSTFLAGS="--cfg fetch_extended_version_info"
ENV CARGO_TERM_COLOR=always

COPY . .

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libclang-dev \
    git \
    libssl-dev \
    pkg-config \
    && cargo build --profile dist --all

FROM debian:bookworm-slim AS production

ENV PATH="${PATH}:/usr/local/bin"

LABEL org.opencontainers.image.source="https://github.com/scylladb/cql-stress"
LABEL org.opencontainers.image.title="ScyllaDB cql-stress"

COPY --from=builder /app/target/dist/cql-stress-cassandra-stress /usr/local/bin/cql-stress-cassandra-stress
COPY --from=builder /app/target/dist/cql-stress-scylla-bench /usr/local/bin/cql-stress-scylla-bench

RUN --mount=type=cache,target=/var/cache/apt apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y libssl3 \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && chmod +x /usr/local/bin/cql-stress-cassandra-stress /usr/local/bin/cql-stress-scylla-bench \
    && ln -s /usr/local/bin/cql-stress-cassandra-stress /usr/local/bin/cassandra-stress \
    && ln -s /usr/local/bin/cql-stress-scylla-bench /usr/local/bin/scylla-bench

ENTRYPOINT [ "bash" ]
