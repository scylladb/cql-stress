# porcupine_checker (porcupine_validator checker-v2), pinned here once and by digest: CI's
# strong consistency tests take the checker from this image too. `COPY --from` does not expand
# an ARG, so the image is a stage of its own. The image is amd64-only for now, and the builder
# below refuses a checker that cannot run on the build platform.
ARG CHECKER_IMAGE=aleksbykov/porcupine_validator:v2-93e504f@sha256:45f5af378a8cd63a11b099f19b88033aab6129333a6f582e489b9e3fbf29e1cd
FROM ${CHECKER_IMAGE} AS checker

# Must be >= the `rust-version` in Cargo.toml (1.89.0). scylla-rust-driver 1.9.0 is
# edition 2024 / rust-version 1.88, so an older toolchain fails outright rather than just
# warning.
FROM rust:1.89-slim-bookworm AS builder

WORKDIR /app

# RUSTFLAGS replaces `.cargo/config.toml`'s `[build] rustflags` rather than adding to it,
# so `--cfg scylla_unstable` has to be repeated here; see that file. It is inert unless the
# `strong-consistency` feature is also on - the driver gates that API on both - but it is set
# unconditionally so the two can never drift apart.
ENV RUSTFLAGS="--cfg fetch_extended_version_info --cfg scylla_unstable"
ENV CARGO_TERM_COLOR=always

# Extra cargo features for the build, empty by default. The strongly consistent image is the
# one caller that sets this:
#
#   docker build --build-arg CARGO_BUILD_FEATURES=strong-consistency .
#
# It is an opt-in because `strong-consistency` pulls in the driver's unstable
# strongly-consistent-tables API, which the ordinary image must not depend on.
ARG CARGO_BUILD_FEATURES=""

COPY . .
COPY --from=checker /porcupine_checker /tmp/porcupine_checker

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libclang-dev \
    git \
    libssl-dev \
    pkg-config \
    && cargo build --profile dist --all ${CARGO_BUILD_FEATURES:+--features "$CARGO_BUILD_FEATURES"} \
    && if echo "$CARGO_BUILD_FEATURES" | tr ', ' '\n\n' | grep -qx strong-consistency; then \
         /tmp/porcupine_checker < /dev/null 2>/dev/null; rc=$? ; \
         [ "$rc" -eq 2 ] || { echo "porcupine_checker does not run here (exit $rc)" >&2; exit 1; } ; \
         cp /tmp/porcupine_checker target/dist/porcupine_checker ; \
       fi

FROM debian:bookworm-slim AS production

ENV PATH="${PATH}:/usr/local/bin"

LABEL org.opencontainers.image.source="https://github.com/scylladb/cql-stress"
LABEL org.opencontainers.image.title="ScyllaDB cql-stress"

# `cql-stress-sc-verify` and the `porcupine_checker` it runs exist only in a
# `strong-consistency` build. A pattern that matches nothing is allowed next to a source that
# matches, so the ordinary image simply lacks them.
COPY --from=builder /app/target/dist/cql-stress-cassandra-stress /app/target/dist/cql-stress-sc-verif[y] /app/target/dist/porcupine_checke[r] /usr/local/bin/
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
