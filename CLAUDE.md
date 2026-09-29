# CLAUDE.md

## Project Overview

`cql-stress` is a benchmarking tool for Scylla/Cassandra written in Rust. It offers two frontend interfaces:
- **cql-stress-cassandra-stress**: Provides compatibility with the original cassandra-stress tool
- **cql-stress-scylla-bench**: Provides compatibility with scylla-bench tool

The tool aims to provide more scalable and performant replacements for the original tools while increasing usage of the scylla-rust-driver in tests.

## Development Commands

### Building
```bash
# Development build
cargo build

# Release build (recommended for benchmarking)
cargo build --release

# Optimized distribution build with LTO (used by CI/CD for releases)
# Note: Longer build time but smaller binary and potentially better performance
cargo build --profile dist

# Build with specific features
cargo build --features "user-profile"
cargo build --no-default-features
```

### Running the Binaries
```bash
# Using cargo run (combines compilation and execution)
cargo run --release --bin cql-stress-cassandra-stress -- <arguments>
cargo run --release --bin cql-stress-scylla-bench -- <arguments>

# Using compiled binaries
./target/release/cql-stress-cassandra-stress <arguments>
./target/release/cql-stress-scylla-bench <arguments>
```

### Testing

#### Prerequisites

**Java Requirement**: Integration tests require Java (JDK 11 or later) for the cassandra-stress tool:
```bash
# Install Java on Ubuntu/Debian/Mint
sudo apt update
sudo apt install openjdk-11-jdk -y

# Verify installation
java --version
```

#### Running tests

The full verify sequence is in `## Commands`. Use these commands for a smaller run:

```bash
# Format the code
cargo fmt --all

# Run one Rust test by name (Scylla must be running)
cargo test <test_name> -- --test-threads=1

# test_with_scylla.py uses the same Scylla container as `## Commands`.
# By default it removes the container after the run. Add `--teardown never` to keep it.
# Run all Rust tests
uv run tools/test_with_scylla.py

# Run the Rust tests that match a filter
uv run tools/test_with_scylla.py --test-filter <filter>

# Run one integration test (after the release build in `## Commands`)
PATH="$PWD/target/release:$PATH" uv run --with pytest --with scylla-driver \
    pytest -s tools/cql-stress-cassandra-stress-ci.py::test_write_and_validate -v

# Run all user profile integration tests
PATH="$PWD/target/release:$PATH" uv run --with pytest --with scylla-driver \
    pytest -s tools/cql-stress-cassandra-stress-ci.py -k "user" -v
```

## Architecture

### High-Level Structure

**Dual Frontend Design**: The project implements two separate command-line frontends that share common core functionality:

1. **Frontend Layer**: Two separate binaries (`cql-stress-cassandra-stress` and `cql-stress-scylla-bench`) in `src/bin/`
2. **Core Library**: Shared functionality in `src/lib.rs` providing configuration, operation execution, and statistics
3. **Operation Framework**: Extensible operation system supporting different workload types

### Key Components

#### Core Library (`src/`)
- **`configuration.rs`**: Defines `Configuration` struct and `Operation`/`OperationFactory` traits
- **`run.rs`**: Runtime execution engine that orchestrates workers and handles concurrency
- **`sharded_stats.rs`**: Thread-safe statistics collection and aggregation
- **`distribution.rs`**: Statistical distributions for data generation

#### Cassandra-Stress Frontend (`src/bin/cql-stress-cassandra-stress/`)
- **`main.rs`**: Entry point, session setup, and runtime coordination
- **`settings/`**: CLI argument parsing and configuration management
- **`operation/`**: Operation implementations (read, write, counter, mixed, user-defined)
- **`stats.rs`**: Statistics collection and reporting
- **`hdr_logger.rs`**: HDR histogram logging for latency analysis

#### Operation Types
- **Write Operations**: Insert data with generated values
- **Read Operations**: Query and validate existing data  
- **Counter Operations**: Update and read counter columns
- **Mixed Operations**: Configurable ratio of different operation types
- **User Operations**: Custom operations defined via YAML profiles (requires `user-profile` feature)

#### User Profiles System
When compiled with the `user-profile` feature (default), supports custom schemas and queries via YAML configuration files in `tools/util/profiles/`. These profiles define:
- Custom table schemas
- Keyspace definitions  
- Named queries with CQL statements

### Test Infrastructure

**Python Test Framework**: Comprehensive integration testing using pytest with utilities for:
- **Scylla Docker Management**: Automated container lifecycle management
- **Cassandra Stress Compatibility**: Testing against original cassandra-stress tool
- **Runtime Configuration**: Parameterized test execution with different workload sizes and concurrency levels
- **HDR Logging Validation**: Testing of histogram-based latency logging

## Features

### Cargo Features
- `user-profile` (default): Enables support for custom user profiles in cassandra-stress frontend
- `strong-consistency` (off by default): Drives strongly consistent (Raft-per-tablet) keyspaces. It uses an unstable driver API that also needs `--cfg scylla_unstable`. `.cargo/config.toml` sets that cfg. When you set `RUSTFLAGS`, add it yourself.
- `timerfd` (off by default): Uses Linux `timerfd` timers for the rate limiter
- To disable the default features: `cargo build --no-default-features`

### Build Profiles
- `dev`: Development build with minimal optimization (default)
- `dev-opt`: Development build with level 2 optimization
- `release`: Standard optimized build without LTO (fast compilation, suitable for local benchmarking)
- `dist`: Distribution build with LTO, maximum optimization and stripped symbols (used in CI/CD pipelines)

## Environment Variables

- `SCYLLA_URI`: Override default Scylla connection (default: "127.0.0.1:9042")
- `RUST_LOG`: Control logging verbosity (default: "warn")

## Example Usage

### Basic Write Workload
```bash
cargo run --release --bin cql-stress-cassandra-stress -- \
    write n=1000000 -pop seq=1..1000000 -rate threads=20 -node 127.0.0.1
```

### Read Validation
```bash
cargo run --release --bin cql-stress-cassandra-stress -- \
    read n=1000000 -pop seq=1..1000000 -rate threads=20 -node 127.0.0.1
```

### User Profile Testing
```bash
cargo run --release --bin cql-stress-cassandra-stress -- \
    user profile=tools/util/profiles/cqlstress_text_profile.yaml \
    "ops(test_query=1)" n=10000 -rate threads=10 -node 127.0.0.1
```

## Commands

```bash
# CI sets these flags for the whole job. One value for all commands keeps one build cache.
export RUSTFLAGS="-D warnings --cfg scylla_unstable"

# Lint
cargo fmt --all -- --check
cargo check --all --all-targets
cargo clippy --tests --no-default-features -- -D warnings
cargo clippy --tests --features "user-profile" -- -D warnings
cargo clippy --all --all-targets --features "strong-consistency" -- -D warnings
cargo clippy --tests --no-default-features --features "strong-consistency" -- -D warnings
RUSTFLAGS="--cfg fetch_extended_version_info --cfg scylla_unstable" cargo clippy --tests -- -D warnings

# Test (needs Scylla)
docker compose -f docker/scylla-test/compose.yml up -d --wait
cargo test --features "user-profile" -- --test-threads=1
cargo test --features "user-profile,strong-consistency" -- --test-threads=1

# Integration tests (need Java and the binary on PATH)
cargo build --release --bin cql-stress-cassandra-stress
PATH="$PWD/target/release:$PATH" uv run --with pytest --with scylla-driver \
    pytest -s tools/cql-stress-cassandra-stress-ci.py
```

<!-- qatools-sdlc:begin -->
## Development flow

This repository uses the `qatools-sdlc` plugin. Every piece of work goes
through its flow: `/qatools-sdlc:intent <KEY>`, then `/qatools-sdlc:spec` or
`/qatools-sdlc:rca` for a bug, then `/qatools-sdlc:plan`, then the code.
Commit each artifact before the stage that consumes it. A review works
through `/qatools-sdlc:review`. File a Jira issue about our work with
`/qatools-sdlc:issue`. It needs the Atlassian connector.
The user may skip the flow for a very small fix when they say so. The pull
request description then states the skip in one line.

If the `/qatools-sdlc:*` skills are not available, stop and ask the user to
run these two commands, then start a new session:

    /plugin marketplace add git@github.com:scylladb/qatools.git
    /plugin install qatools-sdlc@qatools

Jira keys: `QATOOLS-<n>`. Task artifacts: `tasks/<KEY>/`, or
`tasks/<PARENT>/<KEY>/` for a subtask. Read `docs/INDEX.md` before any task
and follow the standards in `docs/standards/`. Suggest
`/qatools-sdlc:standards-update` when a convention comes up that no standard
holds. Verify sequence: section `Commands` of this file.
<!-- qatools-sdlc:end -->
