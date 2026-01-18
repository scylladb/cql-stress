# AGENTS.md

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

#### Unit Tests
```bash
# Manual run all tests (requires Scylla instance running)
docker compose -f docker/scylla-test/compose.yml up -d --wait
cargo test -- --test-threads=1

# Run specific Rust test by name
cargo test <test_name> -- --test-threads=1

# Run tests with filter
python3 tools/test_with_scylla.py --test-filter <filter>

# Using the automated test script with Docker (recommended)
# Note: Requires Python 3, Docker and Docker Compose V2
python3 tools/test_with_scylla.py

# Build tests first (to verify compilation before starting Scylla)
cargo build --tests --features "user-profile"


```

#### Integration Tests
```bash
# Set up Python virtual environment (first time only)
python3 -m venv venv
source venv/bin/activate
pip install pytest scylla-driver

# Start Scylla for testing
docker compose -f docker/scylla-test/compose.yml up -d --wait

# Set custom Scylla URI if needed
export SCYLLA_URI="127.0.0.1:9042"

# Run Python integration tests (activate venv first if not already active)
source venv/bin/activate
pytest -s tools/cql-stress-cassandra-stress-ci.py

# Run a single integration test (most basic example)
source venv/bin/activate
pytest -s tools/cql-stress-cassandra-stress-ci.py::test_write_and_validate -v

# Run specific test with more verbose output
pytest -s tools/cql-stress-cassandra-stress-ci.py::test_equal_db -v

# Run all user profile tests
pytest -s tools/cql-stress-cassandra-stress-ci.py -k "user" -v
```

### Code Quality
```bash
# Format code
cargo fmt

# Lint with clippy
cargo clippy --tests --no-default-features -- -D warnings
cargo clippy --tests --features "user-profile" -- -D warnings

# Check compilation without building
cargo check --all --all-targets
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
- To disable: `cargo build --no-default-features`

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
