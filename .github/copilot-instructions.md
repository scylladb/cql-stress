# CQL Stress Development Instructions

CQL Stress is a Rust-based benchmarking tool for Scylla/Cassandra that provides command-line interfaces compatible with both `cassandra-stress` and `scylla-bench`. It consists of two main binaries for performance testing database clusters.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Bootstrap and Build
- **Install Rust**: Requires Rust 1.85.0 or newer. Use `rustc --version` to check.
- **Build release version**: `cargo build --release` -- takes 2-3 minutes. NEVER CANCEL. Set timeout to 5+ minutes.
- **Build distribution version**: `cargo build --profile dist` -- takes 2-3 minutes. NEVER CANCEL. Set timeout to 5+ minutes.
- **Development build**: `cargo build` -- faster for development but poor performance.

### Running the Tools
- **Scylla-bench frontend**: `./target/release/cql-stress-scylla-bench <arguments>`
- **Cassandra-stress frontend**: `./target/release/cql-stress-cassandra-stress <arguments>`
- **Combined build+run**: `cargo run --release --bin cql-stress-scylla-bench -- <arguments>`
- **Get help**: 
  - `./target/release/cql-stress-scylla-bench` (running without arguments shows usage)
  - `./target/release/cql-stress-cassandra-stress help`

### Testing
- **CRITICAL**: All tests require a running Scylla instance.
- **Start Scylla**: `docker compose -f docker/scylla-test/compose.yml up -d --wait` -- takes 1-2 minutes. NEVER CANCEL.
- **Automated testing**: `python3 tools/test_with_scylla.py` -- takes 1-2 minutes. NEVER CANCEL. Set timeout to 5+ minutes.
- **Manual testing**: `cargo test -- --test-threads=1` (requires SCYLLA_URI env var if non-standard)
- **Integration tests**: Install `pip install scylla-driver pytest`, then `pytest -s tools/cql-stress-cassandra-stress-ci.py` -- takes 2-3 minutes. NEVER CANCEL.
- **Stop Scylla**: `docker compose -f docker/scylla-test/compose.yml down`

### Linting and Formatting
- **Check formatting**: `cargo fmt --check` -- passes cleanly
- **Apply formatting**: `cargo fmt`
- **Basic linting**: `cargo clippy` -- has warnings but builds successfully  
- **Strict linting**: `cargo clippy -- -D warnings` -- currently fails due to lifetime warnings
- **CI linting variations**:
  - `cargo clippy --tests --no-default-features -- -D warnings`
  - `cargo clippy --tests --features "user-profile" -- -D warnings`
  - `RUSTFLAGS="--cfg fetch_extended_version_info" cargo clippy --tests -- -D warnings`

## Validation
- ALWAYS manually test both binaries after making changes:
  - Test scylla-bench: `./target/release/cql-stress-scylla-bench -mode write -workload uniform -partition-count 1000 -clustering-row-count 10 -concurrency 2 -max-rate 100 -duration 10s -nodes 127.0.0.1:9042`
  - Test cassandra-stress write: `./target/release/cql-stress-cassandra-stress write n=1000 -pop seq=1..1000 -rate threads=2 -node 127.0.0.1`
  - Test cassandra-stress read: `./target/release/cql-stress-cassandra-stress read n=500 -pop seq=1..1000 -rate threads=2 -node 127.0.0.1`
- ALWAYS run the Python integration tests before submitting changes.
- ALWAYS run `cargo fmt` before committing or the CI will fail.

## Common Tasks

### Project Structure
```
cql-stress/
├── src/
│   ├── bin/
│   │   ├── cql-stress-cassandra-stress/  # Cassandra-stress compatible frontend
│   │   └── cql-stress-scylla-bench/      # Scylla-bench compatible frontend  
│   ├── lib.rs                            # Common library code
│   └── ...
├── tools/
│   ├── test_with_scylla.py              # Automated test script
│   └── cql-stress-cassandra-stress-ci.py # Integration tests
├── docker/scylla-test/compose.yml       # Test database setup
└── Cargo.toml                           # Build configuration
```

### Key Features
- **Default feature**: `user-profile` - enables custom user profiles in cassandra-stress
- **Build option**: `--cfg fetch_extended_version_info` - enables extended version information
- **Build profiles**: `release` (optimized), `dist` (distribution), `dev` (development)

### Environment Variables
- **SCYLLA_URI**: Override default Scylla connection (default: 127.0.0.1:9042)
- **RUSTFLAGS**: Set to `--cfg fetch_extended_version_info` for extended version info

### Dependencies
- **Rust toolchain**: 1.85.0+
- **Docker**: For running Scylla test instance
- **Docker Compose V2**: For test orchestration 
- **Python 3**: For integration tests
- **scylla-driver**: Python driver for integration tests
- **pytest**: Python testing framework

### Timing Expectations
- **Cargo build --release**: 2-3 minutes
- **Cargo build --profile dist**: 2-3 minutes  
- **Cargo test**: 1-2 minutes (with Scylla running)
- **Python integration tests**: 2-3 minutes
- **Scylla startup**: 1-2 minutes
- **NEVER CANCEL**: Any build or test operation. Always wait for completion.

### Example Workloads
- **Write test**: Creates keyspace and table, inserts data with configurable patterns
- **Read test**: Reads previously written data with validation
- **Mixed workloads**: Combination of reads/writes with configurable ratios
- **User profiles**: Custom schemas and queries via YAML configuration files

### CI/CD Integration
- **GitHub Actions**: `.github/workflows/rust.yml` defines the complete CI pipeline
- **Docker builds**: Multi-arch Docker images for distribution
- **Release artifacts**: Binaries for multiple platforms in GitHub releases
- **Critical CI steps**: check, fmt, build, test, integration-tests