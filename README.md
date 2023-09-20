# Cql Stress

A benchmarking tool for Scylla/Cassandra, written in Rust, offering a command line interface compatible both with [cassandra-stress](https://cassandra.apache.org/doc/latest/cassandra/tools/cassandra_stress.html) and [scylla-bench](https://github.com/scylladb/scylla-bench/).
The aim of the tool is to provide a more scalable and performant replacements of the original tools, and increase the usage of scylla-rust-driver in tests.

The `scylla-bench` frontend is feature-complete, `cassandra-stress` is a _work in progress_.

## Usage

`cql-stress` is not published on crates.io yet, therefore in order to use it you need to clone it and build from source.
See the [Development](#development) section for more details.

### Scylla Bench

See the documentation of the original [`scylla-bench`](https://github.com/scylladb/scylla-bench/blob/master/README.md#usage) for a comprehensive explanation of the most important parameters.
To see a list of all the parameters currently supported by the tool, use `cql-stress-scylla-bench -help`.

### Cassandra Stress

(Work in progress)

## Development

You need the `cargo` command in order to build the tool:

```bash
# The --release flag can be omitted when developing features and testing them,
# but don't forget to include it when building for benchmarking purposes
cargo build --release
```

Then, run the frontend of your choice:

```bash
./target/release/cql-stress-scylla-bench <arguments>
./target/release/cql-stress-cassandra-stress <arguments>
```

Alternatively you can combine compilation and running in a single step:

```bash
cargo run --release --bin cql-stress-scylla-bench -- <arguments>
cargo run --release --bin cql-stress-cassandra-stress -- <arguments>
```

### Running tests

The easiest way to set up the necessary environment and run the tests is to use the `tools/test_with_scylla.py` script.
The script requires Python 3, Docker and Docker Compose V2 in order to work.
It will run a Docker container with Scylla and will automatically remove after the tests have completed.

Alternatively, you can set up Scylla yourself and keep it up between test runs. The easiest way to do it is by using Docker:

```bash
# Downloads and runs Scylla in Docker
docker run --name scylla-ci -d -p 9042:9042 scylladb/scylla
```

Then, you can run the tests like this:

```bash
cargo test -- --test-threads=1
```

If you are using a non-standard IP address or port for your Scylla instance, you can pass it through the `SCYLLA_URI` environment variable:

```bash
SCYLLA_URI=172.16.0.1:9042 cargo test -- --test-threads=1
```

#### cassandra-stress frontend python tests
To run the test cases used during CI (defined in `./tools/cassandra_stress_ci.py`), you can make use of [pytest](https://pytest.org).
Before running the tests, make sure you have scylla up and running.
If you use some non-standard scylla URI, you can specify it via `SCYLLA_URI` env variable.
```bash
docker compose -f docker/scylla_test/compose.yml up -d --wait
export SCYLLA_URI="127.0.0.1:9042"
pytest -s ./tools/cassandra_stress_ci.py
```
