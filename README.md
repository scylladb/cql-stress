# Cql Stress

A benchmarking tool for Scylla/Cassandra, written in Rust, offering a command line interface compatible both with [cassandra-stress](https://cassandra.apache.org/doc/latest/cassandra/tools/cassandra_stress.html) and [scylla-bench](https://github.com/scylladb/scylla-bench/).

The project is in an early stage of development.

## Development

### Running tests

The easiest way to set up the necessary environment and run the tests is to use the `tools/test_with_scylla.py` script.
The script requires Python 3 and Docker in order to work.
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
