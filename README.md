# Cql Stress

A benchmarking tool for Scylla/Cassandra, written in Rust, offering a command line interface compatible both with [cassandra-stress](https://cassandra.apache.org/doc/latest/cassandra/tools/cassandra_stress.html) and [scylla-bench](https://github.com/scylladb/scylla-bench/).
The aim of the tool is to provide a more scalable and performant replacements of the original tools, and increase the usage of scylla-rust-driver in tests.

The `scylla-bench` frontend is feature-complete, `cassandra-stress` is a _work in progress_.

## Usage

`cql-stress` is not published on crates.io yet, therefore in order to use it you need to clone it and build from source.
See the [Development](#development) section for more details.

## Crate features

List of the crate features:
- `user-profile` - enables support for `user` command and custom user profiles in `cassandra-stress` frontend. This feature is enabled by default. To disable it, pass `--no-default-features` flag when building the tool.

### Scylla Bench

See the documentation of the original [`scylla-bench`](https://github.com/scylladb/scylla-bench/blob/master/README.md#usage) for a comprehensive explanation of the most important parameters.
To see a list of all the parameters currently supported by the tool, use `cql-stress-scylla-bench -help`.

### Cassandra Stress

See the documentation of the original [`cassandra-stress`](https://cassandra.apache.org/doc/stable/cassandra/tools/cassandra_stress.html) for a comprehensive explanation of the most important commands and options.

To see a list of all commands and options currently supported by the tool, use `cql-stress-cassandra-stress help`. To see a list of parameters supported for a given command/option, use `cql-stress-cassandra-stress help <command/option>`.

#### Populating the cluster

To populate a local cluster, make use of `write` command:
```
cql-stress-cassandra-stress write n=1000000 -pop seq=1..1000000 -rate threads=20 -node 127.0.0.1
```

Since some of the options and parameters were not provided, the tool will make use of some default values. This will result in:
- creating a `keyspace1` keyspace (if not exists)
- creating a `keyspace1.standard1` table (if not exists)
- populating the table with 1000000 generated rows

#### Validating cluster contents after write

To validate that the data inserted in the previous step is correct, make use of `read` command:
```
cql-stress-cassandra-stress read n=1000000 -pop seq=1..1000000 -rate threads=20 -node 127.0.0.1
```

#### User profiles

Commands mentioned above are very limited. They do not, for example, allow to test other native types than `blob`.

To test more complex schemas, make use of user profiles (`user` command). User profiles allow to define custom schemas and custom statements used to stress the database.

Users can define custom statements via user profile yaml file. See the exemplary yaml files under `tools/util/profiles`. The path to profile file can be provided via `profile=` parameter of `user` command.

Notice that the tool reserves an `insert` operation name and predefines the behaviour
of this operation. User can execute this operation (with a given sample ratio weight)
by providing it to `ops()` parameter along with other operations defined by the user in the yaml file. This operation will simply generate and insert a full row to the stressed table. It's analogous to `write` command - the only difference is that it operates on the custom schema.

To enable the `user` mode, the tool needs to be compiled with `user-profile` feature. This feature is enabled by default.

#### Strongly consistent keyspaces

ScyllaDB supports strongly consistent (Raft-per-tablet) tables, where each tablet has a
Raft group with a distinguished leader that coordinates writes and most reads. To benchmark
these meaningfully the client must route requests to the leader rather than spreading them
across replicas — otherwise most requests land on a follower and are bounced to the leader,
adding a hop that destroys the measurement.

`cql-stress` can create such a keyspace itself. `consistency` is a **cql-stress extension**
to the `replication(...)` sub-parameters: it is not passed through into the CQL replication
map, but lifted out into the top-level `consistency` keyspace property.

```
cql-stress-cassandra-stress write cl=QUORUM duration=10m \
  -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3,consistency=global)' \
  -rate threads=100 -node 127.0.0.1
```

emits

```sql
CREATE KEYSPACE IF NOT EXISTS "keyspace1"
  WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}
  AND consistency = 'global';
```

Accepted values are `global` and `eventual`. When `consistency` is not given the clause is
omitted from the DDL entirely, so existing invocations are unaffected.

Requirements and caveats:

- The server must run with `--experimental-features=strongly-consistent-tables`. Without it
  the `consistency` option is rejected outright, and the driver's leader-aware
  `TABLETS_ROUTING_V2` extension is not advertised at all.
- The keyspace must be tablet-based. `NetworkTopologyStrategy` enables tablets by default on
  recent ScyllaDB; `SimpleStrategy` keyspaces may not get tablets, and non-tablet keyspaces
  reject the `consistency` option.
- Leader routing is **disabled at `cl=one` and `cl=local_one`**, which keep normal spread
  routing. Note that `local_one` is the default — pass `cl=QUORUM` explicitly.
- `CREATE KEYSPACE IF NOT EXISTS` will not upgrade a pre-existing eventually consistent
  keyspace. Drop it between consistency-mode changes.
- Only `write`/`counterwrite` create the keyspace; a `read`-only run requires it to already
  exist.

When `consistency=global` is requested, `cql-stress` reads the keyspace's consistency mode
back from the driver's cluster metadata at startup and **fails immediately** if it is not
`Global`, rather than producing plausible but meaningless numbers.

#### Verifying leader routing

Pass `-log coordinators=true` to tally operations per coordinator host. The distribution is
printed in the run summary:

```
Operations per coordinator:
  172.17.0.3:9042                   412031 ( 68.4%)
  172.17.0.2:9042                   109882 ( 18.2%)
  172.17.0.4:9042                    80512 ( 13.4%)
```

Interpreting it:

- On a **strongly consistent** keyspace at `cl=quorum`, the distribution must follow the
  leader distribution across tablets. It will not be uniform.
- On an **eventually consistent** keyspace — the control — the same workload spreads across
  all replicas.
- On a strongly consistent keyspace at `cl=local_one`, the distribution also spreads, because
  the driver disables leader routing at that level.

Run the control comparison; a single distribution in isolation does not prove much. Accounting
is off by default and the per-coordinator map is not touched at all when disabled.

Note that the driver only learns a tablet's leader ordering after it has seen a
`TABLETS_ROUTING_V2` payload for it, so the first requests to each tablet are not leader-routed.
Discard a warm-up window before drawing conclusions from a short run.

## Development

### Prerequisites

You need the `cargo` command and OpenSSL development libraries to build the tool.

**Install system dependencies:**

On Ubuntu/Debian/Linux Mint:
```bash
sudo apt update && sudo apt install libssl-dev pkg-config
```

On Fedora/RHEL/CentOS:
```bash
sudo dnf install openssl-devel pkg-config
```

On macOS:
```bash
brew install openssl pkg-config
```

### Building

```bash
# Development build (fastest compilation)
cargo build

# Release build (optimized, suitable for most benchmarking)
cargo build --release

# Distribution build (maximum optimization with LTO, used in CI/CD)
# Use this for production releases, but note it has longer build times
cargo build --profile dist
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
