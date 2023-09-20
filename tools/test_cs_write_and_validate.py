#! /usr/bin/env python3

from util.cassandra_stress import CassandraStress, CqlStressCassandraStress
from util.cassandra_stress import generate_random_keyspaces, CSCliRuntimeArguments
from util.scylla_docker import ScyllaDockerNode

# This test populates the DB with cassandra-stress,
# validates the contents with cql-stress-cassandra-stress,
# and vice-versa.


def run(runtime_args: CSCliRuntimeArguments, node: ScyllaDockerNode,
        cs: CassandraStress, cql_stress: CqlStressCassandraStress):
    keyspaces = generate_random_keyspaces()
    ks_cassandra = keyspaces.ks_cassandra
    ks_cqlstress = keyspaces.ks_cqlstress

    # All setup, start the test.
    print("\n=== Starting the test... ===")

    # Populate DB with cassandra-stress.
    print(
        f"\n=== Populating database (keyspace={ks_cassandra}) with cassandra-stress... ===\n")
    cs.run(command="write", node_ip=node.ip,
           keyspace=ks_cassandra, runtime_args=runtime_args)
    # Validate contents with cql-stress.
    print(
        f"\n=== Validating database contents (keyspace={ks_cassandra}) with cql-stress... ===\n")
    cql_stress.run(command="read", node_ip=node.ip,
                   keyspace=ks_cassandra, runtime_args=runtime_args)

    # Populate DB with cql-stress.
    print(
        f"\n=== Populating database (keyspace={ks_cqlstress}) with cql-stress... ===\n")
    cql_stress.run(command="write", node_ip=node.ip,
                   keyspace=ks_cqlstress, runtime_args=runtime_args)
    # Validate contents with cassandra-stress
    print(
        f"\n=== Validating database contents (keyspace={ks_cqlstress}) with cassandra-stress... ===\n")
    cql_stress.run(command="read", node_ip=node.ip,
                   keyspace=ks_cqlstress, runtime_args=runtime_args)

    print("\n=== Test successful ===\n")
