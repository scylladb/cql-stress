import os
import random

import pytest
from util.scylla_docker import ScyllaDockerNode
from util.cassandra_stress import CassandraStress, CqlStressCassandraStress, CSCliRuntimeArguments
from test_cs_write_and_validate import run as run_write_and_validate
from test_cs_equal_db import run as run_equal_db, run_user
from test_hdr_logging import run as run_hdr_logging
from test_cs_strong_consistency import (
    leader_aware_routing_supported,
    run_eventually_consistent_keyspace_is_rejected,
    run_local_one_warns,
    run_strong_consistency,
)


# Utils for test cases

@pytest.fixture
def default_runtime_args():
    return CSCliRuntimeArguments(workload_size="100", concurrency="1", hdr_log_file=None, log_interval=1, throttle=None)

@pytest.fixture
def hdr_log_runtime_args():
    # Throttle to 1000 ops/sec with 3000 operations to ensure test runs for at least 3 seconds
    return CSCliRuntimeArguments(workload_size="3000", concurrency="1", hdr_log_file="test.hdr", log_interval=1, throttle="1000/s")

DEFAULT_SCYLLA_URI = "127.0.0.1:9042"
# The `strong-consistency` compose profile binds the same ports as the ordinary test
# node - the two are mutually exclusive. Override with SCYLLA_SC_URI if needed.
DEFAULT_SCYLLA_SC_URI = "127.0.0.1:9042"


@pytest.fixture
def scylla_docker_node():
    scylla_uri = os.getenv("SCYLLA_URI", DEFAULT_SCYLLA_URI).split(':', 1)
    return ScyllaDockerNode(ip=scylla_uri[0], port=scylla_uri[1])


@pytest.fixture
def cassandra_stress():
    return CassandraStress()


@pytest.fixture
def cql_stress():
    return CqlStressCassandraStress()


# Test cases


def test_write_and_validate(default_runtime_args, scylla_docker_node,
                            cassandra_stress, cql_stress):
    run_write_and_validate(runtime_args=default_runtime_args, node=scylla_docker_node,
                           cs=cassandra_stress, cql_stress=cql_stress)

def test_write_and_validate_with_hdr_log(hdr_log_runtime_args, scylla_docker_node,
                                        cql_stress):
    run_hdr_logging(runtime_args=hdr_log_runtime_args, node=scylla_docker_node,
                           cql_stress=cql_stress)


def test_equal_db(default_runtime_args, scylla_docker_node,
                  cassandra_stress, cql_stress):
    run_equal_db(runtime_args=default_runtime_args, node=scylla_docker_node,
                 cs=cassandra_stress, cql_stress=cql_stress)


def test_user_blob_type(default_runtime_args, scylla_docker_node,
                        cassandra_stress, cql_stress):
    run_user(runtime_args=default_runtime_args, type_name="blob",
             node=scylla_docker_node, cs=cassandra_stress, cql_stress=cql_stress)


def test_user_text_type(default_runtime_args, scylla_docker_node,
                        cassandra_stress, cql_stress):
    run_user(runtime_args=default_runtime_args, type_name="text",
             node=scylla_docker_node, cs=cassandra_stress, cql_stress=cql_stress)


def test_user_tinyint_type(default_runtime_args, scylla_docker_node,
                           cassandra_stress, cql_stress):
    run_user(runtime_args=default_runtime_args, type_name="tinyint",
             node=scylla_docker_node, cs=cassandra_stress, cql_stress=cql_stress)


def test_user_smallint_type(default_runtime_args, scylla_docker_node,
                            cassandra_stress, cql_stress):
    run_user(runtime_args=default_runtime_args, type_name="smallint",
             node=scylla_docker_node, cs=cassandra_stress, cql_stress=cql_stress)


def test_user_int_type(default_runtime_args, scylla_docker_node,
                       cassandra_stress, cql_stress):
    run_user(runtime_args=default_runtime_args, type_name="int",
             node=scylla_docker_node, cs=cassandra_stress, cql_stress=cql_stress)


def test_user_bigint_type(default_runtime_args, scylla_docker_node,
                          cassandra_stress, cql_stress):
    run_user(runtime_args=default_runtime_args, type_name="bigint",
             node=scylla_docker_node, cs=cassandra_stress, cql_stress=cql_stress)


# Test for booleans is missing, since we are not compatible with original c-s.
# C-s has a bug and always generates `true` value.


def test_user_float_type(default_runtime_args, scylla_docker_node,
                         cassandra_stress, cql_stress):
    run_user(runtime_args=default_runtime_args, type_name="float",
             node=scylla_docker_node, cs=cassandra_stress, cql_stress=cql_stress)


def test_user_double_type(default_runtime_args, scylla_docker_node,
                          cassandra_stress, cql_stress):
    run_user(runtime_args=default_runtime_args, type_name="double",
             node=scylla_docker_node, cs=cassandra_stress, cql_stress=cql_stress)


def test_user_inet_type(default_runtime_args, scylla_docker_node,
                        cassandra_stress, cql_stress):
    run_user(runtime_args=default_runtime_args, type_name="inet",
             node=scylla_docker_node, cs=cassandra_stress, cql_stress=cql_stress)


def test_user_varint_type(default_runtime_args, scylla_docker_node,
                          cassandra_stress, cql_stress):
    run_user(runtime_args=default_runtime_args, type_name="varint",
             node=scylla_docker_node, cs=cassandra_stress, cql_stress=cql_stress)


def test_user_decimal_type(default_runtime_args, scylla_docker_node,
                           cassandra_stress, cql_stress):
    run_user(runtime_args=default_runtime_args, type_name="decimal",
             node=scylla_docker_node, cs=cassandra_stress, cql_stress=cql_stress)


def test_user_uuid_type(default_runtime_args, scylla_docker_node,
                        cassandra_stress, cql_stress):
    run_user(runtime_args=default_runtime_args, type_name="uuid",
             node=scylla_docker_node, cs=cassandra_stress, cql_stress=cql_stress)


# Strong consistency (Raft-per-tablet) tests.
#
# These target a separate node started with
# `--experimental-features=strongly-consistent-tables` - see the `strong-consistency`
# profile in docker/scylla-test/compose.yml. They are additive: the eventually consistent
# suite above must stay green unchanged.


@pytest.fixture
def strong_consistency_node():
    scylla_uri = os.getenv("SCYLLA_SC_URI", DEFAULT_SCYLLA_SC_URI).split(':', 1)
    return ScyllaDockerNode(ip=scylla_uri[0], port=scylla_uri[1])


@pytest.fixture
def strong_consistency_session(strong_consistency_node, cql_stress):
    """A CQL session against the strongly-consistent-tables node.

    Skips - rather than fails - when that node is not running or cannot support
    leader-aware routing, so the suite stays runnable against an ordinary ScyllaDB.
    """
    try:
        from cassandra.cluster import Cluster
    except ImportError:
        pytest.skip("scylla-driver is not installed")

    try:
        cluster = Cluster([strong_consistency_node.ip],
                          port=int(strong_consistency_node.port))
        session = cluster.connect()
    except Exception as e:
        pytest.skip(f"strongly-consistent-tables node is not reachable: {e}")

    try:
        unsupported = leader_aware_routing_supported(
            session, strong_consistency_node, cql_stress)
        if unsupported:
            pytest.skip(unsupported)
        yield session
    finally:
        cluster.shutdown()


@pytest.fixture
def strong_consistency_keyspace(strong_consistency_session):
    """A keyspace name that is guaranteed not to exist yet.

    `CREATE KEYSPACE IF NOT EXISTS` will not upgrade a leftover eventually consistent
    keyspace, so a stale one from a previous run would make the test meaningless.
    """
    keyspace = f"ks_sc_{random.randint(0, 100000)}"
    strong_consistency_session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
    yield keyspace
    strong_consistency_session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")


def test_strong_consistency(strong_consistency_node, strong_consistency_session,
                            strong_consistency_keyspace, cql_stress):
    run_strong_consistency(node=strong_consistency_node,
                           session=strong_consistency_session,
                           cql_stress=cql_stress,
                           keyspace=strong_consistency_keyspace)


def test_strong_consistency_local_one_warns(strong_consistency_node,
                                            strong_consistency_keyspace, cql_stress):
    run_local_one_warns(node=strong_consistency_node, cql_stress=cql_stress,
                        keyspace=strong_consistency_keyspace)


def test_strong_consistency_rejects_eventually_consistent_keyspace(
        strong_consistency_node, strong_consistency_keyspace, cql_stress):
    run_eventually_consistent_keyspace_is_rejected(
        node=strong_consistency_node, cql_stress=cql_stress,
        keyspace=strong_consistency_keyspace)
