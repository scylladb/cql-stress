import os

import pytest
from util.scylla_docker import ScyllaDockerNode
from util.cassandra_stress import CassandraStress, CqlStressCassandraStress, CSCliRuntimeArguments
from test_cs_write_and_validate import run as run_write_and_validate
from test_cs_equal_db import run as run_equal_db, run_user


# Utils for test cases

@pytest.fixture
def default_runtime_args():
    return CSCliRuntimeArguments(workload_size="100", concurrency="1")


DEFAULT_SCYLLA_URI = "127.0.0.1:9042"


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
