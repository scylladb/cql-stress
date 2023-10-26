import os

import pytest
from util.scylla_docker import ScyllaDockerNode
from util.cassandra_stress import CassandraStress, CqlStressCassandraStress, CSCliRuntimeArguments
from test_cs_write_and_validate import run as run_write_and_validate
from test_cs_equal_db import run as run_equal_db


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
