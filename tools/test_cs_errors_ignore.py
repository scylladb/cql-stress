#! /usr/bin/env python3

import subprocess

from util.cassandra_stress import (
    CqlStressCassandraStress,
    CSCliRuntimeArguments,
    generate_random_keyspaces,
)
from util.scylla_docker import ScyllaDockerNode


def prepare_missing_rows_read_args(
    node_ip, keyspace, runtime_args: CSCliRuntimeArguments, error_args
):
    workload_size = int(runtime_args.workload_size)
    population_start = workload_size + 1
    population_end = 2 * workload_size

    return [
        "read",
        "no-warmup",
        f"n={workload_size}",
        "-node",
        node_ip,
        "-rate",
        f"threads={runtime_args.concurrency}",
        "-schema",
        f"keyspace={keyspace}",
        "-pop",
        f"seq={population_start}..{population_end}",
    ] + error_args


def parse_total_errors(output):
    for line in output.splitlines():
        if line.startswith("Total errors"):
            return int(line.split(":")[1].strip())
    return None


def run(
    runtime_args: CSCliRuntimeArguments,
    node: ScyllaDockerNode,
    cql_stress: CqlStressCassandraStress,
):
    keyspaces = generate_random_keyspaces()
    ks_cqlstress = keyspaces.ks_cqlstress

    print("\n=== Starting the -errors ignore test... ===")

    cql_stress.run(
        command="write",
        node_ip=node.ip,
        keyspace=ks_cqlstress,
        runtime_args=runtime_args,
    )

    print("\n=== Reading the rows that were never inserted ===\n")
    failing_run = subprocess.run(
        args=cql_stress.stress_cmd
        + prepare_missing_rows_read_args(node.ip, ks_cqlstress, runtime_args, []),
        capture_output=True,
        text=True,
        check=False,
    )
    print(failing_run.stdout)

    if failing_run.returncode == 0:
        raise RuntimeError(
            "The benchmark must fail when the read errors are not ignored. "
            f"Stderr: {failing_run.stderr}"
        )

    print("\n=== Reading the same rows with -errors ignore ===\n")
    ignoring_run = subprocess.run(
        args=cql_stress.stress_cmd
        + prepare_missing_rows_read_args(
            node.ip, ks_cqlstress, runtime_args, ["-errors", "ignore", "retries=1"]
        ),
        capture_output=True,
        text=True,
        check=False,
    )
    print(ignoring_run.stdout)

    if ignoring_run.returncode != 0:
        raise RuntimeError(
            f"The benchmark must succeed with -errors ignore. Stderr: {ignoring_run.stderr}"
        )

    total_errors = parse_total_errors(ignoring_run.stdout)
    if total_errors is None:
        raise RuntimeError("The summary must contain a 'Total errors' line")
    if total_errors == 0:
        raise RuntimeError("The summary must report the ignored errors, but reports 0")

    print(
        f"\n=== -errors ignore test successful ({total_errors} errors reported) ===\n"
    )
