#! /usr/bin/env python3

"""Tests for strongly consistent (Raft-per-tablet) keyspaces and leader-aware routing.

These require a ScyllaDB started with `--experimental-features=strongly-consistent-tables`
(see the `strong-consistency` profile in docker/scylla-test/compose.yml). Without it the
server does not accept the `consistency` keyspace option *and* does not advertise the
driver's TABLETS_ROUTING_V2 extension, so there is nothing to test - the tests skip.

What is being guarded here is not that a run produces numbers, but that a run which has
silently lost strong consistency cannot produce numbers at all.
"""

import random

from util.cassandra_stress import CqlStressCassandraStress
from util.scylla_docker import ScyllaDockerNode


def leader_aware_routing_supported(session, node, cql_stress) -> str:
    """Returns "" when the server can support leader-aware routing, else why not.

    Two independent server capabilities are needed, and a released ScyllaDB can have the
    first without the second:

    1. the `strongly-consistent-tables` experimental feature, which makes
       `consistency = 'global'` an accepted keyspace option. Probed with real DDL, not by
       inspecting metadata: the `consistency` column of `system_schema.scylla_keyspaces`
       exists even when the feature is disabled, so its presence proves nothing.

    2. the `TABLETS_ROUTING_V2_EXPERIMENTAL` protocol extension. Without it the driver
       never learns a leader-ordered replica list, so requests are spread over the
       tablet's replicas and bounced to the leader. There is no CQL-visible signal for
       this - it is advertised in the `SUPPORTED` frame - so it is probed through
       cql-stress, which asks the node itself and refuses to run without it.
    """
    probe = f"ks_sc_probe_{random.randint(0, 100000)}"
    session.execute(f"DROP KEYSPACE IF EXISTS {probe}")
    try:
        try:
            session.execute(
                f"CREATE KEYSPACE {probe} WITH REPLICATION = "
                "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                "AND consistency = 'global'"
            )
        except Exception as e:
            return f"server does not support the strongly-consistent-tables feature: {e}"

        # n must stay above 1: a single-operation run cannot build its sequence
        # distribution, and the probe would report every server as unsupported.
        result = cql_stress.run_raw(
            stress_args("write", node, probe, cl="QUORUM", consistency="global", n=10),
            check=False)
        if "Leader-aware routing: enabled" not in result.stdout:
            return ("server does not advertise TABLETS_ROUTING_V2_EXPERIMENTAL, so the "
                    "driver cannot do leader-aware routing (the keyspace is 'global' "
                    "server-side, but no leader-ordered replica list ever reaches the "
                    "driver)")
    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {probe}")
    return ""


def keyspace_consistency(session, keyspace: str):
    """The keyspace's `consistency` property, or None if unset/absent."""
    rows = list(session.execute(
        "SELECT consistency FROM system_schema.scylla_keyspaces "
        "WHERE keyspace_name = %s", (keyspace,)
    ))
    return rows[0].consistency if rows else None


def schema_args(keyspace: str, consistency: str = None, replication_factor: int = 1):
    replication = [
        "strategy=NetworkTopologyStrategy",
        f"replication_factor={replication_factor}",
    ]
    if consistency:
        replication.append(f"consistency={consistency}")
    return ["-schema", f"replication({','.join(replication)})", f"keyspace={keyspace}"]


def stress_args(command, node: ScyllaDockerNode, keyspace, cl, consistency=None,
                n=200, coordinators=False):
    # The strongly consistent node runs on a non-default port, so address it explicitly.
    args = [command, "no-warmup", f"n={n}", f"cl={cl}",
            "-node", f"{node.ip}:{node.port}",
            "-rate", "threads=4"]
    args += schema_args(keyspace, consistency)
    if coordinators:
        args += ["-log", "coordinators=true"]
    return args


def run_strong_consistency(node: ScyllaDockerNode, session,
                           cql_stress: CqlStressCassandraStress, keyspace: str):
    """cql-stress creates the strongly consistent keyspace itself and drives it."""
    print("\n=== Writing to a strongly consistent keyspace at cl=QUORUM ===\n")
    result = cql_stress.run_raw(stress_args(
        "write", node, keyspace, cl="QUORUM", consistency="global", coordinators=True))

    assert "consistency mode: Global" in result.stdout, (
        "cql-stress did not report the keyspace as strongly consistent")
    assert "Leader-aware routing: enabled" in result.stdout, (
        "cql-stress did not confirm that the node can route to tablet leaders")
    assert "Operations per coordinator:" in result.stdout, (
        "coordinator accounting was requested but not reported")

    # This is the exact row the driver reads to enable leader-aware routing.
    assert keyspace_consistency(session, keyspace) == "global", (
        f"{keyspace} is not recorded as globally consistent in system_schema.scylla_keyspaces")

    print("\n=== Reading back at cl=QUORUM ===\n")
    result = cql_stress.run_raw(stress_args(
        "read", node, keyspace, cl="QUORUM", consistency="global", coordinators=True))
    assert "consistency mode: Global" in result.stdout


def run_local_one_warns(node: ScyllaDockerNode,
                        cql_stress: CqlStressCassandraStress, keyspace: str):
    """cl=local_one disables leader routing; cql-stress must warn, not refuse to start.

    Whether the run then *completes* is the server's call, not ours: a ScyllaDB that
    supports leader-aware routing rejects strongly consistent writes below
    QUORUM/LOCAL_QUORUM outright, so every operation may fail. What is guarded here is
    that cql-stress lets the run start and says why the measurement would be
    meaningless - the warning is the only thing standing between a user and a
    plausible-looking result set that never went near a leader.
    """
    print("\n=== Writing to a strongly consistent keyspace at cl=local_one ===\n")
    result = cql_stress.run_raw(stress_args(
        "write", node, keyspace, cl="local_one", consistency="global"), check=False)

    output = result.stdout + result.stderr
    assert "disables leader-aware routing" in result.stdout, (
        "no warning was emitted for a strongly consistent keyspace driven at cl=local_one")
    assert result.returncode == 0 or "QUORUM/LOCAL_QUORUM" in output, (
        f"the run failed for a reason other than the server rejecting the CL:\n{output}")


def run_eventually_consistent_keyspace_is_rejected(
        node: ScyllaDockerNode, cql_stress: CqlStressCassandraStress, keyspace: str):
    """A leftover eventually consistent keyspace must be a startup failure.

    `CREATE KEYSPACE IF NOT EXISTS` no-ops over it, so without this guard the run would
    quietly measure eventual consistency and report plausible numbers.
    """
    print("\n=== Creating an eventually consistent keyspace ===\n")
    cql_stress.run_raw(stress_args("write", node, keyspace, cl="QUORUM"))

    print("\n=== Requesting consistency=global over it - must fail at startup ===\n")
    result = cql_stress.run_raw(stress_args(
        "write", node, keyspace, cl="QUORUM", consistency="global"), check=False)

    output = result.stdout + result.stderr
    assert result.returncode != 0, (
        "run succeeded against an eventually consistent keyspace despite consistency=global")
    assert "Results:" not in result.stdout, (
        "the run produced a result summary; it must fail before doing any work")
    # Either the startup guard catches it, or the server rejects the DDL outright -
    # whether `CREATE KEYSPACE IF NOT EXISTS` validates options for an existing
    # keyspace is up to the server. Both are correct; silently proceeding is not.
    assert ("Requested consistency=global" in output
            or "Failed to create schema" in output), (
        f"failed, but for an unexpected reason:\n{output}")
