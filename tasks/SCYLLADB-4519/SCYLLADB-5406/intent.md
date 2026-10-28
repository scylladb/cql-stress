# SCYLLADB-5406 — many driver connections per shard in the verified load

## Problem

`cql-stress-sc-verify` always talks to the cluster over one driver connection
per shard per node. All clients of a checked row share the single connection
to the shard that owns the row's tablet. Raising `--clients-per-row` adds
requests on that connection, never connections. A verified load therefore
can't test strongly consistent tables under many client connections to the
same shard and row, which is what real applications with large connection
pools do.

The cassandra-stress front end of the same repository can already set the
connection pool per shard or per host, the local port range of shard-aware
connections, and address reuse on connection sockets; the verifier can't set
any of them. Without the last two, one loader IP is capped at 16,384
shard-aware connections across all nodes.

## Who it affects

QA engineers who run the verified load in SCT on strongly consistent tables:
the check job and the research job in `configurations/sc_verify/`
(scylla-cluster-tests), and any run that wants to prove linearizability while
the cluster serves many connections.

## Evidence

Jira: SCYLLADB-5406, a subtask of SCYLLADB-4519.

The verifier builds its sessions without a pool size, so the driver uses its
default of one connection per shard
(`src/bin/cql-stress-sc-verify/startup.rs`):

```rust
pub async fn connect(cli: &Cli, tls: Option<SslContext>) -> Result<Session> {
    let profile = checked_profile(cli.consistency, cli.request_timeout);
    let mut builder = SessionBuilder::new()
        .known_nodes(&cli.nodes)
        .default_execution_profile_handle(profile.into_handle());
    if let (Some(user), Some(password)) = (&cli.user, &cli.password) {
        builder = builder.user(user, password);
    }
    if let Some(tls) = tls {
        builder = builder.tls_context(Some(tls));
    }
    builder
        .build()
        .await
        .with_context(|| format!("Failed to connect to {}", cli.nodes.join(",")))
```

The cassandra-stress front end exposes the pool size
(`src/bin/cql-stress-cassandra-stress/settings/option/mode.rs`):

```rust
    let connections_per_shard = parser.simple_param(
        "connectionsPerShard=",
        Some("1"),
        "Number of connections per shard",
        false,
    );
```

with a per-host pool beside it, and the two options that lift the
per-IP ceiling (same file):

```rust
    let connections_per_host = parser.simple_param(
        "connectionsPerHost=",
        None,
        "Number of connections per host",
        false,
    );
    // Both knobs below lift the per-client-IP ceiling on shard-aware connections.
    // The driver binds each shard-aware connection to a local port from a fixed range
    // (49152..65535 by default) without SO_REUSEADDR, which caps one client IP at
    // 16384 such connections across all nodes. The range only matters for a per-shard
    // pool (the driver picks shard-aware ports only for `PoolSize::PerShard`), so it is
    // absent from the `connectionsPerHost` group; SO_REUSEADDR is set on every
    // connection socket and stays available in both.
    let shard_aware_port_range = parser.simple_param(
        "shardAwarePortRange=",
        None,
        "Inclusive local port range for shard-aware connections, e.g. 1024..65535 (default: 49152..65535; connectionsPerShard only)",
        false,
    );
    let tcp_reuse_address = parser.simple_param(
        "tcpReuseAddress=",
        None,
        "Set SO_REUSEADDR on connection sockets (true/false), so a local port can be reused towards each node",
        false,
    );
```

and passes them to the driver (`src/bin/cql-stress-cassandra-stress/main.rs`):

```rust
    builder = builder.pool_size(settings.mode.pool_size);
    if let Some(range) = settings.mode.shard_aware_port_range.clone() {
        builder = builder.shard_aware_local_port_range(range);
    }
    if let Some(reuse) = settings.mode.tcp_reuse_address {
        builder = builder.tcp_reuse_address(reuse);
    }
```

Research run c2594c71 (Argus, 2026-10-14; 6 nodes × 14 shards, 4 loaders with
1000, 16, 32 and 48 clients per row, 2,000 to 8,192 clients each), Prometheus
at 12:50 UTC under load:

```
sum(scylla_transport_current_connections) = 340
sum by (instance) (scylla_transport_current_connections):
  10.4.11.130 57, 10.4.11.224 56, 10.4.8.127 57, 10.4.8.159 57, 10.4.8.230 56, 10.4.8.81 57
```

340 = 4 loaders × (84 shard connections + 1 control connection): one
connection per shard per loader, whatever the loader's client count.

## What good looks like

A verified-load run can choose how many connections each loader opens, per
shard or per host, and can lift the 16,384 shard-aware connections ceiling of
one loader IP with the local port range and address reuse. With N per shard,
the cluster's client connection metrics show about nodes × shards × N
connections from each loader, the checked rows' clients spread their requests
over those connections, and the run's verdicts are unchanged in meaning.

## Out of scope

- Checking more than 16 operations in flight on one row: that needs a
  different checker, not more connections.
- The SCT configs that use the new options; they follow once an image with
  them exists.
