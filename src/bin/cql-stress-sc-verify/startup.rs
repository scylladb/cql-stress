//! Start-up: create the schema from the profile, then refuse to run against a keyspace that
//! is not strongly consistent or a table that differs from the profile (spec §7.1, §15.3).

use anyhow::{Context, Result};
use cql_stress::strong_consistency::{keyspace_consistency_mode, unavailable_error};
use openssl::ssl::SslContext;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::client::PoolSize;
use scylla::cluster::metadata::ConsistencyMode;
use scylla::routing::ShardAwarePortRange;

use crate::cli::Cli;
use crate::ops::checked_profile;
use crate::profile::Profile;

/// One column as `system_schema.columns` describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub cql_type: String,
    pub kind: String,
}

/// A session of the run, checked or bulk: every statement on it runs with [`checked_profile`],
/// over the pool of the connection flags (spec §15.4).
pub async fn connect(cli: &Cli, tls: Option<SslContext>) -> Result<Session> {
    let profile = checked_profile(cli.consistency, cli.request_timeout);
    let pool_size = cli.pool_size();
    let mut builder = SessionBuilder::new()
        .known_nodes(&cli.nodes)
        .default_execution_profile_handle(profile.into_handle())
        .pool_size(pool_size)
        .tcp_reuse_address(cli.tcp_reuse_address);
    if let PoolSize::PerShard(_) = pool_size {
        builder = builder.shard_aware_local_port_range(
            ShardAwarePortRange::new(cli.shard_aware_port_range.clone())
                .context("Invalid --shard-aware-port-range")?,
        );
    }
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
}

/// The shard count of every node that reports one. The session waits for every pool's first
/// connection, so this is known once [`connect`] returns. A node without one is not sharded:
/// its connections are not shard-aware and take no port of the range.
pub fn shard_counts(session: &Session) -> Vec<usize> {
    session
        .get_cluster_state()
        .get_nodes_info()
        .iter()
        .filter_map(|node| node.sharder())
        .map(|sharder| sharder.nr_shards.get().into())
        .collect()
}

/// A per-shard pool must fit its local port range (spec §15.4), or the run would hold fewer
/// connections than its `start` line claims. Without address reuse every shard-aware
/// connection of the loader takes its own port; with it, only those towards one node.
pub fn check_pool(cli: &Cli, sessions: usize, shards: &[usize]) -> Result<()> {
    let PoolSize::PerShard(n) = cli.pool_size() else {
        return Ok(());
    };
    let range = &cli.shard_aware_port_range;
    let ports = usize::from(range.end() - range.start()) + 1;
    let (shards, towards, fix) = if cli.tcp_reuse_address {
        let busiest = shards.iter().copied().max().unwrap_or(0);
        (busiest, " towards one node", "widen the range")
    } else {
        let all = shards.iter().sum();
        (all, "", "widen the range or set --tcp-reuse-address true")
    };
    let needed = sessions * shards * n.get();
    anyhow::ensure!(
        needed <= ports,
        "the per-shard pool needs {sessions} session{} × {shards} shards × {n} = {needed} local \
         ports{towards}, and --shard-aware-port-range {}..{} has {ports}; {fix}",
        if sessions == 1 { "" } else { "s" },
        range.start(),
        range.end()
    );
    Ok(())
}

/// Creates whatever of the schema is missing, then checks all of it. Every error here is a
/// set-up failure: nothing has been written yet.
pub async fn startup(session: &Session, profile: &Profile, cli: &Cli) -> Result<()> {
    let keyspace = &profile.keyspace;
    session
        .query_unpaged(profile.keyspace_ddl(!cli.unsafe_eventual), ())
        .await
        .with_context(|| format!("Failed to create keyspace {keyspace}"))?;

    // Before any table goes into it: a leftover eventually consistent keyspace is not
    // upgraded by CREATE KEYSPACE IF NOT EXISTS.
    let mode = keyspace_consistency_mode(session, keyspace).await?;
    if cli.unsafe_eventual {
        // The control needs a keyspace whose reads may really be stale.
        anyhow::ensure!(
            !matches!(mode, Some(ConsistencyMode::Global)),
            "--unsafe-eventual needs an eventually consistent keyspace; {keyspace} is strongly \
             consistent"
        );
    } else if !matches!(mode, Some(ConsistencyMode::Global)) {
        let reported = match &mode {
            Some(mode) => format!("{mode:?}"),
            None => String::from("unknown (keyspace not found in cluster metadata)"),
        };
        return Err(unavailable_error(
            keyspace,
            &reported,
            &profile.keyspace_ddl(true),
            &cli.nodes,
            cli.ssl,
        )
        .await);
    }

    let mut tables = vec![profile.table.as_str()];
    if profile.bulk_table_name() != profile.table {
        tables.push(profile.bulk_table_name());
    }
    let expected = expected_columns(profile);
    for table in tables {
        session
            .query_unpaged(profile.table_ddl(table), ())
            .await
            .with_context(|| format!("Failed to create table {keyspace}.{table}"))?;
        let live = live_columns(session, keyspace, table).await?;
        let mismatches = diff(&expected, &live);
        anyhow::ensure!(
            mismatches.is_empty(),
            "Table {keyspace}.{table} does not match the profile:\n  {}",
            mismatches.join("\n  ")
        );
    }
    Ok(())
}

/// The columns the profile defines, in table order.
fn expected_columns(profile: &Profile) -> Vec<Column> {
    let key = [
        ("pk", "bigint", "partition_key"),
        ("gen", "bigint", "clustering"),
        ("ck", "int", "clustering"),
    ]
    .map(|(name, cql_type, kind)| (name.to_owned(), cql_type, kind));
    let cells = profile
        .cells
        .iter()
        .enumerate()
        .map(|(i, cell)| (format!("c{i}"), cell.cql_name(), "regular"));
    key.into_iter()
        .chain(cells)
        .map(|(name, cql_type, kind)| Column {
            name,
            cql_type: cql_type.to_owned(),
            kind: kind.to_owned(),
        })
        .collect()
}

async fn live_columns(session: &Session, keyspace: &str, table: &str) -> Result<Vec<Column>> {
    let rows = session
        .query_unpaged(
            "SELECT column_name, type, kind FROM system_schema.columns \
             WHERE keyspace_name = ? AND table_name = ?",
            (keyspace, table),
        )
        .await
        .with_context(|| format!("Failed to read the columns of {keyspace}.{table}"))?
        .into_rows_result()
        .context("Failed to read the columns as rows")?;
    rows.rows::<(String, String, String)>()?
        .map(|row| {
            let (name, cql_type, kind) = row?;
            Ok(Column {
                name,
                cql_type,
                kind,
            })
        })
        .collect()
}

/// One line per column that differs, in profile order, then the columns the profile lacks.
fn diff(expected: &[Column], live: &[Column]) -> Vec<String> {
    let find = |columns: &[Column], name: &str| columns.iter().find(|c| c.name == name).cloned();
    let mut lines = Vec::new();
    for want in expected {
        match find(live, &want.name) {
            None => lines.push(format!(
                "{}: missing from the table, profile says {} ({})",
                want.name, want.cql_type, want.kind
            )),
            Some(have) if have != *want => lines.push(format!(
                "{}: table has {} ({}), profile says {} ({})",
                want.name, have.cql_type, have.kind, want.cql_type, want.kind
            )),
            Some(_) => {}
        }
    }
    for have in live {
        if find(expected, &have.name).is_none() {
            lines.push(format!(
                "{}: table has {} ({}), not in the profile",
                have.name, have.cql_type, have.kind
            ));
        }
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, cql_type: &str, kind: &str) -> Column {
        Column {
            name: name.to_owned(),
            cql_type: cql_type.to_owned(),
            kind: kind.to_owned(),
        }
    }

    fn expected() -> Vec<Column> {
        vec![
            col("pk", "bigint", "partition_key"),
            col("gen", "bigint", "clustering"),
            col("ck", "int", "clustering"),
            col("c0", "bigint", "regular"),
            col("c1", "text", "regular"),
        ]
    }

    #[test]
    fn expected_columns_follow_the_profile_test() {
        let profile = Profile::parse(
            "{keyspace: k, replication_factor: 1, tablets_initial: 1, table: t, \
             cells: [bigint, text], text_size: 32, blob_size: 64}",
        )
        .unwrap();
        assert_eq!(expected_columns(&profile), expected());
    }

    #[test]
    fn equal_tables_have_no_diff_test() {
        let mut live = expected();
        live.reverse(); // system_schema order is not the profile's
        assert!(diff(&expected(), &live).is_empty());
    }

    #[test]
    fn diff_names_every_mismatch_test() {
        let live = vec![
            col("pk", "bigint", "partition_key"),
            col("gen", "bigint", "partition_key"),
            col("ck", "int", "clustering"),
            col("c0", "bigint", "regular"),
            col("c9", "blob", "regular"),
        ];
        assert_eq!(
            diff(&expected(), &live),
            [
                "gen: table has bigint (partition_key), profile says bigint (clustering)",
                "c1: missing from the table, profile says text (regular)",
                "c9: table has blob (regular), not in the profile",
            ]
        );

        let mut changed = expected();
        changed[4].cql_type = "int".to_owned();
        assert_eq!(
            diff(&expected(), &changed),
            ["c1: table has int (regular), profile says text (regular)"]
        );
    }

    fn cli(args: &str) -> Cli {
        Cli::parse_checked(
            [
                "cql-stress-sc-verify",
                "--profile",
                "p.yaml",
                "--duration",
                "1m",
            ]
            .into_iter()
            .chain(args.split_ascii_whitespace()),
        )
        .unwrap()
    }

    #[test]
    fn check_pool_test() {
        let six_nodes = [14; 6];
        // 1 × 84 × 195 = 16,380 of the default 16,384 ports.
        assert!(check_pool(&cli("--connections-per-shard 195"), 1, &six_nodes).is_ok());
        let err = check_pool(&cli("--connections-per-shard 100"), 2, &six_nodes)
            .unwrap_err()
            .to_string();
        assert!(err.contains("16800") && err.contains("16384"), "{err}");
        // With reuse each node has its own ports: 2 × 14 × 100 = 2,800.
        let reuse = cli("--connections-per-shard 100 --tcp-reuse-address true");
        assert!(check_pool(&reuse, 2, &six_nodes).is_ok());
        // A per-host pool uses no port of the range.
        assert!(check_pool(&cli("--connections-per-host 10000"), 2, &six_nodes).is_ok());

        // 10 ports: 2 × (4 + 4) × 1 = 16 don't fit; with reuse 2 × 4 × 1 = 8 do.
        let small = "--shard-aware-port-range 1024..1033";
        assert!(check_pool(&cli(small), 2, &[4, 4]).is_err());
        let small_reuse = cli(&format!("{small} --tcp-reuse-address true"));
        assert!(check_pool(&small_reuse, 2, &[4, 4]).is_ok());
        assert!(check_pool(&small_reuse, 2, &[4, 6]).is_err());
    }

    /// Needs Scylla: `SCYLLA_URI`, default `127.0.0.1:9042`.
    #[tokio::test]
    async fn pool_connects_test() {
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_owned());
        let cli = cli(&format!("--nodes {uri} --connections-per-shard 2"));
        let session = connect(&cli, None).await.unwrap();
        let shards = shard_counts(&session);
        assert!(
            !shards.is_empty() && shards.iter().all(|&n| n >= 1),
            "{shards:?}"
        );
        check_pool(&cli, 1, &shards).unwrap();
    }
}
