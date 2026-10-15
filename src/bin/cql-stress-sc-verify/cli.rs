//! The command line of `cql-stress-sc-verify`. Defaults follow the sc-verify spec, §15.2.

use std::num::NonZeroUsize;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use cql_stress::java_generate::distribution::parse_population;
use openssl::ssl::{SslContext, SslContextBuilder, SslFiletype, SslMethod, SslVerifyMode};
use scylla::client::PoolSize;
use scylla::routing::ShardAwarePortRange;

use crate::report::Connections;

#[derive(Parser, Debug)]
#[command(
    name = "cql-stress-sc-verify",
    version,
    about = "A verified load for strongly consistent tables"
)]
pub struct Cli {
    /// The YAML profile: the only definition of the keyspace, the table and its cells.
    #[arg(long)]
    pub profile: PathBuf,
    #[arg(long, value_enum, default_value_t = Mode::Both)]
    pub mode: Mode,

    // Connection.
    /// Contact points, comma separated: `host` or `host:port`.
    #[arg(long, value_delimiter = ',', default_value = "127.0.0.1")]
    pub nodes: Vec<String>,
    #[arg(long, requires = "password")]
    pub user: Option<String>,
    #[arg(long, requires = "user")]
    pub password: Option<String>,
    /// Connect over TLS.
    #[arg(long)]
    pub ssl: bool,
    /// PEM file of the CA that signed the server certificates; without it they are not verified.
    #[arg(long, requires = "ssl")]
    pub ssl_ca: Option<PathBuf>,
    /// PEM client certificate, for servers that require one.
    #[arg(long, requires_all = ["ssl", "ssl_key"])]
    pub ssl_cert: Option<PathBuf>,
    /// PEM private key of `--ssl-cert`.
    #[arg(long, requires = "ssl_cert")]
    pub ssl_key: Option<PathBuf>,
    /// Consistency level of the checked stream.
    #[arg(long, value_enum, default_value_t = CheckedConsistency::Quorum)]
    pub consistency: CheckedConsistency,
    /// Driver connections to every shard of every node, per session (spec §15.4).
    #[arg(long, default_value_t = 1, conflicts_with = "connections_per_host")]
    pub connections_per_shard: usize,
    /// Driver connections to every node, per session; not shard-aware.
    #[arg(long)]
    pub connections_per_host: Option<usize>,
    /// Local ports of a per-shard pool, inclusive, e.g. `1024..65535`.
    #[arg(
        long,
        default_value = "49152..65535",
        value_parser = port_range,
        conflicts_with = "connections_per_host"
    )]
    pub shard_aware_port_range: RangeInclusive<u16>,
    /// Set SO_REUSEADDR on connection sockets, so a local port can be reused towards each node.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    pub tcp_reuse_address: bool,

    /// How long to run, e.g. `30s`, `5m`, `4h`.
    #[arg(long, value_parser = parse_duration)]
    pub duration: Option<Duration>,
    /// Number of bulk operations; `--mode bulk` only.
    #[arg(short = 'n')]
    pub ops: Option<u64>,

    // Checked stream (modes verify and both).
    /// Partition keys of checked rows, e.g. `seq=0..1023`.
    #[arg(long, default_value = "seq=0..1023", value_parser = population)]
    pub pop: String,
    #[arg(long, default_value_t = 32)]
    pub slots: usize,
    #[arg(long, default_value_t = 8)]
    pub clients_per_row: usize,
    #[arg(long, default_value_t = 200)]
    pub ops_per_gen: u64,
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    pub max_gen_duration: Duration,
    #[arg(long, default_value_t = 16)]
    pub burst_ops: u64,
    #[arg(long, default_value = "1s", value_parser = parse_duration)]
    pub burst_interval: Duration,
    #[arg(long, default_value_t = 0.5, value_parser = ratio)]
    pub read_ratio: f64,
    #[arg(long, default_value_t = 0.5, value_parser = ratio)]
    pub insert_ratio: f64,
    #[arg(long, default_value = "5s", value_parser = parse_duration)]
    pub request_timeout: Duration,
    #[arg(long, default_value_t = 10)]
    pub sweep_retries: u32,
    #[arg(long, default_value = "1s", value_parser = parse_duration)]
    pub sweep_backoff: Duration,
    /// Seal a row early once this many of its writes are indeterminate.
    #[arg(long, default_value_t = 8)]
    pub max_indeterminate: u64,
    /// Record `Unavailable` as a failed write instead of an indeterminate one.
    #[arg(long)]
    pub unavailable_is_fail: bool,
    #[arg(long, default_value_t = 10)]
    pub max_workload_errors: u64,
    /// Row TTL in seconds; 0 means rows never expire.
    #[arg(long, default_value_t = 0)]
    pub ttl: u32,
    #[arg(long, default_value = "/history")]
    pub history_dir: PathBuf,
    #[arg(long, default_value_t = 50)]
    pub check_rows: usize,
    #[arg(long, default_value = "5m", value_parser = parse_duration)]
    pub check_age: Duration,
    /// Check sealed rows with porcupine_checker; `off` only writes the check files.
    #[arg(long, value_enum, default_value_t = Checker::On)]
    pub checker: Checker,
    #[arg(long, default_value = "/usr/local/bin/porcupine_checker")]
    pub checker_bin: PathBuf,
    #[arg(long, default_value_t = 2)]
    pub checker_workers: usize,
    /// A checker child running longer is killed; its unfinished rows are `unknown`.
    #[arg(long, default_value = "600s", value_parser = parse_duration)]
    pub checker_timeout: Duration,
    /// Memory for one checker child, e.g. `4G` or `512M`.
    #[arg(long, default_value = "4G", value_parser = parse_size)]
    pub checker_mem: u64,
    /// At the end of the run, how long the queued files may still be checked.
    #[arg(long, default_value = "10m", value_parser = parse_duration)]
    pub checker_deadline: Duration,
    /// Check files waiting for a worker; when full, a file is archived unchecked.
    #[arg(long, default_value_t = 256)]
    pub queue_max: usize,
    /// One canary, a deliberately corrupted history, every this many check files.
    #[arg(long, default_value_t = 100)]
    pub canary_every: u64,
    /// The checker's time limit for one row.
    #[arg(long, default_value = "10s", value_parser = parse_duration)]
    pub key_timeout: Duration,
    /// At most this many visualizations of illegal rows per check file.
    #[arg(long, default_value_t = 3)]
    pub max_viz: usize,
    /// Read every retired row back at the end of the run.
    #[arg(long, value_enum, default_value_t = Readback::On)]
    pub readback: Readback,
    /// The share of retired rows the read-back reads.
    #[arg(long, default_value_t = 1.0, value_parser = ratio)]
    pub readback_sample: f64,
    #[arg(long, default_value_t = 64)]
    pub readback_concurrency: usize,

    // Bulk (modes bulk and both).
    #[arg(long, value_enum, default_value_t = BulkOp::Mixed)]
    pub bulk_op: BulkOp,
    #[arg(long, default_value_t = 0.5, value_parser = ratio)]
    pub bulk_read_ratio: f64,
    #[arg(long, default_value = "seq=1099511627776..1099522113535", value_parser = population)]
    pub bulk_pop: String,
    #[arg(long, default_value_t = 64)]
    pub bulk_threads: u64,
    /// Bulk operations per second; 0 means unthrottled.
    #[arg(long, default_value_t = 0)]
    pub bulk_rate: u64,
    #[arg(long, value_enum, default_value_t = BulkReadConsistency::Quorum)]
    pub bulk_read_consistency: BulkReadConsistency,
    #[arg(long, default_value_t = 9)]
    pub bulk_retries: usize,

    #[arg(long, default_value = "10s", value_parser = parse_duration)]
    pub report_interval: Duration,

    /// Test only: with this probability a read returns the cells of the row's first read
    /// instead, a stale read the invariants must catch (milestone 1 "can fail").
    #[arg(long, hide = true, default_value_t = 0.0, value_parser = ratio)]
    pub fault_stale_reads: f64,

    /// Test only: with this probability, right after a write is acknowledged, an older
    /// acknowledged write of the row that a newer one superseded is sent again, unrecorded.
    /// The database applies it, so later reads show values no order explains.
    #[arg(long, hide = true, default_value_t = 0.0, value_parser = ratio)]
    pub fault_replay_writes: f64,

    /// Test only: the keyspace is created, and required to be, eventually consistent, and
    /// `--consistency one` is allowed, so a cluster fault produces real stale reads the
    /// verifier must catch. Never for a real test of strong consistency.
    #[arg(long, hide = true)]
    pub unsafe_eventual: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Verify,
    Bulk,
    Both,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckedConsistency {
    Quorum,
    LocalQuorum,
    /// Only with `--unsafe-eventual`: strongly consistent tables reject it.
    One,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Checker {
    On,
    Off,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Readback {
    On,
    Off,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkOp {
    Write,
    Read,
    Mixed,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkReadConsistency {
    Quorum,
    One,
}

impl Cli {
    /// Parses the arguments and checks the rules clap cannot express.
    pub fn parse_checked<I, T>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let cli = Self::try_parse_from(args)?;
        anyhow::ensure!(
            cli.ops.is_none() || cli.mode == Mode::Bulk,
            "-n is only allowed with --mode bulk; checked rows run for --duration"
        );
        anyhow::ensure!(
            cli.duration.is_some() || cli.ops.is_some(),
            "give --duration, or -n with --mode bulk"
        );
        anyhow::ensure!(
            cli.consistency != CheckedConsistency::One || cli.unsafe_eventual,
            "--consistency one is only for --unsafe-eventual, a test of the verifier itself"
        );
        // Zero here would panic (an interval of 0) or spin (rows sealed with no operations).
        for (name, zero) in [
            ("--burst-interval", cli.burst_interval.is_zero()),
            ("--report-interval", cli.report_interval.is_zero()),
            ("--check-age", cli.check_age.is_zero()),
            ("--slots", cli.slots == 0),
            ("--clients-per-row", cli.clients_per_row == 0),
            ("--ops-per-gen", cli.ops_per_gen == 0),
            ("--burst-ops", cli.burst_ops == 0),
            ("--max-indeterminate", cli.max_indeterminate == 0),
            ("--sweep-retries", cli.sweep_retries == 0),
            ("--check-rows", cli.check_rows == 0),
            ("--checker-workers", cli.checker_workers == 0),
            ("--checker-timeout", cli.checker_timeout.is_zero()),
            ("--checker-mem", cli.checker_mem == 0),
            ("--queue-max", cli.queue_max == 0),
            ("--canary-every", cli.canary_every == 0),
            ("--key-timeout", cli.key_timeout.is_zero()),
            ("--readback-sample", cli.readback_sample == 0.0),
            ("--readback-concurrency", cli.readback_concurrency == 0),
            ("--connections-per-shard", cli.connections_per_shard == 0),
            (
                "--connections-per-host",
                cli.connections_per_host == Some(0),
            ),
        ] {
            anyhow::ensure!(!zero, "{name} must be greater than 0");
        }
        // A cell expiring mid-round would look exactly like a lost write (spec §7.4).
        let longest_round = cli.max_gen_duration
            + cli.request_timeout
            + (cli.sweep_backoff + cli.request_timeout) * cli.sweep_retries;
        anyhow::ensure!(
            cli.ttl == 0 || Duration::from_secs(cli.ttl.into()) >= longest_round * 10,
            "--ttl {} is shorter than 10 rounds of the longest possible length ({}s)",
            cli.ttl,
            (longest_round * 10).as_secs()
        );
        Ok(cli)
    }

    /// The driver pool of every session: per host when `--connections-per-host` is given.
    pub fn pool_size(&self) -> PoolSize {
        // parse_checked rejects 0 for both.
        let n = |n: usize| NonZeroUsize::new(n).unwrap();
        match self.connections_per_host {
            Some(per_host) => PoolSize::PerHost(n(per_host)),
            None => PoolSize::PerShard(n(self.connections_per_shard)),
        }
    }

    /// The pool settings for the `start` line.
    pub fn connections(&self) -> Connections {
        let range = &self.shard_aware_port_range;
        match self.connections_per_host {
            Some(per_host) => Connections {
                pool: "per-host",
                pool_size: per_host,
                shard_aware_port_range: None,
                tcp_reuse_address: self.tcp_reuse_address,
            },
            None => Connections {
                pool: "per-shard",
                pool_size: self.connections_per_shard,
                shard_aware_port_range: Some(format!("{}..{}", range.start(), range.end())),
                tcp_reuse_address: self.tcp_reuse_address,
            },
        }
    }

    /// The TLS context for the sessions, `None` without `--ssl`.
    pub fn tls_context(&self) -> Result<Option<SslContext>> {
        if !self.ssl {
            return Ok(None);
        }
        let mut builder = SslContextBuilder::new(SslMethod::tls())?;
        builder.set_verify(SslVerifyMode::NONE);
        if let Some(ca) = &self.ssl_ca {
            builder
                .set_ca_file(ca)
                .with_context(|| format!("Failed to load --ssl-ca {}", ca.display()))?;
            builder.set_verify(SslVerifyMode::PEER);
        }
        if let (Some(cert), Some(key)) = (&self.ssl_cert, &self.ssl_key) {
            builder
                .set_certificate_file(cert, SslFiletype::PEM)
                .with_context(|| format!("Failed to load --ssl-cert {}", cert.display()))?;
            builder
                .set_private_key_file(key, SslFiletype::PEM)
                .with_context(|| format!("Failed to load --ssl-key {}", key.display()))?;
        }
        Ok(Some(builder.build()))
    }
}

/// `<number><unit>` with the unit one of `ms`, `s`, `m`, `h`.
fn parse_duration(s: &str) -> Result<Duration> {
    let split = s
        .find(|c: char| !c.is_ascii_digit())
        .with_context(|| format!("Duration {s:?} has no unit (ms, s, m, h)"))?;
    let (number, unit) = s.split_at(split);
    let number: u64 = number
        .parse()
        .with_context(|| format!("Invalid duration {s:?}"))?;
    let seconds = match unit {
        "ms" => return Ok(Duration::from_millis(number)),
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        _ => anyhow::bail!("Duration {s:?} has an unknown unit; use ms, s, m or h"),
    };
    Ok(Duration::from_secs(number * seconds))
}

/// `<number>[K|M|G]` bytes, in binary units.
fn parse_size(s: &str) -> Result<u64> {
    let (number, shift) = match s.strip_suffix(['K', 'M', 'G']) {
        Some(number) => (
            number,
            10 * (1 + "KMG".find(&s[s.len() - 1..]).unwrap() as u32),
        ),
        None => (s, 0),
    };
    let number: u64 = number
        .parse()
        .with_context(|| format!("Invalid size {s:?}: use bytes, or a K, M or G suffix"))?;
    number
        .checked_shl(shift)
        .filter(|bytes| bytes >> shift == number)
        .with_context(|| format!("Size {s:?} is too large"))
}

fn ratio(s: &str) -> Result<f64> {
    let value: f64 = s.parse().with_context(|| format!("Invalid ratio {s:?}"))?;
    anyhow::ensure!((0.0..=1.0).contains(&value), "Ratio {s} is not in 0..1");
    Ok(value)
}

/// `<first>..<last>`, inclusive, with the checks and messages of the cassandra-stress
/// `shardAwarePortRange=` parameter.
fn port_range(s: &str) -> Result<RangeInclusive<u16>> {
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    let (start, end) = s
        .split_once("..")
        .filter(|(start, end)| digits(start) && digits(end))
        .with_context(|| format!(r"Invalid value {s}; must match pattern ^[0-9]+\.\.[0-9]+$"))?;
    let start = start
        .parse::<u16>()
        .with_context(|| format!("Invalid port range start: {start}"))?;
    let end = end
        .parse::<u16>()
        .with_context(|| format!("Invalid port range end: {end}"))?;
    ShardAwarePortRange::new(start..=end).with_context(|| {
        format!("Invalid port range {s}; must be a non-empty sub-range of 1024..65535")
    })?;
    Ok(start..=end)
}

/// Checks the population at parse time; the slots build their generators from the text.
fn population(s: &str) -> Result<String> {
    parse_population(s)?;
    Ok(s.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &str) -> Result<Cli> {
        Cli::parse_checked(
            ["cql-stress-sc-verify", "--profile", "p.yaml"]
                .into_iter()
                .chain(args.split_ascii_whitespace()),
        )
    }

    #[test]
    fn defaults_follow_the_spec_test() {
        let cli = parse("--duration 4h").unwrap();
        assert_eq!(cli.mode, Mode::Both);
        assert_eq!(cli.nodes, ["127.0.0.1"]);
        assert_eq!(
            (cli.connections_per_shard, cli.connections_per_host),
            (1, None)
        );
        assert_eq!(cli.shard_aware_port_range, 49152..=65535);
        assert!(!cli.tcp_reuse_address);
        assert_eq!(cli.consistency, CheckedConsistency::Quorum);
        assert_eq!(cli.duration, Some(Duration::from_secs(4 * 3600)));
        assert_eq!(cli.pop, "seq=0..1023");
        assert_eq!((cli.slots, cli.clients_per_row), (32, 8));
        assert_eq!(cli.ops_per_gen, 200);
        assert_eq!(cli.max_gen_duration, Duration::from_secs(30));
        assert_eq!(
            (cli.burst_ops, cli.burst_interval),
            (16, Duration::from_secs(1))
        );
        assert_eq!((cli.read_ratio, cli.insert_ratio), (0.5, 0.5));
        assert_eq!(cli.request_timeout, Duration::from_secs(5));
        assert_eq!(
            (cli.sweep_retries, cli.sweep_backoff),
            (10, Duration::from_secs(1))
        );
        assert_eq!(cli.max_indeterminate, 8);
        assert!(!cli.unavailable_is_fail);
        assert_eq!((cli.max_workload_errors, cli.ttl), (10, 0));
        assert_eq!(cli.history_dir, PathBuf::from("/history"));
        assert_eq!(
            (cli.check_rows, cli.check_age),
            (50, Duration::from_secs(300))
        );
        assert_eq!(cli.checker, Checker::On);
        assert_eq!(
            cli.checker_bin,
            PathBuf::from("/usr/local/bin/porcupine_checker")
        );
        assert_eq!(
            (cli.checker_workers, cli.queue_max, cli.max_viz),
            (2, 256, 3)
        );
        assert_eq!(cli.checker_timeout, Duration::from_secs(600));
        assert_eq!(cli.checker_mem, 4 << 30);
        assert_eq!(cli.checker_deadline, Duration::from_secs(600));
        assert_eq!(
            (cli.canary_every, cli.key_timeout),
            (100, Duration::from_secs(10))
        );
        assert_eq!(cli.readback, Readback::On);
        assert_eq!((cli.readback_sample, cli.readback_concurrency), (1.0, 64));
        assert_eq!((cli.bulk_op, cli.bulk_read_ratio), (BulkOp::Mixed, 0.5));
        assert_eq!(cli.bulk_pop, "seq=1099511627776..1099522113535");
        assert_eq!(
            (cli.bulk_threads, cli.bulk_rate, cli.bulk_retries),
            (64, 0, 9)
        );
        assert_eq!(cli.bulk_read_consistency, BulkReadConsistency::Quorum);
        assert_eq!(cli.report_interval, Duration::from_secs(10));
        assert!(!cli.ssl);
        assert_eq!(
            (cli.fault_stale_reads, cli.fault_replay_writes),
            (0.0, 0.0),
            "the faults are off unless asked for"
        );
    }

    #[test]
    fn rejected_combinations_test() {
        for bad in [
            "--mode verify -n 10",
            "--mode both -n 10",
            "--mode verify",
            "--duration 1m --checker maybe",
            "--duration 1m --checker-mem 4X",
            "--duration 1m --checker-mem 0",
            "--duration 1m --checker-workers 0",
            "--duration 1m --queue-max 0",
            "--duration 1m --readback-sample 0",
            "--duration 1m --readback-sample 1.5",
            "--duration 1m --pop seq=5..1",
            "--duration 1m --read-ratio 1.5",
            "--duration 1x",
            "--duration 1m --user u",
            "--duration 1m --ssl-ca ca.pem",
            "--duration 1m --ssl --ssl-cert c.pem",
            "--duration 1m --ssl --ssl-key k.pem",
            "--duration 1m --ttl 300",
            "--duration 1m --burst-interval 0s",
            "--duration 1m --report-interval 0ms",
            "--duration 1m --slots 0",
            "--duration 1m --ops-per-gen 0",
            "--duration 1m --consistency one",
            "--duration 1m --connections-per-shard 2 --connections-per-host 2",
            "--duration 1m --connections-per-host 2 --shard-aware-port-range 2000..3000",
            "--duration 1m --connections-per-shard 0",
            "--duration 1m --connections-per-host 0",
            "--duration 1m --shard-aware-port-range 1000..2000",
            "--duration 1m --shard-aware-port-range 3000..2000",
            "--duration 1m --shard-aware-port-range 2000",
            "--duration 1m --tcp-reuse-address maybe",
        ] {
            assert!(parse(bad).is_err(), "{bad:?} must be rejected");
        }
        assert!(parse("--mode bulk -n 10").is_ok());
        let cli = parse("--duration 1m --checker off --readback off --checker-mem 512M").unwrap();
        assert_eq!((cli.checker, cli.readback), (Checker::Off, Readback::Off));
        assert_eq!(cli.checker_mem, 512 << 20);
        // 10 × (30 s + 5 s + 10 × (1 s + 5 s)) = 950 s.
        assert!(parse("--duration 1m --ttl 950").is_ok());
        assert!(parse("--duration 1m --ttl 949").is_err());
        assert!(parse("--duration 250ms --nodes a,b:9043 --consistency local-quorum").is_ok());
        // ONE only for the eventually consistent control, never on a checked SC run.
        let cli = parse("--duration 1m --unsafe-eventual --consistency one").unwrap();
        assert!(cli.unsafe_eventual && cli.consistency == CheckedConsistency::One);
        assert!(!parse("--duration 1m").unwrap().unsafe_eventual);
        // The cassandra-stress text for a range of the wrong shape.
        let err = parse("--duration 1m --shard-aware-port-range 2000").unwrap_err();
        assert!(
            format!("{err:#}")
                .contains(r"Invalid value 2000; must match pattern ^[0-9]+\.\.[0-9]+$"),
            "{err:#}"
        );
    }

    #[test]
    fn connection_flags_test() {
        // PoolSize doesn't derive PartialEq.
        let cli = parse("--duration 1m --connections-per-shard 4 --shard-aware-port-range 1024..65535 --tcp-reuse-address true").unwrap();
        assert!(matches!(cli.pool_size(), PoolSize::PerShard(n) if n.get() == 4));
        assert_eq!(cli.shard_aware_port_range, 1024..=65535);
        assert!(cli.tcp_reuse_address);

        let start = serde_json::to_string(&cli.connections()).unwrap();
        assert_eq!(
            start,
            r#"{"pool":"per-shard","pool_size":4,"shard_aware_port_range":"1024..65535","tcp_reuse_address":true}"#
        );

        let cli = parse("--duration 1m --connections-per-host 8").unwrap();
        assert!(matches!(cli.pool_size(), PoolSize::PerHost(n) if n.get() == 8));
        let start = serde_json::to_string(&cli.connections()).unwrap();
        assert_eq!(
            start,
            r#"{"pool":"per-host","pool_size":8,"shard_aware_port_range":null,"tcp_reuse_address":false}"#
        );
        assert!(matches!(
            parse("--duration 1m").unwrap().pool_size(),
            PoolSize::PerShard(n) if n.get() == 1
        ));
    }

    #[test]
    fn tls_context_test() {
        let cli = parse("--duration 1m").unwrap();
        assert!(cli.tls_context().unwrap().is_none());

        let cli = parse("--duration 1m --ssl").unwrap();
        assert!(cli.tls_context().unwrap().is_some());

        let cli = parse("--duration 1m --ssl --ssl-ca /nonexistent/ca.pem").unwrap();
        assert!(cli.tls_context().is_err(), "a missing CA file is an error");
    }
}
