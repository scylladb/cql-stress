use std::iter::Iterator;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use scylla::statement::Consistency;

use crate::distribution::{parse_distribution, Distribution, Fixed};
use crate::gocompat::flags::{GoValue, ParserBuilder};

// Explicitly marked as `pub(crate)`, because with `pub` rustc doesn't
// complain about fields which are never read
pub(crate) struct ScyllaBenchArgs {
    pub workload: WorkloadType,
    pub consistency_level: Consistency,
    pub replication_factor: i64,
    pub nodes: Vec<String>,
    pub ca_cert_file: String,
    pub client_cert_file: String,
    pub client_key_file: String,
    pub server_name: String,
    pub host_verification: bool,
    pub client_compression: bool,
    // connectionCount   int // the driver will automatically choose this
    pub page_size: i64,
    pub partition_offset: i64,

    // (Timeseries-related parameters)
    // writeRate    int64
    // distribution string
    // var startTimestamp int64

    // hostSelectionPolicy string
    pub tls_encryption: bool,
    pub keyspace_name: String,
    pub table_name: String,
    pub username: String,
    pub password: String,
    pub mode: Mode,
    // latencyType    string
    // maxErrorsAtRow int
    pub concurrency: u64,
    pub maximum_rate: u64,

    pub test_duration: Duration,
    pub partition_count: u64,
    pub clustering_row_count: u64,
    pub clustering_row_size_dist: Arc<dyn Distribution>,

    pub rows_per_request: u64,
    pub provide_upper_bound: bool,
    pub in_restriction: bool,
    // selectOrderBy       string
    // selectOrderByParsed []string
    pub no_lower_bound: bool,
    // bypassCache         bool

    // rangeCount int
    pub timeout: Duration,
    pub iterations: u64,
    // // Any error response that comes with delay greater than errorToTimeoutCutoffTime
    // // to be considered as timeout error and recorded to histogram as such
    // measureLatency           bool
    // hdrLatencyFile           string
    // hdrLatencyUnits          string
    // hdrLatencySigFig         int
    pub validate_data: bool,
}

// Parses and validates scylla bench params.
pub(crate) fn parse_scylla_bench_args<I, S>(mut args: I) -> Option<ScyllaBenchArgs>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let program_name = args.next().unwrap();

    let mut flag = ParserBuilder::new();

    let workload = flag.string_var("workload", "", "workload: sequential, uniform, timeseries");
    let consistency_level = flag.string_var("consistency-level", "quorum", "consistency level");
    let replication_factor = flag.i64_var("replication-factor", 1, "replication factor");

    let nodes = flag.string_var("nodes", "127.0.0.1:9042", "cluster contact nodes");
    let server_name = flag.string_var(
        "tls-server-name",
        "",
        "TLS server hostname (currently unimplemented)",
    );
    let host_verification =
        flag.bool_var("tls-host-verification", false, "verify server certificate");
    let client_compression = flag.bool_var(
        "client-compression",
        true,
        "use compression for client-coordinator communication",
    );
    let ca_cert_file = flag.string_var(
        "tls-ca-cert-file",
        "",
        "path to CA certificate file, needed to enable encryption",
    );
    let client_cert_file = flag.string_var(
        "tls-client-cert-file",
        "",
        "path to client certificate file, needed to enable client certificate authentication",
    );
    let client_key_file = flag.string_var(
        "tls-client-key-file",
        "",
        "path to client key file, needed to enable client certificate authentication",
    );

    let _connection_count = flag.i64_var(
        "connection-count",
        4,
        "number of connections (currently ignored)",
    );
    let page_size = flag.i64_var("page-size", 1000, "page size");
    let partition_offset = flag.i64_var(
        "partition-offset",
        0,
        "start of the partition range (only for sequential workload)",
    );

    let tls_encryption = flag.bool_var(
        "tls",
        false,
        "use TLS encryption for clien-coordinator communication",
    );
    let keyspace_name = flag.string_var("keyspace", "scylla_bench", "keyspace to use");
    let table_name = flag.string_var("table", "test", "table to use");
    let username = flag.string_var("username", "", "cql username for authentication");
    let password = flag.string_var("password", "", "cql password for authentication");
    let mode = flag.string_var(
        "mode",
        "",
        "operating mode: write, read, counter_update, counter_read, scan",
    );
    let concurrency = flag.u64_var("concurrency", 16, "number of used tasks");
    let maximum_rate = flag.u64_var(
        "max-rate",
        0,
        "the maximum rate of outbound requests in op/s (0 for unlimited)",
    );

    let test_duration = flag.duration_var(
        "duration",
        Duration::ZERO,
        "duration of the test in seconds (0 for unlimited)",
    );
    let partition_count = flag.u64_var("partition-count", 10_000, "number of partitions");
    let clustering_row_count = flag.u64_var(
        "clustering-row-count",
        100,
        "number of clustering rows in a partition",
    );
    let default_dist: Arc<dyn Distribution> = Arc::new(Fixed(4));
    let clustering_row_size_dist = flag.var(
        "clustering-row-size",
        ScyllaBenchDistribution(default_dist),
        "size of a single clustering row, can use random values",
    );

    let rows_per_request =
        flag.u64_var("rows-per-request", 1, "clustering rows per single request");
    let provide_upper_bound = flag.bool_var(
        "provide-upper-bound",
        false,
        "whether read requests should provide an upper bound",
    );
    let in_restriction = flag.bool_var(
        "in-restriction",
        false,
        "use IN restriction in read requests",
    );
    let no_lower_bound = flag.bool_var(
        "no-lower-bound",
        false,
        "do not provide lower bound in read requests",
    );

    let timeout = flag.duration_var("timeout", Duration::from_secs(5), "request timeout");
    let iterations = flag.u64_var(
        "iterations",
        1,
        "number of iterations to run (0 for unlimited, relevant only for workloads \
        that have a defined number of ops to execute)",
    );

    let validate_data = flag.bool_var(
        "validate-data",
        false,
        "write meaningful data and validate while reading",
    );

    let (parser, desc) = flag.build();

    let result = move || -> Result<ScyllaBenchArgs> {
        parser.parse_args(args)?;

        let nodes = nodes.get().split(',').map(str::to_string).collect();
        let workload = parse_workload(&workload.get())?;
        let mode = parse_mode(&mode.get())?;
        let consistency_level = parse_consistency_level(&consistency_level.get())?;

        Ok(ScyllaBenchArgs {
            workload,
            consistency_level,
            replication_factor: replication_factor.get(),
            nodes,
            ca_cert_file: ca_cert_file.get(),
            client_cert_file: client_cert_file.get(),
            client_key_file: client_key_file.get(),
            server_name: server_name.get(),
            host_verification: host_verification.get(),
            client_compression: client_compression.get(),
            page_size: page_size.get(),
            partition_offset: partition_offset.get(),
            tls_encryption: tls_encryption.get(),
            keyspace_name: keyspace_name.get(),
            table_name: table_name.get(),
            username: username.get(),
            password: password.get(),
            mode,
            concurrency: concurrency.get(),
            maximum_rate: maximum_rate.get(),
            test_duration: test_duration.get(),
            partition_count: partition_count.get(),
            clustering_row_count: clustering_row_count.get(),
            clustering_row_size_dist: clustering_row_size_dist.get().0,
            rows_per_request: rows_per_request.get(),
            provide_upper_bound: provide_upper_bound.get(),
            in_restriction: in_restriction.get(),
            no_lower_bound: no_lower_bound.get(),
            timeout: timeout.get(),
            iterations: iterations.get(),
            validate_data: validate_data.get(),
        })
    }();

    match result {
        Ok(config) => Some(config),
        Err(err) => {
            // TODO: Should we print to stdout or stderr?
            println!("Failed to parse flags: {}", err);
            desc.print_help(program_name.as_ref());
            None
        }
    }
}

struct ScyllaBenchDistribution(Arc<dyn Distribution>);

impl GoValue for ScyllaBenchDistribution {
    fn parse(s: &str) -> Result<Self> {
        let dist = parse_distribution(s)?.into();
        Ok(ScyllaBenchDistribution(dist))
    }

    fn to_string(&self) -> String {
        self.0.describe()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mode {
    Write,
    Read,
    CounterUpdate,
    CounterRead,
    Scan,
}

fn parse_mode(s: &str) -> Result<Mode> {
    match s {
        "write" => Ok(Mode::Write),
        "read" => Ok(Mode::Read),
        "counter_update" => Ok(Mode::CounterUpdate),
        "counter_read" => Ok(Mode::CounterRead),
        "scan" => Ok(Mode::Scan),
        "" => Err(anyhow::anyhow!("mode needs to be specified")),
        _ => Err(anyhow::anyhow!("unknown mode: {}", s)),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkloadType {
    Sequential,
    Uniform,
    Timeseries,
    Scan,
}

fn parse_workload(s: &str) -> Result<WorkloadType> {
    match s {
        "sequential" => Ok(WorkloadType::Sequential),
        "uniform" => Ok(WorkloadType::Uniform),
        "timeseries" => Ok(WorkloadType::Timeseries),
        "scan" => Ok(WorkloadType::Scan),
        "" => Err(anyhow::anyhow!("workload type needs to be specified")),
        _ => Err(anyhow::anyhow!("unknown workload type: {}", s)),
    }
}

fn parse_consistency_level(s: &str) -> Result<Consistency> {
    let level = match s {
        "any" => Consistency::Any,
        "one" => Consistency::One,
        "two" => Consistency::Two,
        "three" => Consistency::Three,
        "quorum" => Consistency::Quorum,
        "all" => Consistency::All,
        "local_quorum" => Consistency::LocalQuorum,
        "each_quorum" => Consistency::EachQuorum,
        "local_one" => Consistency::LocalOne,
        _ => return Err(anyhow::anyhow!("Unknown consistency level: {}", s)),
    };
    Ok(level)
}
