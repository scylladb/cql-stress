use std::collections::HashMap;
#[cfg(feature = "strong-consistency")]
use std::collections::HashSet;
use std::iter::Iterator;

mod command;
mod option;
mod param;
#[cfg(feature = "strong-consistency")]
mod protocol_extensions;
use anyhow::Context;
use anyhow::Result;

#[cfg(test)]
mod test;

pub use command::Command;
pub use command::CommandParams;
pub use command::MixedSubcommand;
pub use command::OperationRatio;
#[cfg(feature = "user-profile")]
pub use command::{OpWeight, PREDEFINED_INSERT_OPERATION};
pub use option::ErrorsOption;
pub use option::LogOption;
pub use option::ThreadsInfo;
use regex::Regex;
use scylla::client::session::Session;
#[cfg(feature = "strong-consistency")]
use scylla::cluster::metadata::ConsistencyMode;
#[cfg(feature = "strong-consistency")]
use scylla::cluster::ClusterState;
#[cfg(feature = "strong-consistency")]
use scylla::statement::Consistency;

use crate::settings::command::print_help;

use self::command::parse_command;
use self::option::ColumnOption;
use self::option::ModeOption;
use self::option::NodeOption;
use self::option::PopulationOption;
use self::option::RateOption;
use self::option::SchemaOption;
use self::option::TransportOption;
#[cfg(feature = "strong-consistency")]
use self::protocol_extensions::fetch_protocol_features;

pub struct CassandraStressSettings {
    pub command: Command,
    pub command_params: CommandParams,
    pub node: NodeOption,
    pub rate: RateOption,
    pub mode: ModeOption,
    pub schema: SchemaOption,
    pub column: ColumnOption,
    pub population: PopulationOption,
    pub log: LogOption,
    pub transport: TransportOption,
    pub errors: ErrorsOption,
}

impl CassandraStressSettings {
    pub fn print_settings(&self) {
        println!("******************** Stress Settings ********************");
        self.command_params.print_settings(&self.command);
        self.rate.print_settings();
        self.mode.print_settings();
        self.node.print_settings();
        self.schema.print_settings();
        self.column.print_settings();
        self.population.print_settings();
        self.log.print_settings();
        self.transport.print_settings();
        self.errors.print_settings();
        println!();
    }

    /// The keyspace this run actually drives.
    ///
    /// A user profile creates and uses the keyspace declared in its yaml; every other
    /// command uses the `-schema keyspace=` value. The two are independent - `-schema` is
    /// not consulted at all in user mode - so every strong-consistency check has to follow
    /// the one the operations will really hit, or it inspects a keyspace nothing in the run
    /// touches.
    #[cfg(feature = "strong-consistency")]
    fn workload_keyspace(&self) -> &str {
        #[cfg(feature = "user-profile")]
        if let Some(user) = &self.command_params.user {
            return &user.keyspace;
        }
        &self.schema.keyspace
    }

    pub async fn create_schema(&self, session: &Session) -> Result<()> {
        #[cfg(feature = "user-profile")]
        if let Some(user) = &self.command_params.user {
            return user.create_schema(session).await;
        }

        if matches!(self.command, Command::Write | Command::CounterWrite) {
            session
                .query_unpaged(self.schema.construct_keyspace_creation_query(), ())
                .await?;
        }

        session.use_keyspace(&self.schema.keyspace, true).await?;

        match self.command {
            Command::Write => {
                session
                    .query_unpaged(
                        self.schema
                            .construct_table_creation_query(&self.column.columns),
                        (),
                    )
                    .await
                    .context("Failed to create standard table")?;
            }
            Command::CounterWrite => {
                session
                    .query_unpaged(
                        self.schema
                            .construct_counter_table_creation_query(&self.column.columns),
                        (),
                    )
                    .await
                    .context("Failed to create counter table")?;
            }
            _ => (),
        }

        Ok(())
    }

    /// Reports the keyspace's consistency mode, so the mode a run actually measured is
    /// recorded alongside its numbers, and refuses to start a run that would not measure
    /// what it claims to.
    ///
    /// The mode comes from the driver rather than from `system_schema` on purpose. The
    /// driver reports [`ConsistencyMode::Global`] only when it *both* negotiated the
    /// `TABLETS_ROUTING_V2` protocol extension *and* read `consistency = 'global'` for the
    /// keyspace - it does not even select that column otherwise. One value therefore proves
    /// both halves of leader-aware routing, including the half no server-side query can see:
    /// that this build of the driver supports it at all. Asking the server directly would
    /// not: ScyllaDB 2026.2.x records `consistency = 'global'` in
    /// `system_schema.scylla_keyspaces` while advertising only `TABLETS_ROUTING_V1`, and a
    /// driver without leader-aware routing reads back exactly the same rows as one with it.
    ///
    /// What it does *not* prove is that every node advertises the extension. The mode is read
    /// over the connection the driver fetches cluster metadata on, and the extension is
    /// negotiated per connection, so a cluster midway through a rolling enable can report
    /// `Global` while some workload connections still cannot be handed a leader-ordered
    /// replica list. There is no public driver API for the aggregate, and probing every node
    /// from here would not work over TLS and would put an extra connection on the startup
    /// path of every healthy run. The empirical check is the coordinator distribution from
    /// `-log coordinators=true`: a mixed cluster shows up there as a spread across replicas
    /// instead of a skew toward leaders, which is why the success message points at it.
    ///
    /// When `consistency=global` was requested, anything short of that is a hard startup
    /// failure. Every way this can go wrong otherwise produces a full, plausible,
    /// meaningless result set:
    /// - `CREATE KEYSPACE IF NOT EXISTS` no-ops over a leftover eventually consistent
    ///   keyspace from an earlier run;
    /// - a `read`-only run never creates the keyspace at all;
    /// - the server lacks `--experimental-features=strongly-consistent-tables`;
    /// - the cluster feature gating strongly consistent tables is not on yet, which is
    ///   the case until every node carries that flag;
    /// - the server takes `consistency = 'global'` but advertises no
    ///   `TABLETS_ROUTING_V2_EXPERIMENTAL`, so the driver never sees a leader-ordered
    ///   replica list and spreads the load over followers.
    ///
    /// When `consistency` was not requested the mode is only reported - existing eventually
    /// consistent runs must keep working unchanged.
    #[cfg(feature = "strong-consistency")]
    pub async fn verify_consistency_mode(&self, session: &Session) -> Result<()> {
        // The DDL above may have raced the background metadata refresh, so force one before
        // reading the mode back: this is the same snapshot the driver's own routing
        // decisions are made from.
        session
            .refresh_metadata()
            .await
            .context("Failed to refresh cluster metadata")?;

        let keyspace = self.workload_keyspace();
        let cluster_state = session.get_cluster_state();
        // `None` means the keyspace does not exist at all, which is a different thing from
        // existing as eventually consistent, and the two get different messages below.
        let mode = cluster_state
            .get_keyspace(keyspace)
            .map(|ks| ks.consistency_mode.clone());

        // `None` and `Eventual` fail for different reasons and deserve different words.
        let reported_mode = match &mode {
            Some(mode) => format!("{mode:?}"),
            None => String::from("unknown (keyspace not found in cluster metadata)"),
        };
        println!("Keyspace '{keyspace}' consistency mode: {reported_mode}");

        // `ConsistencyMode` is `#[non_exhaustive]`: match the one variant that means strong
        // consistency rather than enumerating the others, so a future variant is treated as
        // "not strongly consistent" instead of failing to compile.
        let strongly_consistent = matches!(mode, Some(ConsistencyMode::Global));

        if !strongly_consistent {
            if self.schema.wants_strong_consistency() {
                // The mode alone cannot say which of the causes applied, so ask the node
                // whether it could ever route to a leader before giving up.
                let diagnosis = self.diagnose_missing_strong_consistency().await;
                anyhow::bail!(strong_consistency_failure_message(
                    keyspace,
                    &reported_mode,
                    &self.schema.construct_keyspace_creation_query(),
                    &diagnosis,
                ));
            }

            return Ok(());
        }

        println!(
            "Leader-aware routing: enabled (keyspace '{keyspace}' is strongly consistent and \
             the driver negotiated TABLETS_ROUTING_V2 with the node it read cluster metadata \
             from). On a cluster where the strongly-consistent-tables flag is still being \
             rolled out, nodes that do not yet carry it cannot hand out a leader-ordered \
             replica list; run with -log coordinators=true and check that the distribution is \
             skewed toward leaders rather than spread evenly across replicas."
        );

        // Keyed on the mode the keyspace actually has, not on what was requested: a
        // pre-provisioned strongly consistent keyspace behaves the same whether or not
        // `consistency=global` was passed, since `CREATE KEYSPACE IF NOT EXISTS` no-ops over
        // it and the mode is a property of the keyspace, not of the CLI flag.
        //
        // The consistency level first: when it is wrong the run cannot start at all, and
        // routing advice for a run that will not happen is just noise.
        self.verify_consistency_level()?;
        self.warn_on_datacenter_preference(&cluster_state);

        Ok(())
    }

    /// Warns when a preferred datacenter quietly narrows leader-aware routing to a fraction
    /// of the tablets.
    ///
    /// A tablet's Raft leader can be in any datacenter - a globally consistent keyspace gains
    /// nothing from locality, so nothing pins the leader near the client. Leader-aware routing
    /// therefore ranks the leader above distance, but only among hosts the load balancing
    /// policy would contact at all. `cql-stress` never enables datacenter failover, so with
    /// `-node datacenter=` a leader in any other datacenter is vetoed: the request goes to a
    /// local replica instead and the server forwards it to the leader. That forward is the
    /// extra hop the whole benchmark exists to avoid, and it is taken for every tablet whose
    /// leader sits elsewhere - so a multi-datacenter run measures a blend of leader-routed and
    /// forwarded requests whose ratio drifts as ScyllaDB rebalances leaders.
    ///
    /// None of that is visible in the numbers: the mode still reads `Global`, leader-aware
    /// routing genuinely is enabled, and the coordinator distribution is still skewed - just
    /// toward local replicas rather than leaders.
    ///
    /// Restricting to one datacenter costs nothing when the cluster only has one, which is the
    /// common case and must stay silent. A preferred *rack* is not affected: the leader
    /// outranks rack, and only the datacenter can veto it.
    #[cfg(feature = "strong-consistency")]
    fn warn_on_datacenter_preference(&self, cluster_state: &ClusterState) {
        let Some(preferred_dc) = self.node.datacenter.as_deref() else {
            return;
        };

        let mut datacenters: Vec<&str> = cluster_state
            .get_nodes_info()
            .iter()
            .filter_map(|node| node.datacenter.as_deref())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if datacenters.len() < 2 {
            return;
        }
        datacenters.sort_unstable();

        let keyspace = self.workload_keyspace();
        println!();
        println!(
            "WARNING: keyspace '{keyspace}' is strongly consistent and this run prefers \
             datacenter '{preferred_dc}', but the cluster spans {count} datacenters \
             ({list}). A tablet's \
             Raft leader can be in any of them, and cql-stress does not enable datacenter \
             failover, so the driver will never send a request to a leader outside \
             '{preferred_dc}' - those requests go to a local replica and are forwarded to the \
             leader, which is exactly the hop leader-aware routing is meant to remove. Only \
             tablets whose leader already sits in '{preferred_dc}' are leader-routed, so this \
             run measures a mixture. Drop datacenter= to measure leader-aware routing across \
             the whole cluster. A preferred rack is fine - the leader outranks rack.",
            count = datacenters.len(),
            list = datacenters.join(", "),
        );
        println!();
    }

    /// Checks `cl=` against what a strongly consistent keyspace actually accepts.
    ///
    /// The server is far stricter here than for an eventually consistent table, and rejects
    /// per request rather than at connect time, so getting this wrong yields a run in which
    /// every single operation fails - numbers that look like a catastrophic cluster problem
    /// and are really a CLI mistake. The accepted sets are asymmetric:
    ///
    /// - **writes** take `QUORUM` and `LOCAL_QUORUM`, nothing else;
    /// - **reads** additionally take `ONE` and `LOCAL_ONE`.
    ///
    /// `ONE`/`LOCAL_ONE` are legal for reads but turn leader-aware routing off: any replica
    /// may serve such a read, so the request keeps normal spread routing. That is a warning
    /// rather than an error - the run works, it just does not measure what it set out to -
    /// and it matters because `local_one` is the default `cl`.
    #[cfg(feature = "strong-consistency")]
    fn verify_consistency_level(&self) -> Result<()> {
        // A user profile does not apply `cl=` to anything: each statement carries the level
        // from its yaml `consistencyLevel:`, or the driver's default. Validating the CLI
        // value here would report on a setting that has no effect on the run. Those
        // statements are checked where they are prepared, against their effective levels -
        // see `UserOperationFactory::verify_profile_consistency_levels`.
        #[cfg(feature = "user-profile")]
        if self.command_params.user.is_some() {
            return Ok(());
        }

        let keyspace = self.workload_keyspace();
        let cl = self.command_params.common.consistency_level;

        if matches!(cl, Consistency::Quorum | Consistency::LocalQuorum) {
            return Ok(());
        }

        anyhow::ensure!(
            matches!(cl, Consistency::One | Consistency::LocalOne),
            "Keyspace '{keyspace}' is strongly consistent, but cl={cl} is not a consistency \
             level it accepts: strongly consistent writes take QUORUM or LOCAL_QUORUM, and \
             strongly consistent reads take QUORUM, LOCAL_QUORUM, ONE or LOCAL_ONE. The \
             server would reject every operation in this run. Use cl=QUORUM."
        );

        // ONE / LOCAL_ONE from here on: accepted for reads, rejected for writes.
        anyhow::ensure!(
            !self.issues_writes(),
            "Keyspace '{keyspace}' is strongly consistent, but cl={cl} cannot be used to \
             write to it: the server accepts only QUORUM and LOCAL_QUORUM for strongly \
             consistent writes and would reject every operation in this run. Use cl=QUORUM."
        );

        println!();
        println!(
            "WARNING: keyspace '{keyspace}' is strongly consistent, but cl={cl} disables \
             leader-aware routing: at ONE and LOCAL_ONE any replica may serve the read, so \
             the driver keeps normal spread routing and requests are forwarded to the leader \
             by whichever replica received them. Note that local_one is the default cl. Use \
             cl=QUORUM to measure strong consistency."
        );
        println!();

        Ok(())
    }

    /// Whether this run issues writes, which decides how strict a strongly consistent
    /// keyspace is about `cl=`.
    ///
    /// A user profile never reaches this: its statements carry their own levels and are
    /// checked individually where they are prepared, so `verify_consistency_level` returns
    /// before asking.
    #[cfg(feature = "strong-consistency")]
    fn issues_writes(&self) -> bool {
        match self.command {
            Command::Write | Command::CounterWrite | Command::Mixed => true,
            Command::Read | Command::CounterRead => false,
            // Not workloads, or checked elsewhere - they never reach this far.
            #[cfg(feature = "user-profile")]
            Command::User => false,
            Command::Help | Command::Version | Command::VersionJson => false,
        }
    }

    /// Explains, as far as it can be established from outside, why the driver does not see
    /// the keyspace as strongly consistent.
    ///
    /// A mode other than `Global` has several independent causes and the value itself cannot
    /// tell them apart. Asking the node whether it advertises `TABLETS_ROUTING_V2_EXPERIMENTAL`
    /// separates the most confusing one - a server that cannot do leader routing at all, yet
    /// happily stores `consistency = 'global'` - from an ordinary "this keyspace was not
    /// created strongly consistent".
    ///
    /// Every configured contact node is asked, not just the first: the session is built from
    /// the whole `-node` list, so generalising from one of them gets the answer exactly
    /// backwards on a cluster midway through a rolling enable, where the first contact point
    /// carries the flag and another does not. When the nodes disagree, that disagreement *is*
    /// the diagnosis and is reported as such.
    ///
    /// Returns a sentence to append to the failure, or an empty string when there is nothing
    /// useful to add. It runs only on the failure path, so a healthy run pays nothing for it.
    #[cfg(feature = "strong-consistency")]
    async fn diagnose_missing_strong_consistency(&self) -> String {
        if self.node.nodes.is_empty() {
            return String::new();
        }

        // The probe speaks plaintext CQL and cannot reach a TLS-only node.
        if self.transport.truststore.is_some() || self.transport.keystore.is_some() {
            return String::from(
                "\nThe configured nodes were not asked whether they advertise \
                 TABLETS_ROUTING_V2_EXPERIMENTAL: the probe speaks plaintext CQL and this \
                 run uses TLS.",
            );
        }

        // A long -node list would make the failure unreadable, and the answer is a cluster
        // property: a handful of nodes is enough to tell a uniform cluster from a mixed one.
        const MAX_PROBED_NODES: usize = 8;
        let probed = &self.node.nodes[..self.node.nodes.len().min(MAX_PROBED_NODES)];

        let outcomes = futures::future::join_all(
            probed
                .iter()
                .map(|node| async move { (node.as_str(), fetch_protocol_features(node).await) }),
        )
        .await;

        let mut with_v2 = Vec::new();
        let mut without_v2 = Vec::new();
        let mut unreachable = Vec::new();
        for (node, result) in &outcomes {
            match result {
                Ok(features) if features.tablets_v2_supported => with_v2.push(*node),
                Ok(_) => without_v2.push(*node),
                Err(error) => unreachable.push(format!("{node} ({error:#})")),
            }
        }

        summarise_v2_probe(
            &with_v2,
            &without_v2,
            &unreachable,
            probed.len(),
            self.node.nodes.len(),
        )
    }
}

/// Marks the startup failure raised when a run asked for `consistency=global` and would not
/// have measured it.
///
/// The integration tests have to tell this apart from a binary that is simply broken - a
/// panic, a renamed CLI option, an unreachable node - because they *skip* on the first and
/// must *fail* on the second. Matching on prose would make every reword a silent un-skip, so
/// the failure carries a stable code and `tools/test_cs_strong_consistency.py` matches on it.
#[cfg(feature = "strong-consistency")]
pub const STRONG_CONSISTENCY_UNAVAILABLE_CODE: &str = "STRONG_CONSISTENCY_UNAVAILABLE";

/// Builds the failure raised when `consistency=global` was requested but the driver does not
/// see the keyspace as strongly consistent.
///
/// Split out from `verify_consistency_mode` so a unit test can pin the diagnostic code
/// without a live cluster - see [`STRONG_CONSISTENCY_UNAVAILABLE_CODE`].
#[cfg(feature = "strong-consistency")]
fn strong_consistency_failure_message(
    keyspace: &str,
    reported_mode: &str,
    ddl: &str,
    diagnosis: &str,
) -> String {
    format!(
        "Requested consistency=global, but the driver does not see keyspace '{keyspace}' as \
         strongly consistent (mode: {reported_mode}). This run would not measure strong \
         consistency. The driver reports Global only once it has both negotiated \
         TABLETS_ROUTING_V2 and read consistency='global' for the keyspace, so any of these \
         breaks it:\n\
         - the server does not run with \
         --experimental-features=strongly-consistent-tables;\n\
         - the cluster feature gating strongly consistent tables is not enabled yet - it \
         turns on only once every node carries that flag, so a partially upgraded cluster \
         lands here;\n\
         - the server does not advertise TABLETS_ROUTING_V2_EXPERIMENTAL, which is a \
         capability separate from accepting consistency='global' (ScyllaDB 2026.2.x has the \
         second without the first);\n\
         - keyspace '{keyspace}' already exists as an eventually consistent keyspace (CREATE \
         KEYSPACE IF NOT EXISTS will not upgrade it - drop it first);\n\
         - the keyspace is not tablet-based (non-tablet keyspaces reject the consistency \
         option; SimpleStrategy may not get tablets).\n\
         DDL used: {ddl}{diagnosis}\n\
         (diagnostic code: {STRONG_CONSISTENCY_UNAVAILABLE_CODE})"
    )
}

/// Turns the per-node `TABLETS_ROUTING_V2_EXPERIMENTAL` answers into the sentence appended to
/// a failed strong-consistency check.
///
/// Split out from the probe itself so the wording - which is the whole point of the
/// diagnostic - can be tested without a server. A conclusion is drawn only when the nodes
/// agree; when they disagree, the disagreement is the diagnosis, because a cluster part-way
/// through enabling the experimental feature is the most confusing state this check can land
/// in and the one a single-node probe reports exactly backwards.
#[cfg(feature = "strong-consistency")]
fn summarise_v2_probe(
    with_v2: &[&str],
    without_v2: &[&str],
    unreachable: &[String],
    probed: usize,
    total: usize,
) -> String {
    let mut report = String::from(
        "\nAsked the configured nodes whether they advertise TABLETS_ROUTING_V2_EXPERIMENTAL",
    );
    if total > probed {
        report.push_str(&format!(
            " (first {probed} of {total}, {} not probed)",
            total - probed
        ));
    }
    report.push_str(":\n");

    match (with_v2.is_empty(), without_v2.is_empty()) {
        // Nobody advertises it: these servers cannot route to leaders at all.
        (true, false) => report.push_str(&format!(
            "- none of them do ({}), so no node can hand the driver a leader-ordered replica \
             list. This is a capability separate from accepting consistency='global': \
             ScyllaDB 2026.2.x stores 'global' in system_schema.scylla_keyspaces while \
             advertising only TABLETS_ROUTING_V1, which is exactly why the mode above reads \
             as eventual.",
            without_v2.join(", ")
        )),
        // All of them do: the extension is not the missing half.
        (false, true) => report.push_str(&format!(
            "- all of them do ({}), so these servers can route to tablet leaders - the \
             keyspace itself is what is not strongly consistent.",
            with_v2.join(", ")
        )),
        // Mixed.
        (false, false) => report.push_str(&format!(
            "- some do ({}) and some do not ({}). The cluster is part-way through enabling \
             --experimental-features=strongly-consistent-tables: the cluster feature that \
             gates consistency='global' stays off until every node carries the flag, so the \
             keyspace cannot be created strongly consistent yet even though some nodes could \
             already route to leaders. Finish the rollout and retry.",
            with_v2.join(", "),
            without_v2.join(", ")
        )),
        // Nothing answered at all.
        (true, true) => report.push_str("- none of them could be reached."),
    }

    if !unreachable.is_empty() {
        report.push_str(&format!(
            "\n- could not be asked: {}",
            unreachable.join(", ")
        ));
    }

    report
}

pub enum CassandraStressParsingResult {
    // HELP, PRINT, VERSION
    SpecialCommand,
    Workload(Box<CassandraStressSettings>),
}

type ParsePayload<'a> = HashMap<String, Vec<&'a str>>;

/// Groups the commands/options and their corresponding parametes.
///
/// cassandra-stress accepts CLI args of the following pattern:
/// ./cassandra-stress COMMAND [command_param...] [OPTION [option_param...]...]
fn prepare_parse_payload(args: &[String]) -> Result<(&str, ParsePayload<'_>)> {
    let mut cl_args: ParsePayload = HashMap::new();

    let mut iter = args.iter();
    let (cmd, mut current) = {
        let cmd = iter.next().ok_or(anyhow::anyhow!("No command specified"))?;
        let current = cmd.to_lowercase();
        cl_args.insert(current.clone(), vec![]);
        (cmd, current)
    };

    for arg in iter {
        let arg: &str = arg.as_ref();

        if arg.starts_with('-') {
            anyhow::ensure!(
                !cl_args.contains_key(arg),
                "{} is defined multiple times. Each option/command can be specified at most once.",
                arg
            );
            current = arg.to_lowercase();
            cl_args.insert(current.clone(), vec![]);
            continue;
        }

        let params = cl_args.get_mut(&current).unwrap();
        params.push(arg);
    }

    Ok((cmd, cl_args))
}

// Regular expressions used in `repair_params` function.
lazy_static! {
    // Removes whitespaces before characters: ,=()
    static ref WHITESPACE_BEFORE: Regex = Regex::new(r"\s+([,=()])").unwrap();
    // Removes whitespaces after characters: ,=(
    static ref WHITESPACE_AFTER: Regex = Regex::new(r"([,=(])\s+").unwrap();

    // Example:
    // write -schema 'replication ( factor = 3 , foo = bar )'
    // will be transformed to:
    // ["write", "-schema", "replication(factor=3,foo=bar)"]
    //
    // The reason why WHITESPACE_AFTER doesn't contain ')' character:
    // Take for example:
    // write -schema 'replication(factor=3) ' keyspace=k
    // After concatenating parameters to single string we get:
    // "write -schema replication(factor=3)  keyspace=k"
    // Note two spaces after ')'.
    // Now if we replaced ")  " with ")", the resulting vector would be:
    // ["write", "-schema", "replication(factor=3)keyspace=k"]

    // Splits the resulting arguments by whitespaces.
    static ref WHITESPACE_REGEX: Regex = Regex::new(r"\s+").unwrap();
}

/// Removes the unnecessary whitespaces from the arguments,
/// and then splits the arguments that contain whitespaces.
/// For example when user passes following arguments (cassandra-stress accepts such command):
/// read -rate 'threads=80 throttle=8000/s'
///
/// Note that 'threads=80 throttle=8000/s' will be treated as a single string,
/// so we need to split this into two separate parameters.
/// The resulting vector would in this case be:
/// ["read", "-rate", "threads=80", "throttle=8000/s"]
fn repair_params<'a, I, S>(args: I) -> Vec<String>
where
    I: Iterator<Item = &'a S>,
    S: AsRef<str> + 'a,
{
    // Concat to single string.
    let args = args.map(|s| s.as_ref()).collect::<Vec<&str>>().join(" ");

    let replaced = WHITESPACE_BEFORE.replace_all(&args, "$1");
    let replaced = WHITESPACE_AFTER.replace_all(&replaced, "$1");
    WHITESPACE_REGEX
        .split(&replaced)
        .map(&str::to_owned)
        .collect()
}

pub fn parse_cassandra_stress_args<I, S>(mut args: I) -> Result<CassandraStressParsingResult>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let _program_name = args.next().unwrap();
    let args: Vec<S> = args.collect();
    let args: Vec<String> = repair_params(args.iter());

    let result = || {
        let (cmd, mut payload) = prepare_parse_payload(&args)?;

        let (command, command_params) = match parse_command(cmd, &mut payload) {
            Ok((_, None)) => return Ok(CassandraStressParsingResult::SpecialCommand),
            Ok((cmd, Some(params))) => (cmd, params),
            Err(e) => return Err(e),
        };

        let node = NodeOption::parse(&mut payload)?;
        let rate = RateOption::parse(&mut payload)?;
        let mode = ModeOption::parse(&mut payload)?;
        let schema = SchemaOption::parse(&mut payload)?;
        let column = ColumnOption::parse(&mut payload)?;
        let log = LogOption::parse(&mut payload)?;
        let transport = TransportOption::parse(&mut payload)?;
        let errors = ErrorsOption::parse(&mut payload)?;

        // The default distribution (if not specified) is SEQ(1..operation_count).
        // If operation_count is not specified, then the default is 1M.
        let operation_count = command_params
            .common
            .operation_count
            .map_or(String::from("1000000"), |op| format!("{op}"));
        let population = PopulationOption::parse(&mut payload, &operation_count)?;

        // List the unknown options along with their parameters.
        let build_unknown_arguments_err_message = || -> String {
            let unknowns = payload
                .iter()
                .map(|(option, params)| {
                    let params_str = params.join(" ");
                    format!("{option} {params_str}")
                })
                .collect::<Vec<_>>();
            unknowns.join("\n")
        };

        // A user profile brings its own keyspace DDL, which cql-stress passes through
        // unchanged, so the `-schema` keyspace creation query is never executed in user mode
        // and `consistency=` in it has no effect whatsoever. Rejecting it is the only way the
        // user finds out: accepted silently it reads like a request that was honoured, and
        // the run then measures an eventually consistent keyspace while claiming otherwise.
        #[cfg(feature = "user-profile")]
        anyhow::ensure!(
            !(matches!(command, Command::User) && schema.consistency.is_some()),
            "-schema replication(consistency=...) has no effect with the 'user' command: a \
             user profile creates its keyspace from the profile's keyspace_definition, which \
             cql-stress executes unchanged. Put `AND consistency = 'global'` in that DDL \
             instead."
        );

        // Ensure that all of the CLI arguments were consumed.
        // If not, then unknown arguments appeared so we return the error.
        anyhow::ensure!(
            payload.is_empty(),
            "Error processing CLI arguments. The following were ignored:\n{}",
            build_unknown_arguments_err_message()
        );

        Ok(CassandraStressParsingResult::Workload(Box::new(
            CassandraStressSettings {
                command,
                command_params,
                node,
                rate,
                mode,
                schema,
                column,
                population,
                log,
                transport,
                errors,
            },
        )))
    };

    match result() {
        Ok(v) => Ok(v),
        Err(e) => {
            print_help();
            Err(e)
        }
    }
}
