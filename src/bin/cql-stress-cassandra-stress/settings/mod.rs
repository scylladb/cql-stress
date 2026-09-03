use std::collections::HashMap;
use std::iter::Iterator;

mod command;
mod option;
mod param;
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
pub use option::LogOption;
pub use option::ThreadsInfo;
use regex::Regex;
use scylla::client::session::Session;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
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
        println!();
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

    /// Reads the keyspace's consistency mode back from the server and reports it, so the
    /// mode a run actually measured is recorded alongside its numbers, and checks that the
    /// node can actually route to tablet leaders.
    ///
    /// When `consistency=global` was requested, anything short of a strongly consistent
    /// keyspace on a leader-routing-capable node is a hard startup failure. Every way this
    /// can go wrong otherwise produces a full, plausible, meaningless result set:
    /// - `CREATE KEYSPACE IF NOT EXISTS` no-ops over a leftover eventually consistent
    ///   keyspace from an earlier run;
    /// - a `read`-only run never creates the keyspace at all;
    /// - the server lacks `--experimental-features=strongly-consistent-tables`, so
    ///   `system_schema.scylla_keyspaces.consistency` does not exist and every keyspace
    ///   reads back as eventual;
    /// - the server takes `consistency = 'global'` but advertises no
    ///   `TABLETS_ROUTING_V2_EXPERIMENTAL`, so the driver never sees a leader-ordered
    ///   replica list and spreads the load over followers.
    ///
    /// When `consistency` was not requested the findings are only logged - existing
    /// eventually consistent runs must keep working unchanged.
    pub async fn verify_consistency_mode(&self, session: &Session) -> Result<()> {
        // The DDL above may have raced the background metadata refresh, so force one before
        // reading the mode back: the keyspace's existence is checked against cluster metadata,
        // and the driver's own routing decisions read the same snapshot.
        session
            .refresh_metadata()
            .await
            .context("Failed to refresh cluster metadata")?;

        let keyspace = &self.schema.keyspace;
        // `None` means the keyspace does not exist at all, which is a different thing from
        // existing as eventually consistent, and the two get different messages below.
        let mode = match session.get_cluster_state().get_keyspace(keyspace) {
            Some(_) => Some(read_consistency_mode(session, keyspace).await?),
            None => None,
        };

        match mode {
            Some(mode) => println!("Keyspace '{keyspace}' consistency mode: {mode:?}"),
            None => println!("Keyspace '{keyspace}' consistency mode: unknown (keyspace not found in cluster metadata)"),
        }

        let wants_strong_consistency = self.schema.wants_strong_consistency();

        if wants_strong_consistency {
            anyhow::ensure!(
                mode == Some(ConsistencyMode::Global),
                "Requested consistency=global, but keyspace '{keyspace}' reports {mode:?}. \
                 This run would not measure strong consistency. Check that:\n\
                 - the server runs with --experimental-features=strongly-consistent-tables;\n\
                 - keyspace '{keyspace}' does not already exist as an eventually consistent \
                 keyspace (CREATE KEYSPACE IF NOT EXISTS will not upgrade it - drop it first);\n\
                 - the keyspace is tablet-based (non-tablet keyspaces reject the consistency \
                 option; SimpleStrategy may not get tablets).\n\
                 DDL used: {ddl}",
                ddl = self.schema.construct_keyspace_creation_query(),
            );
        }

        if mode != Some(ConsistencyMode::Global) {
            return Ok(());
        }

        // Leader routing is gated on the request's consistency level: the driver keeps
        // normal spread routing at ONE/LOCAL_ONE. See `DefaultPolicy::should_route_to_leader`.
        // `local_one` is the default `cl`, so this is a real drift hazard.
        //
        // This and the extension check below are keyed on the mode the keyspace actually
        // has, not on what was requested: a pre-provisioned strongly consistent keyspace is
        // leader-routed whether or not `consistency=global` was passed, since
        // `CREATE KEYSPACE IF NOT EXISTS` no-ops over it and the mode is a property of the
        // keyspace, not of the CLI flag.
        let cl = self.command_params.common.consistency_level;
        if matches!(cl, Consistency::One | Consistency::LocalOne) {
            println!();
            println!(
                "WARNING: keyspace '{keyspace}' is strongly consistent, but cl={cl} \
                 disables leader-aware routing - the driver keeps normal spread routing \
                 at ONE and LOCAL_ONE (see DefaultPolicy::should_route_to_leader). \
                 Requests will be spread over replicas and bounced to the leader. \
                 Use cl=QUORUM to measure strong consistency."
            );
            println!();
        }

        self.verify_leader_aware_routing(wants_strong_consistency)
            .await
    }

    /// Checks that the node can hand the driver a leader-ordered replica list at all, which
    /// is a server capability separate from accepting `consistency = 'global'`: ScyllaDB
    /// 2026.2.x accepts the keyspace option while advertising only `TABLETS_ROUTING_V1`.
    ///
    /// Without the V2 extension every request is spread over the tablet's replicas and
    /// bounced to the leader, which is exactly the extra hop a strong-consistency benchmark
    /// is meant to measure the absence of - so for a run that asked for `consistency=global`
    /// this is a startup failure, the same as an eventually consistent keyspace would be.
    ///
    /// The probe needs its own plaintext connection, so a TLS run cannot have one; nor can a
    /// run whose contact point is unreachable by the time this executes. Neither is evidence
    /// that routing is broken, so both only warn.
    async fn verify_leader_aware_routing(&self, wants_strong_consistency: bool) -> Result<()> {
        let keyspace = &self.schema.keyspace;

        let Some(node) = self.node.nodes.first() else {
            return Ok(());
        };

        if self.transport.truststore.is_some() || self.transport.keystore.is_some() {
            println!();
            println!(
                "WARNING: cannot verify that node '{node}' supports leader-aware routing: \
                 the protocol extension probe speaks plaintext CQL and this run uses TLS. \
                 Confirm the routing from the operations-per-coordinator distribution \
                 (-log coordinators=true) instead."
            );
            println!();
            return Ok(());
        }

        let features = match fetch_protocol_features(node).await {
            Ok(features) => features,
            Err(error) => {
                println!();
                println!(
                    "WARNING: cannot verify that node '{node}' supports leader-aware routing: \
                     {error:#}. Confirm the routing from the operations-per-coordinator \
                     distribution (-log coordinators=true) instead."
                );
                println!();
                return Ok(());
            }
        };

        if features.tablets_v2_supported {
            println!(
                "Leader-aware routing: enabled (node '{node}' advertises \
                 TABLETS_ROUTING_V2_EXPERIMENTAL)"
            );
            return Ok(());
        }

        anyhow::ensure!(
            !wants_strong_consistency,
            "Requested consistency=global, and keyspace '{keyspace}' is strongly consistent, \
             but node '{node}' does not advertise the TABLETS_ROUTING_V2_EXPERIMENTAL \
             protocol extension. This is a capability separate from accepting \
             consistency='global' (e.g. ScyllaDB 2026.2.x accepts the option while \
             advertising only TABLETS_ROUTING_V1). Without it the driver never receives a \
             leader-ordered replica list, so requests are spread over the tablet's replicas \
             and bounced to the leader - this run would measure that extra hop, not strong \
             consistency."
        );

        println!();
        println!(
            "WARNING: keyspace '{keyspace}' is strongly consistent, but node '{node}' does \
             not advertise TABLETS_ROUTING_V2_EXPERIMENTAL - the driver cannot route to \
             tablet leaders, so requests will be spread over replicas and bounced to the \
             leader."
        );
        println!();

        Ok(())
    }
}

/// The consistency mode of a keyspace, as reported by the `consistency` column of
/// `system_schema.scylla_keyspaces`.
///
/// Mirrors the driver's own `ConsistencyMode`, which is deliberately crate-private: strong
/// consistency is an experimental server-side feature, so the driver does not want to freeze
/// these names in its public API yet, and it exposes no accessor for the mode either. Reading
/// the column directly is the only way to report the mode, and it is the same column the
/// driver's routing decision is based on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConsistencyMode {
    /// Eventual consistency. Covers every non-tablet keyspace and every keyspace on a
    /// server that does not report a consistency mode.
    Eventual,
    /// Reserved for a future per-datacenter strong-consistency mode (`consistency = 'local'`).
    Local,
    /// Global strong consistency (`consistency = 'global'`): the keyspace uses
    /// strongly-consistent (Raft-based) tablets.
    Global,
}

/// Reads `keyspace`'s consistency mode from `system_schema.scylla_keyspaces`.
///
/// The whole (tiny) table is scanned and filtered here rather than queried by primary key, so
/// that the statement carries no values and is therefore never prepared: on a server without
/// the table a prepare would fail differently from a request, and one error shape to recognise
/// is enough.
///
/// A keyspace with no row there, an unrecognised value, a missing `consistency` column and a
/// missing table all mean the same thing - eventual consistency - exactly as they do in the
/// driver.
async fn read_consistency_mode(session: &Session, keyspace: &str) -> Result<ConsistencyMode> {
    let result = match session
        .query_unpaged(
            "SELECT keyspace_name, consistency FROM system_schema.scylla_keyspaces",
            (),
        )
        .await
    {
        Ok(result) => result,
        // Cassandra has no `scylla_keyspaces` table, and ScyllaDB versions predating strong
        // consistency have the table but not the column. Both answer `Invalid`, which is the
        // same signal the driver itself keys on.
        Err(error) if is_missing_table_or_column(&error) => return Ok(ConsistencyMode::Eventual),
        Err(error) => {
            return Err(error).context("Failed to query system_schema.scylla_keyspaces");
        }
    };

    let rows_result = result
        .into_rows_result()
        .context("Failed to convert system_schema.scylla_keyspaces result to rows")?;

    for row in rows_result
        .rows::<(String, Option<String>)>()
        .context("Failed to deserialize system_schema.scylla_keyspaces")?
    {
        let (keyspace_name, consistency) =
            row.context("Failed to deserialize a system_schema.scylla_keyspaces row")?;
        if keyspace_name == keyspace {
            return Ok(match consistency.as_deref() {
                Some("global") => ConsistencyMode::Global,
                Some("local") => ConsistencyMode::Local,
                _ => ConsistencyMode::Eventual,
            });
        }
    }

    Ok(ConsistencyMode::Eventual)
}

/// Whether the error says that a ScyllaDB-specific system table or column is not there.
///
/// Same detection as the driver's, down to the caveat: this catches every database error
/// carrying `Invalid`, not only the ones a missing table or column causes.
fn is_missing_table_or_column(error: &ExecutionError) -> bool {
    matches!(
        error,
        ExecutionError::LastAttemptError(RequestAttemptError::DbError(DbError::Invalid, _))
    )
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
