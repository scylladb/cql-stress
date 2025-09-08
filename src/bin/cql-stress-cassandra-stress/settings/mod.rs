use std::collections::HashMap;
use std::iter::Iterator;

mod command;
mod option;
mod param;
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

use crate::settings::command::print_help;

use self::command::parse_command;
use self::option::ColumnOption;
use self::option::ModeOption;
use self::option::NodeOption;
use self::option::PopulationOption;
use self::option::RateOption;
use self::option::SchemaOption;
use self::option::TransportOption;

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
