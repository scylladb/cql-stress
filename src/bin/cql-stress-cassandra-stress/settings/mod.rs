use std::collections::HashMap;
use std::iter::Iterator;

mod command;
mod option;
mod param;
use anyhow::Result;

#[cfg(test)]
mod test;

use command::Command;
use command::CommandParams;
use regex::Regex;

use crate::settings::command::print_help;

use self::command::parse_command;

pub struct CassandraStressSettings {
    pub command: Command,
    pub params: CommandParams,
}

impl CassandraStressSettings {
    fn new(command: Command, params: CommandParams) -> Self {
        Self { command, params }
    }

    pub fn print_settings(&self) {
        println!("******************** Stress Settings ********************");
        self.params.print_settings(&self.command);
        println!();
    }
}

pub enum CassandraStressParsingResult {
    // HELP, PRINT, VERSION
    SpecialCommand,
    Workload(Box<CassandraStressSettings>),
}

type ParsePayload<'a> = HashMap<&'a str, Vec<&'a str>>;

/// Groups the commands/options and their corresponding parametes.
///
/// cassandra-stress accepts CLI args of the following pattern:
/// ./cassandra-stress COMMAND [command_param...] [OPTION [option_param...]...]
fn prepare_parse_payload(args: &[String]) -> Result<(&str, ParsePayload)> {
    let mut cl_args: ParsePayload = HashMap::new();
    let mut current: &str = "";
    let mut cmd: &str = "";
    for (i, arg) in args.iter().enumerate() {
        let arg = arg.as_ref();
        if i == 0 {
            cmd = arg;
        }
        if i == 0 || arg.starts_with('-') {
            anyhow::ensure!(
                !cl_args.contains_key(arg),
                "{} is defined multiple times. Each option/command can be specified at most once.",
                arg
            );
            current = arg;
            cl_args.insert(arg, vec![]);
            continue;
        }

        let params = cl_args.get_mut(current).unwrap();
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

        let (cmd, cmd_params) = match parse_command(cmd, &mut payload) {
            Ok((_, CommandParams::Special)) => {
                return Ok(CassandraStressParsingResult::SpecialCommand)
            }
            Ok((cmd, params)) => (cmd, params),
            Err(e) => return Err(e),
        };

        // TODO: parse options.

        Ok(CassandraStressParsingResult::Workload(Box::new(
            CassandraStressSettings::new(cmd, cmd_params),
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
