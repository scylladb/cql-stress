use anyhow::Context;
use std::convert::AsRef;
use std::str::FromStr;
use strum::IntoEnumIterator;
use strum::ParseError;
use strum_macros::AsRefStr;
use strum_macros::EnumIter;
use strum_macros::EnumString;

use anyhow::Result;

mod common;
mod counter;
mod help;
mod mixed;

use self::common::{parse_common_params, print_help_common};
use self::counter::print_help_counter;
use self::counter::CounterParams;

pub use help::print_help;

use super::ParsePayload;
use common::CommonParams;
use help::parse_help_command;
pub use mixed::MixedSubcommand;
pub use mixed::OperationRatio;

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, AsRefStr, EnumString)]
#[strum(serialize_all = "snake_case")]
#[strum(ascii_case_insensitive)]
pub enum Command {
    Help,
    Write,
    Read,
    CounterWrite,
    CounterRead,
}

impl Command {
    fn parse(cmd: &str) -> Result<Self, ParseError> {
        Self::from_str(cmd)
    }

    fn parse_params(&self, payload: &mut ParsePayload) -> Result<Option<CommandParams>> {
        match self {
            Command::Read | Command::Write | Command::CounterRead => {
                Ok(Some(parse_common_params(self, payload)?))
            }
            Command::CounterWrite => Ok(Some(CounterParams::parse(self, payload)?)),
            Command::Help => {
                parse_help_command(payload)?;
                Ok(None)
            }
        }
    }

    pub fn show(&self) -> &str {
        self.as_ref()
    }

    fn print_short_description(&self) {
        let desc = match self {
            Command::Read => "Multiple concurrent reads - the cluster must first be populated by a write test.",
            Command::Write => "Multiple concurrent writes against the cluster.",
            Command::CounterWrite => "Multiple concurrent updates of counters.",
            Command::CounterRead => "Multiple concurrent reads of counters. The cluster must first be populated by a counterwrite test.",
            Command::Help => "Print help for a command or option",
        };

        println!("{:<20} : {}", self.show(), desc);
    }

    fn print_generic_help() {
        println!("---Commands---");
        for cmd in Self::iter() {
            cmd.print_short_description();
        }
    }

    fn print_help(&self) {
        match self {
            Command::Read | Command::Write | Command::CounterRead => print_help_common(self.show()),
            Command::CounterWrite => print_help_counter(self.show()),
            Command::Help => help::print_help(),
        }
    }
}

pub struct CommandParams {
    // Parameters shared across all of the commands
    pub common: CommonParams,
    pub counter: Option<CounterParams>,
}

impl CommandParams {
    pub fn print_settings(&self, cmd: &Command) {
        self.common.print_settings(cmd);
        if let Some(counter) = &self.counter {
            counter.print_settings()
        }
    }
}

pub fn parse_command(
    command_str: &str,
    cl_args: &mut ParsePayload,
) -> Result<(Command, Option<CommandParams>)> {
    let command = Command::parse(command_str).context("No command specified")?;
    let params = command.parse_params(cl_args)?;
    Ok((command, params))
}
