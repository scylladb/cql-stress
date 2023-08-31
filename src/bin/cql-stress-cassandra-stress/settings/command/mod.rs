use anyhow::Context;
use std::convert::AsRef;
use std::str::FromStr;
use strum::IntoEnumIterator;
use strum::ParseError;
use strum_macros::AsRefStr;
use strum_macros::EnumIter;
use strum_macros::EnumString;

use anyhow::Result;

mod help;
mod read_write;

use self::read_write::{parse_read_write_params, print_help_read_write};
pub use help::print_help;

use super::ParsePayload;
use help::parse_help_command;
use read_write::ReadWriteParams;

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

    fn parse_params(&self, payload: &mut ParsePayload) -> Result<CommandParams> {
        match self {
            Command::Read | Command::Write | Command::CounterRead | Command::CounterWrite => {
                parse_read_write_params(self, payload)
            }
            Command::Help => parse_help_command(payload),
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
            Command::Read | Command::Write | Command::CounterRead | Command::CounterWrite => {
                print_help_read_write(self.show())
            }
            Command::Help => help::print_help(),
        }
    }
}

pub enum CommandParams {
    // HELP, PRINT, VERSION
    Special,

    // READ, WRITE, COUNTER_READ, COUNTER_WRITE
    BasicParams(ReadWriteParams),
    // MIXED
    // TODO: MixedParams,

    // USER
    // TODO: UserParams,
}

impl CommandParams {
    pub fn print_settings(&self, cmd: &Command) {
        if let Self::BasicParams(params) = self {
            params.print_settings(cmd);
        }
    }
}

pub fn parse_command(
    command_str: &str,
    cl_args: &mut ParsePayload,
) -> Result<(Command, CommandParams)> {
    let command = Command::parse(command_str).context("No command specified")?;
    let params = command.parse_params(cl_args)?;
    Ok((command, params))
}
