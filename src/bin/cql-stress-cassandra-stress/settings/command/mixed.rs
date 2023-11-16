use std::collections::HashSet;

use crate::{
    java_generate::distribution::enumerated::EnumeratedDistribution,
    settings::param::types::Parsable,
};
use anyhow::{Context, Result};

use super::Command;

// Available subcommands for mixed command.
#[derive(Copy, Clone, Hash, PartialEq, Eq)]
pub enum MixedSubcommand {
    Read,
    Write,
    CounterRead,
    CounterWrite,
}

impl std::fmt::Display for MixedSubcommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MixedSubcommand::Read => "read",
            MixedSubcommand::Write => "write",
            MixedSubcommand::CounterRead => "counter_read",
            MixedSubcommand::CounterWrite => "counter_write",
        };
        write!(f, "{}", s)
    }
}

pub type OperationRatio = EnumeratedDistribution<MixedSubcommand>;

// There are 4 suboperations which can be sampled during mixed workloads:
// - read
// - write
// - counter_read
// - counter_write
//
// A user can specify a ratio with which the suboperations will be sampled.
// The syntax for this parameter is (op1=x, op2=y, op3=z, ...)
// where op1..n are one of the 4 operations mentioned above, and x,y,z are floats.
//
// For example:
// ratio(read=1, write=2) means that there will be approximately 1 read operation per 2 write operations.
impl Parsable for OperationRatio {
    type Parsed = Self;

    fn parse(s: &str) -> Result<Self::Parsed> {
        Self::do_parse(s).with_context(|| format!("invalid operation ratio specification: {}", s))
    }
}

impl OperationRatio {
    fn parse_command_weight(s: &str) -> Result<(MixedSubcommand, f64)> {
        let (cmd, weight) = {
            let mut iter = s.split('=').fuse();
            match (iter.next(), iter.next(), iter.next()) {
                (Some(cmd), Some(w), None) => (cmd, w),
                _ => anyhow::bail!(
                    "Command weight specification should match pattern <command>=<f64>"
                ),
            }
        };

        let command = match Command::parse(cmd)? {
            Command::Read => MixedSubcommand::Read,
            Command::Write => MixedSubcommand::Write,
            Command::CounterRead => MixedSubcommand::CounterRead,
            Command::CounterWrite => MixedSubcommand::CounterWrite,
            _ => anyhow::bail!("Invalid command for mixed workload: {}", cmd),
        };
        let weight = weight.parse::<f64>()?;
        Ok((command, weight))
    }

    fn do_parse(s: &str) -> Result<Self> {
        // Remove wrapping parenthesis.
        let arg = {
            let mut chars = s.chars();
            anyhow::ensure!(
                chars.next() == Some('(') && chars.next_back() == Some(')'),
                "Invalid operation ratio specification: {}",
                s
            );
            chars.as_str()
        };

        let mut command_set = HashSet::<MixedSubcommand>::new();
        let weights = arg
            .split(',')
            .map(|s| -> Result<(MixedSubcommand, f64)> {
                let (command, weight) = Self::parse_command_weight(s)?;
                anyhow::ensure!(
                    !command_set.contains(&command),
                    "{} command has been specified more than once",
                    command
                );
                command_set.insert(command);
                Ok((command, weight))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Self::new(weights)
    }
}
