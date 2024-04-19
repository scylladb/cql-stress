use std::collections::HashSet;

use crate::{
    java_generate::distribution::{enumerated::EnumeratedDistribution, DistributionFactory},
    settings::{
        param::{types::Parsable, ParamsParser, SimpleParamHandle},
        ParsePayload,
    },
};
use anyhow::{Context, Result};

use super::{common::CommonParamHandles, counter::CounterParams, Command, CommandParams};

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

pub struct MixedParamHandles {
    operation_ratio: SimpleParamHandle<OperationRatio>,
    clustering: SimpleParamHandle<Box<dyn DistributionFactory>>,
}

pub struct MixedParams {
    pub operation_ratio: OperationRatio,
    pub clustering: Box<dyn DistributionFactory>,
}

impl MixedParams {
    pub fn print_settings(&self) {
        println!("Command ratios: {}", self.operation_ratio);
        println!("Command clustering distribution: {}", self.clustering);
    }

    pub fn parse(cmd: &Command, payload: &mut ParsePayload) -> Result<CommandParams> {
        let args = payload.remove(cmd.show()).unwrap();
        let (parser, common_handles, counter_add_distribution_handle, mixed_handles) =
            prepare_parser(cmd.show());
        parser.parse(args)?;
        Ok(CommandParams {
            common: super::common::parse_with_handles(common_handles),
            counter: Some(CounterParams {
                add_distribution: counter_add_distribution_handle.get().unwrap(),
            }),
            mixed: Some(MixedParams {
                operation_ratio: mixed_handles.operation_ratio.get().unwrap(),
                clustering: mixed_handles.clustering.get().unwrap(),
            }),
        })
    }
}

fn prepare_parser(
    cmd: &str,
) -> (
    ParamsParser,
    CommonParamHandles,
    SimpleParamHandle<Box<dyn DistributionFactory>>,
    MixedParamHandles,
) {
    let mut parser = ParamsParser::new(cmd);

    let mut counter_payload = super::counter::add_counter_param_groups(&mut parser);

    let operation_ratio = parser.simple_param("ratio", Some("(read=1,write=1)"), "Specify the ratios for operations to perform; e.g. ratio(read=2,write=1) will perform 2 reads for each write. Available commands are: read, write, counter_write, counter_read.", false);
    let clustering = parser.distribution_param(
        "clustering=",
        Some("GAUSSIAN(1..10)"),
        "Distribution clustering runs of operations of the same kind",
        false,
    );

    for group in counter_payload.groups.iter_mut() {
        group.push(Box::new(operation_ratio.clone()));
        group.push(Box::new(clustering.clone()));
        parser.group_iter(group.iter().map(|e| e.as_ref()))
    }

    (
        parser,
        counter_payload.common_handles,
        counter_payload.add_distribution_handle,
        MixedParamHandles {
            operation_ratio,
            clustering,
        },
    )
}

pub fn print_help_mixed(command_str: &str) {
    let (parser, _, _, _) = prepare_parser(command_str);
    parser.print_help();
}
