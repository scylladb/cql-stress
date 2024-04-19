use anyhow::Result;

use crate::{
    java_generate::distribution::DistributionFactory,
    settings::{
        param::{ParamHandle, ParamsParser, SimpleParamHandle},
        ParsePayload,
    },
};

use super::{common::CommonParamHandles, Command, CommandParams};

pub struct CounterParams {
    pub add_distribution: Box<dyn DistributionFactory>,
}

impl CounterParams {
    pub fn print_settings(&self) {
        println!("  Counter Increment Distibution: {}", self.add_distribution)
    }

    pub fn parse(cmd: &Command, payload: &mut ParsePayload) -> Result<CommandParams> {
        let args = payload.remove(cmd.show()).unwrap();
        let (parser, common_handles, add_distribution) = prepare_parser(cmd.show());
        parser.parse(args)?;
        Ok(CommandParams {
            common: super::common::parse_with_handles(common_handles),
            counter: Some(CounterParams {
                add_distribution: add_distribution.get().unwrap(),
            }),
            mixed: None,
        })
    }
}

pub struct CounterParamGroups {
    pub groups: Vec<Vec<Box<dyn ParamHandle>>>,
    pub common_handles: CommonParamHandles,
    pub add_distribution_handle: SimpleParamHandle<Box<dyn DistributionFactory>>,
}

pub fn add_counter_param_groups(parser: &mut ParamsParser) -> CounterParamGroups {
    let (mut groups, common_handles) = super::common::add_common_param_groups(parser);

    let add_distribution_handle = parser.distribution_param(
        "add=",
        Some("fixed(1)"),
        "Distribution of value of counter increments",
        false,
    );

    for group in groups.iter_mut() {
        group.push(Box::new(add_distribution_handle.clone()));
    }

    CounterParamGroups {
        groups,
        common_handles,
        add_distribution_handle,
    }
}

fn prepare_parser(
    cmd: &str,
) -> (
    ParamsParser,
    CommonParamHandles,
    SimpleParamHandle<Box<dyn DistributionFactory>>,
) {
    let mut parser = ParamsParser::new(cmd);

    let mut counter_payload = add_counter_param_groups(&mut parser);

    for group in counter_payload.groups.iter_mut() {
        parser.group_iter(group.iter().map(|e| e.as_ref()))
    }

    (
        parser,
        counter_payload.common_handles,
        counter_payload.add_distribution_handle,
    )
}

pub fn print_help_counter(command_str: &str) {
    let (parser, _, _) = prepare_parser(command_str);
    parser.print_help();
}
