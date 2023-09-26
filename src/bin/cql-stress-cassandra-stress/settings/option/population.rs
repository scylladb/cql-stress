use anyhow::Result;

use crate::{
    java_generate::distribution::DistributionFactory,
    settings::{
        param::{ParamsParser, SimpleParamHandle},
        ParsePayload,
    },
};

pub struct PopulationOption {
    pub pk_seed_distribution: Box<dyn DistributionFactory>,
}

impl PopulationOption {
    pub const CLI_STRING: &str = "-pop";

    pub fn description() -> &'static str {
        "Population distribution"
    }

    pub fn parse(cl_args: &mut ParsePayload, operation_count: &str) -> Result<Self> {
        let params = cl_args.remove(Self::CLI_STRING).unwrap_or_default();
        let (parser, handles) = prepare_parser(operation_count);
        parser.parse(params)?;
        Ok(Self::from_handles(handles))
    }

    pub fn print_help() {
        let (parser, _) = prepare_parser("1000000");
        parser.print_help();
    }

    pub fn print_settings(&self) {
        println!("Population:");
        println!(
            "  Partition key seed distribution: {}",
            self.pk_seed_distribution
        );
    }

    fn from_handles(handles: PopulationParamHandles) -> Self {
        let pk_seed_distribution = handles.pk_seed_distribution.get().unwrap();

        Self {
            pk_seed_distribution,
        }
    }
}

struct PopulationParamHandles {
    pk_seed_distribution: SimpleParamHandle<Box<dyn DistributionFactory>>,
}

fn prepare_parser(operation_count: &str) -> (ParamsParser, PopulationParamHandles) {
    let mut parser = ParamsParser::new(PopulationOption::CLI_STRING);

    let pk_seed_distribution = parser.distribution_param(
        "dist=",
        Some(&format!("seq(1..{operation_count})")),
        "Seeds are selected from this distribution. By default the distribution is seq(1..N) where N is operation count if specified, 1000000 otherwise.",
        false,
    );

    // $ ./cassandra-stress help -pop
    // Usage: -pop [dist=DIST(?)]
    parser.group(&[&pk_seed_distribution]);

    (
        parser,
        PopulationParamHandles {
            pk_seed_distribution,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::prepare_parser;

    #[test]
    fn pop_default_params_test() {
        let args = vec![];
        let (parser, _) = prepare_parser("100");

        assert!(parser.parse(args).is_ok());
    }
}
