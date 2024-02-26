use anyhow::Result;

use crate::{
    java_generate::distribution::{sequence::SeqDistributionFactory, DistributionFactory},
    settings::{
        param::{
            types::{Count, Parsable, Range},
            ParamsParser, SimpleParamHandle,
        },
        ParsePayload,
    },
};

pub struct PopulationOption {
    pub pk_seed_distribution: Box<dyn DistributionFactory>,
}

impl PopulationOption {
    pub const CLI_STRING: &'static str = "-pop";

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
        let pk_seed_distribution = match handles.bash_friendly_seq_distribution.get() {
            Some(dist) => dist,
            None => handles.pk_seed_distribution.get().unwrap(),
        };

        Self {
            pk_seed_distribution,
        }
    }
}

/// Cassandra-Stress supports bash-friendly syntax for SEQ distribution: -pop seq=1..10000
/// This is equivalent to: -pop 'dist=SEQ(1..1000)'
struct BashFriendlySeqDistribution;
impl Parsable for BashFriendlySeqDistribution {
    type Parsed = Box<dyn DistributionFactory>;

    fn parse(s: &str) -> Result<Self::Parsed> {
        let (from, to) = <Range<Count> as Parsable>::parse(s)?;
        let dist = SeqDistributionFactory::new(from as i64, to as i64)?;
        Ok(Box::new(dist))
    }
}

struct PopulationParamHandles {
    pk_seed_distribution: SimpleParamHandle<Box<dyn DistributionFactory>>,
    bash_friendly_seq_distribution: SimpleParamHandle<BashFriendlySeqDistribution>,
}

fn prepare_parser(operation_count: &str) -> (ParamsParser, PopulationParamHandles) {
    let mut parser = ParamsParser::new(PopulationOption::CLI_STRING);

    let bash_friendly_seq_distribution = parser.simple_param("seq=", Some(&format!("1..{operation_count}")), "Generate all seeds in sequence. The default value is 1..N where N is operation count if specified, 1000000 otherwise.", false);
    let pk_seed_distribution = parser.distribution_param(
        "dist=",
        None,
        "Seeds are selected from this distribution.",
        false,
    );

    // $ ./cassandra-stress help -pop
    // Usage: -pop [seq=?]
    //   OR
    // Usage: -pop [dist=DIST(?)]
    parser.group(&[&bash_friendly_seq_distribution]);
    parser.group(&[&pk_seed_distribution]);

    (
        parser,
        PopulationParamHandles {
            pk_seed_distribution,
            bash_friendly_seq_distribution,
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
