use anyhow::{Context, Result};

use cql_stress::distribution::Description;

use super::{Distribution, DistributionFactory};

/// Distribution that always returns fixed value.
/// See: https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/DistributionFixed.java.
pub struct FixedDistribution {
    value: i64,
}

impl FixedDistribution {
    pub fn new(value: i64) -> Self {
        Self { value }
    }
}

impl Distribution for FixedDistribution {
    fn next_i64(&self) -> i64 {
        self.value
    }

    fn next_f64(&self) -> f64 {
        self.value as f64
    }

    fn set_seed(&self, _seed: i64) {}
}

pub struct FixedDistributionFactory(pub i64);

impl DistributionFactory for FixedDistributionFactory {
    fn create(&self) -> Box<dyn Distribution> {
        Box::new(FixedDistribution::new(self.0))
    }
}

impl FixedDistributionFactory {
    pub fn parse_from_description(desc: Description<'_>) -> Result<Box<dyn DistributionFactory>> {
        let result = || -> Result<Box<dyn DistributionFactory>> {
            desc.check_argument_count(1)?;
            let value = desc.args[0].parse::<i64>()?;

            Ok(Box::new(FixedDistributionFactory(value)))
        }();

        result.with_context(|| {
            format!(
                "Invalid parameter list for fixed distribution: {:?}",
                desc.args
            )
        })
    }

    pub fn help_description() -> String {
        format!(
            "      {:<36} A fixed distribution, always returning the same value",
            "FIXED(val)"
        )
    }
}
