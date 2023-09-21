use anyhow::{Context, Result};
use cql_stress::distribution::Description;

use super::{Distribution, DistributionFactory, ThreadLocalRandom};

/// Normal distribution based on https://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/src-html/org/apache/commons/math3/distribution/NormalDistribution.
struct NormalDistribution {
    min: i64,
    max: i64,
    mean: f64,
    standard_deviation: f64,
    rng: ThreadLocalRandom,
}

impl NormalDistribution {
    fn verify_args(min: i64, max: i64, standard_deviation: f64) -> Result<()> {
        anyhow::ensure!(
            min < max,
            "Upper bound ({}) for normal distribution is not higher than the lower bound ({}).",
            max,
            min
        );
        anyhow::ensure!(
            standard_deviation > 0f64,
            "Standard deviation must be positive"
        );

        Ok(())
    }

    pub fn new(min: i64, max: i64, mean: f64, standard_deviation: f64) -> Result<Self> {
        Self::verify_args(min, max, standard_deviation)?;
        Ok(Self {
            min,
            max,
            mean,
            standard_deviation,
            rng: ThreadLocalRandom::new(),
        })
    }

    fn sample(&self) -> f64 {
        self.standard_deviation * self.rng.get().next_gaussian() + self.mean
    }
}

impl Distribution for NormalDistribution {
    fn next_i64(&self) -> i64 {
        (self.sample() as i64).clamp(self.min, self.max)
    }

    fn next_f64(&self) -> f64 {
        self.sample().clamp(self.min as f64, self.max as f64)
    }

    fn set_seed(&self, seed: i64) {
        self.rng.get().set_seed(seed as u64)
    }
}

pub struct NormalDistributionFactory {
    min: i64,
    max: i64,
    mean: f64,
    standard_deviation: f64,
}

impl NormalDistributionFactory {
    fn new(min: i64, max: i64, mean: f64, standard_deviation: f64) -> Result<Self> {
        NormalDistribution::verify_args(min, max, standard_deviation)?;
        Ok(Self {
            min,
            max,
            mean,
            standard_deviation,
        })
    }
}

impl DistributionFactory for NormalDistributionFactory {
    fn create(&self) -> Box<dyn Distribution> {
        Box::new(
            NormalDistribution::new(self.min, self.max, self.mean, self.standard_deviation)
                .unwrap(),
        )
    }
}

impl NormalDistributionFactory {
    fn do_parse_from_description(desc: &Description<'_>) -> Result<Box<dyn DistributionFactory>> {
        // See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/settings/OptionDistribution.java#L202.
        desc.check_minimum_argument_count(2)?;
        let mut iter = desc.args_fused();

        let (min, max) = (
            iter.next().unwrap().parse::<i64>()?,
            iter.next().unwrap().parse::<i64>()?,
        );

        let (mean, stdev) = match (iter.next(), iter.next(), iter.next()) {
            (Some(mean), Some(stdev), None) => (mean.parse::<f64>()?, stdev.parse::<f64>()?),
            (maybe_stdvrng, None, None) => {
                let stdevs_to_edge = maybe_stdvrng
                    .map(|s| s.parse::<f64>())
                    .unwrap_or(Ok(3f64))?;

                let mean = ((min + max) as f64) / 2f64;
                let stdev = (((max - min) as f64) / 2f64) / stdevs_to_edge;
                (mean, stdev)
            }
            _ => anyhow::bail!("Invalid arguments count"),
        };

        Ok(Box::new(Self::new(min, max, mean, stdev)?))
    }

    pub fn parse_from_description(desc: Description<'_>) -> Result<Box<dyn DistributionFactory>> {
        Self::do_parse_from_description(&desc).with_context(|| {
            format!(
                "Invalid parameter list for normal distribution: {:?}",
                desc.args
            )
        })
    }

    pub fn help_description_two_args() -> String {
        format!(
            "      {:<36} A gaussian/normal distribution, where mean=(min+max)/2, and stdev=(mean-min)/3. Aliases: GAUSS, NORMAL, NORM",
            "GAUSSIAN(min..max)"
        )
    }

    pub fn help_description_three_args() -> String {
        format!(
            "      {:<36} A gaussian/normal distribution, where mean=(min+max)/2, and stdev=(mean-min)/stdvrng. Aliases: GAUSS, NORMAL, NORM",
            "GAUSSIAN(min..max,stdvrng)"
        )
    }

    pub fn help_description_four_args() -> String {
        format!(
            "      {:<36} A gaussian/normal distribution, with explicitly defined mean and stdev. Aliases: GAUSS, NORMAL, NORM",
            "GAUSSIAN(min..max,mean,stdev)"
        )
    }
}

impl std::fmt::Display for NormalDistributionFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GAUSSIAN({}..{},mean={},stdev={})",
            self.min, self.max, self.mean, self.standard_deviation,
        )
    }
}
