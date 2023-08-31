use std::sync::atomic::{AtomicI64, Ordering};

use super::{Distribution, DistributionFactory};
use anyhow::{Context, Result};
use cql_stress::distribution::Description;

/// Sequence distribution. Samples values from `start` to `end` in a sequence manner.
/// Once the `end` is sampled, the cycle starts over again. It means that the sequence of the sampled values will look like:
/// `start`, `start` + 1, `start` + 2, ..., `end`, `start`, `start` + 1, ..., etc.
/// See: https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/DistributionSequence.java.
///
/// `seed` in this case is just an atomic counter. It's incremented each time we sample from this distribution.
///
/// Note - the distribution constructed with `new` constructor is always deterministic. It initiates the `seed` counter with 0.
pub struct SeqDistribution {
    start: i64,
    end: i64,
    seed: AtomicI64,
}

impl SeqDistribution {
    pub fn new(start: i64, end: i64) -> Result<Self> {
        anyhow::ensure!(
            start < end,
            "Upper bound ({}) for sequence distribution is smaller or equal than the lower bound ({}).",
            end,
            start
        );

        Ok(Self {
            start,
            end,
            // Since the users of this distribution expect it to be deterministic,
            // we initiate the `seed` (counter) with 0.
            seed: AtomicI64::new(0),
        })
    }

    fn total(&self) -> i64 {
        self.end - self.start + 1
    }
}

impl Distribution for SeqDistribution {
    fn next_i64(&self) -> i64 {
        let seed = self.seed.fetch_add(1, Ordering::Relaxed);
        self.start + seed % self.total()
    }

    fn next_f64(&self) -> f64 {
        self.next_i64() as f64
    }

    fn set_seed(&self, seed: i64) {
        self.seed.store(seed, Ordering::Relaxed);
    }
}

pub struct SeqDistributionFactory {
    min: i64,
    max: i64,
}

impl SeqDistributionFactory {
    pub fn new(min: i64, max: i64) -> Result<Self> {
        anyhow::ensure!(
            min < max,
            "Upper bound ({}) for sequence distribution is smaller or equal than the lower bound ({}).",
            max,
            min
        );

        Ok(Self { min, max })
    }
}

impl DistributionFactory for SeqDistributionFactory {
    fn create(&self) -> Box<dyn Distribution> {
        Box::new(SeqDistribution::new(self.min, self.max).unwrap())
    }
}

impl SeqDistributionFactory {
    pub fn parse_from_description(desc: Description<'_>) -> Result<Box<dyn DistributionFactory>> {
        let result = || -> Result<Box<dyn DistributionFactory>> {
            desc.check_argument_count(2)?;
            let (min, max) = (desc.args[0].parse::<i64>()?, desc.args[1].parse::<i64>()?);

            Ok(Box::new(SeqDistributionFactory::new(min, max)?))
        }();

        result.with_context(|| {
            format!(
                "Invalid parameter list for sequence distribution: {:?}",
                desc.args
            )
        })
    }

    pub fn help_description() -> String {
        format!(
            "      {:<36} A fixed sequence, returning values in the range min to max sequentially (starting based on seed), wrapping if necessary.",
            "SEQ(min..max)"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::SeqDistribution;
    use crate::java_generate::distribution::Distribution;

    #[test]
    fn sequence_distribution_test() {
        let seq = SeqDistribution::new(1, 100).unwrap();
        for _ in 0..5 {
            for i in 0..100 {
                assert_eq!(i + 1, seq.next_i64());
            }
        }

        seq.set_seed(103);
        assert_eq!(4, seq.next_i64());
    }
}
