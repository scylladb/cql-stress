use anyhow::Result;

use super::{Distribution, ThreadLocalRandom};

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
