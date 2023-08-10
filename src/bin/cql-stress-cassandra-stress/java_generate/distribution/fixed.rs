use super::Distribution;

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
