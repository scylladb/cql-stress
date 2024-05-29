use super::distribution::{uniform::UniformDistribution, Distribution};
use scylla::{
    frame::response::result::CqlValue,
    transport::partitioner::{Murmur3Partitioner, Partitioner},
};

pub mod blob;
pub mod hex_blob;

pub use blob::Blob;
pub use hex_blob::HexBlob;

/// Generic generator of random values.
/// Holds the distributions that the seeds and sizes are sampled from.
/// Wraps the actual generator which makes use of the distributions.
pub struct Generator<T: ValueGenerator> {
    salt: i64,
    identity_distribution: Box<dyn Distribution>,
    size_distribution: Box<dyn Distribution>,
    gen: T,
}

impl<T: ValueGenerator> Generator<T> {
    pub fn new(gen: T, config: GeneratorConfig) -> Self {
        let salt = config.salt;
        let identity_distribution = match config.identity_distribution {
            Some(dist) => dist,
            None => Self::default_identity_distribution(),
        };
        let size_distribution = match config.size_distribution {
            Some(dist) => dist,
            None => Self::default_size_distribution(),
        };

        Self {
            salt,
            identity_distribution,
            size_distribution,
            gen,
        }
    }

    pub fn set_seed(&mut self, seed: i64) {
        self.identity_distribution.set_seed(seed ^ self.salt);
    }

    pub fn generate(&mut self) -> CqlValue {
        self.gen.generate(
            self.identity_distribution.as_mut(),
            self.size_distribution.as_mut(),
        )
    }

    /// See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/values/Generator.java#L59
    fn default_identity_distribution() -> Box<dyn Distribution> {
        Box::new(UniformDistribution::new(1.0, 100_000_000_000.0).unwrap())
    }

    /// See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/values/Generator.java#L64
    fn default_size_distribution() -> Box<dyn Distribution> {
        Box::new(UniformDistribution::new(4.0, 8.0).unwrap())
    }
}

/// Generator config - used to construct new Generator instance.
pub struct GeneratorConfig {
    salt: i64,
    identity_distribution: Option<Box<dyn Distribution>>,
    size_distribution: Option<Box<dyn Distribution>>,
}

impl GeneratorConfig {
    /// As in c-s, receive some seed string which is hashed to retrieve the
    /// salt value. See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/values/GeneratorConfig.java#L39.
    pub fn new(
        seed_str: &str,
        identity_distribution: Option<Box<dyn Distribution>>,
        size_distribution: Option<Box<dyn Distribution>>,
    ) -> Self {
        let bytes = seed_str.as_bytes();
        let salt = Murmur3Partitioner.hash_one(bytes);
        Self {
            salt: salt.value(),
            identity_distribution,
            size_distribution,
        }
    }
}

/// The actual value Generator trait.
pub trait ValueGenerator {
    fn generate(
        &mut self,
        identity_distribution: &mut dyn Distribution,
        size_distribution: &mut dyn Distribution,
    ) -> CqlValue;
}

#[cfg(test)]
mod tests {
    use super::{blob::Blob, Generator, GeneratorConfig};

    #[test]
    fn generator_config_salt_test() {
        let blob_gen = Blob::default();
        // "randomstr<column_name>" is the seed string passed to the generator.
        // It used used to compute the salt which is applied to the seed when seeding underlying rng.
        let config = GeneratorConfig::new("randomstrC0", None, None);
        let gen = Generator::new(blob_gen, config);
        // This value was computed using Java's implementation of Generator.
        assert_eq!(gen.salt, 5919258029671157411);
    }
}
