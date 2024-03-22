use super::distribution::{uniform::UniformDistribution, Distribution};
#[cfg(feature = "user-profile")]
use scylla::transport::topology::CqlType;
use scylla::{
    frame::response::result::CqlValue,
    transport::partitioner::{Murmur3Partitioner, Partitioner},
};

#[cfg(feature = "user-profile")]
use anyhow::Result;

pub mod blob;
pub mod hex_blob;

pub use blob::Blob;
pub use hex_blob::HexBlob;

/// Generic generator of random values.
/// Holds the distributions that the seeds and sizes are sampled from.
/// Wraps the actual generator which makes use of the distributions.
pub struct Generator {
    salt: i64,
    identity_distribution: Box<dyn Distribution>,
    size_distribution: Box<dyn Distribution>,
    gen: Box<dyn ValueGenerator>,
    // Allow unused in case `user-profile` feature is not enabled.
    #[allow(unused)]
    col_name: String,
}

impl Generator {
    pub fn new(gen: Box<dyn ValueGenerator>, config: GeneratorConfig, col_name: String) -> Self {
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
            col_name,
        }
    }

    #[cfg(feature = "user-profile")]
    pub fn new_generator_factory_from_cql_type(
        typ: &CqlType,
    ) -> Result<Box<dyn ValueGeneratorFactory>> {
        use self::blob::BlobFactory;

        match typ {
            CqlType::Native(native_type) => match native_type {
                scylla::transport::topology::NativeType::Blob => Ok(Box::new(BlobFactory)),
                _ => anyhow::bail!(
                    "Column type {:?} is not yet supported by the tool!",
                    native_type
                ),
            },
            CqlType::Collection { .. } => anyhow::bail!(
                "Unsupported column type: {:?}. Collection types are not yet supported by the tool!",
                typ
            ),
            CqlType::Tuple(_) => anyhow::bail!(
                "Unsupported column type: {:?}. Tuples are not yet supported by the tool!",
                typ
            ),
            CqlType::UserDefinedType { .. } => anyhow::bail!(
                "Unsupported column type: {:?}. UDTs are not yet supported by the tool!",
                typ
            ),
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

    #[cfg(feature = "user-profile")]
    pub fn get_col_name(&self) -> &str {
        &self.col_name
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
pub trait ValueGenerator: Send + Sync + 'static {
    fn generate(
        &mut self,
        identity_distribution: &mut dyn Distribution,
        size_distribution: &mut dyn Distribution,
    ) -> CqlValue;
}

/// This trait provides an infallible way to create a corresponding
/// [`ValueGenerator`] once the native type is deduced from metadata.
///
/// - Why not just clone a ValueGenerator once created?
/// Since we make use of trait objects, we cannot expect [`ValueGenerator`]
/// to implement [`Clone`] as well.
#[cfg(feature = "user-profile")]
pub trait ValueGeneratorFactory: Send + Sync {
    fn create(&self) -> Box<dyn ValueGenerator>;
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
        let gen = Generator::new(Box::new(blob_gen), config, String::from("C0"));
        // This value was computed using Java's implementation of Generator.
        assert_eq!(gen.salt, 5919258029671157411);
    }
}
