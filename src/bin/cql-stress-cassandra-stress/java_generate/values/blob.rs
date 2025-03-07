use std::cmp::min;

use scylla::value::CqlValue;

use super::ValueGenerator;
use crate::java_generate::distribution::Distribution;
use crate::java_generate::faster_random::FasterRandom;

/// Blob generator based on c-s Bytes generator.
/// See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/values/Bytes.java#L41
#[derive(Default)]
pub struct Blob {
    rng: FasterRandom,
}

impl ValueGenerator for Blob {
    fn generate(
        &mut self,
        identity_distribution: &mut dyn Distribution,
        size_distribution: &mut dyn Distribution,
    ) -> CqlValue {
        let seed = identity_distribution.next_i64();
        size_distribution.set_seed(seed);
        self.rng.set_seed(!seed);
        let size = size_distribution.next_i64() as usize;

        let mut result = Vec::with_capacity(size);
        let mut i = 0;
        while i < size {
            let v = self.rng.next_i64().to_le_bytes();
            let n = min(size - i, v.len());
            result.extend_from_slice(&v[0..n]);
            i += n;
        }

        CqlValue::Blob(result)
    }
}

#[cfg(feature = "user-profile")]
pub struct BlobFactory;

#[cfg(feature = "user-profile")]
impl super::ValueGeneratorFactory for BlobFactory {
    fn create(&self) -> Box<dyn ValueGenerator> {
        Box::<Blob>::default()
    }
}

#[cfg(test)]
mod tests {
    use crate::java_generate::{
        distribution::fixed::FixedDistribution,
        values::{Generator, GeneratorConfig},
    };
    use scylla::value::CqlValue;

    use super::Blob;

    /// Utility function that maps u8 vector values to i8 values.
    fn to_vec_i8(v: CqlValue) -> Vec<i8> {
        v.as_blob()
            .unwrap()
            .iter()
            .map(|x| *x as i8)
            .collect::<Vec<i8>>()
    }

    #[test]
    /// This test is based on cassandra-stress Bytes generator.
    /// The command cassandra-stress was run with:
    /// write n=5 no-warmup -node 172.17.0.2 -pop seq=1..5 -col size=FIXED(5) -rate threads=1
    fn blob_generator_test() {
        // Generator of one column (by default named C0).
        let config = GeneratorConfig::new(
            // "Random" string used to generate the generator's salt.
            // In c-s, it's always "randomstr" + `column_name`.
            // See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/settings/SettingsCommandPreDefined.java#L86
            "randomstrC0",
            // Default identity distribution will be used.
            // https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/values/Generator.java#L59
            None,
            // Size distribution = FIXED(5), as stated in the command.
            Some(Box::new(FixedDistribution::new(5))),
        );
        let blob = Blob::default();
        let mut gen = Generator::new(Box::new(blob), config, String::from("C0"));

        // In cassandra-stress, the seed is obtained from the generated key.
        // Then, this seed is reused for every column in the row.
        // Currently we don't have this logic implemented, and the seeds are
        // taken from the exemplary c-s run. Note that it's deterministic, and
        // the exact same seeds will appear each time we run c-s with the command specified above.
        gen.set_seed(1338786723438483);
        let row1 = gen.generate();
        assert_eq!(to_vec_i8(row1), vec![-123, 24, 47, -33, -25]);

        gen.set_seed(2138651199823976);
        let row2 = gen.generate();
        assert_eq!(to_vec_i8(row2), vec![-72, -32, 83, 32, -51]);

        gen.set_seed(2158326113993629);
        let row3 = gen.generate();
        assert_eq!(to_vec_i8(row3), vec![95, -16, -124, 89, -52]);

        gen.set_seed(1575090586760464);
        let row4 = gen.generate();
        assert_eq!(to_vec_i8(row4), vec![16, -15, -35, 111, -21]);

        gen.set_seed(1502598601642299);
        let row5 = gen.generate();
        assert_eq!(to_vec_i8(row5), vec![-36, -98, 27, -16, 94]);
    }
}
