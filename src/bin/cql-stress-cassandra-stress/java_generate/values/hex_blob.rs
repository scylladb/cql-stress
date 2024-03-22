use scylla::frame::response::result::CqlValue;

use super::ValueGenerator;
use crate::java_generate::distribution::Distribution;

/// Based on c-s HexBytes generator.
/// Used by c-s to generate blob partition keys.
/// It generates random hexadecimal digits ranging from 0x0 to 0xF.
pub struct HexBlob;

impl ValueGenerator for HexBlob {
    fn generate(
        &mut self,
        identity_distribution: &mut dyn Distribution,
        size_distribution: &mut dyn Distribution,
    ) -> CqlValue {
        let seed = identity_distribution.next_i64();
        size_distribution.set_seed(seed);
        let size = size_distribution.next_i64() as usize;

        let mut result = Vec::with_capacity(size);
        let mut i = 0;

        // Check https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/values/HexBytes.java#L44
        while i < size {
            let mut value = identity_distribution.next_i64();
            let mut j = 0;
            while j < 16 && i + j < size {
                // Get 4 LSBs.
                let v = (value & 0xF) as i32;
                // Convert 4 LSBs to hex-digit.
                let hex_digit = ((if v < 10 { '0' as i32 } else { 'A' as i32 }) + v) as u8;
                result.push(hex_digit);
                // Unsigned right shift.
                value = ((value as u64) >> 4) as i64;
                j += 1;
            }
            i += 16;
        }

        CqlValue::Blob(result)
    }
}

#[cfg(feature = "user-profile")]
pub struct HexBlobFactory;

#[cfg(feature = "user-profile")]
impl super::ValueGeneratorFactory for HexBlobFactory {
    fn create(&self) -> Box<dyn ValueGenerator> {
        Box::new(HexBlob)
    }
}

#[cfg(test)]
mod tests {
    use crate::java_generate::{
        distribution::{fixed::FixedDistribution, sequence::SeqDistribution, Distribution},
        values::{Generator, GeneratorConfig},
    };
    use scylla::frame::response::result::CqlValue;

    use super::HexBlob;

    fn to_vec_i8(v: CqlValue) -> Vec<i8> {
        v.as_blob()
            .unwrap()
            .iter()
            .map(|x| *x as i8)
            .collect::<Vec<i8>>()
    }

    #[test]
    /// This test is based on cassandra-stress HexBytes generator.
    /// The command cassandra-stress was being run with:
    /// write n=5 no-warmup -node 172.17.0.2 -pop seq=1..5 -col size=FIXED(5) -rate threads=1
    fn hex_blob_generator_test() {
        // Generator of partition key column (by default named "key").
        let config = GeneratorConfig::new(
            // "Random" string used to generate the generator's salt.
            // In c-s, it's always "randomstr" + `column_name`.
            // See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/settings/SettingsCommandPreDefined.java#L86
            "randomstrkey",
            None,
            // By default the keysize=10
            Some(Box::new(FixedDistribution::new(10))),
        );
        let hex_blob = HexBlob;
        let mut gen = Generator::new(Box::new(hex_blob), config, String::from("key"));

        // -pop seq=1..5
        // Samples from this distrubtion are the seeds to the partition key generator.
        let seq = SeqDistribution::new(1, 5).unwrap();

        gen.set_seed(seq.next_i64());
        let key1 = gen.generate();
        assert_eq!(
            to_vec_i8(key1),
            vec![48, 80, 51, 55, 55, 48, 57, 80, 50, 49]
        );

        gen.set_seed(seq.next_i64());
        let key2 = gen.generate();
        assert_eq!(
            to_vec_i8(key2),
            vec![79, 56, 76, 75, 55, 57, 76, 79, 54, 49]
        );

        gen.set_seed(seq.next_i64());
        let key3 = gen.generate();
        assert_eq!(
            to_vec_i8(key3),
            vec![79, 80, 48, 48, 49, 76, 53, 57, 51, 48]
        );

        gen.set_seed(seq.next_i64());
        let key4 = gen.generate();
        assert_eq!(
            to_vec_i8(key4),
            vec![57, 78, 53, 52, 78, 75, 52, 56, 54, 49]
        );

        gen.set_seed(seq.next_i64());
        let key5 = gen.generate();
        assert_eq!(
            to_vec_i8(key5),
            vec![55, 55, 53, 57, 54, 77, 79, 50, 51, 48]
        );
    }

    #[test]
    /// This test is based on cassandra-stress HexBytes generator.
    /// The command cassandra-stress was being run with:
    /// write n=5 no-warmup -node 172.17.0.2 -pop seq=1..5 -col size=FIXED(5) -rate threads=1
    fn hex_blob_generator_big_pk_test() {
        // Generator of partition key column (by default named "key").
        let config = GeneratorConfig::new(
            // "Random" string used to generate the generator's salt.
            // In c-s, it's always "randomstr" + `column_name`.
            // See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/settings/SettingsCommandPreDefined.java#L86
            "randomstrkey",
            None,
            // keysize = 50
            Some(Box::new(FixedDistribution::new(50))),
        );
        let hex_blob = HexBlob;
        let mut gen = Generator::new(Box::new(hex_blob), config, String::from("key"));

        // -pop seq=1..5
        // Samples from this distrubtion are the seeds to the partition key generator.
        let seq = SeqDistribution::new(1, 5).unwrap();

        gen.set_seed(seq.next_i64());
        let key1 = gen.generate();
        assert_eq!(
            to_vec_i8(key1),
            vec![
                48, 80, 51, 55, 55, 48, 57, 80, 50, 49, 48, 48, 48, 48, 48, 48, 57, 76, 77, 75, 56,
                78, 50, 56, 78, 48, 48, 48, 48, 48, 48, 48, 56, 75, 80, 80, 77, 56, 53, 51, 54, 49,
                48, 48, 48, 48, 48, 48, 53, 76
            ]
        );

        gen.set_seed(seq.next_i64());
        let key2 = gen.generate();
        assert_eq!(
            to_vec_i8(key2),
            vec![
                79, 56, 76, 75, 55, 57, 76, 79, 54, 49, 48, 48, 48, 48, 48, 48, 55, 75, 54, 57, 79,
                57, 49, 56, 51, 48, 48, 48, 48, 48, 48, 48, 50, 76, 50, 76, 54, 78, 51, 50, 49, 49,
                48, 48, 48, 48, 48, 48, 78, 78
            ]
        );

        gen.set_seed(seq.next_i64());
        let key3 = gen.generate();
        assert_eq!(
            to_vec_i8(key3),
            vec![
                79, 80, 48, 48, 49, 76, 53, 57, 51, 48, 48, 48, 48, 48, 48, 48, 51, 77, 50, 54, 76,
                78, 56, 77, 48, 49, 48, 48, 48, 48, 48, 48, 78, 76, 53, 54, 48, 50, 50, 49, 77, 48,
                48, 48, 48, 48, 48, 48, 56, 78
            ]
        );

        gen.set_seed(seq.next_i64());
        let key4 = gen.generate();
        assert_eq!(
            to_vec_i8(key4),
            vec![
                57, 78, 53, 52, 78, 75, 52, 56, 54, 49, 48, 48, 48, 48, 48, 48, 57, 56, 77, 53, 78,
                48, 56, 80, 48, 49, 48, 48, 48, 48, 48, 48, 50, 78, 52, 75, 79, 50, 78, 80, 48, 48,
                48, 48, 48, 48, 48, 48, 53, 52
            ]
        );

        gen.set_seed(seq.next_i64());
        let key5 = gen.generate();
        assert_eq!(
            to_vec_i8(key5),
            vec![
                55, 55, 53, 57, 54, 77, 79, 50, 51, 48, 48, 48, 48, 48, 48, 48, 53, 75, 48, 52, 51,
                78, 54, 80, 54, 48, 48, 48, 48, 48, 48, 48, 79, 75, 53, 52, 80, 79, 51, 51, 51, 49,
                48, 48, 48, 48, 48, 48, 48, 52
            ]
        );
    }
}
