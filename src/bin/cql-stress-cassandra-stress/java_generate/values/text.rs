use std::cmp::min;

use scylla::frame::response::result::CqlValue;

use crate::java_generate::{distribution::Distribution, faster_random::FasterRandom};

use super::{ValueGenerator, ValueGeneratorFactory};

/// Text generator based on c-s Strings generator.
/// See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/values/Strings.java
#[derive(Default)]
pub struct Text {
    rng: FasterRandom,
}

impl ValueGenerator for Text {
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
            let v = self
                .rng
                .next_i64()
                .to_le_bytes()
                .map(|byte| ((byte & 127) + 32) & 127);

            let n = min(size - i, v.len());
            result.extend_from_slice(&v[0..n]);
            i += n;
        }

        CqlValue::Text(String::from_utf8(result).expect("Invalid utf-8 text generated."))
    }
}

pub struct TextFactory;

impl ValueGeneratorFactory for TextFactory {
    fn create(&self) -> Box<dyn ValueGenerator> {
        Box::<Text>::default()
    }
}

#[cfg(test)]
mod tests {
    use crate::java_generate::{
        distribution::fixed::FixedDistribution,
        values::{Generator, GeneratorConfig},
    };

    use super::Text;

    #[test]
    fn small_text_generator_test() {
        let config = GeneratorConfig::new(
            "randomstrC0",
            None,
            Some(Box::new(FixedDistribution::new(5))),
        );
        let text_gen = Box::<Text>::default();
        let mut gen = Generator::new(text_gen, config, String::from("C0"));

        // Results were generated from original cassandra-stress.
        gen.set_seed(0);
        let results = (0..5)
            .map(|_| gen.generate().into_string().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                "I\t\u{0011}J-",
                "\\czv[",
                "zN\u{0008}34",
                "EWVyW",
                "z\u{0002}i$}",
            ],
            results
        );

        gen.set_seed(0xdeadcafe);
        let results = (0..5)
            .map(|_| gen.generate().into_string().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                "vFtqJ",
                "Q\u{001E}\u{0006}\u{0019}6",
                "o\u{0001}u\u{0007}f",
                "\u{0013}Z+M8",
                "y\u{001F}q~\u{001A}",
            ],
            results
        );

        gen.set_seed(i64::MIN);
        let results = (0..5)
            .map(|_| gen.generate().into_string().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                "I\t\u{0011}J-",
                "\\czv[",
                "zN\u{0008}34",
                "EWVyW",
                "z\u{0002}i$}",
            ],
            results
        );

        gen.set_seed(i64::MAX);
        let results = (0..5)
            .map(|_| gen.generate().into_string().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                "\u{0008}Z&\u{0018}\u{001B}",
                "\u{001D}\u{0018}ua_",
                "$wYR\u{0008}",
                "\u{001B}2\u{0019}\u{0013}\u{001A}",
                ":\u{001F}5Q\u{001B}",
            ],
            results
        );
    }
}
