use scylla::value::CqlValue;

use crate::java_generate::distribution::Distribution;

use super::{ValueGenerator, ValueGeneratorFactory};

#[derive(Default)]
pub struct Float;

impl ValueGenerator for Float {
    fn generate(
        &mut self,
        identity_distribution: &mut dyn Distribution,
        _size_distribution: &mut dyn Distribution,
    ) -> CqlValue {
        CqlValue::Float(identity_distribution.next_f64() as f32)
    }
}

pub struct FloatFactory;

impl ValueGeneratorFactory for FloatFactory {
    fn create(&self) -> Box<dyn ValueGenerator> {
        Box::<Float>::default()
    }
}

#[derive(Default)]
pub struct Double;

impl ValueGenerator for Double {
    fn generate(
        &mut self,
        identity_distribution: &mut dyn Distribution,
        _size_distribution: &mut dyn Distribution,
    ) -> CqlValue {
        CqlValue::Double(identity_distribution.next_f64())
    }
}

pub struct DoubleFactory;

impl ValueGeneratorFactory for DoubleFactory {
    fn create(&self) -> Box<dyn ValueGenerator> {
        Box::<Double>::default()
    }
}

#[cfg(test)]
mod tests {
    use crate::java_generate::{
        distribution::fixed::FixedDistribution,
        values::{Generator, GeneratorConfig},
    };

    use super::{Double, Float};

    #[test]
    fn small_float_generator_test() {
        let config = GeneratorConfig::new(
            "randomstrC0",
            None,
            Some(Box::new(FixedDistribution::new(5))),
        );
        let text_gen = Box::<Float>::default();
        let mut gen = Generator::new(text_gen, config, String::from("C0"));

        // Results were generated from original cassandra-stress.
        gen.set_seed(0);
        let results = (0..5)
            .map(|_| gen.generate().as_float().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                4.0527745E10,
                7.275834E10,
                5.1163283E10,
                7.3862234E10,
                2.6689604E10,
            ],
            results
        );

        gen.set_seed(0xdeadcafe);
        let results = (0..5)
            .map(|_| gen.generate().as_float().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                2.662249E10,
                1.4318812E9,
                2.6582477E10,
                6.2694973E10,
                8.258508E10,
            ],
            results
        );

        gen.set_seed(i64::MIN);
        let results = (0..5)
            .map(|_| gen.generate().as_float().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                4.0527745E10,
                7.275834E10,
                5.1163283E10,
                7.3862234E10,
                2.6689604E10,
            ],
            results
        );

        gen.set_seed(i64::MAX);
        let results = (0..5)
            .map(|_| gen.generate().as_float().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                5.94633E10,
                5.25223E10,
                7.878691E10,
                2.2825302E10,
                1.5681513E10,
            ],
            results
        );
    }

    #[test]
    fn small_double_generator_test() {
        let config = GeneratorConfig::new(
            "randomstrC0",
            None,
            Some(Box::new(FixedDistribution::new(5))),
        );
        let text_gen = Box::<Double>::default();
        let mut gen = Generator::new(text_gen, config, String::from("C0"));

        // Results were generated from original cassandra-stress.
        gen.set_seed(0);
        let results = (0..5)
            .map(|_| gen.generate().as_double().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                4.052774365638973E10,
                7.275834129052333E10,
                5.116328236284534E10,
                7.38622308026015E10,
                2.6689604229831688E10,
            ],
            results
        );

        gen.set_seed(0xdeadcafe);
        let results = (0..5)
            .map(|_| gen.generate().as_double().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                2.6622490754583496E10,
                1.431881157104631E9,
                2.6582476501090935E10,
                6.26949736733251E10,
                8.258508527927824E10,
            ],
            results
        );

        gen.set_seed(i64::MIN);
        let results = (0..5)
            .map(|_| gen.generate().as_double().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                4.052774365638973E10,
                7.275834129052333E10,
                5.116328236284534E10,
                7.38622308026015E10,
                2.6689604229831688E10,
            ],
            results
        );

        gen.set_seed(i64::MAX);
        let results = (0..5)
            .map(|_| gen.generate().as_double().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                5.946329817132646E10,
                5.2522298470748795E10,
                7.878690858538501E10,
                2.2825301439990566E10,
                1.5681513599571617E10,
            ],
            results
        );
    }
}
