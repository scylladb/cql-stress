use scylla::frame::{response::result::CqlValue, value::CqlDecimal};

use crate::java_generate::distribution::Distribution;

use super::{ValueGenerator, ValueGeneratorFactory};

#[derive(Default)]
pub struct Decimal;

impl ValueGenerator for Decimal {
    fn generate(
        &mut self,
        identity_distribution: &mut dyn Distribution,
        _size_distribution: &mut dyn Distribution,
    ) -> CqlValue {
        // The comment of Java's `BigDecimal::valueOf(long)` mentions:
        // ```Translates a long value into a BigDecimal with a scale of zero.```
        //
        // The native representation of `decimal` consists of a `varint` value
        // and a 32-bit scale/exponent.
        // This means that we simply need to convert generated i64 value to
        // `varint`'s native representation and provide a 0-scale.
        CqlValue::Decimal(CqlDecimal::from_signed_be_bytes_slice_and_exponent(
            &identity_distribution.next_i64().to_be_bytes(),
            0,
        ))
    }
}

pub struct DecimalFactory;

impl ValueGeneratorFactory for DecimalFactory {
    fn create(&self) -> Box<dyn ValueGenerator> {
        Box::<Decimal>::default()
    }
}

#[cfg(test)]
mod tests {
    use bigdecimal::BigDecimal;

    use crate::java_generate::{
        distribution::fixed::FixedDistribution,
        values::{decimal::Decimal, Generator, GeneratorConfig},
    };

    fn bigdecimals_from_i64(values: impl IntoIterator<Item = i64>) -> Vec<BigDecimal> {
        values.into_iter().map(BigDecimal::from).collect()
    }

    #[test]
    fn small_decimal_generator_test() {
        let config = GeneratorConfig::new(
            "randomstrC0",
            None,
            Some(Box::new(FixedDistribution::new(5))),
        );
        let inet_gen = Box::<Decimal>::default();
        let mut gen = Generator::new(inet_gen, config, String::from("C0"));

        // Values which we test against are generated from c-s.
        gen.set_seed(0);
        let results = (0..5)
            .map(|_| -> BigDecimal { gen.generate().into_cql_decimal().unwrap().into() })
            .collect::<Vec<_>>();
        assert_eq!(
            bigdecimals_from_i64([
                40527743656,
                72758341290,
                51163282362,
                73862230802,
                26689604229,
            ]),
            results
        );

        gen.set_seed(0xdeadcafe);
        let results = (0..5)
            .map(|_| -> BigDecimal { gen.generate().into_cql_decimal().unwrap().into() })
            .collect::<Vec<_>>();
        assert_eq!(
            bigdecimals_from_i64([
                26622490754,
                1431881157,
                26582476501,
                62694973673,
                82585085279,
            ]),
            results
        );

        gen.set_seed(i64::MIN);
        let results = (0..5)
            .map(|_| -> BigDecimal { gen.generate().into_cql_decimal().unwrap().into() })
            .collect::<Vec<_>>();
        assert_eq!(
            bigdecimals_from_i64([
                40527743656,
                72758341290,
                51163282362,
                73862230802,
                26689604229,
            ]),
            results
        );

        gen.set_seed(i64::MAX);
        let results = (0..5)
            .map(|_| -> BigDecimal { gen.generate().into_cql_decimal().unwrap().into() })
            .collect::<Vec<_>>();
        assert_eq!(
            bigdecimals_from_i64([
                59463298171,
                52522298470,
                78786908585,
                22825301439,
                15681513599,
            ]),
            results
        );
    }
}
