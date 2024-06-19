use scylla::frame::{response::result::CqlValue, value::CqlVarint};

use crate::java_generate::distribution::Distribution;

use super::{ValueGenerator, ValueGeneratorFactory};

#[derive(Default)]
pub struct VarInt;

impl ValueGenerator for VarInt {
    fn generate(
        &mut self,
        identity_distribution: &mut dyn Distribution,
        _size_distribution: &mut dyn Distribution,
    ) -> CqlValue {
        // The native representation of `varint` is a signed representation
        // in big-endian order.
        CqlValue::Varint(CqlVarint::from_signed_bytes_be_slice(
            &identity_distribution.next_i64().to_be_bytes(),
        ))
    }
}

pub struct VarIntFactory;

impl ValueGeneratorFactory for VarIntFactory {
    fn create(&self) -> Box<dyn ValueGenerator> {
        Box::<VarInt>::default()
    }
}

#[cfg(test)]
mod tests {
    use num_bigint::BigInt;

    use crate::java_generate::{
        distribution::fixed::FixedDistribution,
        values::{varint::VarInt, Generator, GeneratorConfig},
    };

    fn num_bigints_from_i64(values: impl IntoIterator<Item = i64>) -> Vec<BigInt> {
        values.into_iter().map(BigInt::from).collect()
    }

    #[test]
    fn small_varint_generator_test() {
        let config = GeneratorConfig::new(
            "randomstrC0",
            None,
            Some(Box::new(FixedDistribution::new(5))),
        );
        let inet_gen = Box::<VarInt>::default();
        let mut gen = Generator::new(inet_gen, config, String::from("C0"));

        // Values which we test against are generated from c-s.
        gen.set_seed(0);
        let results = (0..5)
            .map(|_| -> BigInt { gen.generate().into_cql_varint().unwrap().into() })
            .collect::<Vec<_>>();
        assert_eq!(
            num_bigints_from_i64([
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
            .map(|_| -> BigInt { gen.generate().into_cql_varint().unwrap().into() })
            .collect::<Vec<_>>();
        assert_eq!(
            num_bigints_from_i64([
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
            .map(|_| -> BigInt { gen.generate().into_cql_varint().unwrap().into() })
            .collect::<Vec<_>>();
        assert_eq!(
            num_bigints_from_i64([
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
            .map(|_| -> BigInt { gen.generate().into_cql_varint().unwrap().into() })
            .collect::<Vec<_>>();
        assert_eq!(
            num_bigints_from_i64([
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
