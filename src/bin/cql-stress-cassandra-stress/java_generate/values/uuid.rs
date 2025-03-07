use scylla::value::CqlValue;

use crate::java_generate::distribution::Distribution;

use super::{ValueGenerator, ValueGeneratorFactory};

#[derive(Default)]
pub struct Uuid;

impl ValueGenerator for Uuid {
    fn generate(
        &mut self,
        identity_distribution: &mut dyn Distribution,
        _size_distribution: &mut dyn Distribution,
    ) -> CqlValue {
        let v = identity_distribution.next_i64();
        CqlValue::Uuid(uuid::Uuid::from_u64_pair(v as u64, v as u64))
    }
}

pub struct UuidFactory;

impl ValueGeneratorFactory for UuidFactory {
    fn create(&self) -> Box<dyn ValueGenerator> {
        Box::<Uuid>::default()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::java_generate::{
        distribution::fixed::FixedDistribution,
        values::{uuid::Uuid, Generator, GeneratorConfig},
    };

    fn uuids_from_str(values: impl IntoIterator<Item = &'static str>) -> Vec<uuid::Uuid> {
        values
            .into_iter()
            .map(|v| uuid::Uuid::from_str(v).unwrap())
            .collect()
    }

    #[test]
    fn small_uuid_generator_test() {
        let config = GeneratorConfig::new(
            "randomstrC0",
            None,
            Some(Box::new(FixedDistribution::new(5))),
        );
        let inet_gen = Box::<Uuid>::default();
        let mut gen = Generator::new(inet_gen, config, String::from("C0"));

        // Values which we test against are generated from c-s.
        gen.set_seed(0);
        let results = (0..5)
            .map(|_| gen.generate().as_uuid().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            uuids_from_str([
                "00000009-6fa4-4aa8-0000-00096fa44aa8",
                "00000010-f0bc-2eaa-0000-0010f0bc2eaa",
                "0000000b-e991-bbba-0000-000be991bbba",
                "00000011-3288-3312-0000-001132883312",
                "00000006-36d3-0a85-0000-000636d30a85"
            ]),
            results
        );

        gen.set_seed(0xdeadcafe);
        let results = (0..5)
            .map(|_| gen.generate().as_uuid().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            uuids_from_str([
                "00000006-32d2-f882-0000-000632d2f882",
                "00000000-5558-c5c5-0000-00005558c5c5",
                "00000006-3070-66d5-0000-0006307066d5",
                "0000000e-98e9-60e9-0000-000e98e960e9",
                "00000013-3a74-655f-0000-00133a74655f",
            ]),
            results
        );

        gen.set_seed(i64::MIN);
        let results = (0..5)
            .map(|_| gen.generate().as_uuid().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            uuids_from_str([
                "00000009-6fa4-4aa8-0000-00096fa44aa8",
                "00000010-f0bc-2eaa-0000-0010f0bc2eaa",
                "0000000b-e991-bbba-0000-000be991bbba",
                "00000011-3288-3312-0000-001132883312",
                "00000006-36d3-0a85-0000-000636d30a85",
            ]),
            results
        );

        gen.set_seed(i64::MAX);
        let results = (0..5)
            .map(|_| gen.generate().as_uuid().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            uuids_from_str([
                "0000000d-d849-ec7b-0000-000dd849ec7b",
                "0000000c-3a92-ac66-0000-000c3a92ac66",
                "00000012-5810-d1a9-0000-00125810d1a9",
                "00000005-507e-75bf-0000-0005507e75bf",
                "00000003-a6b0-e87f-0000-0003a6b0e87f",
            ]),
            results
        );
    }
}
