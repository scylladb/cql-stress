use scylla::value::CqlValue;

use crate::java_generate::distribution::Distribution;

use super::ValueGenerator;
use super::ValueGeneratorFactory;

macro_rules! impl_value_generator_for_fixed_integer {
    ($cql_type:tt, $cql_type_factory:tt, $rust_type:ty) => {
        #[derive(Default)]
        pub struct $cql_type;

        impl ValueGenerator for $cql_type {
            fn generate(
                &mut self,
                identity_distribution: &mut dyn Distribution,
                _size_distribution: &mut dyn Distribution,
            ) -> CqlValue {
                CqlValue::$cql_type(identity_distribution.next_i64() as $rust_type)
            }
        }

        pub struct $cql_type_factory;

        impl ValueGeneratorFactory for $cql_type_factory {
            fn create(&self) -> Box<dyn ValueGenerator> {
                Box::new($cql_type)
            }
        }
    };
}

impl_value_generator_for_fixed_integer!(BigInt, BigIntFactory, i64);
impl_value_generator_for_fixed_integer!(Int, IntFactory, i32);
impl_value_generator_for_fixed_integer!(SmallInt, SmallIntFactory, i16);
impl_value_generator_for_fixed_integer!(TinyInt, TinyIntFactory, i8);
