use scylla::value::CqlValue;

use crate::java_generate::distribution::Distribution;

use super::{ValueGenerator, ValueGeneratorFactory};

#[derive(Default)]
pub struct Boolean;

impl ValueGenerator for Boolean {
    fn generate(
        &mut self,
        identity_distribution: &mut dyn Distribution,
        _size_distribution: &mut dyn Distribution,
    ) -> CqlValue {
        // For some reason original c-s returns:
        // identity_distribution.next_i64() % 1 == 0 which is always true.
        //
        // We decided not to follow c-s here and to return truly random boolean.
        CqlValue::Boolean(identity_distribution.next_i64() % 2 == 1)
    }
}

pub struct BooleanFactory;

impl ValueGeneratorFactory for BooleanFactory {
    fn create(&self) -> Box<dyn ValueGenerator> {
        Box::<Boolean>::default()
    }
}
