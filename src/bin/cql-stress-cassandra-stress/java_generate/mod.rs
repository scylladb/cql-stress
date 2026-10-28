pub use cql_stress::java_generate::faster_random;
pub mod values;

/// The library's distributions, plus the one only cassandra-stress uses: `-mixed ratio(...)`
/// adds its parsers to `EnumeratedDistribution`, so the type has to be defined in this crate.
pub mod distribution {
    pub use cql_stress::java_generate::distribution::*;
    pub mod enumerated;
}
