mod row_generator;
mod write;

use anyhow::Result;
use std::num::Wrapping;

pub use row_generator::RowGeneratorFactory;
use scylla::{frame::response::result::CqlValue, QueryResult};
pub use write::WriteOperationFactory;

/// See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/PartitionIterator.java#L725.
fn recompute_seed(seed: i64, partition_key: &CqlValue) -> i64 {
    match partition_key {
        CqlValue::Blob(key) => {
            let mut wrapped = Wrapping(seed);
            for byte in key {
                wrapped = (wrapped * Wrapping(31)) + Wrapping(*byte as i64);
            }
            wrapped.0
        }
        _ => todo!("Implement recompute_seed for other CqlValues"),
    }
}
