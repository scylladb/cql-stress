mod sequential;
mod uniform;

pub use sequential::{SequentialConfig, SequentialFactory};
pub use uniform::{UniformConfig, UniformFactory};

pub trait WorkloadFactory: Sync + Send {
    fn create(&self) -> Box<dyn Workload>;
}

pub trait Workload: Sync + Send {
    /// Generates the partition key and clustering keys to be inserted in this operation.
    fn generate_keys(&mut self, ck_count: usize) -> Option<(i64, Vec<i64>)>;
}
