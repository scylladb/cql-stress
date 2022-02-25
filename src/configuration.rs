use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

/// Defines the configuration of a benchmark.
pub struct Configuration {
    /// The maximum duration of the test.
    ///
    /// Depending on the workload, the test may finish earlier than
    /// the specified duration, but it will be immediately stopped if it lasts
    /// longer than `max_duration`.
    ///
    /// If `None`, the test duration is unlimited.
    pub max_duration: Option<Duration>,

    /// The concurrency with which the benchmark operations will be performed.
    ///
    /// The tool will spawn as many tokio tasks as this number specifies,
    /// and each task will sequentially perform the benchmark operations.
    ///
    /// Must not be zero.
    pub concurrency: u64,

    /// The maximum number of operations to be performed per second.
    /// If `None`, then there is no rate limit imposed.
    pub rate_limit_per_second: Option<f64>,

    /// Represents an operation to be repeatedly performed during the stress.
    pub operation: Arc<dyn Operation>,
}

/// Contains all necessary context needed to execute an Operation.
pub struct OperationContext {
    /// The current ID of the operation being performed.
    ///
    /// The tool tries to issue operation IDs sequentially, however because
    /// of the parallelism the operations can be reordered. To be more precise,
    /// if an operation with ID `X` > 0 was issued, then the tool has attempted
    /// or will attempt to execute operations of IDs less than `X`.
    pub operation_id: u64,
}

/// Represents an operation which is repeatedly performed during the stress.
#[async_trait]
pub trait Operation: Send + Sync {
    /// Executes the operation, given information in the OperationContext.
    ///
    /// The operation should behave deterministically, i.e. the same action
    /// should be performed when given exactly the same OperationContext.
    /// This enables deterministic behavior of the tool and makes it possible
    /// to control the retry logic outside the Operation.
    ///
    /// Returns ControlFlow::Break if it should finish work, for example
    /// if the operation ID has exceeded the configured operation count.
    /// In other cases, it returns ControlFlow::Continue.
    async fn execute(&self, ctx: &OperationContext) -> Result<ControlFlow<()>>;
}
