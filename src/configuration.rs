use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::time::Instant;

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

    /// A factory which creates operations that will be executed'
    /// during the stress.
    pub operation_factory: Arc<dyn OperationFactory>,
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

    /// The time of the supposed operation start time.
    ///
    /// If rate limiting is enabled, then each operation has a scheduled
    /// start time. If the run does not keep up and operations take longer
    /// than expected, operations will be executed past their schedule.
    /// In order to account for the coordinated omission problem, latency
    /// should be measured as the duration between the scheduled start time
    /// and the actual operation end time.
    ///
    /// If rate limiting is disabled, this will always be equal to `now`.
    pub scheduled_start_time: Instant,
}

/// Creates operations which can later be used by workers during the stress.
pub trait OperationFactory: Send + Sync {
    /// Creates an Operation.
    ///
    /// The single operation will be used from within a single worker.
    /// It can have its own state.
    fn create(&self) -> Box<dyn Operation>;
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
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>>;
}
