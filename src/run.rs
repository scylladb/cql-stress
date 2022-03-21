use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::future::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::time::Instant;

use crate::configuration::{Configuration, Operation, OperationContext};

// Rate limits operations by issuing timestamps indicating when the next
// operation should happen. Uses atomics, can be shared between threads.
struct RateLimiter {
    base: Instant,
    increment_nanos: u64,
    nanos_counter: AtomicU64,
}

impl RateLimiter {
    pub fn new(base: Instant, ops_per_second: f64) -> Self {
        let increment_nanos = (1_000_000_000f64 / ops_per_second) as u64;
        Self {
            base,
            increment_nanos,
            nanos_counter: AtomicU64::new(0),
        }
    }

    pub fn issue_next_start_time(&self) -> Instant {
        let nanos = self
            .nanos_counter
            .fetch_add(self.increment_nanos, Ordering::Relaxed);

        self.base + Duration::from_nanos(nanos)
    }
}

// When an operation ID equal or larger to this value is issued, the worker
// task will stop itself. This is used in the `ask_to_stop` method
// which sets the operation_counter to this value. The value of this constant
// is chosen to be very large so that it is impossible to reach it, and
// small enough so that operation execution attempts which happen after
// `ask_to_stop` do not overflow it.
const INVALID_OP_ID_THRESHOLD: u64 = 1u64 << 63u64;

// Represents shareable state and configuration of a worker.
struct WorkerContext {
    operation_counter: AtomicU64,
    operation: Arc<dyn Operation>,

    rate_limiter: Option<RateLimiter>,
}

impl WorkerContext {
    pub fn new(config: &Configuration, now: Instant) -> Self {
        Self {
            operation_counter: AtomicU64::new(0),
            operation: Arc::clone(&config.operation),

            rate_limiter: config
                .rate_limit_per_second
                .map(|rate| RateLimiter::new(now, rate)),
        }
    }

    // Prevents more operations from being issued
    pub fn ask_to_stop(&self) {
        self.operation_counter
            .store(INVALID_OP_ID_THRESHOLD, Ordering::Relaxed);
    }

    // Issues the next operation id. If the context got a signal to stop
    // the stress operation, it will return `None`.
    fn issue_operation_id(&self) -> Option<u64> {
        let id = self.operation_counter.fetch_add(1, Ordering::Relaxed);
        (id < INVALID_OP_ID_THRESHOLD).then(|| id)
    }

    // Repeatedly runs the `operation` until it is asked to stop
    // or an execution of the `operation` will either return `Err`
    // or `ControlFlow::Break`.
    pub async fn run_worker(&self) -> Result<()> {
        while let Some(op_id) = self.issue_operation_id() {
            if let Some(rate_limiter) = &self.rate_limiter {
                let start_time = rate_limiter.issue_next_start_time();
                tokio::time::sleep_until(start_time).await;
            }

            let ctx = OperationContext {
                operation_id: op_id,
            };

            // TODO: Allow specifying a strategy for retrying in case of error
            match self.operation.execute(&ctx).await {
                Ok(ControlFlow::Continue(_)) => continue,
                Ok(ControlFlow::Break(_)) => break,
                Err(err) => return Err(err),
            }
        }

        Ok(())
    }
}

pub async fn run(config: Configuration) -> Result<()> {
    let start_time = Instant::now();
    let ctx = Arc::new(WorkerContext::new(&config, start_time));

    // Spawn as many worker tasks as the concurrency allows
    let mut worker_handles = (0..config.concurrency)
        .map(|_| {
            let ctx_clone = Arc::clone(&ctx);
            let (fut, handle) = async move { ctx_clone.run_worker().await }.remote_handle();
            tokio::task::spawn(fut);
            handle
        })
        .collect::<FuturesUnordered<_>>();

    // If there is a time limit, spawn a task which will ask_to_stop
    // after the bench period has elapsed
    let ctx_clone = Arc::clone(&ctx);
    let _stopper_handle = config.max_duration.map(move |duration| {
        let (fut, handle) = async move {
            tokio::time::sleep_until(start_time + duration).await;
            ctx_clone.ask_to_stop();
        }
        .remote_handle();
        tokio::task::spawn(fut);
        handle
    });

    let mut result: Result<()> = Ok(());

    // TODO: Collect all errors and report them
    while let Some(worker_result) = worker_handles.next().await {
        if let Err(err) = worker_result {
            result = Err(err);
            ctx.ask_to_stop();
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    use tokio::time::Instant;

    use super::*;
    use crate::configuration::{Configuration, Operation, OperationContext};

    #[test]
    fn test_rate_limiter() {
        let count_in_period = |ops: f64, period: Duration| -> usize {
            let start = Instant::now();
            let end = start + period;
            let limiter = RateLimiter::new(start, ops);

            let mut count = 0;
            while limiter.issue_next_start_time() < end {
                count += 1;
            }
            count
        };

        let sec = Duration::from_secs(1);

        assert_eq!(count_in_period(1.0, 10 * sec), 10);
        assert_eq!(count_in_period(0.5, 10 * sec), 5);
        assert_eq!(count_in_period(0.1, 10 * sec), 1);
        assert_eq!(count_in_period(2.0, 10 * sec), 20);
    }

    fn make_test_cfg(op: impl Operation + 'static) -> Configuration {
        Configuration {
            max_duration: None,
            concurrency: 10,
            rate_limit_per_second: None,
            operation: Arc::new(op),
        }
    }

    #[tokio::test]
    async fn test_run_to_completion() {
        let counter = Arc::new(AtomicU64::new(0));

        struct Op(Arc<AtomicU64>);

        #[async_trait]
        impl Operation for Op {
            async fn execute(&self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
                if ctx.operation_id >= 1000 {
                    return Ok(ControlFlow::Break(()));
                }
                self.0.fetch_add(ctx.operation_id, Ordering::SeqCst);
                Ok(ControlFlow::Continue(()))
            }
        }

        let cfg = make_test_cfg(Op(counter.clone()));

        run(cfg).await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 499500);
    }

    #[tokio::test]
    async fn test_run_to_error() {
        let counter = Arc::new(AtomicU64::new(0));

        struct Op(Arc<AtomicU64>);

        #[async_trait]
        impl Operation for Op {
            async fn execute(&self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
                if ctx.operation_id >= 500 {
                    return Err(anyhow::anyhow!("failure"));
                }
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(ControlFlow::Continue(()))
            }
        }

        let cfg = make_test_cfg(Op(counter.clone()));

        run(cfg).await.unwrap_err();
        assert_eq!(counter.load(Ordering::SeqCst), 500);
    }

    #[tokio::test]
    async fn test_run_to_max_duration() {
        // We can't reliably check the number of `execute` invocations
        // because they are racing with the max duration period.
        // We just check that `run` stops at all.

        struct Op;

        #[async_trait]
        impl Operation for Op {
            async fn execute(&self, _ctx: &OperationContext) -> Result<ControlFlow<()>> {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(ControlFlow::Continue(()))
            }
        }

        let mut cfg = make_test_cfg(Op);
        cfg.max_duration = Some(Duration::from_millis(100));

        run(cfg).await.unwrap();
    }
}
