use std::future::Future;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use futures::future::{AbortHandle, Abortable, Fuse, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_timerfd::Delay;

use crate::configuration::{Configuration, OperationContext};

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

    rate_limiter: Option<RateLimiter>,
    max_retries_per_op: usize,
}

impl WorkerContext {
    pub fn new(config: &Configuration, now: Instant) -> Self {
        Self {
            operation_counter: AtomicU64::new(0),

            rate_limiter: config
                .rate_limit_per_second
                .map(|rate| RateLimiter::new(now, rate)),
            max_retries_per_op: config.max_retries_per_op,
        }
    }

    // Prevents more operations from being issued
    pub fn ask_to_stop(&self) {
        self.operation_counter
            .store(INVALID_OP_ID_THRESHOLD, Ordering::Relaxed);
    }

    // Was the worker asked to stop?
    pub fn should_stop(&self) -> bool {
        self.operation_counter.load(Ordering::Relaxed) >= INVALID_OP_ID_THRESHOLD
    }

    // Issues the next operation id. If the context got a signal to stop
    // the stress operation, it will return `None`.
    fn issue_operation_id(&self) -> Option<u64> {
        let id = self.operation_counter.fetch_add(1, Ordering::Relaxed);
        (id < INVALID_OP_ID_THRESHOLD).then_some(id)
    }
}

pub struct WorkerSession {
    context: Arc<WorkerContext>,
    op_id: u64,
    trial_idx: usize,
}

// Not the most beautiful interface, but it works - unlike async callbacks,
// which I also tried, but failed to make the types work.
impl WorkerSession {
    fn new(context: Arc<WorkerContext>) -> Self {
        Self {
            context,
            op_id: 0,
            trial_idx: 0,
        }
    }

    // Should be called before starting an operation.
    pub async fn start_operation(&mut self) -> Result<Option<OperationContext>> {
        if self.trial_idx == 0 {
            let Some(next_op_id) = self.context.issue_operation_id() else {
                return Ok(None);
            };
            self.op_id = next_op_id;
        }

        let scheduled_start_time = if let Some(rate_limiter) = &self.context.rate_limiter {
            let start_time = rate_limiter.issue_next_start_time();
            Delay::new(start_time.into())?.await?;
            start_time
        } else {
            Instant::now()
        };
        let actual_start_time = Instant::now();

        Ok(Some(OperationContext {
            operation_id: self.op_id,
            scheduled_start_time,
            actual_start_time,
        }))
    }

    // Should be called after ending an operation.
    pub fn end_operation(&mut self, result: Result<ControlFlow<()>>) -> Result<ControlFlow<()>> {
        match result {
            Ok(flow) => {
                self.trial_idx = 0;
                Ok(flow)
            }
            Err(err) if self.trial_idx >= self.context.max_retries_per_op => Err(err),
            Err(err) if self.context.should_stop() => Err(err),
            Err(_) => {
                self.trial_idx += 1;
                Ok(ControlFlow::Continue(()))
            }
        }
    }
}

/// Allows controlling the state of the run.
///
/// Currently, the `RunController` is only able to either gracefully stop
/// or abort the run.
pub struct RunController {
    stop_sender: Mutex<Option<oneshot::Sender<()>>>,
    abort_handle: AbortHandle,
}

impl RunController {
    /// Asks the run to stop gracefully.
    ///
    /// Each worker task will stop after completing their current operation.
    ///
    /// This method can be called multiple times on the same `RunController`.
    pub fn ask_to_stop(&self) {
        // Just drop the sender handle. This will notify the receiver.
        self.stop_sender.lock().unwrap().take();
    }

    /// Aborts the run.
    ///
    /// Each worker task will stop immediately and some operations may be
    /// only be executed partially.
    ///
    /// This method can be called multiple times on the same `RunController`.
    pub fn abort(&self) {
        self.abort_handle.abort();
    }
}

/// Runs an operation multiple times in parallel, according to config.
///
/// Returns a pair (controller, future), where:
/// - `controller` is an object that can be used to control the state of the run,
/// - `future` is a future which can be waited on in order to obtain the result
///   of the run. It does not need to be polled in order for the run to progress.
pub fn run(config: Configuration) -> (RunController, impl Future<Output = Result<()>>) {
    let (stop_sender, stop_receiver) = oneshot::channel();
    let (result_sender, result_receiver) = oneshot::channel();

    let fut = async move {
        let res = do_run(config, stop_receiver).await;
        let _ = result_sender.send(res);
    };

    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    let fut = Abortable::new(fut, abort_registration);
    tokio::task::spawn(fut);

    let controller = RunController {
        stop_sender: Mutex::new(Some(stop_sender)),
        abort_handle,
    };

    let result_fut = async move {
        // If the run was aborted before it completed, the result channel
        // will be closed without sending a result.
        let result: Result<Result<()>, _> = result_receiver.await;
        result.unwrap_or_else(|_| Err(anyhow::anyhow!("The run was aborted")))
    };

    (controller, result_fut)
}

async fn do_run(config: Configuration, stop_receiver: oneshot::Receiver<()>) -> Result<()> {
    let start_time = Instant::now();
    let ctx = Arc::new(WorkerContext::new(&config, start_time));

    // Spawn as many worker tasks as the concurrency allows
    let mut worker_handles = (0..config.concurrency)
        .map(|_| {
            let ctx_clone = Arc::clone(&ctx);
            let session = WorkerSession::new(ctx_clone);
            let mut operation = config.operation_factory.create();
            let (fut, handle) = async move { operation.run(session).await }.remote_handle();
            tokio::task::spawn(fut);
            handle
        })
        .collect::<FuturesUnordered<_>>();

    // If there is a time limit, stop the run after the defined duration
    let ctx_clone = Arc::clone(&ctx);
    let sleeper = match config.max_duration {
        Some(duration) => tokio::time::sleep_until(start_time + duration).fuse(),
        None => Fuse::terminated(),
    };
    let _stopper_handle = {
        let (fut, handle) = async move {
            futures::pin_mut!(sleeper);
            futures::future::select(sleeper, stop_receiver).await;
            ctx_clone.ask_to_stop();
        }
        .remote_handle();
        tokio::task::spawn(fut);
        handle
    };

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
    use std::collections::HashSet;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use std::sync::Mutex;

    use tokio::sync::Semaphore;
    use tokio::time::Instant;

    use super::*;
    use crate::configuration::{
        make_runnable, Configuration, Operation, OperationContext, OperationFactory,
    };

    struct FnOperationFactory<F>(pub F);

    impl<T, F> OperationFactory for FnOperationFactory<F>
    where
        T: Operation + 'static,
        F: Fn() -> T + Send + Sync,
    {
        fn create(&self) -> Box<dyn Operation> {
            Box::new((self.0)())
        }
    }

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

    fn make_test_cfg<T, F>(f: F) -> Configuration
    where
        T: Operation + 'static,
        F: Fn() -> T + Send + Sync + 'static,
    {
        Configuration {
            max_duration: None,
            concurrency: 10,
            rate_limit_per_second: None,
            operation_factory: Arc::new(FnOperationFactory(f)),
            max_retries_per_op: 0,
        }
    }

    #[tokio::test]
    async fn test_run_to_completion() {
        let counter = Arc::new(AtomicU64::new(0));

        struct Op(Arc<AtomicU64>);
        make_runnable!(Op);

        impl Op {
            async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
                if ctx.operation_id >= 1000 {
                    return Ok(ControlFlow::Break(()));
                }
                self.0.fetch_add(ctx.operation_id, Ordering::SeqCst);
                Ok(ControlFlow::Continue(()))
            }
        }

        let cfg = {
            let counter = counter.clone();
            make_test_cfg(move || Op(counter.clone()))
        };

        let (_, fut) = run(cfg);
        fut.await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 499500);
    }

    #[tokio::test]
    async fn test_run_to_error() {
        let counter = Arc::new(AtomicU64::new(0));

        struct Op(Arc<AtomicU64>);

        make_runnable!(Op);
        impl Op {
            async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
                if ctx.operation_id >= 500 {
                    return Err(anyhow::anyhow!("failure"));
                }
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(ControlFlow::Continue(()))
            }
        }

        let cfg = {
            let counter = counter.clone();
            make_test_cfg(move || Op(counter.clone()))
        };

        let (_, fut) = run(cfg);
        fut.await.unwrap_err();
        assert_eq!(counter.load(Ordering::SeqCst), 500);
    }

    struct IdleOp;

    make_runnable!(IdleOp);
    impl IdleOp {
        async fn execute(&mut self, _ctx: &OperationContext) -> Result<ControlFlow<()>> {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(ControlFlow::Continue(()))
        }
    }

    #[tokio::test]
    async fn test_run_to_max_duration() {
        let mut cfg = make_test_cfg(|| IdleOp);
        cfg.max_duration = Some(Duration::from_millis(100));

        let (_, fut) = run(cfg);
        fut.await.unwrap();
    }

    #[tokio::test]
    async fn test_run_until_asked_to_stop() {
        let cfg = make_test_cfg(|| IdleOp);

        let (ctrl, fut) = run(cfg);
        tokio::time::sleep(Duration::from_millis(100)).await;
        ctrl.ask_to_stop();
        fut.await.unwrap();
    }

    struct StuckOp(pub Arc<Semaphore>);

    make_runnable!(StuckOp);
    impl StuckOp {
        async fn execute(&mut self, _ctx: &OperationContext) -> Result<ControlFlow<()>> {
            // Mark that we begun the operation and became "stuck"
            self.0.add_permits(1);
            // The `pending()` future never resolves
            futures::future::pending().await
        }
    }

    #[tokio::test]
    async fn test_run_until_aborted() {
        let sem = Arc::new(Semaphore::new(0));
        let sem_clone = Arc::clone(&sem);

        let cfg = make_test_cfg(move || StuckOp(Arc::clone(&sem_clone)));
        let concurrency = cfg.concurrency as u32;

        let (ctrl, fut) = run(cfg);

        // Wait until all operations become stuck
        let _ = sem.acquire_many(concurrency).await.unwrap();

        // Abort and check that the stuck operations weren't a problem
        ctrl.abort();
        fut.await.unwrap_err();
    }

    struct AlternatingSuccessFailOp {
        tried_ops: Mutex<HashSet<u64>>,
    }

    make_runnable!(AlternatingSuccessFailOp);
    impl AlternatingSuccessFailOp {
        fn new() -> Self {
            AlternatingSuccessFailOp {
                tried_ops: Mutex::new(HashSet::new()),
            }
        }

        async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
            if ctx.operation_id >= 100 {
                return Ok(ControlFlow::Break(()));
            }

            let mut lock = self.tried_ops.lock().unwrap();
            let was_missing = lock.insert(ctx.operation_id);
            if was_missing {
                // First visit, so fail
                Err(anyhow::anyhow!("oops"))
            } else {
                // Already tried and failed - return success this time
                Ok(ControlFlow::Continue(()))
            }
        }
    }

    #[tokio::test]
    async fn test_retrying() {
        let cfg = make_test_cfg(AlternatingSuccessFailOp::new);
        let (_, fut) = run(cfg);
        fut.await.unwrap_err(); // Expect error as there were no retries

        let mut cfg = make_test_cfg(AlternatingSuccessFailOp::new);
        cfg.max_retries_per_op = 1;
        let (_, fut) = run(cfg);
        fut.await.unwrap(); // Expect success as each op was retried
    }

    struct AlwaysFailsOp(pub Option<Arc<Semaphore>>);

    make_runnable!(AlwaysFailsOp);
    impl AlwaysFailsOp {
        async fn execute(&mut self, _ctx: &OperationContext) -> Result<ControlFlow<()>> {
            if let Some(s) = self.0.take() {
                s.add_permits(1);
            }
            tokio::time::sleep(Duration::from_millis(10)).await; // Make sure we don't enter a spin loop
            Err(anyhow::anyhow!("fail"))
        }
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn test_ask_to_stop_on_constant_failures() {
        let sem = Arc::new(Semaphore::new(0));
        let sem_clone = Arc::clone(&sem);

        let mut cfg = make_test_cfg(move || AlwaysFailsOp(Some(sem_clone.clone())));
        cfg.max_retries_per_op = usize::MAX;
        let concurrency = cfg.concurrency as u32;

        let (ctrl, fut) = run(cfg);

        // Wait until all ops got stuck in retry loop
        let _ = sem.acquire_many(concurrency).await.unwrap();

        // Ask to stop and make sure that the workload finishes
        ctrl.ask_to_stop();
        fut.await.unwrap_err();
    }
}
