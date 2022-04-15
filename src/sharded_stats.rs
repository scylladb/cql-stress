use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};
use thread_local::ThreadLocal;

pub trait StatsFactory: Send + Sync {
    type Stats: Stats;
    fn create(&self) -> Self::Stats;
}

pub trait Stats: Sync + Send {
    fn clear(&mut self);
    fn combine(&mut self, other: &Self);
}

pub struct ShardedStats<F: StatsFactory> {
    shards: ThreadLocal<Arc<Mutex<F::Stats>>>,
    all: Mutex<Vec<Arc<Mutex<F::Stats>>>>,
    factory: Arc<F>,
}

/// A sharded statistics structure.
///
/// For each thread, a separate instance of the stats structure is kept,
/// and that thread keeps accounting its own statistics in that instance.
/// When it is time to report the statistics, stats from all threads are
/// collected into one object and returned, while the per-thread stats objects
/// are cleared.
///
/// Each shard is protected by a separate parking_lot::Mutex - assuming that
/// the structure is read unfrequently, they will be uncontended most of the time.
/// When getting a combined histogram instance, the reader has to get access to
/// all shards, locks them one at a time, and returns the combined result.
impl<F: StatsFactory> ShardedStats<F> {
    /// Creates a new ShardedStats with given factory.
    pub fn new(factory: Arc<F>) -> Self {
        Self {
            shards: ThreadLocal::new(),
            all: Mutex::new(Vec::new()),
            factory,
        }
    }

    /// Gets and locks access to this thread's stats structure.
    pub fn get_shard_mut(&self) -> MutexGuard<'_, F::Stats> {
        self.shards
            .get_or(|| {
                let shard = Arc::new(Mutex::new(self.factory.create()));
                self.all.lock().push(shard.clone());
                shard
            })
            .lock()
    }

    /// Combines statistics from all threads and clears all threads' stats.
    pub fn get_combined_and_clear(&self) -> F::Stats {
        let mut hist = self.factory.create();
        for shard in self.all.lock().iter() {
            let shard = &mut shard.lock();
            hist.combine(shard);
            shard.clear();
        }
        hist
    }
}

pub struct NoStatsFactory;

impl StatsFactory for NoStatsFactory {
    type Stats = NoStats;
    fn create(&self) -> NoStats {
        NoStats
    }
}

pub struct NoStats;

impl Stats for NoStats {
    fn clear(&mut self) {}
    fn combine(&mut self, _other: &Self) {}
}
