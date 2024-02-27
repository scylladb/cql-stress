use std::{
    cell::{RefCell, RefMut},
    time::{SystemTime, UNIX_EPOCH},
};

use thread_local::ThreadLocal;

use super::Random;

pub mod enumerated;
pub mod fixed;
pub mod normal;
pub mod sequence;
pub mod uniform;

/// A distribution that atomically performs the operations.
/// It implies that the distribution can be safely used in a multi-threaded environment.
pub trait Distribution: Send + Sync {
    fn next_i64(&self) -> i64;
    fn next_f64(&self) -> f64;
    fn set_seed(&self, seed: i64);
}

/// A thread_local wrapper for [java_random::Random].
/// Used by distributions to implement `atomic` sampling.
struct ThreadLocalRandom {
    rng: ThreadLocal<RefCell<Random>>,
}

impl ThreadLocalRandom {
    fn new() -> Self {
        Self {
            rng: ThreadLocal::new(),
        }
    }

    fn get(&self) -> RefMut<'_, Random> {
        self.rng
            .get_or(|| {
                RefCell::new(Random::with_seed(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|duration| duration.as_millis() as u64)
                        .unwrap_or_default(),
                ))
            })
            .borrow_mut()
    }
}

pub trait DistributionFactory: Send + Sync + std::fmt::Display {
    fn create(&self) -> Box<dyn Distribution>;
}
