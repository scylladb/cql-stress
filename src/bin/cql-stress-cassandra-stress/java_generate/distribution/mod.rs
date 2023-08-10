/// A distribution that atomically performs the operations.
/// It implies that the distribution can be safely used in a multi-threaded environment.
pub trait Distribution: Send + Sync {
    fn next_i64(&self) -> i64;
    fn next_f64(&self) -> f64;
    fn set_seed(&self, seed: i64);
}

pub trait DistributionFactory {
    type Distribution;

    fn get(&self) -> Self::Distribution;
}
