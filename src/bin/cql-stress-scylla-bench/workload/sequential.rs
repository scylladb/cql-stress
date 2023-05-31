use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;

use super::{Workload, WorkloadFactory};

struct SharedState {
    pub next_pk: AtomicU64,
}

/// Creates workloads which write data sequentially.
///
/// See [SequentialConfig] for more detailed information about the workloads'
/// behavior.
pub struct SequentialFactory {
    config: SequentialConfig,
    shared_state: Arc<SharedState>,
}

struct Sequential {
    config: SequentialConfig,
    shared_state: Arc<SharedState>,
    current_pk: u64,
    current_ck: u64,
}

/// Defines parameters of a sequential workload.
///
/// The data set consists of `pks` partitions, each having `cks_per_pk`
/// clustering keys. Partition keys are numbered `0..pk`, clustering
/// keys are numbered `0..cks_per_pk`.
///
/// Partition keys are filled up in windows, controlled by `pk_parallelism`.
/// Within a single window, pks are filled up in a round robin fashion.
/// When cks in a given window are inserted, the workload proceeds to
/// the next window. If `pk_parallelism` does not divide `pks`, the last
/// full window will be enlarged to contain the remainder.
///
/// The `rows_per_op` parameter controls how many rows are inserted in each
/// iteration. If this number does not divide `cks_per_pk`, the last
/// operation on a given partition may insert less rows.
///
/// The whole data set will be written one or more times, depending on
/// the `iterations` parameter.
#[derive(Clone)]
pub struct SequentialConfig {
    pub iterations: u64,
    pub partition_offset: i64,
    pub pks: u64,
    pub cks_per_pk: u64,
}

impl SequentialFactory {
    pub fn new(config: SequentialConfig) -> Result<Self> {
        anyhow::ensure!(config.pks > 0, "Partition count must be greater than zero");
        anyhow::ensure!(
            config.cks_per_pk > 0,
            "Clustering key per partition count must be greater than zero",
        );

        let shared_state = Arc::new(SharedState {
            next_pk: AtomicU64::new(0),
        });

        Ok(Self {
            config,
            shared_state,
        })
    }
}

impl WorkloadFactory for SequentialFactory {
    fn create(&self) -> Box<dyn Workload> {
        Box::new(Sequential::new(
            self.config.clone(),
            self.shared_state.clone(),
        ))
    }
}

impl Sequential {
    fn new(config: SequentialConfig, shared_state: Arc<SharedState>) -> Self {
        // This is dummy state, just in order to trigger choosing pk
        // on first `generate_keys` invocation
        let current_ck = config.cks_per_pk;
        Sequential {
            config,
            shared_state,
            current_pk: 0,
            current_ck,
        }
    }
}

impl Workload for Sequential {
    fn generate_keys(&mut self, ck_count: usize) -> Option<(i64, Vec<i64>)> {
        if self.current_ck >= self.config.cks_per_pk {
            self.current_ck = 0;
            self.current_pk = self.shared_state.next_pk.fetch_add(1, Ordering::Relaxed);
            if self.config.iterations > 0
                && self.current_pk >= self.config.pks * self.config.iterations
            {
                return None;
            }
        }

        let pk = (self.current_pk % self.config.pks) as i64 + self.config.partition_offset;
        let ck_end = std::cmp::min(self.current_ck + ck_count as u64, self.config.cks_per_pk);
        let cks = (self.current_ck..ck_end).map(|x| x as i64).collect();
        self.current_ck = ck_end;

        Some((pk, cks))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_workload() {
        let check = |config: SequentialConfig, rpk: usize, expected: &[(i64, Vec<i64>)]| {
            let factory = SequentialFactory::new(config).unwrap();
            let mut seq = factory.create();
            let mut actual = Vec::new();
            while let Some((pk, cks)) = seq.generate_keys(rpk) {
                actual.push((pk, cks));
            }

            assert_eq!(actual, expected);
        };

        // Basic test
        check(
            SequentialConfig {
                iterations: 1,
                partition_offset: 0,
                pks: 3,
                cks_per_pk: 1,
            },
            1,
            &[(0, vec![0]), (1, vec![0]), (2, vec![0])],
        );

        // Two iterations
        check(
            SequentialConfig {
                iterations: 2,
                partition_offset: 0,
                pks: 3,
                cks_per_pk: 1,
            },
            1,
            &[
                (0, vec![0]),
                (1, vec![0]),
                (2, vec![0]),
                (0, vec![0]),
                (1, vec![0]),
                (2, vec![0]),
            ],
        );

        // Two clustering keys
        check(
            SequentialConfig {
                iterations: 1,
                partition_offset: 0,
                pks: 3,
                cks_per_pk: 2,
            },
            1,
            &[
                (0, vec![0]),
                (0, vec![1]),
                (1, vec![0]),
                (1, vec![1]),
                (2, vec![0]),
                (2, vec![1]),
            ],
        );

        // Multiple clustering keys, multiple rows per op, not divisible by ck count
        check(
            SequentialConfig {
                iterations: 1,
                partition_offset: 0,
                pks: 2,
                cks_per_pk: 5,
            },
            3,
            &[
                (0, vec![0, 1, 2]),
                (0, vec![3, 4]),
                (1, vec![0, 1, 2]),
                (1, vec![3, 4]),
            ],
        );
    }
}
