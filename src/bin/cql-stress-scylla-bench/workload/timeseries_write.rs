use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;

use super::{Workload, WorkloadFactory};

pub struct TimeseriesWriteFactory {
    config: TimeseriesWriteConfig,
    shared_state: Arc<SharedState>,
}

struct SharedState {
    pub counter: AtomicU64,
}

struct TimeseriesWrite {
    config: TimeseriesWriteConfig,
    period_nanos: u64,
    shared_state: Arc<SharedState>,
}

#[derive(Clone)]
pub struct TimeseriesWriteConfig {
    pub partition_offset: i64,
    pub pks_per_generation: u64,
    pub cks_per_pk: u64,
    pub start_nanos: u64,
    pub max_rate: u64,
}

impl TimeseriesWriteFactory {
    pub fn new(config: TimeseriesWriteConfig) -> Result<TimeseriesWriteFactory> {
        let shared_state = Arc::new(SharedState {
            counter: AtomicU64::new(0),
        });

        Ok(Self {
            config,
            shared_state,
        })
    }
}

impl WorkloadFactory for TimeseriesWriteFactory {
    fn create(&self) -> Box<dyn Workload> {
        Box::new(TimeseriesWrite::new(
            self.config.clone(),
            Arc::clone(&self.shared_state),
        ))
    }
}

impl TimeseriesWrite {
    fn new(config: TimeseriesWriteConfig, shared_state: Arc<SharedState>) -> TimeseriesWrite {
        let period_nanos = (1_000_000_000 * config.pks_per_generation) / config.max_rate;

        TimeseriesWrite {
            config,
            period_nanos,
            shared_state,
        }
    }
}

impl Workload for TimeseriesWrite {
    fn generate_keys(&mut self, _ck_count: usize) -> Option<(i64, Vec<i64>)> {
        let x = self.shared_state.counter.fetch_add(1, Ordering::Relaxed);
        let pk_position = x % self.config.pks_per_generation;
        let ck_position = x / self.config.pks_per_generation;
        let pk_generation = ck_position / self.config.cks_per_pk;

        let pk = (pk_position << 32) | pk_generation;
        let ck = -((self.config.start_nanos + self.period_nanos * ck_position as u64) as i64);

        Some((pk as i64, vec![ck]))
    }
}
