use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Result;
use rand::Rng;
use rand_distr::{Distribution, StandardNormal};

use crate::args::TimeseriesDistribution;
use crate::distribution::RngGen;

use super::{Workload, WorkloadFactory};

pub struct TimeseriesReadFactory {
    config: TimeseriesReadConfig,
    shared_state: Arc<SharedState>,
}

struct SharedState {
    pub counter: AtomicU64,
}

struct TimeseriesRead {
    config: TimeseriesReadConfig,
    gen: RngGen,
    shared_state: Arc<SharedState>,
}

#[derive(Clone)]
pub struct TimeseriesReadConfig {
    // FIXME: why is this not used??
    pub _partition_offset: i64,
    pub pks_per_generation: u64,
    pub cks_per_pk: u64,
    pub start_nanos: u64,
    pub period_nanos: u64,
    pub distribution: TimeseriesDistribution,
}

impl TimeseriesReadFactory {
    pub fn new(config: TimeseriesReadConfig) -> Result<TimeseriesReadFactory> {
        let shared_state = Arc::new(SharedState {
            counter: AtomicU64::new(0),
        });

        Ok(Self {
            config,
            shared_state,
        })
    }
}

impl WorkloadFactory for TimeseriesReadFactory {
    fn create(&self) -> Box<dyn Workload> {
        Box::new(TimeseriesRead::new(
            self.config.clone(),
            Arc::clone(&self.shared_state),
        ))
    }
}

impl TimeseriesRead {
    fn new(config: TimeseriesReadConfig, shared_state: Arc<SharedState>) -> TimeseriesRead {
        TimeseriesRead {
            config,
            gen: RngGen::new(rand::thread_rng().gen()),
            shared_state,
        }
    }
}

impl Workload for TimeseriesRead {
    fn generate_keys(&mut self, ck_count: usize) -> Option<(i64, Vec<i64>)> {
        let x = self.shared_state.counter.fetch_add(1, Ordering::Relaxed);
        let pk_position = x % self.config.pks_per_generation;

        let now_nanos = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_nanos() as u64;
        let max_generation = (now_nanos - self.config.start_nanos)
            / (self.config.period_nanos * self.config.cks_per_pk)
            + 1;
        let pk_generation = self.random_int(max_generation);

        let pk = (pk_position << 32) | pk_generation;

        // We are OK with ck duplicates - at least scylla-bench is
        let cks = (0..ck_count)
            .map(|_| {
                let max_range = std::cmp::min(
                    self.config.cks_per_pk,
                    (now_nanos - self.config.start_nanos) / self.config.period_nanos + 1,
                );
                let ck_position =
                    pk_generation * self.config.cks_per_pk + self.random_int(max_range);

                -((self.config.start_nanos + self.config.period_nanos * ck_position) as i64)
            })
            .collect();

        Some((pk as i64, cks))
    }
}

impl TimeseriesRead {
    fn random_int(&mut self, max_value: u64) -> u64 {
        match self.config.distribution {
            TimeseriesDistribution::HalfNormal => {
                let mut base =
                    <StandardNormal as Distribution<f64>>::sample(&StandardNormal, &mut self.gen)
                        .abs();
                if base > 4.0 {
                    base = 4.0;
                }
                ((1.0 - base * 0.25) * max_value as f64) as u64
            }
            TimeseriesDistribution::Uniform => self.gen.gen_range(0..max_value),
        }
    }
}
