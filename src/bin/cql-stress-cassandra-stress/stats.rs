use std::sync::Arc;

use anyhow::Result;
use cql_stress::{configuration::OperationContext, sharded_stats};
use hdrhistogram::Histogram;
use tokio::time::Instant;

use crate::settings::{CassandraStressSettings, ThreadsInfo};

/// An interface for latency calculation logic.
/// c-s can display either raw or coordinated-omission-fixed latencies.
trait LatencyCalculator: Send + Sync {
    fn calculate(&self, ctx: &OperationContext) -> u64;
}

struct RawLatencyCalculator;
struct CoordinatedOmissionFixedLatencyCalculator;

impl LatencyCalculator for RawLatencyCalculator {
    fn calculate(&self, ctx: &OperationContext) -> u64 {
        let now = Instant::now();
        (now - ctx.actual_start_time).as_nanos() as u64
    }
}

impl LatencyCalculator for CoordinatedOmissionFixedLatencyCalculator {
    fn calculate(&self, ctx: &OperationContext) -> u64 {
        let now = Instant::now();
        (now - ctx.scheduled_start_time).as_nanos() as u64
    }
}

pub type ShardedStats = sharded_stats::ShardedStats<StatsFactory>;

pub struct StatsFactory {
    coordinated_omission_fixed: bool,
}

pub struct Stats {
    operations: u64,
    errors: u64,
    latency_calculator: Box<dyn LatencyCalculator>,
    latency_histogram: Histogram<u64>,
}

impl StatsFactory {
    pub fn new(settings: &Arc<CassandraStressSettings>) -> Self {
        let coordinated_omission_fixed = match settings.rate.threads_info {
            ThreadsInfo::Fixed {
                threads: _,
                throttle: _,
                co_fixed,
            } => co_fixed,
            ThreadsInfo::Auto { .. } => false,
        };

        Self {
            coordinated_omission_fixed,
        }
    }
}

impl sharded_stats::StatsFactory for StatsFactory {
    type Stats = Stats;

    fn create(&self) -> Self::Stats {
        Stats {
            operations: 0,
            errors: 0,
            // This cannot panic since 1 <= sigfig <= 5.
            // 3 is the recommended value, as well as used in Java's c-s implementation.
            // AFAIK, there is no c-s option which lets the user define this value.
            latency_histogram: Histogram::new(3).unwrap(),
            latency_calculator: if self.coordinated_omission_fixed {
                Box::new(CoordinatedOmissionFixedLatencyCalculator)
            } else {
                Box::new(RawLatencyCalculator)
            },
        }
    }
}

impl Stats {
    pub fn account_operation<T, E>(&mut self, ctx: &OperationContext, result: &Result<T, E>) {
        self.operations += 1;
        match result {
            Ok(_) => {
                self.latency_histogram
                    .record(self.latency_calculator.calculate(ctx))
                    .unwrap();
            }
            Err(_) => {
                self.errors += 1;
            }
        }
    }
}

impl sharded_stats::Stats for Stats {
    fn clear(&mut self) {
        self.operations = 0;
        self.errors = 0;
        self.latency_histogram.reset();
    }

    fn combine(&mut self, other: &Self) {
        self.operations += other.operations;
        self.errors += other.errors;
        self.latency_histogram
            .add(&other.latency_histogram)
            .unwrap();
    }
}
