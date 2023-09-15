use std::{sync::Arc, time::Duration};

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

    fn op_rate(&self, interval_duration: Duration) -> f64 {
        self.operations as f64 / interval_duration.as_secs_f64()
    }

    fn mean_latency_ms(&self) -> f64 {
        self.latency_histogram.mean() * 1e-6
    }

    fn latency_at_quantile_ms(&self, quantile: f64) -> f64 {
        self.latency_histogram.value_at_quantile(quantile) as f64 * 1e-6
    }

    fn median_latency_ms(&self) -> f64 {
        self.latency_at_quantile_ms(0.5)
    }

    fn max_latency_ms(&self) -> f64 {
        self.latency_histogram.max() as f64 * 1e-6
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

pub struct StatsPrinter {
    start_time: Instant,
    previous_time: Instant,
    total_ops: u64,
}

impl StatsPrinter {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            previous_time: Instant::now(),
            total_ops: 0,
        }
    }

    pub fn print_header(&self) {
        println!(
            "{:10},{:>8},{:>8},{:>8},{:>8},{:>8},{:>8},{:>8},{:>7},{:>7}",
            "total ops", "op/s", "mean", "med", ".95", ".99", ".999", "max", "time", "errors"
        );
    }

    pub fn print_partial(&mut self, partial_stats: &Stats) {
        self.total_ops += partial_stats.operations;
        let now = Instant::now();
        let total_time_secs = (now - self.start_time).as_secs_f64();
        let interval_duration = now - self.previous_time;
        self.previous_time = now;

        println!(
            "{:10},{:>8.0},{:>8.1},{:>8.1},{:>8.1},{:>8.1},{:>8.1},{:>8.1},{:>7.1},{:>7.0}",
            self.total_ops,
            partial_stats.op_rate(interval_duration),
            partial_stats.mean_latency_ms(),
            partial_stats.median_latency_ms(),
            partial_stats.latency_at_quantile_ms(0.95),
            partial_stats.latency_at_quantile_ms(0.99),
            partial_stats.latency_at_quantile_ms(0.999),
            partial_stats.max_latency_ms(),
            total_time_secs,
            partial_stats.errors,
        );
    }

    pub fn print_summary(&self, final_stats: &Stats) {
        let now = Instant::now();
        let benchmark_duration = now - self.start_time;

        println!();
        println!("Results:");

        println!(
            "Op rate                   : {:>8.0} op/s",
            final_stats.op_rate(benchmark_duration)
        );
        println!(
            "Latency mean              : {:>6.1} ms",
            final_stats.mean_latency_ms()
        );
        println!(
            "Latency median            : {:>6.1} ms",
            final_stats.median_latency_ms()
        );
        println!(
            "Latency 95th percentile   : {:>6.1} ms",
            final_stats.latency_at_quantile_ms(0.95)
        );
        println!(
            "Latency 99th percentile   : {:>6.1} ms",
            final_stats.latency_at_quantile_ms(0.99)
        );
        println!(
            "Latency 99.9th percentile : {:>6.1} ms",
            final_stats.latency_at_quantile_ms(0.999)
        );
        println!(
            "Latency max               : {:>6.1} ms",
            final_stats.max_latency_ms()
        );
        println!("Total operations          : {:>10}", final_stats.operations);
        println!("Total errors              : {:>10}", final_stats.errors);

        let seconds = benchmark_duration.as_secs() % 60;
        let minutes = (benchmark_duration.as_secs() / 60) % 60;
        let hours = (benchmark_duration.as_secs() / 60) / 60;
        println!(
            "Total operation time      : {:0>2}:{:0>2}:{:0>2}",
            hours, minutes, seconds
        )
    }
}
