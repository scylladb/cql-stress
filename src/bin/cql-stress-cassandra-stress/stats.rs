use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use cql_stress::{configuration::OperationContext, sharded_stats};
use hdrhistogram::Histogram;
use tokio::time::Instant;

use crate::settings::{CassandraStressSettings, ThreadsInfo};

const HISTOGRAM_PRECISION: u8 = 3;

/// A struct to hold different types of latency measurements
struct LatencyMetrics {
    /// Service time (now - actual_start_time)
    service_time: u64,
    /// Response time (now - scheduled_start_time)
    response_time: Option<u64>,
    /// Wait time (actual_start_time - scheduled_start_time)
    wait_time: Option<u64>,
}

/// An interface for latency calculation logic.
/// c-s can display either raw or coordinated-omission-fixed latencies.
trait LatencyCalculator: Send + Sync {
    /// Calculate different types of latency metrics
    fn calculate(&self, ctx: &OperationContext) -> LatencyMetrics;

    /// Returns the default latency value to be used for overall statistics
    fn default_latency(&self, metrics: &LatencyMetrics) -> u64;
}

struct RawLatencyCalculator;
struct CoordinatedOmissionFixedLatencyCalculator;

impl LatencyCalculator for RawLatencyCalculator {
    fn calculate(&self, ctx: &OperationContext) -> LatencyMetrics {
        let now = Instant::now();
        let service_time = (now - ctx.actual_start_time).as_nanos() as u64;

        LatencyMetrics {
            service_time,
            response_time: None,
            wait_time: None,
        }
    }

    fn default_latency(&self, metrics: &LatencyMetrics) -> u64 {
        metrics.service_time
    }
}

impl LatencyCalculator for CoordinatedOmissionFixedLatencyCalculator {
    fn calculate(&self, ctx: &OperationContext) -> LatencyMetrics {
        let now = Instant::now();
        let service_time = (now - ctx.actual_start_time).as_nanos() as u64;
        let response_time = (now - ctx.scheduled_start_time).as_nanos() as u64;
        let wait_time = (ctx.actual_start_time - ctx.scheduled_start_time).as_nanos() as u64;

        LatencyMetrics {
            service_time,
            response_time: Some(response_time),
            wait_time: Some(wait_time),
        }
    }

    fn default_latency(&self, metrics: &LatencyMetrics) -> u64 {
        metrics.response_time.unwrap_or(metrics.service_time)
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
    latency_histogram: Histogram<u64>, // combined histograms across all tags
    histograms: HashMap<String, Histogram<u64>>, // Map of tag to histogram
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
            latency_histogram: Histogram::new(HISTOGRAM_PRECISION).unwrap(),
            latency_calculator: if self.coordinated_omission_fixed {
                Box::new(CoordinatedOmissionFixedLatencyCalculator)
            } else {
                Box::new(RawLatencyCalculator)
            },
            histograms: HashMap::new(),
        }
    }
}

impl Stats {
    pub fn account_operation<T, E>(
        &mut self,
        ctx: &OperationContext,
        result: &Result<T, E>,
        tag: &str,
    ) {
        self.operations += 1;
        match result {
            Ok(_) => {
                let metrics = self.latency_calculator.calculate(ctx);
                let default_latency = self.latency_calculator.default_latency(&metrics);
                self.latency_histogram.record(default_latency).unwrap();

                let service_time_tag = format!("{tag}-st");
                let service_time_histogram = self
                    .histograms
                    .entry(service_time_tag)
                    .or_insert_with(|| Histogram::new(HISTOGRAM_PRECISION).unwrap());
                service_time_histogram.record(metrics.service_time).unwrap();

                if let Some(response_time) = metrics.response_time {
                    let response_time_tag = format!("{tag}-rt");
                    let response_time_histogram = self
                        .histograms
                        .entry(response_time_tag)
                        .or_insert_with(|| Histogram::new(HISTOGRAM_PRECISION).unwrap());
                    response_time_histogram.record(response_time).unwrap();
                }

                if let Some(wait_time) = metrics.wait_time {
                    let wait_time_tag = format!("{tag}-wt");
                    let wait_time_histogram = self
                        .histograms
                        .entry(wait_time_tag)
                        .or_insert_with(|| Histogram::new(HISTOGRAM_PRECISION).unwrap());
                    wait_time_histogram.record(wait_time).unwrap();
                }
            }
            Err(_) => {
                self.errors += 1;
            }
        }
    }

    pub fn get_histograms(&self) -> &HashMap<String, Histogram<u64>> {
        &self.histograms
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
        self.histograms.clear();
    }

    fn combine(&mut self, other: &Self) {
        self.operations += other.operations;
        self.errors += other.errors;
        self.latency_histogram
            .add(&other.latency_histogram)
            .unwrap();
        for (tag, other_hist) in &other.histograms {
            let hist = self
                .histograms
                .entry(tag.clone())
                .or_insert_with(|| Histogram::new(HISTOGRAM_PRECISION).unwrap());
            hist.add(other_hist).unwrap();
        }
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
        println!("Total operation time      : {hours:0>2}:{minutes:0>2}:{seconds:0>2}");
    }
}
