use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use cql_stress::{configuration::OperationContext, sharded_stats};
use hdrhistogram::Histogram;
use scylla::client::session::Session;
use tokio::time::Instant;

use uuid::Uuid;

use crate::operation::OperationOutcome;
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
    track_coordinators: bool,
}

pub struct Stats {
    operations: u64,
    errors: u64,
    latency_calculator: Box<dyn LatencyCalculator>,
    latency_histogram: Histogram<u64>, // combined histograms across all tags
    histograms: HashMap<String, Histogram<u64>>, // Map of tag to histogram
    /// Operations per coordinator host, enabled by `-log coordinators=true`.
    ///
    /// `None` when disabled, so the map is never touched on the hot path. When enabled,
    /// bumping an existing entry does not allocate - the map holds one entry per node.
    coordinators: Option<HashMap<Uuid, u64>>,
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
            track_coordinators: settings.log.coordinators,
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
            coordinators: self.track_coordinators.then(HashMap::new),
        }
    }
}

impl Stats {
    pub fn account_operation<E>(
        &mut self,
        ctx: &OperationContext,
        result: &Result<OperationOutcome, E>,
        tag: &str,
    ) {
        self.operations += 1;
        match result {
            Ok(outcome) => {
                if let (Some(coordinators), Some(host_id)) =
                    (self.coordinators.as_mut(), outcome.coordinator)
                {
                    *coordinators.entry(host_id).or_insert(0) += 1;
                }

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
        // Retain the capacity - the key set is the (small, fixed) set of cluster nodes.
        if let Some(coordinators) = self.coordinators.as_mut() {
            coordinators.clear();
        }
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
        if let (Some(coordinators), Some(other_coordinators)) =
            (self.coordinators.as_mut(), other.coordinators.as_ref())
        {
            for (host_id, count) in other_coordinators {
                *coordinators.entry(*host_id).or_insert(0) += count;
            }
        }
    }
}

pub struct StatsPrinter {
    start_time: Instant,
    previous_time: Instant,
    total_ops: u64,
    /// Used to resolve coordinator host IDs to node addresses when printing the
    /// per-coordinator distribution. `None` when coordinator accounting is disabled.
    session: Option<Arc<Session>>,
}

impl StatsPrinter {
    pub fn new(session: Option<Arc<Session>>) -> Self {
        Self {
            start_time: Instant::now(),
            previous_time: Instant::now(),
            total_ops: 0,
            session,
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

        self.print_coordinators(final_stats);
    }

    /// Prints the distribution of operations across coordinator hosts.
    ///
    /// On a strongly consistent keyspace at `cl=quorum` this must be concentrated on the
    /// tablet leaders. A uniform spread across all replicas means leader-aware routing is
    /// not in effect - the same shape an eventually consistent keyspace produces.
    fn print_coordinators(&self, final_stats: &Stats) {
        let Some(coordinators) = final_stats.coordinators.as_ref() else {
            return;
        };

        println!();
        println!("Operations per coordinator:");
        if coordinators.is_empty() {
            println!("  (no operations recorded)");
            return;
        }

        // Resolve host IDs to addresses. New nodes that joined mid-run and unknown IDs
        // simply fall back to printing the raw host ID.
        let addresses: HashMap<Uuid, String> = self
            .session
            .as_ref()
            .map(|session| {
                session
                    .get_cluster_state()
                    .get_nodes_info()
                    .iter()
                    .map(|node| (node.host_id, node.address.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let total: u64 = coordinators.values().sum();
        let mut entries = coordinators.iter().collect::<Vec<_>>();
        // Descending by count, then by host ID so the output is deterministic.
        entries.sort_unstable_by_key(|(host_id, count)| (std::cmp::Reverse(**count), **host_id));

        for (host_id, count) in entries {
            let share = *count as f64 * 100.0 / total as f64;
            match addresses.get(host_id) {
                Some(address) => println!("  {address:<24} {count:>12} ({share:>5.1}%)"),
                None => println!("  {host_id:<24} {count:>12} ({share:>5.1}%)"),
            }
        }
    }
}
