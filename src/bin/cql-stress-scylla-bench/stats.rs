use std::io::Write;
use std::time::Duration;

use anyhow::Result;
use hdrhistogram::Histogram;
use tokio::time::Instant;

use cql_stress::configuration::OperationContext;
use cql_stress::sharded_stats;

use crate::gocompat::strconv::format_duration;

pub type ShardedStats = sharded_stats::ShardedStats<StatsFactory>;

pub struct StatsFactory;

impl sharded_stats::StatsFactory for StatsFactory {
    type Stats = Stats;

    fn create(&self) -> Stats {
        Stats {
            operations: 0,
            clustering_rows: 0,
            errors: 0,
            raw_latency: Histogram::new(3).unwrap(), // TODO: This shouldn't be hardcoded
            co_fixed_latency: Histogram::new(3).unwrap(), // TODO: This shouldn't be hardcoded

            latency_resolution: 1,
        }
    }
}

pub struct Stats {
    pub operations: u64,
    pub clustering_rows: u64,
    pub errors: u64,

    // Latency, measured both with and without the coordinated omission fix
    pub raw_latency: Histogram<u64>,
    pub co_fixed_latency: Histogram<u64>,

    // Do not change in workloads, this should be constant
    pub latency_resolution: u64,
}

impl sharded_stats::Stats for Stats {
    fn clear(&mut self) {
        self.operations = 0;
        self.clustering_rows = 0;
        self.errors = 0;
        self.raw_latency.reset();
        self.co_fixed_latency.reset();
    }

    fn combine(&mut self, other: &Self) {
        self.operations += other.operations;
        self.clustering_rows += other.clustering_rows;
        self.errors += other.errors;
        self.raw_latency.add(&other.raw_latency).unwrap();
        self.co_fixed_latency.add(&other.co_fixed_latency).unwrap();
    }
}

impl Stats {
    pub fn account_op(&mut self, ctx: &OperationContext, result: &Result<()>, rows: usize) {
        self.operations += 1;
        match result {
            Ok(()) => {
                self.clustering_rows += rows as u64;
                self.account_latency(ctx);
            }
            Err(_) => {
                self.errors += 1;
            }
        }
    }

    pub fn account_latency(&mut self, ctx: &OperationContext) {
        let now = Instant::now();
        let _ = self
            .raw_latency
            .record((now - ctx.actual_start_time).as_nanos() as u64);
        let _ = self
            .co_fixed_latency
            .record((now - ctx.scheduled_start_time).as_nanos() as u64);
    }

    pub fn get_histogram(&self, typ: LatencyType) -> &Histogram<u64> {
        match typ {
            LatencyType::Raw => &self.raw_latency,
            LatencyType::AdjustedForCoordinatorOmission => &self.co_fixed_latency,
        }
    }
}

#[derive(Clone, Copy)]
pub enum LatencyType {
    Raw,
    AdjustedForCoordinatorOmission,
}

// TODO: Should we have two impls, one with latency and another without?
pub struct StatsPrinter {
    start_time: Instant,
    latency_type: Option<LatencyType>,
}

impl StatsPrinter {
    pub fn new(latency_type: Option<LatencyType>) -> Self {
        Self {
            start_time: Instant::now(),
            latency_type,
        }
    }

    pub fn print_header(&self, out: &mut impl Write) -> Result<()> {
        if self.latency_type.is_some() {
            writeln!(
                out,
                "{:9} {:>7} {:>7} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}",
                "time",
                "ops/s",
                "rows/s",
                "errors",
                "max",
                "99.9th",
                "99th",
                "95th",
                "90th",
                "median",
                "mean"
            )?;
        } else {
            writeln!(
                out,
                "{:6} {:>7} {:>7} {:>6}",
                "time", "ops/s", "rows/s", "errors",
            )?;
        }

        Ok(())
    }

    pub fn print_partial(&self, stats: &Stats, out: &mut impl Write) -> Result<()> {
        let time = Instant::now() - self.start_time;

        if let Some(typ) = self.latency_type {
            let histogram = stats.get_histogram(typ);

            let p50 = Duration::from_nanos(histogram.value_at_quantile(0.5));
            let p90 = Duration::from_nanos(histogram.value_at_quantile(0.9));
            let p95 = Duration::from_nanos(histogram.value_at_quantile(0.95));
            let p99 = Duration::from_nanos(histogram.value_at_quantile(0.99));
            let p999 = Duration::from_nanos(histogram.value_at_quantile(0.999));
            let max = Duration::from_nanos(histogram.max());
            let mean = Duration::from_nanos(histogram.mean() as u64);
            writeln!(
                out,
                "{:9} {:>7} {:>7} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}",
                format_duration(time),
                stats.operations,
                stats.clustering_rows,
                stats.errors,
                format_duration(max),
                format_duration(p999),
                format_duration(p99),
                format_duration(p95),
                format_duration(p90),
                format_duration(p50),
                format_duration(mean),
            )?;
        } else {
            writeln!(
                out,
                "{:6} {:>7} {:>7} {:>6}",
                format_duration(time),
                stats.operations,
                stats.clustering_rows,
                stats.errors,
            )?;
        }

        Ok(())
    }

    pub fn print_final(&self, stats: &Stats, out: &mut impl Write) -> Result<()> {
        let time = Instant::now() - self.start_time;
        writeln!(out)?;
        writeln!(out, "Results:")?;
        writeln!(out, "Time (avg):\t{}", format_duration(time))?;
        writeln!(out, "Total ops:\t{}", stats.operations)?;
        writeln!(out, "Total rows:\t{}", stats.clustering_rows)?;
        if stats.errors != 0 {
            writeln!(out, "Total errors:\t{}", stats.errors)?;
        }

        let ops_per_second = stats.operations as f64 / time.as_secs_f64();
        writeln!(out, "Operations/s:\t{}", ops_per_second)?;

        let rows_per_second = stats.clustering_rows as f64 / time.as_secs_f64();
        writeln!(out, "Rows/s:\t\t{}", rows_per_second)?;

        self.print_final_latency_histogram("raw latency", &stats.raw_latency, out)?;
        self.print_final_latency_histogram("c-o fixed latency", &stats.co_fixed_latency, out)?;

        // TODO: "critical errors"

        Ok(())
    }

    fn print_final_latency_histogram(
        &self,
        name: &str,
        latency: &Histogram<u64>,
        out: &mut impl Write,
    ) -> Result<()> {
        // TODO: Use non-shortened version of the format_duration
        writeln!(out, "{}:", name)?;

        let p50 = Duration::from_nanos(latency.value_at_quantile(0.5));
        let p90 = Duration::from_nanos(latency.value_at_quantile(0.9));
        let p95 = Duration::from_nanos(latency.value_at_quantile(0.95));
        let p99 = Duration::from_nanos(latency.value_at_quantile(0.99));
        let p999 = Duration::from_nanos(latency.value_at_quantile(99.9));
        let max = Duration::from_nanos(latency.max());
        let mean = Duration::from_nanos(latency.mean() as u64);

        writeln!(out, "  max:\t\t{}", format_duration(max))?;
        writeln!(out, "  99.9th:\t{}", format_duration(p999))?;
        writeln!(out, "  99h:\t\t{}", format_duration(p99))?;
        writeln!(out, "  95h:\t\t{}", format_duration(p95))?;
        writeln!(out, "  90h:\t\t{}", format_duration(p90))?;
        writeln!(out, "  median:\t{}", format_duration(p50))?;
        writeln!(out, "  mean:\t\t{}", format_duration(mean))?;

        Ok(())
    }
}
