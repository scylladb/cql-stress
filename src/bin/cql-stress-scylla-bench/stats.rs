use std::io::Write;
use std::time::Duration;

use anyhow::Result;
use hdrhistogram::Histogram;
use tokio::time::Instant;

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
            latency: Histogram::new(3).unwrap(), // TODO: This shouldn't be hardcoded

            latency_resolution: 1,
        }
    }
}

pub struct Stats {
    pub operations: u64,
    pub clustering_rows: u64,
    pub errors: u64,
    pub latency: Histogram<u64>,

    // Do not change in workloads, this should be constant
    pub latency_resolution: u64,
}

impl sharded_stats::Stats for Stats {
    fn clear(&mut self) {
        self.operations = 0;
        self.clustering_rows = 0;
        self.errors = 0;
        self.latency.reset();
    }

    fn combine(&mut self, other: &Self) {
        self.operations += other.operations;
        self.clustering_rows += other.clustering_rows;
        self.errors += other.errors;
        self.latency.add(&other.latency).unwrap();
    }
}

impl Stats {
    pub fn account_op(&mut self, op_start: Instant, result: &Result<()>, rows: usize) {
        self.operations += 1;
        match result {
            Ok(()) => {
                self.clustering_rows += rows as u64;
                self.account_latency(op_start);
            }
            Err(_) => {
                self.errors += 1;
            }
        }
    }

    pub fn account_latency(&mut self, op_start: Instant) {
        let op_latency = Instant::now() - op_start;
        let _ = self.latency.record(op_latency.as_nanos() as u64);
    }
}

// TODO: Should we have two impls, one with latency and another without?
pub struct StatsPrinter {
    start_time: Instant,
    with_latency: bool,
}

impl StatsPrinter {
    pub fn new(with_latency: bool) -> Self {
        Self {
            start_time: Instant::now(),
            with_latency,
        }
    }

    pub fn print_header(&self, out: &mut impl Write) -> Result<()> {
        if self.with_latency {
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
        if self.with_latency {
            let p50 = Duration::from_nanos(stats.latency.value_at_quantile(0.5));
            let p90 = Duration::from_nanos(stats.latency.value_at_quantile(0.9));
            let p95 = Duration::from_nanos(stats.latency.value_at_quantile(0.95));
            let p99 = Duration::from_nanos(stats.latency.value_at_quantile(0.99));
            let p999 = Duration::from_nanos(stats.latency.value_at_quantile(0.999));
            let max = Duration::from_nanos(stats.latency.max());
            let mean = Duration::from_nanos(stats.latency.mean() as u64);
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

        // TODO: co-fixed latency
        self.print_final_latency_histogram("raw latency", &stats.latency, out)?;

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
