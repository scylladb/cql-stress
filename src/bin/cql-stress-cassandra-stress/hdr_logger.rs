use std::{fs::File, path::Path, time::SystemTime};

use crate::stats::Stats;
use anyhow::Result;
use cql_stress::version::get_version_info;
use hdrhistogram::serialization::interval_log;
use hdrhistogram::Histogram;
use tokio::time::Instant;

/// Writes histogram data to a file using HDR format.
///
/// This struct manages a log writer for recording performance histograms,
/// tracking the start time and last write time for accurate timing.
pub struct HdrLogWriter {
    log_writer: interval_log::IntervalLogWriter<
        'static,
        'static,
        File,
        hdrhistogram::serialization::V2Serializer,
    >,
    start_timestamp: Instant,
    last_hdr_write: Instant,
}

impl HdrLogWriter {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path)?;
        let start_time = SystemTime::now();

        let serializer = Box::new(hdrhistogram::serialization::V2Serializer::new());
        // Note: Box::leak is used to satisfy 'static lifetime requirements of IntervalLogWriter.
        let log_writer = interval_log::IntervalLogWriterBuilder::new()
            .add_comment(
                format!(
                    "[Logged with Cql-stress {}]",
                    get_version_info().cql_stress_version
                )
                .as_str(),
            )
            .with_start_time(start_time)
            .with_base_time(start_time)
            .with_max_value_divisor(1000000.0)
            .begin_log_with(Box::leak(Box::new(file)), Box::leak(serializer))
            .unwrap();

        Ok(Self {
            log_writer,
            start_timestamp: Instant::now(),
            last_hdr_write: Instant::now(),
        })
    }

    /// Writes combined statistics histograms to the HDR log file.
    ///
    /// # Arguments
    /// * `partial_stats` - The statistics containing histograms to log.
    ///
    /// # Errors
    /// Returns an error if writing to the log file fails.
    pub fn write_to_hdr_log(&mut self, partial_stats: &Stats) -> Result<()> {
        let duration = self.last_hdr_write.elapsed();
        let elapsed = self.start_timestamp.elapsed();

        let mut tag_histograms: Vec<(&String, &Histogram<u64>)> =
            partial_stats.get_histograms().iter().collect();
        tag_histograms.sort_by(|(tag_a, _), (tag_b, _)| tag_a.cmp(tag_b));

        for (tag, histogram) in tag_histograms {
            self.log_writer.write_histogram(
                histogram,
                elapsed,
                duration,
                interval_log::Tag::new(tag),
            )?;
        }
        self.last_hdr_write = Instant::now();
        Ok(())
    }
}
