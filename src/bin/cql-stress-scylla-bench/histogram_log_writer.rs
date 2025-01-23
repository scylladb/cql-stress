use std::io::Result;
use std::marker::Unpin;
use std::ops::Range;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use hdrhistogram::serialization::{Serializer, V2DeflateSerializer};
use hdrhistogram::Histogram;
use tokio::io::{AsyncWrite, AsyncWriteExt};

const LOG_FORMAT_VERSION: &str = "1.3";

pub struct HistogramLogOptions<'t> {
    pub interval_seconds: Range<f64>,
    pub tag: &'t str,
}

pub struct HistogramLogWriter<W: AsyncWrite + Unpin> {
    writer: W,
}

impl<W: AsyncWrite + Unpin> HistogramLogWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub async fn output_log_format_version(&mut self) -> Result<()> {
        let line = format!("#[Histogram log format version {}]\n", LOG_FORMAT_VERSION);
        self.writer.write_all(line.as_bytes()).await
    }

    pub async fn output_comment(&mut self, s: &str) -> Result<()> {
        let line = format!("#{}\n", s);
        self.writer.write_all(line.as_bytes()).await
    }

    pub async fn output_base_time(&mut self, base_time: SystemTime) -> Result<()> {
        let secs = base_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let line = format!("#[Basetime: {} (seconds since epoch)]\n", secs);
        self.writer.write_all(line.as_bytes()).await
    }

    pub async fn output_start_time(&mut self, base_time: SystemTime) -> Result<()> {
        let secs = base_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let millis = base_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let rounded_base_time = std::time::UNIX_EPOCH
            .checked_add(Duration::from_millis(millis as u64))
            .unwrap();
        let utc_base_time: DateTime<Utc> = rounded_base_time.into();
        let line = format!(
            "#[StartTime: {} (seconds since epoch), {}]\n",
            secs,
            utc_base_time.to_rfc3339(),
        );
        self.writer.write_all(line.as_bytes()).await
    }

    pub async fn output_legend(&mut self) -> Result<()> {
        self.writer.write_all("\"StartTimestamp\",\"Interval_Length\",\"Interval_Max\",\"Interval_Compressed_Histogram\"\n".as_bytes()).await
    }

    pub async fn output_interval_histogram(
        &mut self,
        histogram: &Histogram<u64>,
        opts: HistogramLogOptions<'_>,
    ) -> Result<()> {
        const MS_TO_NS_RATIO: f64 = 1_000_000.0;
        let max_value = histogram.max() as f64 / MS_TO_NS_RATIO;
        let mut raw_encoded_histogram = Vec::new();
        V2DeflateSerializer::new()
            .serialize(histogram, &mut raw_encoded_histogram)
            .unwrap();
        let line = format!(
            "Tag={tag},{start_time},{end_time},{max_value},{encoded}\n",
            tag = opts.tag,
            start_time = opts.interval_seconds.start,
            end_time = opts.interval_seconds.end,
            max_value = max_value,
            encoded = base64::encode(&raw_encoded_histogram),
        );
        self.writer.write_all(line.as_bytes()).await
    }
}
