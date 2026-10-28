//! Progress for SCT: `SCV` lines on stdout and `report.json` (spec §14.4).
//!
//! Human-readable logs go to stdout as usual; machine-readable lines start with `SCV `
//! followed by one JSON object.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use serde::{Serialize, Serializer};

/// The pool every session of the run was built with, defaults included (spec §15.4), so no
/// result is read without knowing how many connections made it.
#[derive(Serialize)]
pub struct Connections {
    /// `"per-shard"` or `"per-host"`.
    pub pool: &'static str,
    pub pool_size: usize,
    /// `"<first>..<last>"`; `null` for a per-host pool, which takes no port of it.
    pub shard_aware_port_range: Option<String>,
    pub tcp_reuse_address: bool,
}

#[derive(Serialize)]
#[serde(tag = "t", rename_all = "lowercase")]
pub enum Scv<'a> {
    /// `pop` and `gen_base` describe the checked stream, `bulk_pop` the bulk load; each is
    /// left out when that part does not run.
    Start {
        mode: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        pop: Option<&'a str>,
        slots: usize,
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "opt_as_string"
        )]
        gen_base: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        bulk_pop: Option<&'a str>,
        #[serde(flatten)]
        connections: Connections,
    },
    /// Printed when the row seals and its check file is archived, so SCT can copy the
    /// evidence before it raises the event; `wall_ms` is when the read exposed it.
    Violation {
        kind: &'a str,
        pk: i64,
        #[serde(serialize_with = "as_string")]
        gen: i64,
        #[serde(serialize_with = "cell_name")]
        cell: usize,
        wall_ms: u64,
        archive: &'a str,
    },
    Stats(IntervalStats),
    /// A check file the checker finished: verdicts of its rows, and where its evidence is
    /// kept unless every row was ok.
    Checked {
        file: String,
        ok: usize,
        illegal: usize,
        unknown: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        archive: Option<&'a str>,
    },
    /// A check file archived unchecked: the queue was full, or the run ended first.
    Skipped {
        file: String,
        rows: usize,
        /// Only when the file was kept (`--keep-skipped on`, or a violation in it).
        #[serde(skip_serializing_if = "Option::is_none")]
        archive: Option<&'a str>,
    },
    /// The end-of-run read-back (spec §12).
    Readback {
        rows: u64,
        ok: u64,
        lost: u64,
        phantom: u64,
        incomplete: u64,
        indet_total: u64,
        indet_landed: u64,
    },
    /// A canary's result; anything but `illegal` is `verifier-broken`.
    Canary {
        result: &'a str,
    },
    End {
        exit: u8,
    },
}

/// `gen` is above 2^53, so it is a string for JSON readers that use doubles.
fn as_string<S: Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

fn opt_as_string<S: Serializer>(value: &Option<i64>, serializer: S) -> Result<S::Ok, S::Error> {
    match value {
        Some(value) => serializer.collect_str(value),
        None => serializer.serialize_none(),
    }
}

fn cell_name<S: Serializer>(cell: &usize, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&format_args!("c{cell}"))
}

pub fn line(scv: &Scv) -> String {
    format!(
        "SCV {}",
        serde_json::to_string(scv).expect("SCV lines always serialize")
    )
}

pub fn print(scv: &Scv) {
    println!("{}", line(scv));
}

/// One `--report-interval` of the checked stream and the bulk load.
#[derive(Serialize, Debug, Clone, Copy, PartialEq, Default)]
pub struct IntervalStats {
    /// When the interval ended, so SCT can place it next to nemesis events.
    pub wall_ms: u64,
    /// Completed operations of the checked stream: successful reads and writes, and writes
    /// with an unknown outcome.
    pub verified_ops_s: f64,
    /// The rest: failed reads (the sweep's too), `fail` writes and workload errors.
    pub failed_ops_s: f64,
    pub bulk_ops_s: f64,
    pub read_p99_ms: f64,
    pub write_p99_ms: f64,
    pub indet_pct: f64,
    /// Check files waiting for a checker worker.
    pub queue: usize,
    /// How late the slots' burst ticks ran: a busy loader stretches operation times.
    pub sched_delay_p99_ms: f64,
}

/// Latencies of the run so far.
pub struct Totals {
    pub read_p99_ms: f64,
    pub write_p99_ms: f64,
    pub sched_delay_p99_ms: f64,
    pub writes_indet: u64,
}

/// Microseconds, up to an hour.
struct Hist {
    interval: Histogram<u64>,
    total: Histogram<u64>,
}

impl Default for Hist {
    fn default() -> Self {
        let new = || Histogram::new_with_bounds(1, 3_600_000_000, 3).expect("valid bounds");
        Self {
            interval: new(),
            total: new(),
        }
    }
}

impl Hist {
    fn record(&mut self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        self.interval.saturating_record(micros);
        self.total.saturating_record(micros);
    }
}

fn p99_ms(histogram: &Histogram<u64>) -> f64 {
    if histogram.is_empty() {
        return 0.0;
    }
    histogram.value_at_quantile(0.99) as f64 / 1000.0
}

/// Shared by the slots: what happened since the last `--report-interval`, and in total.
/// Latencies count successful operations only.
#[derive(Default)]
pub struct Stats {
    read: Mutex<Hist>,
    write: Mutex<Hist>,
    sched: Mutex<Hist>,
    ops: AtomicU64,
    writes: AtomicU64,
    indet: AtomicU64,
    indet_total: AtomicU64,
    failed: AtomicU64,
}

impl Stats {
    pub fn read(&self, latency: Duration) {
        self.read.lock().record(latency);
        self.ops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn write(&self, latency: Duration) {
        self.write.lock().record(latency);
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn write_indeterminate(&self) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.indet.fetch_add(1, Ordering::Relaxed);
        self.indet_total.fetch_add(1, Ordering::Relaxed);
    }

    /// A failed operation other than an indeterminate write, as `rows.jsonl` counts `errors`.
    pub fn failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn sched_delay(&self, late: Duration) {
        self.sched.lock().record(late);
    }

    /// The interval that just ended, `elapsed` long; the next one starts empty.
    pub fn take_interval(&self, elapsed: Duration) -> IntervalStats {
        let ops = self.ops.swap(0, Ordering::Relaxed);
        let writes = self.writes.swap(0, Ordering::Relaxed);
        let indet = self.indet.swap(0, Ordering::Relaxed);
        let failed = self.failed.swap(0, Ordering::Relaxed);
        let p99_and_reset = |hist: &Mutex<Hist>| {
            let mut hist = hist.lock();
            let p99 = p99_ms(&hist.interval);
            hist.interval.reset();
            p99
        };
        IntervalStats {
            wall_ms: crate::slot::unix_ms(),
            verified_ops_s: ops as f64 / elapsed.as_secs_f64(),
            failed_ops_s: failed as f64 / elapsed.as_secs_f64(),
            bulk_ops_s: 0.0,
            read_p99_ms: p99_and_reset(&self.read),
            write_p99_ms: p99_and_reset(&self.write),
            indet_pct: if writes == 0 {
                0.0
            } else {
                indet as f64 * 100.0 / writes as f64
            },
            queue: 0,
            sched_delay_p99_ms: p99_and_reset(&self.sched),
        }
    }

    pub fn totals(&self) -> Totals {
        Totals {
            read_p99_ms: p99_ms(&self.read.lock().total),
            write_p99_ms: p99_ms(&self.write.lock().total),
            sched_delay_p99_ms: p99_ms(&self.sched.lock().total),
            writes_indet: self.indet_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Serialize, Default, Debug, Clone, Copy)]
pub struct ReadbackTotals {
    pub rows: u64,
    pub ok: u64,
    pub lost: u64,
    pub phantom: u64,
    pub incomplete: u64,
    pub expired: u64,
    pub indet_total: u64,
    pub indet_landed: u64,
}

impl From<crate::readback::Summary> for ReadbackTotals {
    fn from(s: crate::readback::Summary) -> Self {
        Self {
            rows: s.rows,
            ok: s.ok,
            lost: s.lost,
            phantom: s.phantom,
            incomplete: s.incomplete,
            expired: s.expired,
            indet_total: s.indet_total,
            indet_landed: s.indet_landed,
        }
    }
}

/// `report.json`: the run's totals, rewritten every `--report-interval` and at exit.
#[derive(Serialize, Default, Debug, Clone)]
pub struct Report {
    pub rows: u64,
    pub ops: u64,
    pub reads: u64,
    pub writes_ok: u64,
    pub writes_indet: u64,
    pub errors: u64,
    pub violations: u64,
    /// Verdicts of the rows the checker decided, and of the rows it never got to.
    pub rows_ok: u64,
    pub rows_illegal: u64,
    pub rows_unknown: u64,
    pub rows_skipped: u64,
    /// Rows the checker judged illegal, including those whose verdict is `violation` because
    /// an invariant fired too: the sum of `illegal` over the `SCV checked` lines.
    pub checker_illegal: u64,
    /// Canaries the checker rejected, as it must; ones it did not (`verifier-broken`); ones
    /// the queue or the end of the run left unchecked.
    pub canaries_ok: u64,
    pub canaries_failed: u64,
    pub canaries_skipped: u64,
    /// The read-back's totals, once it ran.
    pub readback: Option<ReadbackTotals>,
    pub read_p99_ms: f64,
    pub write_p99_ms: f64,
    pub sched_delay_p99_ms: f64,
    pub bulk_ops: u64,
    /// Bulk reads that found no row: a preload check wants 0.
    pub bulk_misses: u64,
    pub bulk_errors: u64,
    /// Set in the last report, at exit.
    pub exit: Option<u8>,
}

impl Report {
    /// Replaces `report.json` in `dir` whole, so a reader never sees half a file.
    pub fn write(&self, dir: &Path) -> Result<()> {
        let temp = dir.join("report.json.tmp");
        std::fs::write(&temp, serde_json::to_vec_pretty(self)?)
            .with_context(|| format!("Failed to write {}", temp.display()))?;
        std::fs::rename(&temp, dir.join("report.json"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scv_lines_test() {
        let start = Scv::Start {
            mode: "verify",
            pop: Some("seq=0..2047"),
            slots: 32,
            gen_base: Some(1878307305715400704),
            bulk_pop: None,
            connections: Connections {
                pool: "per-shard",
                pool_size: 1,
                shard_aware_port_range: Some("49152..65535".to_owned()),
                tcp_reuse_address: false,
            },
        };
        assert_eq!(
            line(&start),
            r#"SCV {"t":"start","mode":"verify","pop":"seq=0..2047","slots":32,"gen_base":"1878307305715400704","pool":"per-shard","pool_size":1,"shard_aware_port_range":"49152..65535","tcp_reuse_address":false}"#
        );
        let bulk = Scv::Start {
            mode: "bulk",
            pop: None,
            slots: 0,
            gen_base: None,
            bulk_pop: Some("seq=1..10"),
            connections: Connections {
                pool: "per-host",
                pool_size: 8,
                shard_aware_port_range: None,
                tcp_reuse_address: true,
            },
        };
        assert_eq!(
            line(&bulk),
            r#"SCV {"t":"start","mode":"bulk","slots":0,"bulk_pop":"seq=1..10","pool":"per-host","pool_size":8,"shard_aware_port_range":null,"tcp_reuse_address":true}"#
        );
        let violation = Scv::Violation {
            kind: "INV-3",
            pk: 7,
            gen: 1878307305715400705,
            cell: 1,
            wall_ms: 1791293683732,
            archive: "archive/12",
        };
        assert_eq!(
            line(&violation),
            r#"SCV {"t":"violation","kind":"INV-3","pk":7,"gen":"1878307305715400705","cell":"c1","wall_ms":1791293683732,"archive":"archive/12"}"#
        );
        let stats = Scv::Stats(IntervalStats {
            wall_ms: 1791293683732,
            verified_ops_s: 1234.5,
            failed_ops_s: 0.5,
            bulk_ops_s: 0.0,
            read_p99_ms: 1.5,
            write_p99_ms: 2.25,
            indet_pct: 0.0,
            queue: 3,
            sched_delay_p99_ms: 0.125,
        });
        assert_eq!(
            line(&stats),
            r#"SCV {"t":"stats","wall_ms":1791293683732,"verified_ops_s":1234.5,"failed_ops_s":0.5,"bulk_ops_s":0.0,"read_p99_ms":1.5,"write_p99_ms":2.25,"indet_pct":0.0,"queue":3,"sched_delay_p99_ms":0.125}"#
        );
        let checked = Scv::Checked {
            file: "12".to_owned(),
            ok: 49,
            illegal: 1,
            unknown: 0,
            archive: Some("archive/12"),
        };
        assert_eq!(
            line(&checked),
            r#"SCV {"t":"checked","file":"12","ok":49,"illegal":1,"unknown":0,"archive":"archive/12"}"#
        );
        let skipped = Scv::Skipped {
            file: "40".to_owned(),
            rows: 50,
            archive: None,
        };
        assert_eq!(
            line(&skipped),
            r#"SCV {"t":"skipped","file":"40","rows":50}"#
        );
        let kept = Scv::Skipped {
            file: "41".to_owned(),
            rows: 50,
            archive: Some("archive/41"),
        };
        assert_eq!(
            line(&kept),
            r#"SCV {"t":"skipped","file":"41","rows":50,"archive":"archive/41"}"#
        );
        assert_eq!(
            line(&Scv::Canary { result: "illegal" }),
            r#"SCV {"t":"canary","result":"illegal"}"#
        );
        assert_eq!(line(&Scv::End { exit: 1 }), r#"SCV {"t":"end","exit":1}"#);
    }

    #[test]
    fn interval_stats_test() {
        let stats = Stats::default();
        for ms in 1..=100 {
            stats.read(Duration::from_millis(ms));
        }
        stats.write(Duration::from_millis(10));
        stats.write_indeterminate();
        stats.write_indeterminate();
        stats.write_indeterminate();
        stats.failed();
        stats.failed();
        stats.sched_delay(Duration::from_micros(250));

        let before = crate::slot::unix_ms();
        let interval = stats.take_interval(Duration::from_secs(2));
        assert!(
            (before..=crate::slot::unix_ms()).contains(&interval.wall_ms),
            "stamped when taken"
        );
        assert_eq!(
            interval.verified_ops_s, 52.0,
            "(100 reads + 4 writes) / 2 s"
        );
        assert!(
            (99.0..=100.0).contains(&interval.read_p99_ms),
            "{}",
            interval.read_p99_ms
        );
        assert!(
            (9.9..=10.1).contains(&interval.write_p99_ms),
            "{}",
            interval.write_p99_ms
        );
        assert_eq!(interval.indet_pct, 75.0, "3 of 4 writes");
        assert_eq!(
            interval.failed_ops_s, 1.0,
            "2 failed / 2 s, counted apart from the completed operations"
        );
        assert!((0.24..=0.26).contains(&interval.sched_delay_p99_ms));

        // An interval starts empty; the totals keep everything.
        let next = stats.take_interval(Duration::from_secs(1));
        assert_eq!(
            (
                next.verified_ops_s,
                next.failed_ops_s,
                next.read_p99_ms,
                next.indet_pct
            ),
            (0.0, 0.0, 0.0, 0.0)
        );
        let totals = stats.totals();
        assert!((99.0..=100.0).contains(&totals.read_p99_ms));
        assert_eq!(totals.writes_indet, 3);
    }

    #[test]
    fn report_json_is_replaced_whole_test() {
        let dir = std::env::temp_dir().join(format!("sc-verify-report-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let report = Report {
            rows: 3,
            ops: 600,
            violations: 0,
            exit: None,
            ..Report::default()
        };
        report.write(&dir).unwrap();
        let first: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.join("report.json")).unwrap())
                .unwrap();
        assert_eq!(
            (first["rows"].as_u64(), first["exit"].is_null()),
            (Some(3), true)
        );
        Report {
            exit: Some(0),
            ..report
        }
        .write(&dir)
        .unwrap();
        let second: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.join("report.json")).unwrap())
                .unwrap();
        assert_eq!(second["exit"].as_u64(), Some(0));
        assert!(!dir.join("report.json.tmp").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
