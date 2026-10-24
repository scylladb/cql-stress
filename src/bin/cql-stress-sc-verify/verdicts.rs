//! What becomes of sealed rows and check files: the run totals, each row's verdict in
//! `rows.jsonl` (spec §4.3), which check files are archived, deleted or skipped (§11.2), and
//! what a canary's answer means (§11.4).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use crate::bulk::BulkStats;
use crate::checker::{CheckQueue, FileVerdicts, Job, RowResult};
use crate::cli::KeepSkipped;
use crate::history::{self, ClosedFile, Recorder};
use crate::report::{self, Report, Scv};
use crate::slot::{Checked, SealedRow};

/// Canaries are queued under sequence numbers from here on, apart from the check files.
const CANARY_SEQ: u64 = 1 << 62;

/// The history writer, the run totals, and, with `--checker on`, what waits for verdicts.
pub struct Verdicts {
    pub recorder: Recorder,
    pub totals: Report,
    /// Closed check files, by sequence number.
    pending: HashMap<u64, ClosedFile>,
    /// Canary files, by their queue sequence number.
    canaries: HashMap<u64, PathBuf>,
    /// Check files closed so far.
    files: u64,
}

impl Verdicts {
    pub fn new(recorder: Recorder) -> Self {
        Self {
            recorder,
            totals: Report::default(),
            pending: HashMap::new(),
            canaries: HashMap::new(),
            files: 0,
        }
    }

    /// Records a sealed row, adds it to the totals, and prints the SCV lines of its violations
    /// once its evidence is archived.
    /// Records a sealed row; returns the check file it closed, for the checker.
    pub fn record(&mut self, checked: &Checked, row: SealedRow) -> Option<ClosedFile> {
        tracing::debug!(
            "sealed pk {} gen {} slot {} wall {}..{} ms: {} ops, {} reads, {} writes ok, \
             {} indeterminate, {} errors, max gap {} ms, stop {}, sweep {}, {} violations",
            row.key.pk,
            row.key.gen,
            row.slot,
            row.wall_start_ms,
            row.wall_end_ms,
            row.ops.len(),
            row.reads,
            row.writes_ok,
            row.writes_indet,
            row.errors,
            row.max_gap_ms,
            row.stop_reason.as_str(),
            if row.sweep_ok { "ok" } else { "incomplete" },
            row.violations.len()
        );
        let totals = &mut self.totals;
        totals.rows += 1;
        totals.ops += row.ops.len() as u64;
        totals.reads += row.reads;
        totals.writes_ok += row.writes_ok;
        totals.errors += row.errors;
        totals.violations += row.violations.len() as u64;
        match self.recorder.record(&row) {
            Ok(recorded) => {
                if let Some(archive) = &recorded.archive {
                    for detected in &row.violations {
                        report::print(&Scv::Violation {
                            kind: &detected.violation.kind.to_string(),
                            pk: row.key.pk,
                            gen: row.key.gen,
                            cell: detected.violation.cell,
                            wall_ms: detected.wall_ms,
                            archive,
                        });
                    }
                }
                recorded.closed
            }
            Err(err) => {
                // Evidence that cannot be written makes every verdict unverifiable.
                eprintln!("error: failed to record a sealed row: {err:#}");
                checked.exit.raise(3);
                checked.stop();
                None
            }
        }
    }

    /// Queues a closed check file; a full queue never waits: the file is skipped. Every
    /// `--canary-every` files, a canary made from this one goes in too.
    pub fn submit(&mut self, checked: &Checked, queue: &CheckQueue, closed: ClosedFile) {
        self.files += 1;
        let canary = (self.files % checked.cli.canary_every == 0)
            .then(|| std::fs::read_to_string(&closed.path).ok())
            .flatten()
            .and_then(|text| history::make_canary(&text));

        let seq = closed.seq;
        let job = Job {
            seq,
            path: closed.path.clone(),
            rows: closed.rows.len(),
            archive_dir: self.recorder.archive_dir(seq),
        };
        match queue.submit(job) {
            Ok(()) => {
                self.pending.insert(closed.seq, closed);
            }
            Err(_) => self.skip(checked, closed),
        }

        if let Some(canary) = canary {
            let path = self.recorder.canary_path(seq);
            if let Err(err) = std::fs::write(&path, canary) {
                tracing::warn!("failed to write canary {}: {err}", path.display());
                return;
            }
            let job = Job {
                seq: CANARY_SEQ + seq,
                path: path.clone(),
                rows: 1,
                archive_dir: self.recorder.archive_dir(history::canary_name(seq)),
            };
            match queue.submit(job) {
                Ok(()) => {
                    self.canaries.insert(CANARY_SEQ + seq, path);
                }
                Err(_) => {
                    self.totals.canaries_skipped += 1;
                    self.discard_canary(seq, &path);
                }
            }
        }
    }

    pub fn verdicts(&mut self, checked: &Checked, seq: u64, verdicts: FileVerdicts) {
        if let Some(path) = self.canaries.remove(&seq) {
            self.canary_verdict(checked, seq - CANARY_SEQ, &path, &verdicts);
        } else if let Some(closed) = self.pending.remove(&seq) {
            self.file_verdicts(checked, closed, &verdicts);
        }
    }

    /// The end of the run: the checker gets until `--checker-deadline` for what is queued;
    /// files left are skipped, canaries left are dropped. report.json stays current meanwhile.
    pub async fn drain(
        &mut self,
        checked: &Checked,
        queue: &mut CheckQueue,
        reports: &mut tokio::time::Interval,
        bulk: Option<&BulkStats>,
    ) {
        queue.close();
        let deadline = tokio::time::sleep(checked.cli.checker_deadline);
        tokio::pin!(deadline);
        while !self.pending.is_empty() || !self.canaries.is_empty() {
            tokio::select! {
                result = queue.next() => {
                    let Some((seq, verdicts)) = result else { break };
                    self.verdicts(checked, seq, verdicts);
                }
                _ = &mut deadline => break,
                _ = reports.tick() => {
                    self.write_report(checked, bulk, None);
                }
            }
        }
        queue.abort().await;
        let mut left: Vec<_> = self.pending.drain().map(|(_, closed)| closed).collect();
        left.sort_by_key(|closed| closed.seq);
        for closed in left {
            self.skip(checked, closed);
        }
        for (seq, path) in std::mem::take(&mut self.canaries) {
            self.totals.canaries_skipped += 1;
            self.discard_canary(seq - CANARY_SEQ, &path);
        }
    }

    /// A canary must come back `illegal`. Anything else means the checker can no longer
    /// reject a known-bad history, so none of its `ok`s can be trusted: `verifier-broken`,
    /// exit 1.
    fn canary_verdict(&mut self, checked: &Checked, n: u64, path: &Path, verdicts: &FileVerdicts) {
        if !verdicts.ran {
            // The checker could not even start: no answer, so no evidence either way.
            self.totals.canaries_skipped += 1;
            self.discard_canary(n, path);
            return;
        }
        let result = match verdicts.per_key.first() {
            Some(RowResult::Illegal) => "illegal",
            Some(RowResult::Ok) => "ok",
            _ => "unknown",
        };
        report::print(&Scv::Canary { result });
        if result == "illegal" {
            self.totals.canaries_ok += 1;
            self.discard_canary(n, path);
            return;
        }
        self.totals.canaries_failed += 1;
        eprintln!(
            "error: verifier-broken: the checker answered {result} to canary {n}, a history no \
             order can explain; it cannot be trusted to reject a real violation"
        );
        checked.exit.raise(1);
        if let Err(err) = self.recorder.archive(history::canary_name(n), path) {
            eprintln!("error: {err:#}");
        }
    }

    /// Deletes a canary that is not evidence, with whatever the checker wrote about it.
    fn discard_canary(&self, n: u64, path: &Path) {
        if let Err(err) = self.recorder.discard(history::canary_name(n), path) {
            tracing::warn!("{err:#}");
        }
    }

    /// Writes the rows' final lines; deletes a file whose rows were all ok, archives any other.
    fn file_verdicts(&mut self, checked: &Checked, closed: ClosedFile, verdicts: &FileVerdicts) {
        if verdicts.killed {
            tracing::warn!(
                "the checker of file {} ran past --checker-timeout and was killed; its \
                 unfinished rows are unknown",
                closed.seq
            );
        }
        let totals = &mut self.totals;
        let (mut ok, mut illegal, mut unknown) = (0, 0, 0);
        let mut all_ok = true;
        for row in &closed.rows {
            let result = verdicts
                .per_key
                .get(row.line.key)
                .copied()
                .unwrap_or(RowResult::Unknown);
            let checker = match result {
                RowResult::Ok => {
                    ok += 1;
                    "ok"
                }
                RowResult::Illegal => {
                    illegal += 1;
                    totals.checker_illegal += 1;
                    "illegal"
                }
                RowResult::Unknown => {
                    unknown += 1;
                    "unknown"
                }
            };
            // An invariant violation outranks whatever the checker says (spec §4.3).
            let verdict = if row.violated { "violation" } else { checker };
            all_ok &= verdict == "ok";
            match verdict {
                "ok" => totals.rows_ok += 1,
                "illegal" => totals.rows_illegal += 1,
                "unknown" => totals.rows_unknown += 1,
                _ => {}
            }
            let mut line = row.line.clone();
            line.verdict = verdict;
            if let Err(err) = self.recorder.write_line(&line) {
                eprintln!("error: {err:#}");
                checked.exit.raise(3);
            }
        }
        // A file with a violation is evidence, whatever the checker says: never deleted.
        let archive = if all_ok {
            if let Err(err) = self.recorder.discard(closed.seq, &closed.path) {
                tracing::warn!("{err:#}");
            }
            None
        } else {
            match self.recorder.archive(closed.seq, &closed.path) {
                Ok(archive) => Some(archive),
                Err(err) => {
                    eprintln!("error: {err:#}");
                    checked.exit.raise(3);
                    None
                }
            }
        };
        report::print(&Scv::Checked {
            file: closed.seq.to_string(),
            ok,
            illegal,
            unknown,
            archive: archive.as_deref(),
        });
    }

    /// Skips a check file unchecked: the queue was full, or the run ended first. Its rows are
    /// `skipped`, or `violation` when an invariant fired. The file is deleted unless
    /// `--keep-skipped on`: hours of a full queue would fill the disk. A file that holds a
    /// violation is archived either way, since `archive/<seq>/` already holds its evidence.
    fn skip(&mut self, checked: &Checked, closed: ClosedFile) {
        for row in &closed.rows {
            let mut line = row.line.clone();
            line.verdict = if row.violated { "violation" } else { "skipped" };
            if !row.violated {
                self.totals.rows_skipped += 1;
            }
            if let Err(err) = self.recorder.write_line(&line) {
                eprintln!("error: {err:#}");
                checked.exit.raise(3);
            }
        }
        let keep = checked.cli.keep_skipped == KeepSkipped::On
            || closed.rows.iter().any(|row| row.violated);
        let kept = if keep {
            self.recorder.archive(closed.seq, &closed.path).map(Some)
        } else {
            self.recorder
                .discard(closed.seq, &closed.path)
                .map(|()| None)
        };
        let archive = kept.unwrap_or_else(|err| {
            eprintln!("error: {err:#}");
            checked.exit.raise(3);
            None
        });
        report::print(&Scv::Skipped {
            file: closed.seq.to_string(),
            rows: closed.rows.len(),
            archive: archive.as_deref(),
        });
    }

    /// Writes report.json from the totals and the current latencies and bulk counters.
    pub fn write_report(&mut self, checked: &Checked, bulk: Option<&BulkStats>, exit: Option<u8>) {
        let totals = &mut self.totals;
        if let Some(bulk) = bulk {
            totals.bulk_ops = bulk.ops.load(Ordering::Relaxed);
            totals.bulk_misses = bulk.misses.load(Ordering::Relaxed);
            totals.bulk_errors = bulk.errors.load(Ordering::Relaxed);
        }
        let latencies = checked.stats.totals();
        totals.read_p99_ms = latencies.read_p99_ms;
        totals.write_p99_ms = latencies.write_p99_ms;
        totals.sched_delay_p99_ms = latencies.sched_delay_p99_ms;
        totals.writes_indet = latencies.writes_indet;
        totals.exit = exit;
        if let Err(err) = totals.write(&checked.cli.history_dir) {
            tracing::warn!("failed to write report.json: {err:#}");
        }
    }
}
