//! `cql-stress-sc-verify`: a verified load for strongly consistent tables.
//! Design: tasks/SCYLLADB-4519/spec.md.

#[macro_use]
extern crate async_trait;

mod bulk;
mod checker;
mod cli;
mod history;
mod invariants;
mod keys;
mod ops;
mod profile;
mod readback;
mod report;
mod slot;
mod startup;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use std::sync::atomic::Ordering;

use anyhow::Result;
use cql_stress::configuration::Configuration;
use cql_stress::java_generate::distribution::parse_population;
use scylla::client::session::Session;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use bulk::{BulkFactory, BulkStats};
use checker::{CheckQueue, CheckerConfig, FileVerdicts, Job, RowResult};
use cli::{Checker, Cli, Mode, Readback};
use history::{ClosedFile, Recorder};
use keys::GenMinter;
use profile::Profile;
use readback::Retired;
use report::{IntervalStats, Report, Scv};
use slot::{Checked, SealedRow};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("warn")))
        .init();
    // Release builds abort on panic, which would skip the exit code and the last SCV line: a
    // crashed tool must still say so, with exit 3 (spec §14.5).
    std::panic::set_hook(Box::new(|panic| {
        eprintln!("error: cql-stress-sc-verify crashed: {panic}");
        report::print(&Scv::End { exit: 3 });
        std::process::exit(3);
    }));

    let cli = Cli::parse_checked(std::env::args_os()).unwrap_or_else(|err| {
        // clap prints --help and --version itself and exits 0; anything else is a usage error.
        match err.downcast::<clap::Error>() {
            Ok(clap_err) => clap_err.exit(),
            Err(err) => exit_setup_failure(err),
        }
    });
    let profile = Profile::load(&cli.profile).unwrap_or_else(|err| exit_setup_failure(err));
    // The checked stream needs a working checker; bulk alone never starts one.
    if cli.checker == Checker::On && cli.mode != Mode::Bulk {
        checker::probe(&cli.checker_bin, checker::PROBE_TIMEOUT)
            .await
            .unwrap_or_else(|err| exit_setup_failure(err));
    }
    // Built up front, so a bad certificate path fails before anything connects.
    let tls = cli
        .tls_context()
        .unwrap_or_else(|err| exit_setup_failure(err));
    let session = startup::connect(&cli, tls)
        .await
        .unwrap_or_else(|err| exit_setup_failure(err));
    startup::startup(&session, &profile, &cli)
        .await
        .unwrap_or_else(|err| exit_setup_failure(err));
    match cli.mode {
        Mode::Verify => run_checked(cli, profile, session, false).await,
        Mode::Both => run_checked(cli, profile, session, true).await,
        Mode::Bulk => run_bulk(cli, profile, session).await,
    }
}

/// The checked stream, alone (`--mode verify`) or with bulk load (`--mode both`).
async fn run_checked(cli: Cli, profile: Profile, session: Session, with_bulk: bool) -> Result<()> {
    // Every checked statement, prepared once; this also proves the cluster accepts them.
    let statements = ops::Statements::prepare(&session, &profile, cli.ttl)
        .await
        .unwrap_or_else(|err| exit_setup_failure(err));
    // Bulk gets its own session, so its requests never queue on the checked connections and
    // stretch the measured operation times (spec §8.3).
    let bulk = if with_bulk {
        let tls = cli
            .tls_context()
            .unwrap_or_else(|err| exit_setup_failure(err));
        let bulk_session = startup::connect(&cli, tls)
            .await
            .unwrap_or_else(|err| exit_setup_failure(err));
        let stats = Arc::new(BulkStats::default());
        let factory = BulkFactory::new(Arc::new(bulk_session), &profile, &cli, stats.clone())
            .await
            .unwrap_or_else(|err| exit_setup_failure(err));
        Some((factory, stats))
    } else {
        None
    };

    let pks = parse_population(&cli.pop)
        .unwrap_or_else(|err| exit_setup_failure(err))
        .create();
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
    let gens = GenMinter::new(now_ms).unwrap_or_else(|err| exit_setup_failure(err));
    let gen_base = gens.base();
    let duration = cli
        .duration
        .expect("--mode verify and --mode both require --duration");
    let with_checker = cli.checker == Checker::On;
    let mut recorder = Recorder::new(
        &cli.history_dir,
        profile.cells.len(),
        cli.check_rows,
        cli.check_age,
        with_checker,
    )
    .unwrap_or_else(|err| exit_setup_failure(err));
    let mut checking = with_checker.then(|| Checking {
        queue: CheckQueue::start(
            CheckerConfig {
                bin: cli.checker_bin.clone(),
                timeout: cli.checker_timeout,
                mem: cli.checker_mem,
                key_timeout: cli.key_timeout,
                max_viz: cli.max_viz,
            },
            cli.checker_workers,
            cli.queue_max,
        ),
        pending: HashMap::new(),
        canaries: HashMap::new(),
        files: 0,
    });
    let checked = Arc::new(Checked::new(
        Arc::new(session),
        statements,
        cli,
        profile.cells.len(),
        pks,
        gens,
    ));

    // Start-up check 4 (spec §15.3): every slot's first row is freshly minted and absent.
    let mut first_rows = Vec::new();
    for _ in 0..checked.cli.slots {
        first_rows.push(checked.mint().await);
    }
    let absent = futures::future::join_all(
        first_rows
            .iter()
            .map(|key| slot::fresh_row_is_absent(&checked, key)),
    )
    .await;
    if absent.contains(&false) {
        exit_setup_failure(anyhow::anyhow!(
            "a freshly minted row is not absent; see above"
        ));
    }

    println!(
        "Start-up checks passed: {}.{} is {} and matches the profile",
        profile.keyspace,
        profile.table,
        consistency_kind(&checked.cli)
    );
    let cli = &checked.cli;
    println!(
        "Checked stream: {} slots of {} clients, pk {}, gen base {gen_base}",
        cli.slots, cli.clients_per_row, cli.pop
    );
    report::print(&Scv::Start {
        mode: if bulk.is_some() { "both" } else { "verify" },
        pop: Some(&cli.pop),
        slots: cli.slots,
        gen_base: Some(gen_base),
        bulk_pop: bulk.is_some().then_some(cli.bulk_pop.as_str()),
    });

    let (sealed_tx, mut sealed) = mpsc::unbounded_channel();
    let slots: Vec<_> = first_rows
        .into_iter()
        .enumerate()
        .map(|(slot, first)| {
            tokio::spawn(slot::run_slot(
                checked.clone(),
                slot,
                first,
                sealed_tx.clone(),
            ))
        })
        .collect();
    drop(sealed_tx);
    let stopper = {
        let checked = checked.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            checked.stop();
        })
    };
    // Dropping the controller asks bulk to stop, so it must live as long as the run.
    let (bulk_controller, bulk_run, bulk_stats) = match bulk {
        Some((factory, stats)) => {
            let (controller, run) =
                cql_stress::run::run(bulk_configuration(&checked.cli, factory, Some(duration)));
            (Some(controller), Some(tokio::spawn(run)), Some(stats))
        }
        None => (None, None, None),
    };

    let history_dir = checked.cli.history_dir.clone();
    let mut totals = Report::default();
    let mut retired: Vec<Retired> = Vec::new();
    let mut reports = tokio::time::interval(checked.cli.report_interval);
    reports.tick().await; // the first tick is immediate
    let mut last_report = Instant::now();
    loop {
        tokio::select! {
            row = sealed.recv() => {
                let Some(row) = row else { break };
                if checked.cli.readback == Readback::On {
                    retired.push(Retired {
                        key: row.key,
                        expected: row.expected.clone(),
                        start_ms: row.wall_start_ms,
                    });
                }
                if let (Some(closed), Some(checking)) =
                    (record(&checked, &mut recorder, &mut totals, row), checking.as_mut())
                {
                    checking.submit(&checked, &mut recorder, &mut totals, closed);
                }
            }
            Some((seq, verdicts)) = next_verdicts(&mut checking) => {
                if let Some(checking) = checking.as_mut() {
                    checking.verdicts(&checked, &mut recorder, &mut totals, seq, verdicts);
                }
            }
            _ = reports.tick() => {
                let elapsed = last_report.elapsed();
                let mut interval = checked.stats.take_interval(elapsed);
                if let Some(stats) = &bulk_stats {
                    interval.bulk_ops_s = stats.take_interval() as f64 / elapsed.as_secs_f64();
                }
                interval.queue = checking.as_ref().map_or(0, |checking| checking.queue.waiting());
                last_report = Instant::now();
                report::print(&Scv::Stats(interval));
                write_report(&checked, bulk_stats.as_deref(), &mut totals, &history_dir, None);
            }
        }
    }
    // The slots are done. After an early stop (exit 3) bulk must stop too, not run on to
    // --duration; on a normal run its own --duration has already passed.
    drop(bulk_controller);
    match recorder.finish() {
        Ok(Some(closed)) => {
            if let Some(checking) = checking.as_mut() {
                checking.submit(&checked, &mut recorder, &mut totals, closed);
            }
        }
        Ok(None) => {}
        Err(err) => {
            eprintln!("error: failed to close the history files: {err:#}");
            checked.exit.raise(3);
        }
    }
    for slot in slots {
        // A panic exits 3 through the hook; this is only a second line of defence.
        if slot.await.is_err() {
            checked.exit.raise(3);
        }
    }
    stopper.abort();
    if let Some(bulk_run) = bulk_run {
        if !matches!(bulk_run.await, Ok(Ok(()))) {
            eprintln!("error: the bulk load failed");
            checked.exit.raise(3);
        }
    }
    if let Some(checking) = checking.as_mut() {
        checking.drain(&checked, &mut recorder, &mut totals).await;
    }
    // After the checker: every retired row read back once (spec §12.2).
    if checked.cli.readback == Readback::On {
        match readback::read_back(&checked, retired, &history_dir).await {
            Ok(summary) => {
                report::print(&Scv::Readback {
                    rows: summary.rows,
                    ok: summary.ok,
                    lost: summary.lost,
                    phantom: summary.phantom,
                    incomplete: summary.incomplete,
                    indet_total: summary.indet_total,
                    indet_landed: summary.indet_landed,
                });
                if summary.expired > 0 {
                    println!(
                        "Read-back: {} rows older than --ttl were not read",
                        summary.expired
                    );
                }
                totals.readback = Some(summary.into());
                if summary.lost + summary.phantom > 0 {
                    eprintln!(
                        "error: the read-back found {} rows lost and {} with phantom values; \
                         see readback.jsonl",
                        summary.lost, summary.phantom
                    );
                    checked.exit.raise(1);
                }
            }
            Err(err) => {
                eprintln!("error: the read-back failed: {err:#}");
                checked.exit.raise(3);
            }
        }
    }
    if totals.violations > 0 {
        checked.exit.raise(1);
    }
    let exit = checked.exit.get();
    write_report(
        &checked,
        bulk_stats.as_deref(),
        &mut totals,
        &history_dir,
        Some(exit),
    );
    println!(
        "Sealed {} rows, {} recorded operations, {} violations",
        totals.rows, totals.ops, totals.violations
    );
    if bulk_stats.is_some() {
        println!(
            "Bulk: {} operations, {} reads found no row, {} failed attempts",
            totals.bulk_ops, totals.bulk_misses, totals.bulk_errors
        );
    }
    report::print(&Scv::End { exit });
    std::process::exit(exit.into())
}

/// Bulk load alone (`--mode bulk`): the preload, its check, or a dedicated load loader.
async fn run_bulk(cli: Cli, profile: Profile, session: Session) -> Result<()> {
    let stats = Arc::new(BulkStats::default());
    let factory = BulkFactory::new(Arc::new(session), &profile, &cli, stats.clone())
        .await
        .unwrap_or_else(|err| exit_setup_failure(err));
    println!(
        "Start-up checks passed: {}.{} is {} and matches the profile",
        profile.keyspace,
        profile.bulk_table_name(),
        consistency_kind(&cli)
    );
    report::print(&Scv::Start {
        mode: "bulk",
        pop: None,
        slots: 0,
        gen_base: None,
        bulk_pop: Some(&cli.bulk_pop),
    });

    let (_controller, run) = cql_stress::run::run(bulk_configuration(&cli, factory, cli.duration));
    tokio::pin!(run);
    let mut reports = tokio::time::interval(cli.report_interval);
    reports.tick().await; // the first tick is immediate
    let mut last_report = Instant::now();
    let result = loop {
        tokio::select! {
            result = &mut run => break result,
            _ = reports.tick() => {
                let ops = stats.take_interval();
                report::print(&Scv::Stats(IntervalStats {
                    wall_ms: slot::unix_ms(),
                    bulk_ops_s: ops as f64 / last_report.elapsed().as_secs_f64(),
                    ..IntervalStats::default()
                }));
                last_report = Instant::now();
            }
        }
    };
    let exit = match result {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("error: the bulk run failed: {err:#}");
            3
        }
    };
    let report = Report {
        bulk_ops: stats.ops.load(Ordering::Relaxed),
        bulk_misses: stats.misses.load(Ordering::Relaxed),
        bulk_errors: stats.errors.load(Ordering::Relaxed),
        exit: Some(exit),
        ..Report::default()
    };
    if let Err(err) = std::fs::create_dir_all(&cli.history_dir)
        .map_err(anyhow::Error::from)
        .and_then(|()| report.write(&cli.history_dir))
    {
        tracing::warn!("failed to write report.json: {err:#}");
    }
    println!(
        "Bulk: {} operations, {} reads found no row, {} failed attempts",
        report.bulk_ops, report.bulk_misses, report.bulk_errors
    );
    report::print(&Scv::End { exit });
    std::process::exit(exit.into())
}

fn bulk_configuration(
    cli: &Cli,
    factory: BulkFactory,
    max_duration: Option<std::time::Duration>,
) -> Configuration {
    Configuration {
        max_duration,
        concurrency: cli.bulk_threads,
        rate_limit_per_second: (cli.bulk_rate > 0).then_some(cli.bulk_rate as f64),
        operation_factory: Arc::new(factory),
        max_retries_per_op: cli.bulk_retries,
        // Bulk is load: an operation that keeps failing is counted and skipped.
        ignore_errors: true,
    }
}

/// Records a sealed row, adds it to the totals, and prints the SCV lines of its violations
/// once its evidence is archived.
/// Records a sealed row; returns the check file it closed, for the checker.
fn record(
    checked: &Checked,
    recorder: &mut Recorder,
    totals: &mut Report,
    row: SealedRow,
) -> Option<ClosedFile> {
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
    totals.rows += 1;
    totals.ops += row.ops.len() as u64;
    totals.reads += row.reads;
    totals.writes_ok += row.writes_ok;
    totals.errors += row.errors;
    totals.violations += row.violations.len() as u64;
    match recorder.record(&row) {
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

/// The next checked file, or never when the checker is off.
async fn next_verdicts(checking: &mut Option<Checking>) -> Option<(u64, FileVerdicts)> {
    match checking {
        Some(checking) => checking.queue.next().await,
        None => std::future::pending().await,
    }
}

/// Canaries are queued under sequence numbers from here on, apart from the check files.
const CANARY_SEQ: u64 = 1 << 62;

/// The checker side of a run (`--checker on`): the queue, and what waits for verdicts.
struct Checking {
    queue: CheckQueue,
    /// Closed check files, by sequence number.
    pending: HashMap<u64, ClosedFile>,
    /// Canary files, by their queue sequence number.
    canaries: HashMap<u64, PathBuf>,
    /// Check files closed so far.
    files: u64,
}

impl Checking {
    /// Queues a closed check file; a full queue never waits: the file is skipped. Every
    /// `--canary-every` files, a canary made from this one goes in too.
    fn submit(
        &mut self,
        checked: &Checked,
        recorder: &mut Recorder,
        totals: &mut Report,
        closed: ClosedFile,
    ) {
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
            archive_dir: checked
                .cli
                .history_dir
                .join(format!("archive/{}", closed.seq)),
        };
        match self.queue.submit(job) {
            Ok(()) => {
                self.pending.insert(closed.seq, closed);
            }
            Err(_) => skip(checked, recorder, totals, closed),
        }

        // A canary is named after the file it was made from, so its name is unique across
        // restarts too.
        if let Some(canary) = canary {
            let n = seq;
            let path = checked
                .cli
                .history_dir
                .join(format!("sealed/canary-{n}.jsonl"));
            if let Err(err) = std::fs::write(&path, canary) {
                tracing::warn!("failed to write canary {}: {err}", path.display());
                return;
            }
            let job = Job {
                seq: CANARY_SEQ + n,
                path: path.clone(),
                rows: 1,
                archive_dir: checked.cli.history_dir.join(format!("archive/canary-{n}")),
            };
            match self.queue.submit(job) {
                Ok(()) => {
                    self.canaries.insert(CANARY_SEQ + n, path);
                }
                Err(_) => {
                    totals.canaries_skipped += 1;
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }

    fn verdicts(
        &mut self,
        checked: &Checked,
        recorder: &mut Recorder,
        totals: &mut Report,
        seq: u64,
        verdicts: FileVerdicts,
    ) {
        if let Some(path) = self.canaries.remove(&seq) {
            canary_verdict(checked, totals, seq - CANARY_SEQ, &path, &verdicts);
        } else if let Some(closed) = self.pending.remove(&seq) {
            file_verdicts(checked, recorder, totals, closed, &verdicts);
        }
    }

    /// The end of the run: the checker gets until `--checker-deadline` for what is queued;
    /// files left are skipped, canaries left are dropped.
    async fn drain(&mut self, checked: &Checked, recorder: &mut Recorder, totals: &mut Report) {
        self.queue.close();
        let deadline = tokio::time::sleep(checked.cli.checker_deadline);
        tokio::pin!(deadline);
        while !self.pending.is_empty() || !self.canaries.is_empty() {
            tokio::select! {
                result = self.queue.next() => {
                    let Some((seq, verdicts)) = result else { break };
                    self.verdicts(checked, recorder, totals, seq, verdicts);
                }
                _ = &mut deadline => break,
            }
        }
        self.queue.abort().await;
        let mut left: Vec<_> = self.pending.drain().map(|(_, closed)| closed).collect();
        left.sort_by_key(|closed| closed.seq);
        for closed in left {
            skip(checked, recorder, totals, closed);
        }
        for (_, path) in self.canaries.drain() {
            totals.canaries_skipped += 1;
            let _ = std::fs::remove_file(path);
        }
    }
}

/// A canary must come back `illegal`. Anything else means the checker can no longer reject
/// a known-bad history, so none of its `ok`s can be trusted: `verifier-broken`, exit 1.
fn canary_verdict(
    checked: &Checked,
    totals: &mut Report,
    n: u64,
    path: &Path,
    verdicts: &FileVerdicts,
) {
    let archive = checked.cli.history_dir.join(format!("archive/canary-{n}"));
    if !verdicts.ran {
        // The checker could not even start: no answer, so no evidence either way.
        totals.canaries_skipped += 1;
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(&archive);
        return;
    }
    let result = match verdicts.per_key.first() {
        Some(RowResult::Illegal) => "illegal",
        Some(RowResult::Ok) => "ok",
        _ => "unknown",
    };
    report::print(&Scv::Canary { result });
    if result == "illegal" {
        totals.canaries_ok += 1;
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(&archive);
        return;
    }
    totals.canaries_failed += 1;
    eprintln!(
        "error: verifier-broken: the checker answered {result} to canary {n}, a history no \
         order can explain; it cannot be trusted to reject a real violation"
    );
    checked.exit.raise(1);
    let _ = std::fs::create_dir_all(&archive);
    let _ = std::fs::rename(path, archive.join(format!("canary-{n}.jsonl")));
}

/// Writes the rows' final lines; deletes a file whose rows were all ok, archives any other.
fn file_verdicts(
    checked: &Checked,
    recorder: &mut Recorder,
    totals: &mut Report,
    closed: ClosedFile,
    verdicts: &FileVerdicts,
) {
    if verdicts.killed {
        tracing::warn!(
            "the checker of file {} ran past --checker-timeout and was killed; its unfinished \
             rows are unknown",
            closed.seq
        );
    }
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
        if let Err(err) = recorder.write_line(&line) {
            eprintln!("error: {err:#}");
            checked.exit.raise(3);
        }
    }
    // A file with a violation is evidence, whatever the checker says: never deleted.
    let archive = if all_ok {
        if let Err(err) = recorder.discard(closed.seq, &closed.path) {
            tracing::warn!("{err:#}");
        }
        None
    } else {
        match recorder.archive(closed.seq, &closed.path) {
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

/// Archives a check file unchecked: the queue was full, or the run ended first. Its rows
/// are `skipped`, or `violation` when an invariant fired.
fn skip(checked: &Checked, recorder: &mut Recorder, totals: &mut Report, closed: ClosedFile) {
    for row in &closed.rows {
        let mut line = row.line.clone();
        line.verdict = if row.violated { "violation" } else { "skipped" };
        if !row.violated {
            totals.rows_skipped += 1;
        }
        if let Err(err) = recorder.write_line(&line) {
            eprintln!("error: {err:#}");
            checked.exit.raise(3);
        }
    }
    if let Err(err) = recorder.archive(closed.seq, &closed.path) {
        eprintln!("error: {err:#}");
        checked.exit.raise(3);
    }
    report::print(&Scv::Skipped {
        file: closed.seq.to_string(),
        rows: closed.rows.len(),
    });
}

fn write_report(
    checked: &Checked,
    bulk: Option<&BulkStats>,
    totals: &mut Report,
    dir: &std::path::Path,
    exit: Option<u8>,
) {
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
    if let Err(err) = totals.write(dir) {
        tracing::warn!("failed to write report.json: {err:#}");
    }
}

fn consistency_kind(cli: &Cli) -> &'static str {
    if cli.unsafe_eventual {
        "eventually consistent (--unsafe-eventual, a test of the verifier only)"
    } else {
        "strongly consistent"
    }
}

/// Exit code 2: the run cannot start as configured (spec §14.5).
fn exit_setup_failure(err: anyhow::Error) -> ! {
    eprintln!("error: {err:#}");
    std::process::exit(2)
}
