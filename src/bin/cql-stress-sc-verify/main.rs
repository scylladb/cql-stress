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
mod verdicts;

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
use checker::{CheckQueue, CheckerConfig, FileVerdicts};
use cli::{Checker, Cli, Mode, Readback};
use history::Recorder;
use keys::GenMinter;
use profile::Profile;
use readback::Retired;
use report::{IntervalStats, Report, Scv};
use slot::Checked;
use verdicts::Verdicts;

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
    let recorder = Recorder::new(
        &cli.history_dir,
        profile.cells.len(),
        cli.check_rows,
        cli.check_age,
        with_checker,
    )
    .unwrap_or_else(|err| exit_setup_failure(err));
    let mut queue = with_checker.then(|| {
        CheckQueue::start(
            CheckerConfig {
                bin: cli.checker_bin.clone(),
                timeout: cli.checker_timeout,
                mem: cli.checker_mem,
                key_timeout: cli.key_timeout,
                max_viz: cli.max_viz,
            },
            cli.checker_workers,
            cli.queue_max,
        )
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
    let mut verdicts = Verdicts::new(recorder);
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
                if let (Some(closed), Some(queue)) =
                    (verdicts.record(&checked, row), queue.as_ref())
                {
                    verdicts.submit(&checked, queue, closed);
                }
            }
            Some((seq, file_verdicts)) = next_verdicts(&mut queue) => {
                verdicts.verdicts(&checked, seq, file_verdicts);
            }
            _ = reports.tick() => {
                let elapsed = last_report.elapsed();
                let mut interval = checked.stats.take_interval(elapsed);
                if let Some(stats) = &bulk_stats {
                    interval.bulk_ops_s = stats.take_interval() as f64 / elapsed.as_secs_f64();
                }
                interval.queue = queue.as_ref().map_or(0, CheckQueue::waiting);
                last_report = Instant::now();
                report::print(&Scv::Stats(interval));
                verdicts.write_report(&checked, bulk_stats.as_deref(), None);
            }
        }
    }
    // The slots are done. After an early stop (exit 3) bulk must stop too, not run on to
    // --duration; on a normal run its own --duration has already passed.
    drop(bulk_controller);
    match verdicts.recorder.finish() {
        Ok(Some(closed)) => {
            if let Some(queue) = queue.as_ref() {
                verdicts.submit(&checked, queue, closed);
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
    if let Some(queue) = queue.as_mut() {
        verdicts
            .drain(&checked, queue, &mut reports, bulk_stats.as_deref())
            .await;
    }
    // After the checker: every retired row read back once (spec §12.2).
    if checked.cli.readback == Readback::On {
        let read_back = readback::read_back(&checked, retired, &history_dir);
        tokio::pin!(read_back);
        // report.json stays current while the rows are read (spec §14).
        let result = loop {
            tokio::select! {
                result = &mut read_back => break result,
                _ = reports.tick() => {
                    verdicts.write_report(&checked, bulk_stats.as_deref(), None);
                }
            }
        };
        match result {
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
                verdicts.totals.readback = Some(summary.into());
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
    if verdicts.totals.violations > 0 {
        checked.exit.raise(1);
    }
    let exit = checked.exit.get();
    verdicts.write_report(&checked, bulk_stats.as_deref(), Some(exit));
    let totals = &verdicts.totals;
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

    // report.json is rewritten every --report-interval and at exit (spec §14), so a loader
    // that SCT kills still leaves its counters.
    let write_bulk_report = |exit: Option<u8>| -> Report {
        let report = Report {
            bulk_ops: stats.ops.load(Ordering::Relaxed),
            bulk_misses: stats.misses.load(Ordering::Relaxed),
            bulk_errors: stats.errors.load(Ordering::Relaxed),
            exit,
            ..Report::default()
        };
        if let Err(err) = std::fs::create_dir_all(&cli.history_dir)
            .map_err(anyhow::Error::from)
            .and_then(|()| report.write(&cli.history_dir))
        {
            tracing::warn!("failed to write report.json: {err:#}");
        }
        report
    };
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
                write_bulk_report(None);
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
    let report = write_bulk_report(Some(exit));
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

/// The next checked file, or never when the checker is off.
async fn next_verdicts(queue: &mut Option<CheckQueue>) -> Option<(u64, FileVerdicts)> {
    match queue {
        Some(queue) => queue.next().await,
        None => std::future::pending().await,
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
