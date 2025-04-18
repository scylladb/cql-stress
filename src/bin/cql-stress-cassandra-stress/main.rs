#[macro_use]
extern crate async_trait;

mod hdr_logger;
mod java_generate;
mod operation;
mod settings;
mod stats;

#[macro_use]
extern crate lazy_static;

use crate::{
    operation::{RegularReadOperationFactory, RowGeneratorFactory},
    settings::{parse_cassandra_stress_args, Command, ThreadsInfo},
};
use anyhow::{Context, Result};
use cql_stress::{
    configuration::{Configuration, OperationFactory},
    run::RunController,
    sharded_stats::Stats as _,
    sharded_stats::StatsFactory as _,
};
use hdr_logger::HdrLogWriter;

#[cfg(feature = "user-profile")]
use operation::UserOperationFactory;
use operation::{
    CounterReadOperationFactory, CounterWriteOperationFactory, MixedOperationFactory,
    WriteOperationFactory,
};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use stats::{ShardedStats, StatsFactory, StatsPrinter};

use std::{env, sync::Arc};
use tracing_subscriber::EnvFilter;

use settings::{CassandraStressParsingResult, CassandraStressSettings};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("warn")))
        .init();

    let settings = match parse_cassandra_stress_args(env::args()) {
        // Special commands: help, print, version
        Ok(CassandraStressParsingResult::SpecialCommand) => return Ok(()),
        Ok(CassandraStressParsingResult::Workload(payload)) => Arc::new(*payload),
        Err(e) => {
            // For some reason cassandra-stress writes all parsing-related
            // error messages to stdout. We will follow the same approach.
            println!("\n{:?}", e);
            return Err(anyhow::anyhow!("Failed to parse CLI arguments."));
        }
    };

    settings.print_settings();

    let stats_factory = Arc::new(StatsFactory::new(&settings));
    let sharded_stats = Arc::new(ShardedStats::new(Arc::clone(&stats_factory)));

    let run_config = prepare_run(Arc::clone(&settings), Arc::clone(&sharded_stats))
        .await
        .context("Failed to prepare benchmark")?;

    let mut combined_stats = stats_factory.create();

    let (ctrl, run_finished) = cql_stress::run::run(run_config);

    // HdrLogWriter is a referential struct. We need to create hdr_file and serializer
    // early so they live long enough to be passed to HdrLogWriter.
    let mut maybe_hdr_file_and_serializer = settings
        .log
        .hdr_file
        .as_ref()
        .map(|hdr_file| -> Result<_> {
            let hdr_log_file = std::fs::File::create(hdr_file).with_context(|| {
                format!("Failed to create HDR log file: {}", hdr_file.display())
            })?;
            let serializer = hdrhistogram::serialization::V2DeflateSerializer::new();
            Ok((hdr_log_file, serializer))
        })
        .transpose()?;
    let mut hdr_log_writer = maybe_hdr_file_and_serializer
        .as_mut()
        .map(|(file, serializer)| {
            HdrLogWriter::new(file, serializer).context("Failed to create HDR log writer")
        })
        .transpose()?;

    // Run a background task waiting for a stop-signal (Ctrl+C).
    tokio::task::spawn(stop_on_signal(ctrl));

    let mut printer = StatsPrinter::new();

    let mut ticker = tokio::time::interval(settings.log.interval);

    // Pin the future so it can be polled in tokio::select.
    tokio::pin!(run_finished);

    // Skip the immediate tick.
    ticker.tick().await;

    printer.print_header();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let partial_stats = sharded_stats.get_combined_and_clear();
                combined_stats.combine(&partial_stats);
                printer.print_partial(&partial_stats);

                // Write histogram data to HDR log file if enabled
                if let Some(ref mut writer) = hdr_log_writer {
                    let _ = writer.write_to_hdr_log(&partial_stats);
                }
            }
            result = &mut run_finished => {
                if result.is_ok() {
                    // Combine stats for the last time
                    let partial_stats = sharded_stats.get_combined_and_clear();
                    combined_stats.combine(&partial_stats);
                    printer.print_summary(&combined_stats);

                    // Final write to HDR log file before exiting
                    if let Some(ref mut writer) = hdr_log_writer {
                        let _ = writer.write_to_hdr_log(&partial_stats);
                    }
                }
                return result.context("An error occurred during the benchmark");
            }
        }
    }
}

async fn stop_on_signal(runner: RunController) {
    // Try stopping gracefully upon receiving first signal.
    tokio::signal::ctrl_c().await.unwrap();
    runner.ask_to_stop();

    // Abort after second signal.
    tokio::signal::ctrl_c().await.unwrap();
    runner.abort();
}

async fn prepare_run(
    settings: Arc<CassandraStressSettings>,
    stats: Arc<ShardedStats>,
) -> Result<Configuration> {
    let mut builder = SessionBuilder::new()
        .known_nodes(&settings.node.nodes)
        .compression(settings.mode.compression);

    if let Some(creds) = &settings.mode.user_credentials {
        builder = builder.user(&creds.username, &creds.password);
    }

    if settings.transport.truststore.is_some() || settings.transport.keystore.is_some() {
        let ssl_ctx = settings.transport.generate_ssl_context()?;
        builder = builder.tls_context(Some(ssl_ctx));
    }

    let default_exec_profile = ExecutionProfile::builder()
        .load_balancing_policy(settings.node.load_balancing_policy())
        .build();
    builder = builder.default_execution_profile_handle(default_exec_profile.into_handle());

    // TODO: Adjust port when `-port` option is supported.
    if let Some(host_filter) = settings.node.host_filter(9042) {
        builder = builder.host_filter(host_filter?)
    }

    builder = builder.pool_size(settings.mode.pool_size);

    let session = builder.build().await?;
    let session = Arc::new(session);

    settings
        .create_schema(&session)
        .await
        .context("Failed to create schema")?;

    let duration = settings.command_params.common.duration;

    let (concurrency, throttle) = match settings.rate.threads_info {
        ThreadsInfo::Fixed {
            threads, throttle, ..
        } => (threads, throttle.map(|th| th as f64)),
        ThreadsInfo::Auto { .. } => {
            anyhow::bail!("Runtime not implemented for auto-adjusting rate configuration");
        }
    };

    let operation_factory = create_operation_factory(session, settings, stats).await?;

    Ok(Configuration {
        max_duration: duration,
        concurrency,
        rate_limit_per_second: throttle,
        operation_factory,
        // TODO: adjust when -errors option is supported
        max_retries_per_op: 9,
    })
}

async fn create_operation_factory(
    session: Arc<Session>,
    settings: Arc<CassandraStressSettings>,
    stats: Arc<ShardedStats>,
) -> Result<Arc<dyn OperationFactory>> {
    let workload_factory = RowGeneratorFactory::new(Arc::clone(&settings));
    match &settings.command {
        Command::Write => Ok(Arc::new(
            WriteOperationFactory::new(settings, session, workload_factory, stats).await?,
        )),
        Command::Read => Ok(Arc::new(
            RegularReadOperationFactory::new(settings, session, workload_factory, stats).await?,
        )),
        Command::CounterWrite => Ok(Arc::new(
            CounterWriteOperationFactory::new(settings, session, workload_factory, stats).await?,
        )),
        Command::CounterRead => Ok(Arc::new(
            CounterReadOperationFactory::new(settings, session, workload_factory, stats).await?,
        )),
        Command::Mixed => Ok(Arc::new(
            MixedOperationFactory::new(settings, session, workload_factory, stats).await?,
        )),
        #[cfg(feature = "user-profile")]
        Command::User => Ok(Arc::new(
            UserOperationFactory::new(settings, session, stats).await?,
        )),
        cmd => Err(anyhow::anyhow!(
            "Runtime for command '{}' not implemented yet.",
            cmd.show()
        )),
    }
}
