#[macro_use]
extern crate async_trait;

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
use operation::{
    CounterReadOperationFactory, CounterWriteOperationFactory, MixedOperationFactory,
    WriteOperationFactory,
};
use scylla::{ExecutionProfile, Session, SessionBuilder};
use stats::{ShardedStats, StatsFactory, StatsPrinter};
use std::{env, sync::Arc, time::Duration};
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

    // Run a background task waiting for a stop-signal (Ctrl+C).
    tokio::task::spawn(stop_on_signal(ctrl));

    let mut printer = StatsPrinter::new();

    // TODO: change the interval based on -log option (when supported).
    let mut ticker = tokio::time::interval(Duration::from_secs(1));

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
            }
            result = &mut run_finished => {
                if result.is_ok() {
                    // Combine stats for the last time
                    let partial_stats = sharded_stats.get_combined_and_clear();
                    combined_stats.combine(&partial_stats);
                    printer.print_summary(&combined_stats);
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

    create_schema(&session, &settings).await?;

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

async fn create_schema(session: &Session, settings: &CassandraStressSettings) -> Result<()> {
    match settings.command {
        Command::User => {
            // 'user' command provided. This unwrap is safe.
            settings
                .command_params
                .user
                .as_ref()
                .unwrap()
                .create_schema(session)
                .await?;
        }
        _ => {
            session
                .query(settings.schema.construct_keyspace_creation_query(), ())
                .await?;
            session
                .use_keyspace(&settings.schema.keyspace, true)
                .await?;
            session
                .query(
                    settings
                        .schema
                        .construct_table_creation_query(&settings.column.columns),
                    (),
                )
                .await
                .context("Failed to create standard table")?;
            session
                .query(
                    settings
                        .schema
                        .construct_counter_table_creation_query(&settings.column.columns),
                    (),
                )
                .await
                .context("Failed to create counter table")?;
        }
    }

    Ok(())
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
        cmd => Err(anyhow::anyhow!(
            "Runtime for command '{}' not implemented yet.",
            cmd.show()
        )),
    }
}
