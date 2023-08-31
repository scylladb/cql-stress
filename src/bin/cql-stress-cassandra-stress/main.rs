#[macro_use]
extern crate async_trait;

mod java_generate;
mod operation;
mod settings;

#[macro_use]
extern crate lazy_static;

use crate::{
    operation::{ReadOperationFactory, RowGeneratorFactory},
    settings::{parse_cassandra_stress_args, Command, ThreadsInfo},
};
use anyhow::{Context, Result};
use cql_stress::{
    configuration::{Configuration, OperationFactory},
    run::RunController,
};
use operation::WriteOperationFactory;
use scylla::{ExecutionProfile, Session, SessionBuilder};
use std::{env, sync::Arc};
use tracing_subscriber::EnvFilter;

use settings::{CassandraStressParsingResult, CassandraStressSettings};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("warn")))
        .init();

    // Cassandra-stress CLI is case-insensitive.
    let settings = match parse_cassandra_stress_args(env::args().map(|arg| arg.to_lowercase())) {
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
    let run_config = prepare_run(Arc::clone(&settings))
        .await
        .context("Failed to prepare benchmark")?;

    let (ctrl, run_finished) = cql_stress::run::run(run_config);

    // Run a background task waiting for a stop-signal (Ctrl+C).
    tokio::task::spawn(stop_on_signal(ctrl));

    run_finished.await
}

async fn stop_on_signal(runner: RunController) {
    // Try stopping gracefully upon receiving first signal.
    tokio::signal::ctrl_c().await.unwrap();
    runner.ask_to_stop();

    // Abort after second signal.
    tokio::signal::ctrl_c().await.unwrap();
    runner.abort();
}

async fn prepare_run(settings: Arc<CassandraStressSettings>) -> Result<Configuration> {
    let mut builder = SessionBuilder::new().known_nodes(&settings.node.nodes);

    let default_exec_profile = ExecutionProfile::builder()
        .load_balancing_policy(settings.node.load_balancing_policy())
        .build();
    builder = builder.default_execution_profile_handle(default_exec_profile.into_handle());

    // TODO: Adjust port when `-port` option is supported.
    if let Some(host_filter) = settings.node.host_filter(9042) {
        builder = builder.host_filter(host_filter?)
    }

    let session = builder.build().await?;
    let session = Arc::new(session);

    create_schema(&session, &settings).await?;

    let duration = settings.command_params.basic_params.duration;

    let (concurrency, throttle) = match settings.rate.threads_info {
        ThreadsInfo::Fixed {
            threads, throttle, ..
        } => (threads, throttle.map(|th| th as f64)),
        ThreadsInfo::Auto { .. } => {
            anyhow::bail!("Runtime not implemented for auto-adjusting rate configuration");
        }
    };

    let operation_factory = create_operation_factory(session, settings).await?;

    Ok(Configuration {
        max_duration: duration,
        concurrency,
        rate_limit_per_second: throttle,
        operation_factory,
        max_retries_per_op: 0,
    })
}

async fn create_schema(session: &Session, settings: &CassandraStressSettings) -> Result<()> {
    session
        .query(settings.schema.construct_keyspace_creation_query(), ())
        .await?;
    session
        .use_keyspace(&settings.schema.keyspace, true)
        .await?;
    session
        .query(settings.schema.construct_table_creation_query(), ())
        .await
        .context("Failed to create standard table")?;
    session
        .query(settings.schema.construct_counter_table_creation_query(), ())
        .await
        .context("Failed to create counter table")?;
    Ok(())
}

async fn create_operation_factory(
    session: Arc<Session>,
    settings: Arc<CassandraStressSettings>,
) -> Result<Arc<dyn OperationFactory>> {
    let workload_factory = RowGeneratorFactory::new(Arc::clone(&settings));
    match &settings.command {
        Command::Write => Ok(Arc::new(
            WriteOperationFactory::new(settings, session, workload_factory).await?,
        )),
        Command::Read => Ok(Arc::new(
            ReadOperationFactory::new(settings, session, workload_factory).await?,
        )),
        cmd => Err(anyhow::anyhow!(
            "Runtime for command '{}' not implemented yet.",
            cmd.show()
        )),
    }
}
