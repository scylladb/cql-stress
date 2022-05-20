#[macro_use]
extern crate async_trait;

mod args;
mod distribution;
mod gocompat;
mod operation;
pub(crate) mod stats;
mod workload;

#[cfg(test)]
mod args_test;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::future;
use openssl::ssl::{SslContext, SslContextBuilder, SslFiletype, SslMethod, SslVerifyMode};
use scylla::{transport::Compression, Session, SessionBuilder};

use cql_stress::configuration::{Configuration, OperationFactory};
use cql_stress::run::RunController;
use cql_stress::sharded_stats::{Stats as _, StatsFactory as _};

use crate::args::{Mode, ScyllaBenchArgs, WorkloadType};
use crate::operation::counter_update::CounterUpdateOperationFactory;
use crate::operation::read::{ReadKind, ReadOperationFactory};
use crate::operation::write::WriteOperationFactory;
use crate::stats::{ShardedStats, StatsFactory, StatsPrinter};
use crate::workload::{
    SequentialConfig, SequentialFactory, TimeseriesReadConfig, TimeseriesReadFactory,
    TimeseriesWriteConfig, TimeseriesWriteFactory, UniformConfig, UniformFactory, WorkloadFactory,
};

// TODO: Return exit code
#[tokio::main]
async fn main() -> Result<()> {
    let sb_config = match args::parse_scylla_bench_args(std::env::args(), true) {
        Some(sb_config) => sb_config,
        None => return Ok(()), // TODO: Return some kind of error
    };
    let sb_config = Arc::new(sb_config);

    sb_config.print_configuration();

    let stats_factory = Arc::new(StatsFactory);
    let sharded_stats = Arc::new(ShardedStats::new(Arc::clone(&stats_factory)));

    let run_config = prepare(sb_config, Arc::clone(&sharded_stats))
        .await
        .context("Failed to prepare the benchmark")?;

    let mut combined_stats = stats_factory.create();

    let (ctrl, run_finished) = cql_stress::run::run(run_config);
    let ctrl = Arc::new(ctrl);

    // Don't care about the leaking task, it won't prevent the runtime
    // from being stopped.
    tokio::task::spawn(stop_on_signal(Arc::clone(&ctrl)));

    let printer = StatsPrinter::new(true);
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    futures::pin_mut!(run_finished);

    // Skip the first tick, which is immediate
    ticker.tick().await;

    printer.print_header(&mut std::io::stdout())?;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let partial_stats = sharded_stats.get_combined_and_clear();
                printer.print_partial(&partial_stats, &mut std::io::stdout())?;
                combined_stats.combine(&partial_stats);
            }
            result = &mut run_finished => {
                if result.is_ok() {
                    // Combine stats for the last time
                    let partial_stats = sharded_stats.get_combined_and_clear();
                    combined_stats.combine(&partial_stats);
                    printer.print_final(&combined_stats, &mut std::io::stdout())?;
                }
                return result.context("An error occurred during the benchmark");
            }
        }
    }
}

async fn stop_on_signal(runner: Arc<RunController>) {
    tokio::signal::ctrl_c().await.unwrap();
    runner.ask_to_stop();

    tokio::signal::ctrl_c().await.unwrap();
    runner.abort();
}

async fn prepare(args: Arc<ScyllaBenchArgs>, stats: Arc<ShardedStats>) -> Result<Configuration> {
    let mut builder = SessionBuilder::new().known_nodes(&args.nodes);

    if !args.username.is_empty() && !args.password.is_empty() {
        builder = builder.user(&args.username, &args.password);
    }

    if args.tls_encryption {
        let ssl_ctx = generate_ssl_context(&args)?;
        builder = builder.ssl_context(Some(ssl_ctx));
    }

    if args.client_compression {
        builder = builder.compression(Some(Compression::Snappy));
    }

    builder = builder.load_balancing(Arc::clone(&args.host_selection_policy));

    let session = builder.build().await?;
    let session = Arc::new(session);

    create_schema(&session, &args).await?;
    let workload_factory = create_workload_factory(&args)?;
    let operation_factory =
        create_operation_factory(session, stats, Arc::clone(&args), workload_factory).await?;

    let max_duration = (args.test_duration > Duration::ZERO).then(|| args.test_duration);
    let rate_limit_per_second = (args.maximum_rate > 0).then(|| args.maximum_rate as f64);

    Ok(Configuration {
        max_duration,
        concurrency: args.concurrency,
        rate_limit_per_second,
        operation_factory,
    })
}

fn generate_ssl_context(args: &ScyllaBenchArgs) -> Result<SslContext> {
    let mut context_builder = SslContextBuilder::new(SslMethod::tls_client())?;

    anyhow::ensure!(
        args.client_key_file.is_empty() == args.client_cert_file.is_empty(),
        "tls-client-cert-file and tls-client-key-file either should be both provided or left empty",
    );

    if args.host_verification {
        context_builder.set_verify(SslVerifyMode::PEER);
    } else {
        context_builder.set_verify(SslVerifyMode::NONE);
    }

    if !args.ca_cert_file.is_empty() {
        let ca_cert_path = std::fs::canonicalize(&args.ca_cert_file)?;
        context_builder.set_ca_file(ca_cert_path)?;
    }
    if !args.client_cert_file.is_empty() {
        let client_cert_path = std::fs::canonicalize(&args.client_cert_file)?;
        context_builder.set_certificate_file(client_cert_path, SslFiletype::PEM)?;
    }
    if !args.client_key_file.is_empty() {
        let client_key_file = std::fs::canonicalize(&args.client_key_file)?;
        context_builder.set_private_key_file(client_key_file, SslFiletype::PEM)?;
    }

    // TODO: Set server name (for SNI)
    // I'm afraid it is impossible to do with the current driver.
    // The hostname must be set on the Ssl object which is created
    // by the driver just before creating a connection, and is not available
    // for customization in the configuration.
    //
    // I believe it's this method:
    // https://docs.rs/openssl/latest/openssl/ssl/struct.Ssl.html#method.set_hostname

    // Silence "unused" warnings for now
    let _ = &args.server_name;

    Ok(context_builder.build())
}

async fn create_schema(session: &Session, args: &ScyllaBenchArgs) -> Result<()> {
    let create_keyspace_query_str = format!(
        "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
        {{'class': 'SimpleStrategy', 'replication_factor': {}}}",
        args.keyspace_name, args.replication_factor,
    );
    session.query(create_keyspace_query_str, ()).await?;
    session.use_keyspace(&args.keyspace_name, true).await?;
    session.await_schema_agreement().await?;

    let create_regular_table_query_str = format!(
        "CREATE TABLE IF NOT EXISTS {} \
        (pk bigint, ck bigint, v blob, PRIMARY KEY (pk, ck)) \
        WITH compression = {{ }}",
        args.table_name,
    );
    let q1 = session.query(create_regular_table_query_str, ());

    let create_counter_table_query_str = format!(
        "CREATE TABLE IF NOT EXISTS {} \
        (pk bigint, ck bigint, c1 counter, c2 counter, c3 counter, c4 counter, c5 counter, PRIMARY KEY (pk, ck)) \
        WITH compression = {{ }}",
        args.counter_table_name,
    );
    let q2 = session.query(create_counter_table_query_str, ());

    future::try_join(q1, q2).await?;
    session.await_schema_agreement().await?;

    Ok(())
}

async fn create_operation_factory(
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    args: Arc<ScyllaBenchArgs>,
    workload_factory: Box<dyn WorkloadFactory>,
) -> Result<Arc<dyn OperationFactory>> {
    match &args.mode {
        Mode::Write => {
            let factory =
                WriteOperationFactory::new(session, stats, workload_factory, args).await?;
            Ok(Arc::new(factory))
        }
        Mode::Read => {
            let factory = ReadOperationFactory::new(
                session,
                stats,
                ReadKind::Regular,
                workload_factory,
                args,
            )
            .await?;
            Ok(Arc::new(factory))
        }
        Mode::CounterUpdate => {
            let factory =
                CounterUpdateOperationFactory::new(session, stats, workload_factory, args).await?;
            Ok(Arc::new(factory))
        }
        Mode::CounterRead => {
            let factory = ReadOperationFactory::new(
                session,
                stats,
                ReadKind::Counter,
                workload_factory,
                args,
            )
            .await?;
            Ok(Arc::new(factory))
        }
        mode => {
            // TODO: Implement more later
            Err(anyhow::anyhow!("Mode not implemented: {:?}", mode))
        }
    }
}

fn create_workload_factory(args: &ScyllaBenchArgs) -> Result<Box<dyn WorkloadFactory>> {
    match (&args.workload, &args.mode) {
        (WorkloadType::Sequential, _) => {
            let seq_config = SequentialConfig {
                iterations: args.iterations,
                partition_offset: args.partition_offset,
                pks: args.partition_count,
                cks_per_pk: args.clustering_row_count,
            };
            Ok(Box::new(SequentialFactory::new(seq_config)?))
        }
        (WorkloadType::Uniform, _) => {
            let uni_config = UniformConfig {
                pk_range: 0..args.partition_count,
                ck_range: 0..args.clustering_row_count,
            };
            Ok(Box::new(UniformFactory::new(uni_config)?))
        }
        (WorkloadType::Timeseries, Mode::Write) => {
            let tsw_config = TimeseriesWriteConfig {
                partition_offset: args.partition_offset,
                pks_per_generation: args.partition_count,
                cks_per_pk: args.clustering_row_count,
                start_nanos: args.start_timestamp,
                max_rate: args.maximum_rate,
            };
            Ok(Box::new(TimeseriesWriteFactory::new(tsw_config)?))
        }
        (WorkloadType::Timeseries, Mode::Read) => {
            let period = 1_000_000_000 / args.write_rate;
            let tsr_config = TimeseriesReadConfig {
                partition_offset: args.partition_offset,
                pks_per_generation: args.partition_count,
                cks_per_pk: args.clustering_row_count,
                start_nanos: args.start_timestamp,
                period_nanos: period,
                distribution: args.distribution.clone(),
            };
            Ok(Box::new(TimeseriesReadFactory::new(tsr_config)?))
        }
        (WorkloadType::Timeseries, _) => Err(anyhow::anyhow!(
            "Timeseries workload supports only write and read modes"
        )),
        (workload, mode) => {
            // TODO: Implement more later
            Err(anyhow::anyhow!(
                "Unsupported combination of workload and mode: {:?}, {:?}",
                workload,
                mode,
            ))
        }
    }
}
