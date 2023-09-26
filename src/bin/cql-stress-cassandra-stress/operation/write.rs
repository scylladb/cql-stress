use std::{ops::ControlFlow, sync::Arc};

use cql_stress::{
    configuration::{Operation, OperationContext, OperationFactory},
    make_runnable,
};

use anyhow::{Context, Result};
use scylla::{prepared_statement::PreparedStatement, Session};

use crate::{settings::CassandraStressSettings, stats::ShardedStats};

use super::row_generator::{RowGenerator, RowGeneratorFactory};

pub struct WriteOperation {
    session: Arc<Session>,
    statement: PreparedStatement,
    workload: RowGenerator,
    max_operations: Option<u64>,
    stats: Arc<ShardedStats>,
}

pub struct WriteOperationFactory {
    session: Arc<Session>,
    statement: PreparedStatement,
    workload_factory: RowGeneratorFactory,
    max_operations: Option<u64>,
    stats: Arc<ShardedStats>,
}

impl OperationFactory for WriteOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        Box::new(WriteOperation {
            session: Arc::clone(&self.session),
            statement: self.statement.clone(),
            workload: self.workload_factory.create(),
            max_operations: self.max_operations,
            stats: Arc::clone(&self.stats),
        })
    }
}

impl WriteOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        let mut statement_str = String::from("INSERT INTO standard1 (key");
        for column in settings.column.columns.iter() {
            statement_str += &format!(", \"{}\"", column);
        }
        statement_str += ") VALUES (?";
        for _ in settings.column.columns.iter() {
            statement_str += ", ?";
        }
        statement_str.push(')');

        let mut statement = session
            .prepare(statement_str)
            .await
            .context("Failed to prepare statement")?;

        statement.set_is_idempotent(true);
        statement.set_consistency(settings.command_params.basic_params.consistency_level);
        statement.set_serial_consistency(Some(
            settings
                .command_params
                .basic_params
                .serial_consistency_level,
        ));

        Ok(Self {
            session,
            statement,
            workload_factory,
            max_operations: settings.command_params.basic_params.operation_count,
            stats,
        })
    }
}

make_runnable!(WriteOperation);
impl WriteOperation {
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        if self
            .max_operations
            .is_some_and(|max_ops| ctx.operation_id >= max_ops)
        {
            return Ok(ControlFlow::Break(()));
        }

        let row = self.workload.generate_row();
        let result = self.session.execute(&self.statement, &row).await;

        if let Err(err) = result.as_ref() {
            tracing::error!(
                error = %err,
                partition_key = ?row[0],
                "write error",
            );
        }

        self.stats.get_shard_mut().account_operation(ctx, &result);
        result?;

        Ok(ControlFlow::Continue(()))
    }
}
