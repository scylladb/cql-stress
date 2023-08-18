use std::{ops::ControlFlow, sync::Arc};

use cql_stress::{
    configuration::{Operation, OperationContext, OperationFactory},
    make_runnable,
};

use anyhow::{Context, Result};
use scylla::{prepared_statement::PreparedStatement, Session};

use crate::settings::CassandraStressSettings;

use super::row_generator::{RowGenerator, RowGeneratorFactory};

pub struct WriteOperation {
    session: Arc<Session>,
    statement: PreparedStatement,
    workload: RowGenerator,
    max_operations: Option<u64>,
}

pub struct WriteOperationFactory {
    session: Arc<Session>,
    statement: PreparedStatement,
    workload_factory: RowGeneratorFactory,
    max_operations: Option<u64>,
}

impl OperationFactory for WriteOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        Box::new(WriteOperation {
            session: Arc::clone(&self.session),
            statement: self.statement.clone(),
            workload: self.workload_factory.create(),
            max_operations: self.max_operations,
        })
    }
}

impl WriteOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
    ) -> Result<Self> {
        let statement_str =
            "INSERT INTO standard1 (key, \"C0\", \"C1\", \"C2\", \"C3\", \"C4\") VALUES (?, ?, ?, ?, ?, ?)";
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

        Ok(ControlFlow::Continue(()))
    }
}
