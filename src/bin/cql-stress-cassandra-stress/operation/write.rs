use std::{ops::ControlFlow, sync::Arc};

use crate::settings::CassandraStressSettings;
use anyhow::{Context, Result};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlValue;

use super::{
    row_generator::RowGenerator, CassandraStressOperation, CassandraStressOperationFactory,
};

pub struct WriteOperation {
    session: Arc<Session>,
    statement: PreparedStatement,
}

pub struct WriteOperationFactory {
    session: Arc<Session>,
    statement: PreparedStatement,
}

impl CassandraStressOperation for WriteOperation {
    type Factory = WriteOperationFactory;

    async fn execute(&self, row: &[CqlValue]) -> Result<ControlFlow<()>> {
        // execute_unpaged, since it's an INSERT statement.
        let result = self.session.execute_unpaged(&self.statement, &row).await;

        if let Err(err) = result.as_ref() {
            tracing::error!(
                error = %err,
                partition_key = ?row[0],
                "write error",
            );
        }

        result?;

        Ok(ControlFlow::Continue(()))
    }

    fn generate_row(&self, row_generator: &mut RowGenerator) -> Vec<CqlValue> {
        row_generator.generate_row()
    }

    fn operation_tag(&self) -> &'static str {
        "WRITE"
    }
}

impl CassandraStressOperationFactory for WriteOperationFactory {
    type Operation = WriteOperation;

    fn create(&self) -> Self::Operation {
        WriteOperation {
            session: Arc::clone(&self.session),
            statement: self.statement.clone(),
        }
    }
}

impl WriteOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
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
        statement.set_consistency(settings.command_params.common.consistency_level);
        statement.set_serial_consistency(Some(
            settings.command_params.common.serial_consistency_level,
        ));

        Ok(Self { session, statement })
    }
}
