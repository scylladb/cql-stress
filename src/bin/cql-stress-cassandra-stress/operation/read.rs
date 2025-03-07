use std::{marker::PhantomData, ops::ControlFlow, sync::Arc};

use crate::settings::CassandraStressSettings;
use anyhow::{Context, Result};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlValue;

use super::{
    row_generator::RowGenerator, CassandraStressOperation, CassandraStressOperationFactory,
    EqualRowValidator, ExistsRowValidator, RowValidator,
};

pub struct ReadOperation<V: RowValidator> {
    session: Arc<Session>,
    statement: PreparedStatement,
    row_validator: V,
}

pub struct GenericReadOperationFactory<V: RowValidator> {
    session: Arc<Session>,
    statement: PreparedStatement,
    _phantom: PhantomData<V>,
}

pub type RegularReadOperation = ReadOperation<EqualRowValidator>;
pub type RegularReadOperationFactory = GenericReadOperationFactory<EqualRowValidator>;

pub type CounterReadOperation = ReadOperation<ExistsRowValidator>;
pub type CounterReadOperationFactory = GenericReadOperationFactory<ExistsRowValidator>;

impl<V: RowValidator> ReadOperation<V> {
    async fn do_execute(&self, row: &[CqlValue]) -> Result<ControlFlow<()>> {
        let pk = &row[0];

        // The tool works in a way, that it generates one row per partition.
        // We make use of `execute_unpaged` here, since we filter the rows
        // with `WHERE PK = ?`. It means, that the result will have AT MOST 1 row.
        let result = self.session.execute_unpaged(&self.statement, (pk,)).await;
        if let Err(err) = result.as_ref() {
            tracing::error!(
                error = %err,
                partition_key = ?pk,
                "read error",
            );
        }

        let validation_result = self.row_validator.validate_row(row, result?);
        if let Err(err) = validation_result.as_ref() {
            tracing::error!(
                error = %err,
                partition_key = ?pk,
                "read validation error",
            );
        }
        validation_result
            .with_context(|| format!("Row with partition_key: {:?} could not be validated.", pk))?;

        Ok(ControlFlow::Continue(()))
    }
}

impl<V: RowValidator> CassandraStressOperation for ReadOperation<V> {
    type Factory = GenericReadOperationFactory<V>;

    async fn execute(&self, row: &[CqlValue]) -> Result<ControlFlow<()>> {
        self.do_execute(row).await
    }

    fn generate_row(&self, row_generator: &mut RowGenerator) -> Vec<CqlValue> {
        row_generator.generate_row()
    }
}

impl<V: RowValidator> CassandraStressOperationFactory for GenericReadOperationFactory<V> {
    type Operation = ReadOperation<V>;

    fn create(&self) -> Self::Operation {
        ReadOperation {
            session: Arc::clone(&self.session),
            statement: self.statement.clone(),
            row_validator: Default::default(),
        }
    }
}

impl<V: RowValidator> GenericReadOperationFactory<V> {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        stressed_table_name: &'static str,
    ) -> Result<Self> {
        let statement_str = format!("SELECT * FROM {} WHERE KEY=?", stressed_table_name);
        let mut statement = session
            .prepare(statement_str)
            .await
            .context("Failed to prepare statement")?;

        statement.set_is_idempotent(true);
        statement.set_consistency(settings.command_params.common.consistency_level);
        statement.set_serial_consistency(Some(
            settings.command_params.common.serial_consistency_level,
        ));

        Ok(Self {
            session,
            statement,
            _phantom: PhantomData,
        })
    }
}
