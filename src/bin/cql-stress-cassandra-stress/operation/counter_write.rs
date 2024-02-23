use anyhow::{Context, Result};

use std::{ops::ControlFlow, sync::Arc};

use scylla::frame::response::result::CqlValue;
use scylla::frame::value::Counter;
use scylla::{prepared_statement::PreparedStatement, Session};

use crate::{java_generate::distribution::Distribution, settings::CassandraStressSettings};

use super::{
    row_generator::RowGenerator, CassandraStressOperation, CassandraStressOperationFactory,
};

pub struct CounterWriteOperation {
    session: Arc<Session>,
    statement: PreparedStatement,
    non_pk_columns_count: usize,
    add_distribution: Box<dyn Distribution>,
}

pub struct CounterWriteOperationFactory {
    session: Arc<Session>,
    statement: PreparedStatement,
    settings: Arc<CassandraStressSettings>,
}

impl CassandraStressOperation for CounterWriteOperation {
    type Factory = CounterWriteOperationFactory;

    async fn execute(&self, row: &[CqlValue]) -> Result<ControlFlow<()>> {
        let result = self.session.execute(&self.statement, row).await;

        if let Err(err) = result.as_ref() {
            tracing::error!(
                error = %err,
                partition_key = ?row.last().unwrap(),
                "counter write error",
            );
        }

        result?;
        Ok(ControlFlow::Continue(()))
    }

    fn generate_row(&self, row_generator: &mut RowGenerator) -> Vec<CqlValue> {
        let mut values: Vec<CqlValue> = Vec::with_capacity(self.non_pk_columns_count + 1);

        for _ in 0..self.non_pk_columns_count {
            values.push(CqlValue::Counter(Counter(self.add_distribution.next_i64())))
        }
        let pk = row_generator.generate_pk();
        values.push(pk);
        values
    }
}

impl CassandraStressOperationFactory for CounterWriteOperationFactory {
    type Operation = CounterWriteOperation;

    fn create(&self) -> Self::Operation {
        CounterWriteOperation {
            session: Arc::clone(&self.session),
            statement: self.statement.clone(),
            non_pk_columns_count: self.settings.column.columns.len(),
            add_distribution: self
                .settings
                .command_params
                .counter
                .as_ref()
                .unwrap()
                .add_distribution
                .create(),
        }
    }
}

impl CounterWriteOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
    ) -> Result<Self> {
        // UPDATE counter1 SET "C0"="C0"+?,"C1"="C1"+?,"C2"="C2"+?,"C3"="C3"+?,"C4"="C4"+? WHERE KEY=?
        let statement_str = Self::build_query(&settings);

        let mut statement = session
            .prepare(statement_str)
            .await
            .context("Failed to prepare statement")?;

        statement.set_consistency(settings.command_params.common.consistency_level);
        statement.set_serial_consistency(Some(
            settings.command_params.common.serial_consistency_level,
        ));

        Ok(Self {
            session,
            statement,
            settings: Arc::clone(&settings),
        })
    }

    fn build_query(settings: &Arc<CassandraStressSettings>) -> String {
        // Assuming there are non-pk columns [C0, C1, C2], it generates:
        // "C0"="C0"+?,"C1"="C1"+?,"C2"="C2"+?
        let columns_str = settings
            .column
            .columns
            .iter()
            .map(|col| format!("\"{0}\"=\"{0}\"+?", col))
            .collect::<Vec<_>>()
            .join(",");

        format!("UPDATE counter1 SET {} WHERE KEY=?", columns_str)
    }
}
