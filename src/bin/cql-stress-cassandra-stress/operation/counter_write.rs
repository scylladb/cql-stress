use anyhow::{Context, Result};

use std::{ops::ControlFlow, sync::Arc};

use cql_stress::{
    configuration::{Operation, OperationContext, OperationFactory},
    make_runnable,
};
use scylla::frame::response::result::CqlValue;
use scylla::frame::value::Counter;
use scylla::{prepared_statement::PreparedStatement, Session};

use crate::{
    java_generate::distribution::Distribution, settings::CassandraStressSettings,
    stats::ShardedStats,
};

use super::{row_generator::RowGenerator, RowGeneratorFactory};

pub struct CounterWriteOperation {
    session: Arc<Session>,
    statement: PreparedStatement,
    workload: RowGenerator,
    max_operations: Option<u64>,
    stats: Arc<ShardedStats>,
    non_pk_columns_count: usize,
    add_distribution: Box<dyn Distribution>,
}

pub struct CounterWriteOperationFactory {
    session: Arc<Session>,
    statement: PreparedStatement,
    workload_factory: RowGeneratorFactory,
    max_operations: Option<u64>,
    stats: Arc<ShardedStats>,
    settings: Arc<CassandraStressSettings>,
}

impl OperationFactory for CounterWriteOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        Box::new(CounterWriteOperation {
            session: Arc::clone(&self.session),
            statement: self.statement.clone(),
            workload: self.workload_factory.create(),
            max_operations: self.max_operations,
            stats: Arc::clone(&self.stats),
            non_pk_columns_count: self.settings.column.columns.len(),
            add_distribution: self
                .settings
                .command_params
                .counter
                .as_ref()
                .unwrap()
                .add_distribution
                .create(),
        })
    }
}

impl CounterWriteOperationFactory {
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

    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
        stats: Arc<ShardedStats>,
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
            workload_factory,
            max_operations: settings.command_params.common.operation_count,
            stats,
            settings: Arc::clone(&settings),
        })
    }
}

make_runnable!(CounterWriteOperation);
impl CounterWriteOperation {
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        if self
            .max_operations
            .is_some_and(|max_ops| ctx.operation_id >= max_ops)
        {
            return Ok(ControlFlow::Break(()));
        }

        let mut values: Vec<CqlValue> = Vec::with_capacity(self.non_pk_columns_count + 1);

        for _ in 0..self.non_pk_columns_count {
            values.push(CqlValue::Counter(Counter(self.add_distribution.next_i64())))
        }
        let pk = self.workload.generate_pk();
        values.push(pk);

        let result = self.session.execute(&self.statement, &values).await;

        if let Err(err) = result.as_ref() {
            tracing::error!(
                error = %err,
                partition_key = ?values.last().unwrap(),
                "counter write error",
            );
        }

        self.stats.get_shard_mut().account_operation(ctx, &result);
        result?;

        Ok(ControlFlow::Continue(()))
    }
}
