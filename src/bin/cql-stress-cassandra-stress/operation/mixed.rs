use anyhow::Result;
use std::{ops::ControlFlow, sync::Arc};

use cql_stress::{
    configuration::{Operation, OperationContext, OperationFactory},
    make_runnable,
};
use scylla::{frame::response::result::CqlValue, Session};

use crate::{
    java_generate::distribution::Distribution,
    settings::{CassandraStressSettings, MixedSubcommand, OperationRatio},
    stats::ShardedStats,
};

use super::{
    counter_write::{CounterWriteOperation, CounterWriteOperationFactory},
    read::{
        CounterReadOperation, CounterReadOperationFactory, RegularReadOperation,
        RegularReadOperationFactory,
    },
    row_generator::RowGenerator,
    write::{WriteOperation, WriteOperationFactory},
    CassandraStressOperation, CassandraStressOperationFactory, RowGeneratorFactory,
    DEFAULT_COUNTER_TABLE_NAME, DEFAULT_TABLE_NAME,
};

pub struct MixedOperation {
    write_operation: WriteOperation,
    counter_write_operation: CounterWriteOperation,
    read_operation: RegularReadOperation,
    counter_read_operation: CounterReadOperation,
    cached_row: Option<Vec<CqlValue>>,
    workload: RowGenerator,
    max_operations: Option<u64>,
    stats: Arc<ShardedStats>,
    operation_ratio: Arc<OperationRatio>,
    clustering_distribution: Box<dyn Distribution>,
    current_operation: MixedSubcommand,
    current_operation_remaining: usize,
}

pub struct MixedOperationFactory {
    settings: Arc<CassandraStressSettings>,
    write_operation_factory: WriteOperationFactory,
    counter_write_operation_factory: CounterWriteOperationFactory,
    read_operation_factory: RegularReadOperationFactory,
    counter_read_operation_factory: CounterReadOperationFactory,
    operation_ratio: Arc<OperationRatio>,
    workload_factory: RowGeneratorFactory,
    max_operations: Option<u64>,
    stats: Arc<ShardedStats>,
}

impl OperationFactory for MixedOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        let mixed_params = self.settings.command_params.mixed.as_ref().unwrap();

        let write_operation = self.write_operation_factory.create();
        let counter_write_operation = self.counter_write_operation_factory.create();
        let read_operation = self.read_operation_factory.create();
        let counter_read_operation = self.counter_read_operation_factory.create();

        Box::new(MixedOperation {
            write_operation,
            counter_write_operation,
            read_operation,
            counter_read_operation,
            cached_row: None,
            workload: self.workload_factory.create(),
            max_operations: self.max_operations,
            stats: Arc::clone(&self.stats),
            operation_ratio: Arc::clone(&self.operation_ratio),
            clustering_distribution: mixed_params.clustering.create(),
            current_operation: MixedSubcommand::Read,
            current_operation_remaining: 0,
        })
    }
}

impl MixedOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        let mixed_params = settings.command_params.mixed.as_ref().unwrap();
        let max_operations = settings.command_params.common.operation_count;
        let operation_ratio = Arc::new(mixed_params.operation_ratio.clone());
        let write_operation_factory =
            WriteOperationFactory::new(settings.clone(), session.clone()).await?;
        let counter_write_operation_factory =
            CounterWriteOperationFactory::new(settings.clone(), session.clone()).await?;
        let read_operation_factory =
            RegularReadOperationFactory::new(settings.clone(), session.clone(), DEFAULT_TABLE_NAME)
                .await?;
        let counter_read_operation_factory =
            CounterReadOperationFactory::new(settings.clone(), session, DEFAULT_COUNTER_TABLE_NAME)
                .await?;

        Ok(Self {
            settings,
            write_operation_factory,
            counter_write_operation_factory,
            read_operation_factory,
            counter_read_operation_factory,
            operation_ratio,
            workload_factory,
            max_operations,
            stats,
        })
    }
}

make_runnable!(MixedOperation);
impl MixedOperation {
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        if self
            .max_operations
            .is_some_and(|max_ops| ctx.operation_id >= max_ops)
        {
            return Ok(ControlFlow::Break(()));
        }

        if self.current_operation_remaining == 0 {
            self.current_operation = self.operation_ratio.sample();
            self.current_operation_remaining =
                (self.clustering_distribution.next_i64() as usize).max(1);
        }

        let result = match &self.current_operation {
            MixedSubcommand::Read => {
                let row = self
                    .cached_row
                    .get_or_insert_with(|| self.read_operation.generate_row(&mut self.workload));
                self.read_operation.execute(row).await
            }
            MixedSubcommand::CounterRead => {
                let row = self.cached_row.get_or_insert_with(|| {
                    self.counter_read_operation.generate_row(&mut self.workload)
                });
                self.counter_read_operation.execute(row).await
            }
            MixedSubcommand::Write => {
                let row = self
                    .cached_row
                    .get_or_insert_with(|| self.write_operation.generate_row(&mut self.workload));
                self.write_operation.execute(row).await
            }
            MixedSubcommand::CounterWrite => {
                let row = self.cached_row.get_or_insert_with(|| {
                    self.counter_write_operation
                        .generate_row(&mut self.workload)
                });
                self.counter_write_operation.execute(row).await
            }
        };

        self.stats.get_shard_mut().account_operation(ctx, &result);

        if result.is_ok() {
            self.current_operation_remaining -= 1;
            self.cached_row = None;
        }

        result
    }
}
