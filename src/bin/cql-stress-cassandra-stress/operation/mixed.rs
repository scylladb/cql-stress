use anyhow::Result;
use futures::Future;
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
    write_operation: Option<WriteOperation>,
    counter_write_operation: Option<CounterWriteOperation>,
    read_operation: Option<RegularReadOperation>,
    counter_read_operation: Option<CounterReadOperation>,
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
    write_operation_factory: Option<WriteOperationFactory>,
    counter_write_operation_factory: Option<CounterWriteOperationFactory>,
    read_operation_factory: Option<RegularReadOperationFactory>,
    counter_read_operation_factory: Option<CounterReadOperationFactory>,
    operation_ratio: Arc<OperationRatio>,
    workload_factory: RowGeneratorFactory,
    max_operations: Option<u64>,
    stats: Arc<ShardedStats>,
}

fn create_operation_opt<Factory: CassandraStressOperationFactory>(
    factory_opt: &Option<Factory>,
) -> Option<Factory::Operation> {
    factory_opt.as_ref().map(|f| f.create())
}

impl OperationFactory for MixedOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        let mixed_params = self.settings.command_params.mixed.as_ref().unwrap();

        let write_operation = create_operation_opt(&self.write_operation_factory);
        let counter_write_operation = create_operation_opt(&self.counter_write_operation_factory);
        let read_operation = create_operation_opt(&self.read_operation_factory);
        let counter_read_operation = create_operation_opt(&self.counter_read_operation_factory);

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
        let write_operation_factory = Self::conditional_create_factory(
            &mixed_params.operation_ratio,
            &MixedSubcommand::Write,
            || WriteOperationFactory::new(settings.clone(), session.clone()),
        )
        .await
        .transpose()?;
        let counter_write_operation_factory = Self::conditional_create_factory(
            &mixed_params.operation_ratio,
            &MixedSubcommand::CounterWrite,
            || CounterWriteOperationFactory::new(settings.clone(), session.clone()),
        )
        .await
        .transpose()?;
        let read_operation_factory = Self::conditional_create_factory(
            &mixed_params.operation_ratio,
            &MixedSubcommand::Read,
            || {
                RegularReadOperationFactory::new(
                    settings.clone(),
                    session.clone(),
                    DEFAULT_TABLE_NAME,
                )
            },
        )
        .await
        .transpose()?;
        let counter_read_operation_factory = Self::conditional_create_factory(
            &mixed_params.operation_ratio,
            &MixedSubcommand::CounterRead,
            || {
                CounterReadOperationFactory::new(
                    settings.clone(),
                    session.clone(),
                    DEFAULT_COUNTER_TABLE_NAME,
                )
            },
        )
        .await
        .transpose()?;

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

    async fn conditional_create_factory<Factory, Fut: Future<Output = Result<Factory>>>(
        ratios: &OperationRatio,
        command_kind: &MixedSubcommand,
        create_factory_fut: impl FnOnce() -> Fut,
    ) -> Option<Result<Factory>> {
        if ratios.contains(command_kind) {
            Some(create_factory_fut().await)
        } else {
            None
        }
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

        // FIXME: Get rid of these unwraps once async traits are considered object-safe.
        let result = match &self.current_operation {
            MixedSubcommand::Read => {
                // This is safe. We create a given operation only if corresponding `MixedSubcommand` is defined in `operation_ratio` map.
                let read_operation = self.read_operation.as_ref().unwrap();
                let row = self
                    .cached_row
                    .get_or_insert_with(|| read_operation.generate_row(&mut self.workload));
                read_operation.execute(row).await
            }
            MixedSubcommand::CounterRead => {
                // This is safe. We create a given operation only if corresponding `MixedSubcommand` is defined in `operation_ratio` map.
                let counter_read_operation = self.counter_read_operation.as_ref().unwrap();
                let row = self
                    .cached_row
                    .get_or_insert_with(|| counter_read_operation.generate_row(&mut self.workload));
                counter_read_operation.execute(row).await
            }
            MixedSubcommand::Write => {
                // This is safe. We create a given operation only if corresponding `MixedSubcommand` is defined in `operation_ratio` map.
                let write_operation = self.write_operation.as_ref().unwrap();
                let row = self
                    .cached_row
                    .get_or_insert_with(|| write_operation.generate_row(&mut self.workload));
                write_operation.execute(row).await
            }
            MixedSubcommand::CounterWrite => {
                // This is safe. We create a given operation only if corresponding `MixedSubcommand` is defined in `operation_ratio` map.
                let counter_write_operation = self.counter_write_operation.as_ref().unwrap();
                let row = self.cached_row.get_or_insert_with(|| {
                    counter_write_operation.generate_row(&mut self.workload)
                });
                counter_write_operation.execute(row).await
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
