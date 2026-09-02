use anyhow::Result;
use futures::Future;
use std::{ops::ControlFlow, sync::Arc};

use crate::{
    java_generate::distribution::Distribution,
    settings::{CassandraStressSettings, MixedSubcommand, OperationRatio},
    stats::ShardedStats,
};
use cql_stress::{
    configuration::{Operation, OperationContext, OperationFactory},
    make_runnable,
};
use scylla::client::session::Session;

use super::{
    counter_write::{CounterWriteOperation, CounterWriteOperationFactory},
    read::{
        CounterReadOperation, CounterReadOperationFactory, RegularReadOperation,
        RegularReadOperationFactory,
    },
    row_generator::RowGenerator,
    write::{WriteOperation, WriteOperationFactory},
    CachedRow, CassandraStressOperation, CassandraStressOperationFactory, RowGeneratorFactory,
    DEFAULT_COUNTER_TABLE_NAME, DEFAULT_TABLE_NAME,
};

pub struct MixedOperation {
    write_operation: Option<WriteOperation>,
    counter_write_operation: Option<CounterWriteOperation>,
    read_operation: Option<RegularReadOperation>,
    counter_read_operation: Option<CounterReadOperation>,
    cached_row: CachedRow,
    workload: RowGenerator,
    max_operations: Option<u64>,
    stats: Arc<ShardedStats>,
    schedule: SubcommandSchedule,
}

struct SubcommandSchedule {
    operation_ratio: Arc<OperationRatio>,
    clustering_distribution: Box<dyn Distribution>,
    current: MixedSubcommand,
    remaining: usize,
}

impl SubcommandSchedule {
    fn new(
        operation_ratio: Arc<OperationRatio>,
        clustering_distribution: Box<dyn Distribution>,
    ) -> Self {
        Self {
            operation_ratio,
            clustering_distribution,
            current: MixedSubcommand::Read,
            remaining: 0,
        }
    }

    fn advance(&mut self, started_new_operation: bool) -> MixedSubcommand {
        if started_new_operation {
            self.remaining = self.remaining.saturating_sub(1);
        }

        if self.remaining == 0 {
            self.current = self.operation_ratio.sample();
            self.remaining = (self.clustering_distribution.next_i64() as usize).max(1);
        }

        self.current
    }
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
            cached_row: CachedRow::default(),
            workload: self.workload_factory.create(),
            max_operations: self.max_operations,
            stats: Arc::clone(&self.stats),
            schedule: SubcommandSchedule::new(
                Arc::clone(&self.operation_ratio),
                mixed_params.clustering.create(),
            ),
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

        let started_new_operation = self.cached_row.begin_operation(ctx);
        let current_operation = self.schedule.advance(started_new_operation);

        let workload = &mut self.workload;

        // FIXME: Get rid of these unwraps once async traits are considered object-safe.
        let result = match current_operation {
            MixedSubcommand::Read => {
                // This is safe. We create a given operation only if corresponding `MixedSubcommand` is defined in `operation_ratio` map.
                let read_operation = self.read_operation.as_ref().unwrap();
                let row = self
                    .cached_row
                    .get_or_generate(|| read_operation.generate_row(workload));
                let result = read_operation.execute(row).await;
                self.stats.get_shard_mut().account_operation(
                    ctx,
                    &result,
                    read_operation.operation_tag(),
                );
                result
            }
            MixedSubcommand::CounterRead => {
                // This is safe. We create a given operation only if corresponding `MixedSubcommand` is defined in `operation_ratio` map.
                let counter_read_operation = self.counter_read_operation.as_ref().unwrap();
                let row = self
                    .cached_row
                    .get_or_generate(|| counter_read_operation.generate_row(workload));
                let result = counter_read_operation.execute(row).await;
                self.stats.get_shard_mut().account_operation(
                    ctx,
                    &result,
                    counter_read_operation.operation_tag(),
                );
                result
            }
            MixedSubcommand::Write => {
                // This is safe. We create a given operation only if corresponding `MixedSubcommand` is defined in `operation_ratio` map.
                let write_operation = self.write_operation.as_ref().unwrap();
                let row = self
                    .cached_row
                    .get_or_generate(|| write_operation.generate_row(workload));
                let result = write_operation.execute(row).await;
                self.stats.get_shard_mut().account_operation(
                    ctx,
                    &result,
                    write_operation.operation_tag(),
                );
                result
            }
            MixedSubcommand::CounterWrite => {
                // This is safe. We create a given operation only if corresponding `MixedSubcommand` is defined in `operation_ratio` map.
                let counter_write_operation = self.counter_write_operation.as_ref().unwrap();
                let row = self
                    .cached_row
                    .get_or_generate(|| counter_write_operation.generate_row(workload));
                let result = counter_write_operation.execute(row).await;
                self.stats.get_shard_mut().account_operation(
                    ctx,
                    &result,
                    counter_write_operation.operation_tag(),
                );
                result
            }
        };

        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::java_generate::distribution::enumerated::EnumeratedDistribution;

    use super::*;

    struct ScriptedDistribution {
        values: Mutex<std::collections::VecDeque<i64>>,
    }

    impl ScriptedDistribution {
        fn boxed(values: &[i64]) -> Box<dyn Distribution> {
            Box::new(Self {
                values: Mutex::new(values.iter().copied().collect()),
            })
        }
    }

    impl Distribution for ScriptedDistribution {
        fn next_i64(&self) -> i64 {
            self.values
                .lock()
                .unwrap()
                .pop_front()
                .expect("clustering distribution sampled more times than the test scripted")
        }

        fn next_f64(&self) -> f64 {
            self.next_i64() as f64
        }

        fn set_seed(&self, _seed: i64) {}
    }

    fn schedule(clustering: &[i64]) -> SubcommandSchedule {
        let ratio = EnumeratedDistribution::new(vec![(MixedSubcommand::Write, 1.0)]).unwrap();
        SubcommandSchedule::new(Arc::new(ratio), ScriptedDistribution::boxed(clustering))
    }

    #[test]
    fn burst_spans_exactly_the_sampled_number_of_operations() {
        let mut schedule = schedule(&[2, 5]);

        assert_eq!(MixedSubcommand::Write, schedule.advance(true));
        assert_eq!(2, schedule.remaining);

        assert_eq!(MixedSubcommand::Write, schedule.advance(true));
        assert_eq!(1, schedule.remaining);

        assert_eq!(MixedSubcommand::Write, schedule.advance(true));
        assert_eq!(5, schedule.remaining);
    }

    #[test]
    fn retries_do_not_consume_the_burst() {
        let mut schedule = schedule(&[2]);

        schedule.advance(true);
        assert_eq!(2, schedule.remaining);

        schedule.advance(false);
        schedule.advance(false);
        assert_eq!(2, schedule.remaining);
    }

    #[test]
    fn zero_length_burst_runs_a_single_operation() {
        let mut schedule = schedule(&[0, 0, 0]);

        schedule.advance(true);
        assert_eq!(1, schedule.remaining);

        schedule.advance(true);
        assert_eq!(1, schedule.remaining);
    }
}
