mod counter_write;
mod mixed;
mod read;
mod row_generator;
#[cfg(feature = "user-profile")]
mod user;
mod write;

use anyhow::Context;
use anyhow::Result;
use cql_stress::configuration::Operation;
use cql_stress::configuration::OperationContext;
use cql_stress::configuration::OperationFactory;
use cql_stress::make_runnable;
#[cfg(feature = "user-profile")]
use rand_distr::{weighted::WeightedIndex, Distribution as _};
use scylla::client::session::Session;
use std::future::Future;
use std::num::Wrapping;
use std::ops::ControlFlow;
use std::sync::Arc;

pub use mixed::MixedOperationFactory;
pub use row_generator::RowGeneratorFactory;
use scylla::response::query_result::QueryResult;
use scylla::value::{CqlValue, Row};

#[cfg(feature = "user-profile")]
pub use user::UserOperationFactory;

#[cfg(feature = "user-profile")]
use crate::java_generate::distribution::{Distribution, DistributionFactory};
use crate::settings::CassandraStressSettings;
use crate::stats::ShardedStats;

use self::row_generator::RowGenerator;

const DEFAULT_TABLE_NAME: &str = "standard1";
const DEFAULT_COUNTER_TABLE_NAME: &str = "counter1";

/// A specific CassandraStress operation.
///
/// The operation implementing this trait should handle
/// sending the actual query to the database.
///
/// This trait is intended to be used by [`GenericCassandraStressOperation`]
/// which encapsulates the specific operation and handles the common logic.
///
/// ## Result of [`CassandraStressOperation::execute`]
/// ### Operation retries
/// During the operation retry (i.e. when `execute` returned and error),
/// we will make use of the same row that we originally used in the previous try.
///
/// We only generate a new row ([`CassandraStressOperation::generate_row`])
/// during the first try to perform an operation.
/// ### Stats recording
/// The result of `execute` is recorded
/// to [`ShardedStats`] - even if the operation failed, so we keep track
/// of number of errors that appeared during the benchmark.
pub trait CassandraStressOperation: Sync + Send {
    type Factory: CassandraStressOperationFactory<Operation = Self>;

    fn execute(&self, row: &[CqlValue]) -> impl Future<Output = Result<ControlFlow<()>>> + Send;
    fn generate_row(&self, row_generator: &mut RowGenerator) -> Vec<CqlValue>;
    fn operation_tag(&self) -> &str;
}

pub trait CassandraStressOperationFactory: Sync + Send + Sized {
    type Operation: CassandraStressOperation<Factory = Self>;

    fn create(&self) -> Self::Operation;
}

#[derive(Default)]
pub struct CachedRow {
    row: Option<Vec<CqlValue>>,
    last_operation_id: Option<u64>,
}

impl CachedRow {
    pub fn begin_operation(&mut self, ctx: &OperationContext) -> bool {
        if self.last_operation_id == Some(ctx.operation_id) {
            return false;
        }

        self.last_operation_id = Some(ctx.operation_id);
        self.row = None;
        true
    }

    pub fn get_or_generate(&mut self, generate: impl FnOnce() -> Vec<CqlValue>) -> &[CqlValue] {
        self.row.get_or_insert_with(generate)
    }
}

/// Generic CassandraStress operation.
///
/// It handles the common logic for all of the operations, such as:
/// - checking whether `max_operations` operations have already been performed
/// - caching the row for operation retries
/// - recording operation result to statistics structure
///
/// Delegates the specific logic to `cs_operation`.
pub struct GenericCassandraStressOperation<O: CassandraStressOperation> {
    cs_operation: O,
    stats: Arc<ShardedStats>,
    workload: RowGenerator,
    max_operations: Option<u64>,
    cached_row: CachedRow,
}

make_runnable!(GenericCassandraStressOperation<O: CassandraStressOperation>);
impl<O: CassandraStressOperation> GenericCassandraStressOperation<O> {
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        if self
            .max_operations
            .is_some_and(|max_ops| ctx.operation_id >= max_ops)
        {
            return Ok(ControlFlow::Break(()));
        }

        self.cached_row.begin_operation(ctx);

        let cs_operation = &self.cs_operation;
        let workload = &mut self.workload;
        let row = self
            .cached_row
            .get_or_generate(|| cs_operation.generate_row(workload));

        let op_result = self.cs_operation.execute(row).await;
        self.stats.get_shard_mut().account_operation(
            ctx,
            &op_result,
            self.cs_operation.operation_tag(),
        );

        op_result
    }
}

pub struct GenericCassandraStressOperationFactory<O: CassandraStressOperation> {
    cs_operation_factory: O::Factory,
    workload_factory: RowGeneratorFactory,
    max_operations: Option<u64>,
    stats: Arc<ShardedStats>,
}

pub type WriteOperationFactory = GenericCassandraStressOperationFactory<write::WriteOperation>;
pub type CounterWriteOperationFactory =
    GenericCassandraStressOperationFactory<counter_write::CounterWriteOperation>;
pub type RegularReadOperationFactory =
    GenericCassandraStressOperationFactory<read::RegularReadOperation>;
pub type CounterReadOperationFactory =
    GenericCassandraStressOperationFactory<read::CounterReadOperation>;

impl WriteOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        let max_operations = settings.command_params.common.operation_count;
        let cs_operation_factory = write::WriteOperationFactory::new(settings, session).await?;

        Ok(Self {
            cs_operation_factory,
            max_operations,
            workload_factory,
            stats,
        })
    }
}

impl CounterWriteOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        let max_operations = settings.command_params.common.operation_count;
        let cs_operation_factory =
            counter_write::CounterWriteOperationFactory::new(settings, session).await?;

        Ok(Self {
            cs_operation_factory,
            max_operations,
            workload_factory,
            stats,
        })
    }
}

impl RegularReadOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        let max_operations = settings.command_params.common.operation_count;
        let cs_operation_factory =
            read::RegularReadOperationFactory::new(settings, session, DEFAULT_TABLE_NAME).await?;

        Ok(Self {
            cs_operation_factory,
            max_operations,
            workload_factory,
            stats,
        })
    }
}

impl CounterReadOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        let max_operations = settings.command_params.common.operation_count;
        let cs_operation_factory =
            read::CounterReadOperationFactory::new(settings, session, DEFAULT_COUNTER_TABLE_NAME)
                .await?;

        Ok(Self {
            cs_operation_factory,
            max_operations,
            workload_factory,
            stats,
        })
    }
}

impl<O: CassandraStressOperation + 'static> OperationFactory
    for GenericCassandraStressOperationFactory<O>
{
    fn create(&self) -> Box<dyn Operation> {
        let cs_operation = self.cs_operation_factory.create();

        Box::new(GenericCassandraStressOperation {
            cs_operation,
            stats: Arc::clone(&self.stats),
            workload: self.workload_factory.create(),
            max_operations: self.max_operations,
            cached_row: CachedRow::default(),
        })
    }
}

/// See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/PartitionIterator.java#L725.
fn recompute_seed(seed: i64, partition_key: &CqlValue) -> i64 {
    match partition_key {
        CqlValue::Blob(key) => {
            let mut wrapped = Wrapping(seed);
            for byte in key {
                wrapped = (wrapped * Wrapping(31)) + Wrapping((*byte as i8) as i64);
            }
            wrapped.0
        }
        _ => todo!("Implement recompute_seed for other CqlValues"),
    }
}

fn extract_single_row_from_query_result(query_result: QueryResult) -> Result<Row> {
    let rows_result = query_result
        .into_rows_result()
        .context("Failed to convert to Rows result")?;

    // Note that row-generation logic behaves in a way that given partition_key,
    // there is exactly one row with this partition_key.
    rows_result
        .single_row::<Row>()
        .context("Failed to extract a single row from the result")
}

pub trait RowValidator: Sync + Send + Default {
    fn validate_row(&self, generated_row: &[CqlValue], query_result: QueryResult) -> Result<()>;
    fn operation_tag() -> &'static str;
}

#[derive(Default)]
pub struct EqualRowValidator;
impl RowValidator for EqualRowValidator {
    fn validate_row(&self, generated_row: &[CqlValue], query_result: QueryResult) -> Result<()> {
        let first_row = extract_single_row_from_query_result(query_result)?;

        anyhow::ensure!(
            first_row.columns.len() == generated_row.len(),
            "Expected row's ({:?}) length: {}. Result row's ({:?}) length: {}",
            generated_row,
            generated_row.len(),
            first_row.columns,
            first_row.columns.len(),
        );

        let result =
            first_row
                .columns
                .iter()
                .zip(generated_row.iter())
                .all(|(maybe_result, expected)| match maybe_result {
                    Some(result) => result == expected,
                    // TODO: For now, we don't permit NULLs.
                    None => false,
                });

        anyhow::ensure!(
            result,
            "The data doesn't match. Result: {:?}. Expected: {:?}.",
            first_row.columns,
            generated_row,
        );
        Ok(())
    }

    fn operation_tag() -> &'static str {
        "READ"
    }
}

#[derive(Default)]
pub struct ExistsRowValidator;
impl RowValidator for ExistsRowValidator {
    fn validate_row(&self, _generated_row: &[CqlValue], query_result: QueryResult) -> Result<()> {
        // We only check that the row with given PK exists, which is equivalent to
        // successfully extracting the first row from the query result.
        let _first_row = extract_single_row_from_query_result(query_result)?;
        Ok(())
    }

    fn operation_tag() -> &'static str {
        "COUNTER_READ"
    }
}

/// A sampler created based on a ratio map and a counter distribution.
///
/// How the sampler works?
/// One iteration consists of:
/// - sampling an item based on ratio map. `current_item_index` is sampled from `item_index_dist`.
///   The item can then be retrieved via this index from `items` vector.
/// - sampling a counter which says how many times to return the current item.
///   The counter is sampled from `counter_dist` distribution.
///
/// The user then can sample the items via `sample` or `previous_sample` method.
///
/// The `sample` method will decrease the counter by 1, and return current item.
/// If the counter reaches 0, new iteration starts.
///
/// The `previous_sample` method returns a current item without decreasing the counter.
/// This is helpful when the user wants to, for example, retry an operation that was
/// sampled before, but failed for some reason.
#[cfg(feature = "user-profile")]
struct OperationSampler<T> {
    counter_dist: Box<dyn Distribution>,
    items: Vec<T>,
    item_index_dist: WeightedIndex<f64>,
    current_item_remaining: u8,
    current_item_index: usize,
}

#[cfg(feature = "user-profile")]
impl<T> OperationSampler<T> {
    pub fn new(
        weights: impl Iterator<Item = (T, f64)>,
        counter_dist_factory: &dyn DistributionFactory,
    ) -> Self {
        let (items, weights): (Vec<_>, Vec<_>) = weights.unzip();
        // We verify the ratio properties during parsing.
        let item_index_dist = WeightedIndex::new(weights).unwrap_or_else(|err| {
            panic!("Failed to create a WeightedIntex from provided ratios: {err}")
        });

        Self {
            counter_dist: counter_dist_factory.create(),
            items,
            item_index_dist,
            current_item_remaining: 0,
            current_item_index: 0,
        }
    }

    pub fn sample(&mut self) -> &T {
        if self.current_item_remaining == 0 {
            self.current_item_index = self.item_index_dist.sample(&mut rand::rng());
            self.current_item_remaining = (self.counter_dist.next_i64() as u8).max(1);
        }
        self.current_item_remaining -= 1;
        &self.items[self.current_item_index]
    }

    pub fn previous_sample(&self) -> &T {
        &self.items[self.current_item_index]
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::time::Instant;

    use super::*;
    use crate::settings::{parse_cassandra_stress_args, CassandraStressParsingResult};
    use crate::stats::StatsFactory;

    struct FailingOperationFactory {
        executed_rows: Arc<Mutex<Vec<Vec<CqlValue>>>>,
    }

    struct FailingOperation {
        executed_rows: Arc<Mutex<Vec<Vec<CqlValue>>>>,
    }

    impl CassandraStressOperationFactory for FailingOperationFactory {
        type Operation = FailingOperation;

        fn create(&self) -> Self::Operation {
            FailingOperation {
                executed_rows: Arc::clone(&self.executed_rows),
            }
        }
    }

    impl CassandraStressOperation for FailingOperation {
        type Factory = FailingOperationFactory;

        async fn execute(&self, row: &[CqlValue]) -> Result<ControlFlow<()>> {
            self.executed_rows.lock().unwrap().push(row.to_vec());
            Err(anyhow::anyhow!("operation failed"))
        }

        fn generate_row(&self, row_generator: &mut RowGenerator) -> Vec<CqlValue> {
            row_generator.generate_row()
        }

        fn operation_tag(&self) -> &'static str {
            "TEST"
        }
    }

    fn make_operation_context(operation_id: u64) -> OperationContext {
        let now = Instant::now();
        OperationContext {
            operation_id,
            scheduled_start_time: now,
            actual_start_time: now,
        }
    }

    #[tokio::test]
    async fn generic_operation_regenerates_row_for_new_operation_id() {
        let args =
            "cql-stress-cassandra-stress write n=100 -node 127.0.0.1".split_ascii_whitespace();
        let settings = match parse_cassandra_stress_args(args).unwrap() {
            CassandraStressParsingResult::Workload(settings) => Arc::new(*settings),
            CassandraStressParsingResult::SpecialCommand => panic!("Expected a workload"),
        };

        let executed_rows = Arc::new(Mutex::new(Vec::new()));
        let stats_factory = Arc::new(StatsFactory::new(&settings));
        let mut operation = GenericCassandraStressOperation {
            cs_operation: FailingOperationFactory {
                executed_rows: Arc::clone(&executed_rows),
            }
            .create(),
            stats: Arc::new(ShardedStats::new(stats_factory)),
            workload: RowGeneratorFactory::new(Arc::clone(&settings)).create(),
            max_operations: None,
            cached_row: CachedRow::default(),
        };

        operation
            .execute(&make_operation_context(0))
            .await
            .unwrap_err();
        operation
            .execute(&make_operation_context(0))
            .await
            .unwrap_err();
        operation
            .execute(&make_operation_context(1))
            .await
            .unwrap_err();

        let rows = executed_rows.lock().unwrap();
        assert_eq!(3, rows.len());
        assert_eq!(rows[0], rows[1]);
        assert_ne!(rows[1], rows[2]);
    }
}
