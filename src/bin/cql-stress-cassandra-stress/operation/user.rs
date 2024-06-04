use std::{collections::HashMap, ops::ControlFlow, sync::Arc};

use cql_stress::{
    configuration::{Operation, OperationContext, OperationFactory},
    make_runnable,
};
use scylla::{
    frame::response::result::CqlValue, prepared_statement::PreparedStatement,
    transport::topology::Table, Session,
};

use anyhow::Result;

use crate::{
    java_generate::{
        distribution::Distribution,
        values::{Generator, GeneratorConfig, ValueGeneratorFactory},
    },
    settings::CassandraStressSettings,
    stats::ShardedStats,
};

use super::{
    row_generator::RowGenerator, CassandraStressOperation, CassandraStressOperationFactory,
};

const SEED_STR: &str = "seed for stress";

pub struct UserDefinedOperation {
    session: Arc<Session>,
    statement: PreparedStatement,
    argument_index: Vec<usize>,
}

impl CassandraStressOperation for UserDefinedOperation {
    type Factory = UserDefinedOperationFactory;

    async fn execute(&self, row: &[CqlValue]) -> Result<ControlFlow<()>> {
        let mut bound_row = Vec::with_capacity(self.argument_index.len());

        for i in &self.argument_index {
            bound_row.push(&row[*i]);
        }

        self.session.execute(&self.statement, bound_row).await?;

        Ok(ControlFlow::Continue(()))
    }

    fn generate_row(&self, row_generator: &mut RowGenerator) -> Vec<CqlValue> {
        row_generator.generate_row()
    }
}

pub struct UserDefinedOperationFactory {
    session: Arc<Session>,
    statement: PreparedStatement,
    argument_index: Vec<usize>,
}

impl CassandraStressOperationFactory for UserDefinedOperationFactory {
    type Operation = UserDefinedOperation;

    fn create(&self) -> Self::Operation {
        UserDefinedOperation {
            session: Arc::clone(&self.session),
            statement: self.statement.clone(),
            argument_index: self.argument_index.clone(),
        }
    }
}

/// A struct that samples the operations.
/// TODO: For now, this is a simple round-robin sampler.
///       Adjust it, when `ops()` and `clustering=` parameters are supported.
///       This will need to be somehow unified with the sampler of `mixed` operation.
///       I think the sampler could cache the row associated with current operation as well.
struct OperationSampler {
    op_map: HashMap<String, UserDefinedOperation>,
    op_keys: Vec<String>,
    current_operation_index: usize,
}

impl OperationSampler {
    fn from_operation_map(op_map: HashMap<String, UserDefinedOperation>) -> Self {
        let op_keys = op_map.keys().cloned().collect::<Vec<_>>();
        let current_operation_index = 0;

        Self {
            op_map,
            op_keys,
            current_operation_index,
        }
    }

    fn sample(&mut self) -> &UserDefinedOperation {
        // op_keys is a vector of keys of op_map. This unwrap is safe.
        let sample = self
            .op_map
            .get(&self.op_keys[self.current_operation_index])
            .unwrap();
        self.current_operation_index = (self.current_operation_index + 1) % self.op_keys.len();
        sample
    }

    fn sample_cached(&self) -> &UserDefinedOperation {
        // op_keys is a vector of keys of op_map. This unwrap is safe.
        self.op_map
            .get(&self.op_keys[self.current_operation_index])
            .unwrap()
    }
}

pub struct UserOperation {
    sampler: OperationSampler,
    workload: RowGenerator,
    stats: Arc<ShardedStats>,
    max_operations: Option<u64>,
    cached_row: Option<Vec<CqlValue>>,
}

make_runnable!(UserOperation);
impl UserOperation {
    pub async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        if self
            .max_operations
            .is_some_and(|max_ops| ctx.operation_id >= max_ops)
        {
            return Ok(ControlFlow::Break(()));
        }

        let (op, row) = match &mut self.cached_row {
            Some(cached_row) => (self.sampler.sample_cached(), cached_row),
            None => {
                let op = self.sampler.sample();
                let row = self.cached_row.insert(op.generate_row(&mut self.workload));
                (op, row)
            }
        };

        let op_result = op.execute(row).await;

        self.stats
            .get_shard_mut()
            .account_operation(ctx, &op_result);

        if op_result.is_ok() {
            // Operation was successful - we will generate new row
            // for the next operation.
            self.cached_row = None;
        }

        op_result
    }
}

pub struct UserOperationFactory {
    session: Arc<Session>,
    pk_seed_distribution: Arc<dyn Distribution>,
    stats: Arc<ShardedStats>,
    table_metadata: Table,
    statements_map: HashMap<String, PreparedStatement>,
    pk_generator_factory: Box<dyn ValueGeneratorFactory>,
    column_generator_factories: Vec<Box<dyn ValueGeneratorFactory>>,
    max_operations: Option<u64>,
}

impl UserOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        // We parsed a user command. This unwrap is safe.
        let user_profile = settings.command_params.user.as_ref().unwrap();

        let query_definitions = &user_profile.queries;
        let cluster_data = session.get_cluster_data();
        let table_metadata = cluster_data
            .get_keyspace_info()
            .get(&user_profile.keyspace)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Cannot find keyspace {} in cluster data.",
                    user_profile.keyspace
                )
            })?
            .tables
            .get(&user_profile.table)
            .ok_or_else(|| {
                anyhow::anyhow!("Cannot find table {} in cluster data.", user_profile.table)
            })?
            .clone();

        anyhow::ensure!(
            table_metadata.partition_key.len() == 1,
            "Compound partition keys are not yet supported by the tool!"
        );

        let mut statements_map = HashMap::new();
        for (q_name, q_def) in query_definitions {
            statements_map.insert(
                q_name.to_owned(),
                q_def.to_prepared_statement(&session).await?,
            );
        }

        let pk_seed_distribution = settings.population.pk_seed_distribution.create().into();
        let max_operations = settings.command_params.common.operation_count;

        let pk_name = &table_metadata.partition_key[0];
        let pk_generator_factory = Generator::new_generator_factory_from_cql_type(
            &table_metadata
                .columns
                .get(pk_name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Table::columns does not contain info about pk {}. Probably a server bug.",
                        pk_name
                    )
                })?
                .type_,
        )?;
        let column_generator_factories = table_metadata
            .columns
            .iter()
            .filter(|&(col_name, _col_def)| (*col_name != *pk_name))
            .map(|(_col_name, col_def)| {
                Generator::new_generator_factory_from_cql_type(&col_def.type_)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            session,
            pk_seed_distribution,
            stats,
            table_metadata,
            statements_map,
            max_operations,
            pk_generator_factory,
            column_generator_factories,
        })
    }

    fn create_workload(&self) -> RowGenerator {
        let pk_name = &self.table_metadata.partition_key[0];
        let pk_generator = Generator::new(
            self.pk_generator_factory.create(),
            GeneratorConfig::new(&format!("{}{}", SEED_STR, pk_name), None, None),
            pk_name.clone(),
        );

        let column_generators = self
            .table_metadata
            .columns
            .iter()
            .filter(|(col_name, _col_def)| **col_name != *pk_name)
            .zip(self.column_generator_factories.iter())
            .map(|((col_name, _), gen_factory)| {
                Generator::new(
                    gen_factory.create(),
                    GeneratorConfig::new(&format!("{}{}", SEED_STR, col_name), None, None),
                    col_name.to_owned(),
                )
            })
            .collect::<Vec<_>>();

        RowGenerator::new(
            Arc::clone(&self.pk_seed_distribution),
            pk_generator,
            column_generators,
        )
    }
}

impl OperationFactory for UserOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        let workload = self.create_workload();

        let operations_map: HashMap<String, UserDefinedOperation> =
            self.statements_map
                .iter()
                .map(|(op_name, stmt)| {
                    let variable_metadata = stmt.get_variable_col_specs();
                    let argument_index = variable_metadata
                        .iter()
                        .map(|col_spec| {
                            workload.row_index_of_column_with_name(&col_spec.name).expect(
                            "Prepared statement metadata is inconsistent with cluster metadata.",
                        )
                        })
                        .collect::<Vec<_>>();
                    (
                        op_name.clone(),
                        UserDefinedOperation {
                            session: Arc::clone(&self.session),
                            statement: stmt.clone(),
                            argument_index,
                        },
                    )
                })
                .collect::<HashMap<_, _>>();

        let sampler = OperationSampler::from_operation_map(operations_map);

        Box::new(UserOperation {
            workload,
            stats: Arc::clone(&self.stats),
            max_operations: self.max_operations,
            sampler,
            cached_row: None,
        })
    }
}
