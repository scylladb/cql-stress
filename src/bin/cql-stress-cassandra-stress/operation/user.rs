use std::{collections::HashMap, ops::ControlFlow, sync::Arc};

use cql_stress::{
    configuration::{Operation, OperationContext, OperationFactory},
    make_runnable,
};
use scylla::client::session::Session;
#[cfg(feature = "strong-consistency")]
use scylla::cluster::metadata::ConsistencyMode;
use scylla::cluster::metadata::Table;
use scylla::statement::prepared::PreparedStatement;
#[cfg(feature = "strong-consistency")]
use scylla::statement::Consistency;
use scylla::value::CqlValue;

use anyhow::{Context, Result};

use crate::{
    java_generate::{
        distribution::{Distribution, DistributionFactory},
        values::{Generator, GeneratorConfig, ValueGeneratorFactory},
    },
    settings::{CassandraStressSettings, OpWeight, PREDEFINED_INSERT_OPERATION},
    stats::ShardedStats,
};

use super::{
    coordinator_of, row_generator::RowGenerator, CachedRow, CassandraStressOperation,
    CassandraStressOperationFactory, OperationOutcome, OperationSampler,
};

const SEED_STR: &str = "seed for stress";

/// What a strongly consistent keyspace makes of one statement's consistency level.
#[cfg(feature = "strong-consistency")]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ConsistencyVerdict {
    /// Works, and routes to the tablet's Raft leader.
    Accepted,
    /// Works, but keeps normal spread routing - so it measures something else.
    NotLeaderRouted,
    /// The server rejects every request at this level.
    Rejected,
}

/// Decides what a strongly consistent keyspace does with `consistency` on a read or a write.
///
/// Measured on 2026.4.0~dev: writes take `QUORUM`/`LOCAL_QUORUM` only; reads additionally
/// take `ONE`/`LOCAL_ONE`, but any replica may serve those, so they are not leader-routed.
/// Everything else - `ALL`, `TWO`, `THREE`, `ANY`, `EACH_QUORUM`, ... - is rejected for both.
#[cfg(feature = "strong-consistency")]
fn classify_consistency(consistency: Consistency, is_read: bool) -> ConsistencyVerdict {
    match consistency {
        Consistency::Quorum | Consistency::LocalQuorum => ConsistencyVerdict::Accepted,
        Consistency::One | Consistency::LocalOne if is_read => ConsistencyVerdict::NotLeaderRouted,
        _ => ConsistencyVerdict::Rejected,
    }
}

pub struct UserDefinedOperation {
    session: Arc<Session>,
    statement: PreparedStatement,
    argument_index: Vec<usize>,
    operation_tag: String,
}

impl CassandraStressOperation for UserDefinedOperation {
    type Factory = UserDefinedOperationFactory;

    async fn execute(&self, row: &[CqlValue]) -> Result<OperationOutcome> {
        let mut bound_row = Vec::with_capacity(self.argument_index.len());

        for i in &self.argument_index {
            bound_row.push(&row[*i]);
        }

        // User can provide a custom query here. In addition, we don't care
        // about the result of this query. This is why we can use `execute_unpaged`.
        let result = self
            .session
            .execute_unpaged(&self.statement, bound_row)
            .await?;

        Ok(OperationOutcome::proceed(coordinator_of(&result)))
    }

    fn generate_row(&self, row_generator: &mut RowGenerator) -> Vec<CqlValue> {
        row_generator.generate_row()
    }

    fn operation_tag(&self) -> &str {
        self.operation_tag.as_str()
    }
}

pub struct UserDefinedOperationFactory {
    session: Arc<Session>,
    statement: PreparedStatement,
    argument_index: Vec<usize>,
    operation_tag: String,
}

impl CassandraStressOperationFactory for UserDefinedOperationFactory {
    type Operation = UserDefinedOperation;

    fn create(&self) -> Self::Operation {
        UserDefinedOperation {
            session: Arc::clone(&self.session),
            statement: self.statement.clone(),
            argument_index: self.argument_index.clone(),
            operation_tag: self.operation_tag.clone(),
        }
    }
}

pub struct UserOperation {
    sampler: OperationSampler<UserDefinedOperation>,
    workload: RowGenerator,
    stats: Arc<ShardedStats>,
    max_operations: Option<u64>,
    cached_row: CachedRow,
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

        let op = if self.cached_row.begin_operation(ctx) {
            self.sampler.sample()
        } else {
            self.sampler.previous_sample()
        };

        let workload = &mut self.workload;
        let row = self
            .cached_row
            .get_or_generate(|| op.generate_row(workload));

        let op_result = op.execute(row).await;

        self.stats
            .get_shard_mut()
            .account_operation(ctx, &op_result, op.operation_tag());

        op_result.map(|outcome| outcome.control_flow)
    }
}

pub struct UserOperationFactory {
    session: Arc<Session>,
    pk_seed_distribution: Arc<dyn Distribution>,
    stats: Arc<ShardedStats>,
    table_metadata: Table,
    queries_payload: HashMap<String, (PreparedStatement, OpWeight)>,
    pk_generator_factory: Box<dyn ValueGeneratorFactory>,
    column_generator_factories: Vec<Box<dyn ValueGeneratorFactory>>,
    max_operations: Option<u64>,
    clustering: Arc<dyn DistributionFactory>,
}

impl UserOperationFactory {
    async fn prepare_insert_statement(
        session: &Arc<Session>,
        table_name: &str,
        table_metadata: &Table,
    ) -> Result<PreparedStatement> {
        let column_names = table_metadata
            .columns
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();

        let column_list_str = column_names.join(", ");
        let column_values_str = std::iter::repeat_n("?", column_names.len())
            .collect::<Vec<_>>()
            .join(", ");

        let statement_str =
            format!("INSERT INTO {table_name} ({column_list_str}) VALUES ({column_values_str})");
        session
            .prepare(statement_str)
            .await
            .context("Failed to prepare statement for 'insert' operation.")
    }

    /// Checks each prepared statement's effective consistency level against what a strongly
    /// consistent keyspace accepts.
    ///
    /// The accepted sets are asymmetric and the server rejects per request rather than at
    /// connect time:
    ///
    /// - **writes** take `QUORUM` and `LOCAL_QUORUM`, nothing else;
    /// - **reads** additionally take `ONE` and `LOCAL_ONE`, at the cost of leader-aware
    ///   routing: any replica may serve such a read, so it keeps normal spread routing.
    ///
    /// Reads and writes are told apart by the prepared statement's own result metadata - a
    /// statement that returns columns is a read - rather than by inspecting the CQL text,
    /// which the server has already parsed for us.
    ///
    /// A statement with no `consistencyLevel:` in the yaml inherits the driver's
    /// execution-profile default, which is `LOCAL_QUORUM` and therefore legal for both. That
    /// covers the predefined `insert` operation, which sets no level of its own.
    ///
    /// Every violation in the profile is reported at once: fixing them one run at a time is
    /// needless, and a profile can easily have several.
    #[cfg(feature = "strong-consistency")]
    fn verify_profile_consistency_levels(
        queries: &HashMap<String, (PreparedStatement, OpWeight)>,
        keyspace: &str,
    ) -> Result<()> {
        // Sorted so the report is stable - `queries` is a HashMap.
        let mut names = queries.keys().collect::<Vec<_>>();
        names.sort_unstable();

        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        for name in names {
            let (statement, _weight) = &queries[name];
            // `None` means the statement carries no level of its own and takes the execution
            // profile's, which cql-stress leaves at the driver's LOCAL_QUORUM default.
            let Some(consistency) = statement.get_consistency() else {
                continue;
            };
            // A statement that returns columns is a read - except a conditional write, whose
            // result set is the `[applied]` flag. Classifying an LWT as a write is both
            // correct and the stricter of the two.
            let is_read = !statement.is_confirmed_lwt()
                && statement.get_current_result_set_col_specs().get().len() > 0;

            match classify_consistency(consistency, is_read) {
                ConsistencyVerdict::Accepted => (),
                ConsistencyVerdict::NotLeaderRouted => warnings.push(format!(
                    "  - '{name}' reads at {consistency}, which is accepted but not \
                     leader-routed: at ONE and LOCAL_ONE any replica may serve the read, so \
                     it keeps normal spread routing"
                )),
                ConsistencyVerdict::Rejected if is_read => errors.push(format!(
                    "  - '{name}' reads at {consistency}; strongly consistent reads take only \
                     QUORUM, LOCAL_QUORUM, ONE or LOCAL_ONE"
                )),
                ConsistencyVerdict::Rejected => errors.push(format!(
                    "  - '{name}' writes at {consistency}; strongly consistent writes take \
                     only QUORUM or LOCAL_QUORUM"
                )),
            }
        }

        if !warnings.is_empty() {
            println!();
            println!(
                "WARNING: keyspace '{keyspace}' is strongly consistent and some queries in \
                 this profile are not leader-routed:\n{}\nUse consistencyLevel: QUORUM to \
                 measure strong consistency.",
                warnings.join("\n")
            );
            println!();
        }

        anyhow::ensure!(
            errors.is_empty(),
            "Keyspace '{keyspace}' is strongly consistent, but this profile declares \
             consistency levels the server would reject on every request:\n{}\nSet \
             consistencyLevel: QUORUM on those queries.",
            errors.join("\n")
        );

        Ok(())
    }

    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        // We parsed a user command. This unwrap is safe.
        let user_profile = settings.command_params.user.as_ref().unwrap();

        let query_definitions = &user_profile.queries_payload;
        let cluster_state = session.get_cluster_state();
        let table_metadata = cluster_state
            .get_keyspace(&user_profile.keyspace)
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

        let queries_payload = {
            let mut queries_payload = HashMap::new();
            for (q_name, (q_def, weight)) in query_definitions {
                queries_payload.insert(
                    q_name.to_owned(),
                    (q_def.to_prepared_statement(&session).await?, *weight),
                );
            }
            // Handle 'insert' operation separately.
            if let Some(insert_weight) = &user_profile.insert_operation_weight {
                let insert_statement =
                    Self::prepare_insert_statement(&session, &user_profile.table, &table_metadata)
                        .await?;
                queries_payload.insert(
                    PREDEFINED_INSERT_OPERATION.to_owned(),
                    (insert_statement, *insert_weight),
                );
            }

            println!("\n========================");
            println!("Operations to be performed and their sample ratio weights:\n");
            for (q_name, (statement, q_weight)) in queries_payload.iter() {
                println!(
                    "- {}: {{ 'cql': '{}', 'weight': {} }}",
                    q_name,
                    statement.get_statement(),
                    q_weight
                );
            }
            println!("========================\n");

            queries_payload
        };

        // A strongly consistent keyspace accepts far fewer consistency levels than an
        // ordinary one and rejects them per request, so an unvalidated profile produces a run
        // in which every operation fails - numbers that read like a catastrophic cluster
        // problem and are really a profile mistake. `cl=` cannot stand in for this check:
        // user mode never applies it to anything. Each statement carries the level from its
        // yaml `consistencyLevel:`, or the driver's default, so the levels have to be read
        // back off the prepared statements.
        #[cfg(feature = "strong-consistency")]
        if matches!(
            cluster_state
                .get_keyspace(&user_profile.keyspace)
                .map(|ks| &ks.consistency_mode),
            Some(ConsistencyMode::Global)
        ) {
            Self::verify_profile_consistency_levels(&queries_payload, &user_profile.keyspace)?;
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
                .typ,
        )?;
        let column_generator_factories = table_metadata
            .columns
            .iter()
            .filter(|&(col_name, _col_def)| (*col_name != *pk_name))
            .map(|(_col_name, col_def)| {
                Generator::new_generator_factory_from_cql_type(&col_def.typ)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            session,
            pk_seed_distribution,
            stats,
            table_metadata,
            queries_payload,
            max_operations,
            pk_generator_factory,
            column_generator_factories,
            clustering: user_profile.clustering.clone(),
        })
    }

    fn create_workload(&self) -> RowGenerator {
        let pk_name = &self.table_metadata.partition_key[0];
        let pk_generator = Generator::new(
            self.pk_generator_factory.create(),
            GeneratorConfig::new(&format!("{SEED_STR}{pk_name}"), None, None),
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
                    GeneratorConfig::new(&format!("{SEED_STR}{col_name}"), None, None),
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

        let weights_iter =
            self.queries_payload
                .iter()
                .map(|(op_name, (stmt, weight))| {
                    let variable_metadata = stmt.get_variable_col_specs();
                    let argument_index = variable_metadata
                        .iter()
                        .map(|col_spec| {
                            workload.row_index_of_column_with_name(col_spec.name()).expect(
                            "Prepared statement metadata is inconsistent with cluster metadata.",
                        )
                        })
                        .collect::<Vec<_>>();
                    (
                        UserDefinedOperation {
                            session: Arc::clone(&self.session),
                            statement: stmt.clone(),
                            argument_index,
                            operation_tag: op_name.clone(),
                        },
                        *weight,
                    )
                });

        let sampler = OperationSampler::new(weights_iter, self.clustering.as_ref());

        Box::new(UserOperation {
            workload,
            stats: Arc::clone(&self.stats),
            max_operations: self.max_operations,
            sampler,
            cached_row: CachedRow::default(),
        })
    }
}

#[cfg(all(test, feature = "strong-consistency"))]
mod tests {
    use super::*;

    /// The rules a strongly consistent keyspace enforces per request. Getting these wrong
    /// does not fail loudly at connect time - it fails every single operation of the run, so
    /// the table is pinned here rather than left to an integration test that needs a server
    /// built with an experimental feature.
    #[test]
    fn strongly_consistent_consistency_levels_test() {
        use ConsistencyVerdict::*;

        // Writes take QUORUM/LOCAL_QUORUM and nothing else.
        for cl in [Consistency::Quorum, Consistency::LocalQuorum] {
            assert_eq!(Accepted, classify_consistency(cl, false), "write at {cl}");
            assert_eq!(Accepted, classify_consistency(cl, true), "read at {cl}");
        }

        // Reads additionally take ONE/LOCAL_ONE, but lose leader-aware routing; the same
        // levels are rejected outright for writes.
        for cl in [Consistency::One, Consistency::LocalOne] {
            assert_eq!(
                NotLeaderRouted,
                classify_consistency(cl, true),
                "read at {cl}"
            );
            assert_eq!(Rejected, classify_consistency(cl, false), "write at {cl}");
        }

        // Everything else is rejected for both.
        for cl in [
            Consistency::All,
            Consistency::Two,
            Consistency::Three,
            Consistency::Any,
            Consistency::EachQuorum,
            Consistency::Serial,
            Consistency::LocalSerial,
        ] {
            assert_eq!(Rejected, classify_consistency(cl, true), "read at {cl}");
            assert_eq!(Rejected, classify_consistency(cl, false), "write at {cl}");
        }
    }
}
