use std::ops::ControlFlow;
use std::sync::Arc;

use anyhow::Result;
use scylla::{prepared_statement::PreparedStatement, Session};
use tracing::error;

use cql_stress::configuration::{make_runnable, Operation, OperationContext, OperationFactory};

use crate::args::ScyllaBenchArgs;
use crate::stats::ShardedStats;
use crate::workload::{Workload, WorkloadFactory};

pub(crate) struct CounterUpdateOperationFactory {
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    statement: PreparedStatement,
    workload_factory: Box<dyn WorkloadFactory>,
}

struct CounterUpdateOperation {
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    statement: PreparedStatement,
    workload: Box<dyn Workload>,
}

impl CounterUpdateOperationFactory {
    pub async fn new(
        session: Arc<Session>,
        stats: Arc<ShardedStats>,
        workload_factory: Box<dyn WorkloadFactory>,
        args: Arc<ScyllaBenchArgs>,
    ) -> Result<Self> {
        let statement_str = format!(
            "UPDATE {} SET c1 = c1 + ?, c2 = c2 + ?, c3 = c3 + ?, c4 = c4 + ?, c5 = c5 + ? \
            WHERE pk = ? AND ck = ?",
            args.counter_table_name,
        );
        let mut statement = session.prepare(statement_str).await?;
        statement.set_consistency(args.consistency_level);
        statement.set_request_timeout(Some(args.timeout));
        Ok(Self {
            session,
            stats,
            statement,
            workload_factory,
        })
    }
}

impl OperationFactory for CounterUpdateOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        Box::new(CounterUpdateOperation {
            session: Arc::clone(&self.session),
            stats: Arc::clone(&self.stats),
            statement: self.statement.clone(),
            workload: self.workload_factory.create(),
        })
    }
}

make_runnable!(CounterUpdateOperation);
impl CounterUpdateOperation {
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        // Counter updates always use one key
        let (pk, cks) = match self.workload.generate_keys(1) {
            Some((pk, cks)) => (pk, cks),
            None => return Ok(ControlFlow::Break(())),
        };

        let result = self.write_single(pk, cks[0]).await;

        if let Err(err) = result.as_ref() {
            error!(
                error = %err,
                partition_key = pk,
                clustering_keys = ?cks,
                "write error",
            );
        }

        let mut stats = self.stats.get_shard_mut();
        stats.account_op(ctx, &result, cks.len());

        result?;
        Ok(ControlFlow::Continue(()))
    }
}

impl CounterUpdateOperation {
    async fn write_single(&mut self, pk: i64, ck: i64) -> Result<()> {
        // execute_npaged, since it's an UPDATE statement.
        self.session
            .execute_unpaged(
                &self.statement,
                (ck + 1, ck + 2, ck + 3, ck + 4, ck + 5, pk, ck),
            )
            .await?;
        Ok(())
    }
}
