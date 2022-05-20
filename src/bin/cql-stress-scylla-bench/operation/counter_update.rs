use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use scylla::{prepared_statement::PreparedStatement, Session};

use cql_stress::configuration::{Operation, OperationContext, OperationFactory};

use crate::args::ScyllaBenchArgs;
use crate::stats::ShardedStats;
use crate::workload::{Workload, WorkloadFactory};

pub(crate) struct CounterUpdateOperationFactory {
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    timeout: Duration,
    statement: PreparedStatement,
    workload_factory: Box<dyn WorkloadFactory>,
}

struct CounterUpdateOperation {
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    timeout: Duration,
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
        Ok(Self {
            session,
            stats,
            timeout: args.timeout,
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
            timeout: self.timeout,
            workload: self.workload_factory.create(),
        })
    }
}

#[async_trait]
impl Operation for CounterUpdateOperation {
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        // Counter updates always use one key
        let (pk, cks) = match self.workload.generate_keys(1) {
            Some((pk, cks)) => (pk, cks),
            None => return Ok(ControlFlow::Break(())),
        };

        let result = self.write_single(pk, cks[0]).await;

        if let Err(err) = result.as_ref() {
            println!("failed to execute a write: {}", err);
        }

        let mut stats = self.stats.get_shard_mut();
        stats.account_op(ctx.scheduled_start_time, &result, cks.len());

        Ok(ControlFlow::Continue(()))
    }
}

impl CounterUpdateOperation {
    async fn write_single(&mut self, pk: i64, ck: i64) -> Result<()> {
        // TODO: Use driver-side timeouts after they are implemented
        tokio::time::timeout(
            self.timeout,
            self.session.execute(
                &self.statement,
                (ck + 1, ck + 2, ck + 3, ck + 4, ck + 5, pk, ck),
            ),
        )
        .await??;
        Ok(())
    }
}
