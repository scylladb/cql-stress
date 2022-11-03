use std::cmp::Ordering;
use std::ops::ControlFlow;
use std::sync::Arc;

use anyhow::Result;
use rand::Rng;
use scylla::{
    batch::{Batch, BatchType},
    prepared_statement::PreparedStatement,
    Session,
};
use tracing::error;

use cql_stress::configuration::{Operation, OperationContext, OperationFactory};

use crate::args::ScyllaBenchArgs;
use crate::distribution::{Distribution, RngGen};
use crate::stats::ShardedStats;
use crate::workload::{Workload, WorkloadFactory};

pub(crate) struct WriteOperationFactory {
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    statement: PreparedStatement,
    workload_factory: Box<dyn WorkloadFactory>,
    args: Arc<ScyllaBenchArgs>,
}

#[derive(Operation)]
struct WriteOperation {
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    statement: PreparedStatement,
    workload: Box<dyn Workload>,
    clustering_row_size_dist: Arc<dyn Distribution>,
    rows_per_op: u64,
    validate_data: bool,

    gen: RngGen,
}

impl WriteOperationFactory {
    pub async fn new(
        session: Arc<Session>,
        stats: Arc<ShardedStats>,
        workload_factory: Box<dyn WorkloadFactory>,
        args: Arc<ScyllaBenchArgs>,
    ) -> Result<Self> {
        let statement_str = format!(
            "INSERT INTO {} (pk, ck, v) VALUES (?, ?, ?)",
            args.table_name,
        );
        let mut statement = session.prepare(statement_str).await?;
        statement.set_is_idempotent(true);
        statement.set_consistency(args.consistency_level);
        statement.set_request_timeout(Some(args.timeout));

        Ok(Self {
            session,
            stats,
            statement,
            workload_factory,
            args,
        })
    }
}

impl OperationFactory for WriteOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        Box::new(WriteOperation {
            session: Arc::clone(&self.session),
            stats: Arc::clone(&self.stats),
            statement: self.statement.clone(),
            workload: self.workload_factory.create(),
            clustering_row_size_dist: Arc::clone(&self.args.clustering_row_size_dist),
            rows_per_op: self.args.rows_per_request,
            validate_data: self.args.validate_data,

            gen: RngGen::new(rand::thread_rng().gen()),
        })
    }
}

impl WriteOperation {
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        let (pk, cks) = match self.workload.generate_keys(self.rows_per_op as usize) {
            Some((pk, cks)) => (pk, cks),
            None => return Ok(ControlFlow::Break(())),
        };

        let result = match cks.len().cmp(&1) {
            Ordering::Equal => self.write_single(pk, cks[0]).await,
            Ordering::Greater => self.write_batch(pk, &cks).await,
            Ordering::Less => Ok(()),
        };

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

impl WriteOperation {
    async fn write_single(&mut self, pk: i64, ck: i64) -> Result<()> {
        let data = self.generate_row(pk, ck);
        self.session
            .execute(&self.statement, (pk, ck, data))
            .await?;
        Ok(())
    }

    async fn write_batch(&mut self, pk: i64, cks: &[i64]) -> Result<()> {
        let mut batch = Batch::new(BatchType::Unlogged);
        batch.set_is_idempotent(true);
        batch.set_consistency(self.statement.get_consistency().unwrap());
        let mut vals = Vec::with_capacity(cks.len());
        for ck in cks {
            let data = self.generate_row(pk, *ck);
            batch.append_statement(self.statement.clone());
            vals.push((pk, ck, data));
        }
        self.session.batch(&batch, vals).await?;
        Ok(())
    }

    fn generate_row(&mut self, pk: i64, ck: i64) -> Vec<u8> {
        let clen = self.clustering_row_size_dist.get_u64(&mut self.gen) as usize;
        if self.validate_data {
            super::generate_row_data(pk, ck, clen)
        } else {
            vec![0; clen]
        }
    }
}
