use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use scylla::frame::value::SerializedValues;
use scylla::{prepared_statement::PreparedStatement, Session};

use cql_stress::configuration::{Operation, OperationContext, OperationFactory};

use crate::args::ScyllaBenchArgs;
use crate::stats::ShardedStats;
use crate::workload::{Workload, WorkloadFactory};

pub(crate) struct ReadOperationFactory {
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    statement: PreparedStatement,
    timeout: Duration,
    workload_factory: Box<dyn WorkloadFactory>,
    read_restriction: ReadRestrictionKind,
    args: Arc<ScyllaBenchArgs>,
}

struct ReadOperation {
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    statement: PreparedStatement,
    timeout: Duration,
    workload: Box<dyn Workload>,
    read_restriction: ReadRestrictionKind,
    validate_data: bool,
}

impl ReadOperationFactory {
    pub async fn new(
        session: Arc<Session>,
        stats: Arc<ShardedStats>,
        workload_factory: Box<dyn WorkloadFactory>,
        args: Arc<ScyllaBenchArgs>,
    ) -> Result<Self> {
        let read_restriction = if args.in_restriction {
            ReadRestrictionKind::InRestriction {
                cks_to_select: args.rows_per_request,
            }
        } else if args.provide_upper_bound {
            ReadRestrictionKind::BothBounds {
                cks_to_select: args.rows_per_request,
            }
        } else if args.no_lower_bound {
            ReadRestrictionKind::NoBounds {
                limit: args.rows_per_request,
            }
        } else {
            ReadRestrictionKind::OnlyLowerBound {
                limit: args.rows_per_request,
            }
        };
        let ck_restriction = read_restriction.as_query_string();

        let statement_str = format!(
            "SELECT ck, v FROM {} WHERE pk = ? {}",
            args.table_name, ck_restriction,
        );
        let mut statement = session.prepare(statement_str).await?;
        statement.set_is_idempotent(true);
        statement.set_page_size(args.page_size.try_into()?);
        statement.set_consistency(args.consistency_level);
        Ok(Self {
            session,
            stats,
            statement,
            timeout: args.timeout,
            workload_factory,
            read_restriction,
            args,
        })
    }
}

impl OperationFactory for ReadOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        Box::new(ReadOperation {
            session: Arc::clone(&self.session),
            stats: Arc::clone(&self.stats),
            statement: self.statement.clone(),
            timeout: self.timeout,
            workload: self.workload_factory.create(),
            read_restriction: self.read_restriction,
            validate_data: self.args.validate_data,
        })
    }
}

#[async_trait]
impl Operation for ReadOperation {
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        let (pk, cks) = match self.read_restriction.generate_values(&mut *self.workload) {
            Some(p) => p,
            None => return Ok(ControlFlow::Break(())),
        };

        let mut values = SerializedValues::new();
        values.add_value(&pk)?;
        for ck in cks {
            values.add_value(&ck)?;
        }

        let mut iter = self
            .session
            .execute_iter(self.statement.clone(), values)
            .await?
            .into_typed::<(i64, Vec<u8>)>();

        let mut rows_read = 0;
        let mut errors = 0;

        // TODO: use driver-side timeouts after they get implemented
        loop {
            let r = tokio::time::timeout(self.timeout, iter.next()).await;
            match r {
                Ok(None) => {
                    // End of the iterator
                    break;
                }
                Ok(Some(Ok((ck, v)))) => {
                    rows_read += 1;
                    if self.validate_data {
                        if let Err(err) = super::validate_row_data(pk, ck, &v) {
                            errors += 1;
                            println!("data corruption in pk({}), ck({}): {}", pk, ck, err);
                        }
                    }
                }
                Ok(Some(Err(_))) => {
                    // Query error
                    errors += 1;
                }
                Err(_) => {
                    // Timeout
                    errors += 1;
                }
            }
        }

        let mut stats_lock = self.stats.get_shard_mut();
        let stats = &mut *stats_lock;
        stats.operations += 1;
        stats.errors += errors;
        stats.clustering_rows += rows_read;
        stats_lock.account_latency(ctx.scheduled_start_time);

        Ok(ControlFlow::Continue(()))
    }
}

#[derive(Copy, Clone)]
pub enum ReadRestrictionKind {
    InRestriction { cks_to_select: u64 },
    BothBounds { cks_to_select: u64 },
    OnlyLowerBound { limit: u64 },
    NoBounds { limit: u64 },
}

impl ReadRestrictionKind {
    fn as_query_string(&self) -> String {
        match *self {
            ReadRestrictionKind::InRestriction { cks_to_select } => {
                if cks_to_select == 0 {
                    return "".to_owned();
                }
                let mut ins = "?,".repeat(cks_to_select as usize);
                ins.pop(); // Remove the last comma
                format!("AND ck IN ({})", ins)
            }
            ReadRestrictionKind::BothBounds { .. } => "AND ck >= ? AND ck < ?".to_owned(),
            ReadRestrictionKind::OnlyLowerBound { limit } => {
                format!("AND ck >= ? LIMIT {}", limit)
            }
            ReadRestrictionKind::NoBounds { limit } => {
                format!("LIMIT {}", limit)
            }
        }
    }

    fn generate_values(&self, workload: &mut dyn Workload) -> Option<(i64, Vec<i64>)> {
        match *self {
            ReadRestrictionKind::InRestriction { cks_to_select } => {
                let (pk, mut cks) = workload.generate_keys(cks_to_select as usize)?;
                // scylla-bench fills up remaining cks with zeros
                cks.extend((cks.len()..cks_to_select as usize).map(|_| 0i64));
                Some((pk, cks))
            }
            ReadRestrictionKind::BothBounds { cks_to_select } => {
                let (pk, mut cks) = workload.generate_keys(1)?;
                cks.push(cks[0] + cks_to_select as i64);
                Some((pk, cks))
            }
            ReadRestrictionKind::OnlyLowerBound { .. } => {
                let (pk, cks) = workload.generate_keys(1)?;
                Some((pk, cks))
            }
            ReadRestrictionKind::NoBounds { .. } => {
                let (pk, cks) = workload.generate_keys(0)?;
                Some((pk, cks))
            }
        }
    }
}
