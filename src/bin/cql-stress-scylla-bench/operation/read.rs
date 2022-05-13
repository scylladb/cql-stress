use std::fmt::Display;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::{stream, StreamExt, TryStreamExt};
use scylla::frame::value::SerializedValues;
use scylla::{prepared_statement::PreparedStatement, Session};

use cql_stress::configuration::{Operation, OperationContext, OperationFactory};

use crate::args::{OrderBy, ScyllaBenchArgs};
use crate::stats::ShardedStats;
use crate::workload::{Workload, WorkloadFactory};

pub(crate) struct ReadOperationFactory {
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    statements: Vec<PreparedStatement>,
    timeout: Duration,
    workload_factory: Box<dyn WorkloadFactory>,
    read_restriction: ReadRestrictionKind,
    args: Arc<ScyllaBenchArgs>,
}

struct ReadOperation {
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    statements: Vec<PreparedStatement>,
    timeout: Duration,
    workload: Box<dyn Workload>,
    read_restriction: ReadRestrictionKind,
    validate_data: bool,

    current_statement_idx: usize,
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

        let statements = stream::iter(&args.select_order_by)
            .then(|order_by| prepare_statement(&*session, &*args, &read_restriction, order_by))
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Self {
            session,
            stats,
            statements,
            timeout: args.timeout,
            workload_factory,
            read_restriction,
            args,
        })
    }
}

async fn prepare_statement(
    session: &Session,
    args: &ScyllaBenchArgs,
    read_restriction: &ReadRestrictionKind,
    order_by: &OrderBy,
) -> Result<PreparedStatement> {
    let selector = read_restriction.get_selector_string();
    let order_by = get_order_by_string(order_by);
    let limit = read_restriction.get_limit_string();

    let mut statement_str = format!(
        "SELECT ck, v FROM {} WHERE pk = ? {} {} {}",
        args.table_name, selector, order_by, limit,
    );
    if args.bypass_cache {
        statement_str += " BYPASS CACHE";
    }
    let mut statement = session.prepare(statement_str).await?;
    statement.set_is_idempotent(true);
    statement.set_page_size(args.page_size.try_into()?);
    statement.set_consistency(args.consistency_level);

    Ok(statement)
}

fn get_order_by_string(order: &OrderBy) -> &'static str {
    match order {
        OrderBy::None => "",
        OrderBy::Asc => "ORDER BY ck ASC",
        OrderBy::Desc => "ORDER BY ck DESC",
    }
}

impl OperationFactory for ReadOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        Box::new(ReadOperation {
            session: Arc::clone(&self.session),
            stats: Arc::clone(&self.stats),
            statements: self.statements.clone(),
            timeout: self.timeout,
            workload: self.workload_factory.create(),
            read_restriction: self.read_restriction,
            validate_data: self.args.validate_data,

            current_statement_idx: 0,
        })
    }
}

#[derive(Default)]
struct ReadContext {
    pub errors: u64,
    pub rows_read: u64,
}

impl ReadContext {
    pub fn failed_read(&mut self, err: &impl Display) {
        println!("failed to execute a read: {}", err);
        self.errors += 1;
    }
    pub fn data_corruption(&mut self, pk: i64, ck: i64, err: &impl Display) {
        println!("data corruption in pk({}), ck({}): {}", pk, ck, err);
        self.errors += 1;
    }
    pub fn row_read(&mut self) {
        self.rows_read += 1;
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

        let stmt = &self.statements[self.current_statement_idx];
        self.current_statement_idx = (self.current_statement_idx + 1) % self.statements.len();

        let mut iter = self
            .session
            .execute_iter(stmt.clone(), values)
            .await?
            .into_typed::<(i64, Vec<u8>)>();

        let mut rctx = ReadContext::default();

        // TODO: use driver-side timeouts after they get implemented
        loop {
            let r = tokio::time::timeout(self.timeout, iter.next()).await;
            match r {
                Ok(None) => {
                    // End of the iterator
                    break;
                }
                Ok(Some(Ok((ck, v)))) => {
                    rctx.row_read();
                    if self.validate_data {
                        if let Err(err) = super::validate_row_data(pk, ck, &v) {
                            rctx.data_corruption(pk, ck, &err);
                        }
                    }
                }
                Ok(Some(Err(err))) => {
                    // Query error
                    rctx.failed_read(&err);
                }
                Err(err) => {
                    // Timeout
                    rctx.failed_read(&err);
                }
            }
        }

        let mut stats_lock = self.stats.get_shard_mut();
        let stats = &mut *stats_lock;
        stats.operations += 1;
        stats.errors += rctx.errors;
        stats.clustering_rows += rctx.rows_read;
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
    fn get_selector_string(&self) -> String {
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
            ReadRestrictionKind::OnlyLowerBound { .. } => "AND ck >= ?".to_string(),
            ReadRestrictionKind::NoBounds { .. } => "".to_string(),
        }
    }

    fn get_limit_string(&self) -> String {
        match *self {
            ReadRestrictionKind::OnlyLowerBound { limit }
            | ReadRestrictionKind::NoBounds { limit } => {
                format!("LIMIT {}", limit)
            }
            _ => "".to_string(),
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
