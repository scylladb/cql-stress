//! Bulk mode: plain, unchecked load on the shared `run.rs` loop (spec §13).
//!
//! It keeps the cluster busy and full of data. Nothing is recorded or checked. A bulk row is
//! `(pk, f(pk), 0)` with `f` a fixed hash into `[0, 2^40)`: a read finds the row an earlier
//! write created with no shared state, and no bulk row is ever a checked row (gen ≥ 2^60).
//! Retries are allowed here, because a repeated bulk write only rewrites unchecked data.

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use cql_stress::configuration::{make_runnable, Operation, OperationContext, OperationFactory};
use cql_stress::java_generate::distribution::{
    parse_population, Distribution, DistributionFactory,
};
use rand::{random, random_bool, random_range};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::Consistency;
use scylla::value::{CqlValue, Row};

use crate::cli::{BulkOp, BulkReadConsistency, CheckedConsistency, Cli};
use crate::profile::{CellType, Profile};

/// The gen of the bulk row for `pk`: a fixed hash (splitmix64) into `[0, 2^40)`.
pub fn bulk_gen(pk: i64) -> i64 {
    let mut z = (pk as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    ((z ^ (z >> 31)) & ((1 << 40) - 1)) as i64
}

/// The pk of operation `op_id`. A retry has the same id, so it gets the same pk, and a `seq`
/// preload with `-n` covers every pk of its range exactly once.
fn pk_for(pks: &dyn Distribution, op_id: u64) -> i64 {
    pks.set_seed(op_id as i64);
    pks.next_i64()
}

/// Counted by every bulk worker.
#[derive(Default)]
pub struct BulkStats {
    pub ops: AtomicU64,
    /// Reads that found no row.
    pub misses: AtomicU64,
    /// Failed attempts, retried or not.
    pub errors: AtomicU64,
    interval_ops: AtomicU64,
}

impl BulkStats {
    /// Operations since the last call.
    pub fn take_interval(&self) -> u64 {
        self.interval_ops.swap(0, Ordering::Relaxed)
    }
}

struct Shared {
    session: Arc<Session>,
    write: PreparedStatement,
    read: PreparedStatement,
    op: BulkOp,
    read_ratio: f64,
    ops_limit: Option<u64>,
    cells: Vec<CellType>,
    text_size: usize,
    blob_size: usize,
    stats: Arc<BulkStats>,
}

pub struct BulkFactory {
    shared: Arc<Shared>,
    pks: Box<dyn DistributionFactory>,
}

impl BulkFactory {
    pub async fn new(
        session: Arc<Session>,
        profile: &Profile,
        cli: &Cli,
        stats: Arc<BulkStats>,
    ) -> Result<Self> {
        let table = format!("{}.{}", profile.keyspace, profile.bulk_table_name());
        let cells: Vec<String> = (0..profile.cells.len()).map(|i| format!("c{i}")).collect();
        let marks = vec!["?"; cells.len() + 3].join(", ");
        let mut write = session
            .prepare(format!(
                "INSERT INTO {table} (pk, gen, ck, {}) VALUES ({marks})",
                cells.join(", ")
            ))
            .await
            .context("Failed to prepare the bulk write")?;
        write.set_is_idempotent(true);
        write.set_consistency(match cli.consistency {
            CheckedConsistency::Quorum => Consistency::Quorum,
            CheckedConsistency::LocalQuorum => Consistency::LocalQuorum,
            CheckedConsistency::One => Consistency::One,
        });
        write.set_request_timeout(Some(cli.request_timeout));
        let mut read = session
            .prepare(format!(
                "SELECT {} FROM {table} WHERE pk = ? AND gen = ? AND ck = ?",
                cells.join(", ")
            ))
            .await
            .context("Failed to prepare the bulk read")?;
        read.set_is_idempotent(true);
        read.set_consistency(match cli.bulk_read_consistency {
            BulkReadConsistency::Quorum => Consistency::Quorum,
            BulkReadConsistency::One => Consistency::One,
        });
        read.set_request_timeout(Some(cli.request_timeout));

        Ok(Self {
            shared: Arc::new(Shared {
                session,
                write,
                read,
                op: cli.bulk_op,
                read_ratio: cli.bulk_read_ratio,
                ops_limit: cli.ops,
                cells: profile.cells.clone(),
                text_size: profile.text_size,
                blob_size: profile.blob_size,
                stats,
            }),
            pks: parse_population(&cli.bulk_pop)?,
        })
    }
}

impl OperationFactory for BulkFactory {
    fn create(&self) -> Box<dyn Operation> {
        Box::new(BulkOperation {
            shared: self.shared.clone(),
            pks: self.pks.create(),
        })
    }
}

struct BulkOperation {
    shared: Arc<Shared>,
    /// Per worker, so that seeding it with the operation id is not raced.
    pks: Box<dyn Distribution>,
}

make_runnable!(BulkOperation);
impl BulkOperation {
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        let shared = &self.shared;
        if shared.ops_limit.is_some_and(|n| ctx.operation_id >= n) {
            return Ok(ControlFlow::Break(()));
        }
        let pk = pk_for(self.pks.as_ref(), ctx.operation_id);
        let key = (pk, bulk_gen(pk), 0i32);
        let read = match shared.op {
            BulkOp::Read => true,
            BulkOp::Write => false,
            BulkOp::Mixed => random_bool(shared.read_ratio),
        };
        let result = if read {
            self.read(key).await
        } else {
            self.write(key).await
        };
        match result {
            Ok(()) => {
                shared.stats.ops.fetch_add(1, Ordering::Relaxed);
                shared.stats.interval_ops.fetch_add(1, Ordering::Relaxed);
                Ok(ControlFlow::Continue(()))
            }
            Err(err) => {
                shared.stats.errors.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("bulk operation on pk {pk} failed: {err:#}");
                Err(err)
            }
        }
    }

    async fn write(&self, (pk, gen, ck): (i64, i64, i32)) -> Result<()> {
        let shared = &self.shared;
        let mut values = vec![
            CqlValue::BigInt(pk),
            CqlValue::BigInt(gen),
            CqlValue::Int(ck),
        ];
        values.extend(shared.cells.iter().map(|cell| {
            match cell {
                CellType::Int => CqlValue::Int(random()),
                CellType::Bigint => CqlValue::BigInt(random()),
                CellType::Text => CqlValue::Text(
                    (0..shared.text_size)
                        .map(|_| random_range(b'a'..=b'z') as char)
                        .collect(),
                ),
                CellType::Blob => CqlValue::Blob((0..shared.blob_size).map(|_| random()).collect()),
            }
        }));
        shared
            .session
            .execute_unpaged(&shared.write, values)
            .await
            .context("Bulk write failed")?;
        Ok(())
    }

    async fn read(&self, key: (i64, i64, i32)) -> Result<()> {
        let shared = &self.shared;
        let row = shared
            .session
            .execute_unpaged(&shared.read, key)
            .await
            .context("Bulk read failed")?
            .into_rows_result()?
            .maybe_first_row::<Row>()?;
        if row.is_none() {
            shared.stats.misses.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cql_stress::java_generate::distribution::parse_population;

    #[test]
    fn bulk_gens_stay_below_2_pow_40_test() {
        for pk in (0..100_000).chain([i64::MIN, -1, 1 << 40, i64::MAX]) {
            let gen = bulk_gen(pk);
            assert!((0..1 << 40).contains(&gen), "pk {pk}: gen {gen}");
            assert_eq!(gen, bulk_gen(pk), "the key follows from pk alone");
        }
        assert_ne!(bulk_gen(1), bulk_gen(2));
    }

    #[test]
    fn a_retried_operation_gets_the_same_pk_test() {
        let dist = parse_population("seq=100..199").unwrap().create();
        assert_eq!(pk_for(dist.as_ref(), 0), 100);
        assert_eq!(pk_for(dist.as_ref(), 7), 107);
        assert_eq!(pk_for(dist.as_ref(), 7), 107, "same operation id, same pk");
        assert_eq!(pk_for(dist.as_ref(), 100), 100, "the sequence wraps");
    }
}
