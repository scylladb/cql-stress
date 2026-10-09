//! Operations on the checked path: driver settings, cell encoders and outcome classes.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use scylla::policies::retry::FallthroughRetryPolicy;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::Consistency;
use scylla::value::{CqlValue, Row};

use crate::cli::CheckedConsistency;
use crate::invariants::Seen;
use crate::keys::RowKey;
use crate::profile::{CellType, Profile};

/// The execution profile of the checked session (spec §8.3).
///
/// A strongly consistent write carries no deduplication token, so a write sent twice is
/// applied twice (spec F8), and one recorded write would become two applied ones. Nothing on
/// this path may resend a request: the retry policy never retries and there is no
/// speculative execution. The driver itself resends without asking the policy only when
/// nothing ran: it moves to the next node when it cannot get a connection, before anything was
/// sent, and it re-prepares and resends after an `Unprepared` error, which the server raises
/// instead of running the statement.
pub fn checked_profile(
    consistency: CheckedConsistency,
    request_timeout: Duration,
) -> ExecutionProfile {
    ExecutionProfile::builder()
        .consistency(match consistency {
            CheckedConsistency::Quorum => Consistency::Quorum,
            CheckedConsistency::LocalQuorum => Consistency::LocalQuorum,
            CheckedConsistency::One => Consistency::One,
        })
        .request_timeout(Some(request_timeout))
        .retry_policy(Arc::new(FallthroughRetryPolicy::new()))
        .speculative_execution_policy(None)
        .build()
}

/// Prepares a checked statement. It is marked not idempotent, so no policy may ever treat it
/// as safe to resend.
pub async fn prepare_checked(session: &Session, query: &str) -> Result<PreparedStatement> {
    let mut statement = session
        .prepare(query)
        .await
        .with_context(|| format!("Failed to prepare {query}"))?;
    statement.set_is_idempotent(false);
    Ok(statement)
}

/// Encodes write id `wid` into a cell of type `cell` (spec §7.2). `size` is the profile's
/// `text_size` or `blob_size` and is ignored for the integer types.
pub fn encode(cell: CellType, wid: u64, size: usize) -> Result<CqlValue> {
    Ok(match cell {
        CellType::Bigint => CqlValue::BigInt(
            i64::try_from(wid).with_context(|| format!("wid {wid} does not fit a bigint cell"))?,
        ),
        CellType::Int => CqlValue::Int(
            i32::try_from(wid).with_context(|| format!("wid {wid} does not fit an int cell"))?,
        ),
        CellType::Text => {
            let mut text = format!("{wid}:");
            text.extend(std::iter::repeat_n('.', size.saturating_sub(text.len())));
            CqlValue::Text(text)
        }
        CellType::Blob => {
            let mut blob = wid.to_le_bytes().to_vec();
            blob.resize(size.max(blob.len()), 0);
            CqlValue::Blob(blob)
        }
    })
}

/// The write id a cell holds. Strict: the value must be exactly what [`encode`] makes of that
/// wid, so a truncated or corrupted value is an error, not a wid (INV-0, spec §10.2).
pub fn decode(cell: CellType, size: usize, value: &CqlValue) -> Result<u64> {
    let wid = match (cell, value) {
        (CellType::Bigint, CqlValue::BigInt(v)) => u64::try_from(*v).unwrap_or(0),
        (CellType::Int, CqlValue::Int(v)) => u64::try_from(*v).unwrap_or(0),
        (CellType::Text, CqlValue::Text(text)) => text
            .split_once(':')
            .and_then(|(digits, _)| digits.parse().ok())
            .unwrap_or(0),
        (CellType::Blob, CqlValue::Blob(blob)) => blob
            .get(..8)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
            .unwrap_or(0),
        _ => anyhow::bail!("a {} cell holds {value:?}", cell.cql_name()),
    };
    // Write ids start at 1, so 0 is never a real one.
    anyhow::ensure!(
        wid != 0 && encode(cell, wid, size)? == *value,
        "{value:?} is not a {} cell written by this tool",
        cell.cql_name()
    );
    Ok(wid)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Read,
    Write,
}

/// What a failed operation means for the history (spec §9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    /// The write may or may not have been applied: recorded with a start and no end.
    Indeterminate,
    /// The write was certainly not applied (`--unavailable-is-fail`): recorded as `fail`, and
    /// its wid is burned.
    Fail,
    /// A bug in the tool or the profile: not recorded, counted, fatal past
    /// `--max-workload-errors`.
    WorkloadError,
    /// A failed read says nothing: not recorded.
    ReadFailed,
}

/// Sorts a failed operation. When in doubt, a write's outcome is unknown: calling an unknown
/// write failed would make false violations, while calling a failed write unknown only costs
/// checker time.
pub fn classify(op: OpKind, error: &ExecutionError, unavailable_is_fail: bool) -> Failure {
    let db_error = match error {
        ExecutionError::LastAttemptError(RequestAttemptError::DbError(db_error, _)) => {
            Some(db_error)
        }
        _ => None,
    };
    let workload_error = matches!(
        db_error,
        Some(
            DbError::SyntaxError
                | DbError::Invalid
                | DbError::Unauthorized
                | DbError::AuthenticationError
                | DbError::ConfigError
                | DbError::AlreadyExists { .. }
        )
    ) || matches!(
        error,
        ExecutionError::BadQuery(_)
            | ExecutionError::LastAttemptError(
                RequestAttemptError::SerializationError(_)
                    | RequestAttemptError::CqlRequestSerialization(_)
            )
    );

    match op {
        _ if workload_error => Failure::WorkloadError,
        OpKind::Read => Failure::ReadFailed,
        OpKind::Write
            if unavailable_is_fail && matches!(db_error, Some(DbError::Unavailable { .. })) =>
        {
            Failure::Fail
        }
        OpKind::Write => Failure::Indeterminate,
    }
}

/// A failed checked operation: its class (spec §9) and the driver's message.
#[derive(Debug)]
pub struct OpError {
    pub class: Failure,
    pub message: String,
}

impl OpError {
    fn new(op: OpKind, error: &ExecutionError, unavailable_is_fail: bool) -> Self {
        Self {
            class: classify(op, error, unavailable_is_fail),
            message: error.to_string(),
        }
    }

    /// A response this tool cannot read: a bug in the tool or the profile.
    fn workload(error: impl std::fmt::Display) -> Self {
        Self {
            class: Failure::WorkloadError,
            message: error.to_string(),
        }
    }
}

/// The write that sets the cells in `mask`: a full-row `INSERT` for all of them, a partial
/// `UPDATE` otherwise (spec §7.2).
fn write_query(profile: &Profile, mask: u8, ttl: u32) -> String {
    let table = format!("{}.{}", profile.keyspace, profile.table);
    let cells: Vec<String> = (0..profile.cells.len())
        .filter(|cell| mask & (1 << cell) != 0)
        .map(|cell| format!("c{cell}"))
        .collect();
    let all = cells.len() == profile.cells.len();
    let using_ttl = |sep: &str| {
        if ttl > 0 {
            format!("{sep}USING TTL {ttl}")
        } else {
            String::new()
        }
    };
    if all {
        let marks = vec!["?"; cells.len() + 3].join(", ");
        format!(
            "INSERT INTO {table} (pk, gen, ck, {}) VALUES ({marks}){}",
            cells.join(", "),
            using_ttl(" ")
        )
    } else {
        let sets: Vec<String> = cells.iter().map(|cell| format!("{cell} = ?")).collect();
        format!(
            "UPDATE {table}{} SET {} WHERE pk = ? AND gen = ? AND ck = ?",
            using_ttl(" "),
            sets.join(", ")
        )
    }
}

/// The checked statements, prepared once at start-up: the read, and one write per cell mask.
pub struct Statements {
    read: PreparedStatement,
    /// `writes[mask]`; index 0 is unused.
    writes: Vec<Option<PreparedStatement>>,
    cells: Vec<CellType>,
    text_size: usize,
    blob_size: usize,
}

impl Statements {
    pub async fn prepare(session: &Session, profile: &Profile, ttl: u32) -> Result<Self> {
        let read = prepare_checked(session, &profile.read_query()).await?;
        let mut writes = vec![None];
        for mask in 1..1u16 << profile.cells.len() {
            let query = write_query(profile, mask as u8, ttl);
            writes.push(Some(prepare_checked(session, &query).await?));
        }
        Ok(Self {
            read,
            writes,
            cells: profile.cells.clone(),
            text_size: profile.text_size,
            blob_size: profile.blob_size,
        })
    }

    /// Writes `wid` into the cells of `mask`.
    pub async fn write(
        &self,
        session: &Session,
        key: &RowKey,
        mask: u8,
        wid: u64,
        unavailable_is_fail: bool,
    ) -> Result<(), OpError> {
        let mut cells = Vec::new();
        for (cell, &cell_type) in self.cells.iter().enumerate() {
            if mask & (1 << cell) != 0 {
                cells
                    .push(encode(cell_type, wid, self.size(cell_type)).map_err(OpError::workload)?);
            }
        }
        let key_values = [
            CqlValue::BigInt(key.pk),
            CqlValue::BigInt(key.gen),
            CqlValue::Int(key.ck),
        ];
        let values: Vec<CqlValue> = if cells.len() == self.cells.len() {
            key_values.into_iter().chain(cells).collect()
        } else {
            cells.into_iter().chain(key_values).collect()
        };
        let statement = self.writes[mask as usize]
            .as_ref()
            .expect("a statement for every non-empty mask");
        session
            .execute_unpaged(statement, values)
            .await
            .map(|_| ())
            .map_err(|error| OpError::new(OpKind::Write, &error, unavailable_is_fail))
    }

    /// Reads every cell of the row; an absent row reads as all null.
    pub async fn read(&self, session: &Session, key: &RowKey) -> Result<Vec<Seen>, OpError> {
        let result = session
            .execute_unpaged(&self.read, (key.pk, key.gen, key.ck))
            .await
            .map_err(|error| OpError::new(OpKind::Read, &error, false))?;
        let row = result
            .into_rows_result()
            .map_err(OpError::workload)?
            .maybe_first_row::<Row>()
            .map_err(OpError::workload)?;
        let Some(row) = row else {
            return Ok(vec![Seen::Null; self.cells.len()]);
        };
        Ok(self
            .cells
            .iter()
            .zip(row.columns)
            .enumerate()
            .map(|(cell, (&cell_type, value))| match value {
                None => Seen::Null,
                Some(value) => match decode(cell_type, self.size(cell_type), &value) {
                    Ok(wid) => Seen::Wid(wid),
                    Err(error) => {
                        // The raw value is the only clue to an INV-0 or a phantom, and the
                        // cluster is gone after the run; cut so a large blob cannot flood the log.
                        let error: String = error.to_string().chars().take(200).collect();
                        tracing::warn!("pk {} gen {} c{cell}: {error}", key.pk, key.gen);
                        Seen::Undecodable
                    }
                },
            })
            .collect())
    }

    fn size(&self, cell_type: CellType) -> usize {
        match cell_type {
            CellType::Text => self.text_size,
            _ => self.blob_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::client::session_builder::SessionBuilder;
    use scylla::errors::{BadQuery, BrokenConnectionErrorKind, ConnectionPoolError, WriteType};
    use scylla::policies::retry::FallthroughRetryPolicy;
    use std::time::Duration;

    fn assert_never_retries(profile: &ExecutionProfile) {
        let retry = profile.get_retry_policy();
        assert!(
            retry
                .as_any()
                .and_then(|any| any.downcast_ref::<FallthroughRetryPolicy>())
                .is_some(),
            "the checked path must never retry: {retry:?}"
        );
        assert!(profile.get_speculative_execution_policy().is_none());
    }

    #[test]
    fn checked_profile_never_retries_test() {
        let profile = checked_profile(CheckedConsistency::Quorum, Duration::from_secs(5));
        assert_never_retries(&profile);
        assert_eq!(profile.get_consistency(), Consistency::Quorum);
        assert_eq!(profile.get_request_timeout(), Some(Duration::from_secs(5)));

        let profile = checked_profile(CheckedConsistency::LocalQuorum, Duration::from_millis(250));
        assert_eq!(profile.get_consistency(), Consistency::LocalQuorum);
        let profile = checked_profile(CheckedConsistency::One, Duration::from_millis(250));
        assert_eq!(profile.get_consistency(), Consistency::One);
        assert_eq!(
            profile.get_request_timeout(),
            Some(Duration::from_millis(250))
        );
    }

    #[test]
    fn cells_round_trip_test() {
        for cell in [
            CellType::Int,
            CellType::Bigint,
            CellType::Text,
            CellType::Blob,
        ] {
            for wid in [1, 2, 9, 10, 4096, i32::MAX as u64] {
                let value = encode(cell, wid, 32).unwrap();
                assert_eq!(decode(cell, 32, &value).unwrap(), wid, "{cell:?} {wid}");
            }
        }
        // Write ids count from 1 within a row; i64::MAX is the largest any cell type can hold.
        let big = i64::MAX as u64;
        for cell in [CellType::Bigint, CellType::Text, CellType::Blob] {
            assert_eq!(
                decode(cell, 32, &encode(cell, big, 32).unwrap()).unwrap(),
                big
            );
        }
    }

    #[test]
    fn cell_encodings_test() {
        assert_eq!(
            encode(CellType::Bigint, 7, 32).unwrap(),
            CqlValue::BigInt(7)
        );
        assert_eq!(encode(CellType::Int, 7, 32).unwrap(), CqlValue::Int(7));
        let CqlValue::Text(text) = encode(CellType::Text, 42, 32).unwrap() else {
            panic!()
        };
        assert_eq!(text.len(), 32);
        assert!(text.starts_with("42:"), "{text}");
        let CqlValue::Blob(blob) = encode(CellType::Blob, 0x0102, 64).unwrap() else {
            panic!()
        };
        assert_eq!(blob.len(), 64);
        assert_eq!(blob[..8], [0x02, 0x01, 0, 0, 0, 0, 0, 0]);
        assert!(
            encode(CellType::Int, 1 << 31, 32).is_err(),
            "int holds wid < 2^31 only"
        );
    }

    #[test]
    fn values_that_do_not_decode_test() {
        let good_text = encode(CellType::Text, 42, 32).unwrap();
        let CqlValue::Text(text) = good_text else {
            panic!()
        };
        let good_blob = encode(CellType::Blob, 42, 32).unwrap();
        let CqlValue::Blob(blob) = good_blob else {
            panic!()
        };
        for (cell, value) in [
            (CellType::Bigint, CqlValue::BigInt(0)),
            (CellType::Bigint, CqlValue::BigInt(-5)),
            (CellType::Int, CqlValue::Int(0)),
            (CellType::Int, CqlValue::BigInt(5)),
            (CellType::Text, CqlValue::Text(text[..31].to_owned())),
            (CellType::Text, CqlValue::Text(text.replace("42:", "42;"))),
            (CellType::Text, CqlValue::Text(format!("x{}", &text[1..]))),
            (CellType::Text, CqlValue::Text(format!("{}y", &text[..31]))),
            (CellType::Blob, CqlValue::Blob(blob[..31].to_vec())),
            (
                CellType::Blob,
                CqlValue::Blob([&blob[..31], &[0xff]].concat()),
            ),
            (CellType::Blob, CqlValue::Blob(vec![0; 32])),
        ] {
            assert!(
                decode(cell, 32, &value).is_err(),
                "{cell:?} {value:?} must not decode"
            );
        }
    }

    fn db(error: DbError) -> ExecutionError {
        ExecutionError::LastAttemptError(RequestAttemptError::DbError(error, "msg".to_owned()))
    }

    fn unavailable() -> ExecutionError {
        db(DbError::Unavailable {
            consistency: Consistency::Quorum,
            required: 2,
            alive: 1,
        })
    }

    #[test]
    fn write_outcomes_test() {
        let indeterminate = [
            db(DbError::WriteTimeout {
                consistency: Consistency::Quorum,
                received: 1,
                required: 2,
                write_type: WriteType::Simple,
            }),
            db(DbError::ServerError),
            db(DbError::Overloaded),
            db(DbError::IsBootstrapping),
            ExecutionError::RequestTimeout(Duration::from_secs(5)),
            ExecutionError::LastAttemptError(RequestAttemptError::BrokenConnectionError(
                BrokenConnectionErrorKind::KeepaliveTimeout([127, 0, 0, 1].into()).into(),
            )),
            ExecutionError::LastAttemptError(RequestAttemptError::UnableToAllocStreamId),
            ExecutionError::EmptyPlan,
            ExecutionError::ConnectionPoolError(ConnectionPoolError::Initializing),
        ];
        for error in indeterminate {
            assert_eq!(
                classify(OpKind::Write, &error, false),
                Failure::Indeterminate,
                "{error}"
            );
            assert_eq!(
                classify(OpKind::Write, &error, true),
                Failure::Indeterminate,
                "{error}"
            );
        }

        assert_eq!(
            classify(OpKind::Write, &unavailable(), false),
            Failure::Indeterminate
        );
        assert_eq!(classify(OpKind::Write, &unavailable(), true), Failure::Fail);
    }

    #[test]
    fn workload_errors_test() {
        for error in [
            db(DbError::Invalid),
            db(DbError::SyntaxError),
            db(DbError::Unauthorized),
            ExecutionError::BadQuery(BadQuery::PartitionKeyExtraction),
        ] {
            for op in [OpKind::Write, OpKind::Read] {
                assert_eq!(
                    classify(op, &error, false),
                    Failure::WorkloadError,
                    "{op:?} {error}"
                );
            }
        }
    }

    #[test]
    fn failed_reads_are_not_recorded_test() {
        for error in [
            unavailable(),
            db(DbError::ServerError),
            ExecutionError::RequestTimeout(Duration::from_secs(5)),
            ExecutionError::EmptyPlan,
        ] {
            assert_eq!(
                classify(OpKind::Read, &error, false),
                Failure::ReadFailed,
                "{error}"
            );
            assert_eq!(
                classify(OpKind::Read, &error, true),
                Failure::ReadFailed,
                "{error}"
            );
        }
    }

    fn test_profile(keyspace: &str) -> Profile {
        Profile::parse(&format!(
            "{{keyspace: {keyspace}, replication_factor: 1, tablets_initial: 1, table: reg, \
             cells: [bigint, text, blob], text_size: 32, blob_size: 64}}"
        ))
        .unwrap()
    }

    #[test]
    fn write_queries_test() {
        let profile = test_profile("ks");
        assert_eq!(
            write_query(&profile, 0b111, 0),
            "INSERT INTO ks.reg (pk, gen, ck, c0, c1, c2) VALUES (?, ?, ?, ?, ?, ?)"
        );
        assert_eq!(
            write_query(&profile, 0b101, 0),
            "UPDATE ks.reg SET c0 = ?, c2 = ? WHERE pk = ? AND gen = ? AND ck = ?"
        );
        assert_eq!(
            write_query(&profile, 0b111, 60),
            "INSERT INTO ks.reg (pk, gen, ck, c0, c1, c2) VALUES (?, ?, ?, ?, ?, ?) USING TTL 60"
        );
        assert_eq!(
            write_query(&profile, 0b010, 60),
            "UPDATE ks.reg USING TTL 60 SET c1 = ? WHERE pk = ? AND gen = ? AND ck = ?"
        );
    }

    /// Writes and reads through the prepared statements. An ordinary keyspace is enough for
    /// binding and decoding, and CI runs these tests against an ordinary node.
    #[tokio::test]
    async fn statements_write_and_read_test() {
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_owned());
        let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
        let keyspace = "sc_verify_ops_test";
        let profile = test_profile(keyspace);
        session
            .query_unpaged(format!("DROP KEYSPACE IF EXISTS {keyspace}"), ())
            .await
            .unwrap();
        session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE {keyspace} WITH replication = \
                     {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
                ),
                (),
            )
            .await
            .unwrap();
        session
            .query_unpaged(profile.table_ddl("reg"), ())
            .await
            .unwrap();

        let statements = Statements::prepare(&session, &profile, 0).await.unwrap();
        let key = RowKey {
            pk: 1,
            gen: 1 << 60,
            ck: 0,
        };
        assert_eq!(
            statements.read(&session, &key).await.unwrap(),
            [Seen::Null; 3]
        );

        statements
            .write(&session, &key, 0b111, 1, false)
            .await
            .unwrap();
        statements
            .write(&session, &key, 0b010, 2, false)
            .await
            .unwrap();
        assert_eq!(
            statements.read(&session, &key).await.unwrap(),
            [Seen::Wid(1), Seen::Wid(2), Seen::Wid(1)]
        );

        // A value no write of this tool produced does not decode.
        session
            .query_unpaged(
                format!("UPDATE {keyspace}.reg SET c1 = 'x' WHERE pk = 1 AND gen = ? AND ck = 0"),
                (key.gen,),
            )
            .await
            .unwrap();
        assert_eq!(
            statements.read(&session, &key).await.unwrap(),
            [Seen::Wid(1), Seen::Undecodable, Seen::Wid(1)]
        );
        session
            .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
            .await
            .unwrap();
    }

    /// The session's statements really get the profile, and are not idempotent.
    #[tokio::test]
    async fn checked_session_and_statements_test() {
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_owned());
        let profile = checked_profile(CheckedConsistency::Quorum, Duration::from_secs(5));
        let session = SessionBuilder::new()
            .known_node(uri)
            .default_execution_profile_handle(profile.into_handle())
            .build()
            .await
            .unwrap();
        assert_never_retries(&session.get_default_execution_profile_handle().to_profile());

        let statement = prepare_checked(&session, "SELECT key FROM system.local")
            .await
            .unwrap();
        assert!(!statement.get_is_idempotent());
        assert!(
            statement.get_execution_profile_handle().is_none(),
            "uses the session's profile"
        );
    }
}
