mod counter_write;
mod read;
mod row_generator;
mod write;

use anyhow::Result;
use std::num::Wrapping;

pub use counter_write::CounterWriteOperationFactory;
pub use read::CounterReadOperationFactory;
pub use read::RegularReadOperationFactory;
pub use row_generator::RowGeneratorFactory;
use scylla::{
    frame::response::result::{CqlValue, Row},
    QueryResult,
};
pub use write::WriteOperationFactory;

/// See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/PartitionIterator.java#L725.
fn recompute_seed(seed: i64, partition_key: &CqlValue) -> i64 {
    match partition_key {
        CqlValue::Blob(key) => {
            let mut wrapped = Wrapping(seed);
            for byte in key {
                wrapped = (wrapped * Wrapping(31)) + Wrapping(*byte as i64);
            }
            wrapped.0
        }
        _ => todo!("Implement recompute_seed for other CqlValues"),
    }
}

fn extract_first_row_from_query_result(query_result: &QueryResult) -> Result<&Row> {
    let rows = match &query_result.rows {
        Some(rows) => rows,
        None => anyhow::bail!("Query result doesn't contain any rows.",),
    };

    match rows.split_first() {
        Some((first_row, remaining_rows)) => {
            // Note that row-generation logic behaves in a way that given partition_key,
            // there is exactly one row with this partition_key.
            anyhow::ensure!(
                remaining_rows.is_empty(),
                "Multiple rows matched the key. Rows: {:?}",
                rows
            );
            Ok(first_row)
        }
        None => anyhow::bail!("Query result doesn't contain any rows.",),
    }
}

pub trait RowValidator: Sync + Send + Default {
    fn validate_row(&self, generated_row: &[CqlValue], query_result: QueryResult) -> Result<()>;
}

#[derive(Default)]
pub struct EqualRowValidator;
impl RowValidator for EqualRowValidator {
    fn validate_row(&self, generated_row: &[CqlValue], query_result: QueryResult) -> Result<()> {
        let first_row = extract_first_row_from_query_result(&query_result)?;

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
}

#[derive(Default)]
pub struct ExistsRowValidator;
impl RowValidator for ExistsRowValidator {
    fn validate_row(&self, _generated_row: &[CqlValue], query_result: QueryResult) -> Result<()> {
        // We only check that the row with given PK exists, which is equivalent to
        // successfully extracting the first row from the query result.
        let _first_row = extract_first_row_from_query_result(&query_result)?;
        Ok(())
    }
}
