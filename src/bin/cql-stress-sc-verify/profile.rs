//! The YAML profile: the only definition of the keyspace, the table and its cells (spec §15.1).

use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

/// A text cell holds `"<wid>:"` plus filler, and a wid is a u64: up to 20 digits.
const MIN_TEXT_SIZE: usize = 21;
/// A blob cell holds the wid as 8 little-endian bytes plus filler.
const MIN_BLOB_SIZE: usize = 8;
const MAX_CELLS: usize = 8;

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub keyspace: String,
    pub replication_factor: u32,
    /// The table's `min_tablet_count`, fixed for the whole run: SC tablets are never split or
    /// merged (spec F9).
    pub tablets_initial: u32,
    pub table: String,
    pub cells: Vec<CellType>,
    pub text_size: usize,
    pub blob_size: usize,
    /// `None`: bulk writes `table`, on its own pk range.
    #[serde(default)]
    pub bulk_table: Option<String>,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CellType {
    Int,
    Bigint,
    Text,
    Blob,
}

impl CellType {
    pub fn cql_name(self) -> &'static str {
        match self {
            CellType::Int => "int",
            CellType::Bigint => "bigint",
            CellType::Text => "text",
            CellType::Blob => "blob",
        }
    }
}

impl Profile {
    pub fn load(path: &Path) -> Result<Self> {
        let yaml = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read the profile {}", path.display()))?;
        Self::parse(&yaml).with_context(|| format!("Invalid profile {}", path.display()))
    }

    pub fn parse(yaml: &str) -> Result<Self> {
        let profile: Self = serde_yaml::from_str(yaml)?;
        profile.validate()?;
        Ok(profile)
    }

    fn validate(&self) -> Result<()> {
        // The names go into DDL verbatim, so they must be plain unquoted identifiers.
        for name in [&self.keyspace, &self.table]
            .into_iter()
            .chain(self.bulk_table.as_ref())
        {
            anyhow::ensure!(
                is_identifier(name),
                "{name:?} is not a lower-case CQL identifier ([a-z_][a-z0-9_]*)"
            );
        }
        anyhow::ensure!(
            (1..=MAX_CELLS).contains(&self.cells.len()),
            "A profile has 1 to {MAX_CELLS} cells, not {}",
            self.cells.len()
        );
        anyhow::ensure!(
            self.replication_factor >= 1,
            "replication_factor must be at least 1"
        );
        anyhow::ensure!(
            self.tablets_initial >= 1,
            "tablets_initial must be at least 1"
        );
        anyhow::ensure!(
            self.text_size >= MIN_TEXT_SIZE,
            "text_size must be at least {MIN_TEXT_SIZE}, so that any write id fits"
        );
        anyhow::ensure!(
            self.blob_size >= MIN_BLOB_SIZE,
            "blob_size must be at least {MIN_BLOB_SIZE}, so that any write id fits"
        );
        Ok(())
    }

    /// `strongly_consistent` is false only for the test-only `--unsafe-eventual`.
    pub fn keyspace_ddl(&self, strongly_consistent: bool) -> String {
        format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = \
             {{'class': 'NetworkTopologyStrategy', 'replication_factor': {}}} \
             AND tablets = {{'enabled': true}}{}",
            self.keyspace,
            self.replication_factor,
            if strongly_consistent {
                " AND consistency = 'global'"
            } else {
                ""
            }
        )
    }

    /// The DDL of the checked table or of the bulk table: both have the same columns. The
    /// tablet count is a table option; ScyllaDB deprecates the keyspace's `initial`.
    pub fn table_ddl(&self, table: &str) -> String {
        let cells: String = self
            .cells
            .iter()
            .enumerate()
            .map(|(i, cell)| format!("c{i} {}, ", cell.cql_name()))
            .collect();
        format!(
            "CREATE TABLE IF NOT EXISTS {}.{table} (pk bigint, gen bigint, ck int, \
             {cells}PRIMARY KEY ((pk), gen, ck)) \
             WITH tablets = {{'min_tablet_count': {}}}",
            self.keyspace, self.tablets_initial
        )
    }

    /// The checked read: every cell of one row by its full key.
    pub fn read_query(&self) -> String {
        let cells: Vec<String> = (0..self.cells.len()).map(|i| format!("c{i}")).collect();
        format!(
            "SELECT {} FROM {}.{} WHERE pk = ? AND gen = ? AND ck = ?",
            cells.join(", "),
            self.keyspace,
            self.table
        )
    }

    pub fn bulk_table_name(&self) -> &str {
        self.bulk_table.as_deref().unwrap_or(&self.table)
    }
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT: &str = "
keyspace: sc_verify
replication_factor: 3
tablets_initial: 128
table: reg
cells: [bigint, text, blob]
text_size: 32
blob_size: 64
bulk_table: null
";

    fn with(line_from: &str, line_to: &str) -> String {
        assert!(DEFAULT.contains(line_from), "{line_from}");
        DEFAULT.replace(line_from, line_to)
    }

    #[test]
    fn default_profile_ddl_test() {
        let profile = Profile::parse(DEFAULT).unwrap();
        assert_eq!(
            profile.keyspace_ddl(true),
            "CREATE KEYSPACE IF NOT EXISTS sc_verify WITH replication = \
             {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} \
             AND tablets = {'enabled': true} AND consistency = 'global'"
        );
        // --unsafe-eventual: the same keyspace without strong consistency.
        assert_eq!(
            profile.keyspace_ddl(false),
            "CREATE KEYSPACE IF NOT EXISTS sc_verify WITH replication = \
             {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} \
             AND tablets = {'enabled': true}"
        );
        assert_eq!(
            profile.table_ddl(&profile.table),
            "CREATE TABLE IF NOT EXISTS sc_verify.reg (pk bigint, gen bigint, ck int, \
             c0 bigint, c1 text, c2 blob, PRIMARY KEY ((pk), gen, ck)) \
             WITH tablets = {'min_tablet_count': 128}"
        );
        assert_eq!(
            profile.cells,
            [CellType::Bigint, CellType::Text, CellType::Blob]
        );
        assert_eq!(profile.bulk_table, None);
        assert_eq!(profile.bulk_table_name(), "reg");
    }

    #[test]
    fn bulk_table_test() {
        let profile = Profile::parse(&with("bulk_table: null", "bulk_table: bulk")).unwrap();
        assert_eq!(profile.bulk_table_name(), "bulk");
        assert!(profile
            .table_ddl("bulk")
            .starts_with("CREATE TABLE IF NOT EXISTS sc_verify.bulk ("));
    }

    #[test]
    fn one_to_eight_cells_of_any_type_test() {
        let eight = with(
            "cells: [bigint, text, blob]",
            "cells: [int, bigint, text, blob, int, int, int, int]",
        );
        let profile = Profile::parse(&eight).unwrap();
        assert_eq!(profile.cells.len(), 8);
        assert!(profile
            .table_ddl("reg")
            .contains("c0 int, c1 bigint, c2 text, c3 blob, c4 int"));
    }

    #[test]
    fn rejected_profiles_test() {
        for (from, to) in [
            ("cells: [bigint, text, blob]", "cells: []"),
            (
                "cells: [bigint, text, blob]",
                "cells: [int, int, int, int, int, int, int, int, int]",
            ),
            ("cells: [bigint, text, blob]", "cells: [varchar]"),
            ("text_size: 32", "text_size: 20"),
            ("blob_size: 64", "blob_size: 7"),
            ("replication_factor: 3", "replication_factor: 0"),
            ("tablets_initial: 128", "tablets_initial: 0"),
            ("keyspace: sc_verify", "keyspace: \"sc; DROP KEYSPACE x\""),
            ("table: reg", "table: Reg"),
            ("bulk_table: null", "bulk_table: \"x y\""),
            ("bulk_table: null", "bulk_tabel: null"),
            ("table: reg\n", ""),
        ] {
            let yaml = with(from, to);
            assert!(Profile::parse(&yaml).is_err(), "must be rejected: {to:?}");
        }
    }
}
