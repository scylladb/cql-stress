use crate::settings::{
    param::{MultiParamAcceptsArbitraryHandle, ParamsParser, SimpleParamHandle},
    ParsePayload,
};
use anyhow::Result;
use std::collections::HashMap;

/// The key recognised inside `replication(...)` which is hoisted out of the CQL
/// replication map into the top-level `consistency` keyspace property.
const CONSISTENCY_KEY: &str = "consistency";

#[derive(Debug)]
pub struct SchemaOption {
    pub replication_opts: HashMap<String, String>,
    pub keyspace: String,
    pub compaction_opts: HashMap<String, String>,
    pub compression: Option<String>,
    /// Keyspace-level consistency mode, i.e. the `consistency` keyspace property.
    ///
    /// `Some("global")` requests a strongly consistent (Raft-per-tablet) keyspace.
    /// `None` means the clause is omitted from the DDL entirely - which is what
    /// every pre-existing invocation must keep producing, since emitting the
    /// clause at all fails on servers without the `strongly-consistent-tables`
    /// experimental feature.
    pub consistency: Option<String>,
}

impl SchemaOption {
    pub fn description() -> &'static str {
        "Replication settings, compression, compaction, etc."
    }

    pub const CLI_STRING: &'static str = "-schema";

    pub fn parse(cl_args: &mut ParsePayload) -> Result<Self> {
        let params = cl_args.remove(Self::CLI_STRING).unwrap_or_default();
        let (parser, handles) = prepare_parser();
        parser.parse(params)?;
        Self::from_handles(handles)
    }

    pub fn print_help() {
        let (parser, _) = prepare_parser();
        parser.print_help();
    }

    pub fn print_settings(&self) {
        println!("Schema:");
        println!("  Keyspace: {}", self.keyspace);
        println!(
            "  Replication Strategy Options: {:?}",
            self.replication_opts
        );
        println!("  Table Compression: {:?}", self.compression);
        println!("  Table Compaction Options: {:?}", self.compaction_opts);
        println!("  Keyspace Consistency: {:?}", self.consistency);
    }

    fn from_handles(handles: SchemaParamHandles) -> Result<Self> {
        let replication_strategy = handles.replication_strategy.get().unwrap();
        let replication_factor = handles.replication_factor.get().unwrap();
        let mut replication_opts = handles.replication_opts.get_arbitrary().unwrap();
        let keyspace = handles.keyspace.get().unwrap();
        let compaction_strategy = handles.compaction_strategy.get();
        let mut compaction_opts = handles.compaction_opts.get_arbitrary().unwrap();
        let compression = handles.compression.get();

        // `consistency` is a top-level keyspace property, not a replication option, so it
        // must be lifted out of the map before the `class`/`replication_factor` defaults
        // are inserted. Leaving it in would produce a replication map that the server
        // rejects, since the map is validated by the replication strategy itself.
        let consistency = replication_opts.remove(CONSISTENCY_KEY);
        if let Some(consistency) = consistency.as_deref() {
            match consistency {
                "global" | "eventual" => (),
                // Mirror the server's own wording so the failure is recognisable, and
                // raise it at parse time rather than after a cluster round-trip.
                "local" => anyhow::bail!("Local consistency is not supported yet"),
                other => anyhow::bail!(
                    "Invalid keyspace consistency: {other}. Must be one of: global, eventual"
                ),
            }
        }

        replication_opts
            .entry(String::from("replication_factor"))
            .or_insert_with(|| replication_factor.to_string());
        replication_opts
            .entry(String::from("class"))
            .or_insert(replication_strategy);

        if let Some(compaction_strategy) = compaction_strategy {
            compaction_opts
                .entry(String::from("class"))
                .or_insert(compaction_strategy);
        }

        Ok(Self {
            replication_opts,
            keyspace,
            compaction_opts,
            compression,
            consistency,
        })
    }

    /// True when the user explicitly asked for a strongly consistent keyspace.
    pub fn wants_strong_consistency(&self) -> bool {
        self.consistency.as_deref() == Some("global")
    }

    fn construct_replication_string(&self) -> String {
        let options_str = self
            .replication_opts
            .iter()
            .map(|(key, value)| format!("'{key}': '{value}'"))
            .collect::<Vec<_>>()
            .join(", ");

        format!("{{{options_str}}}")
    }

    pub fn construct_keyspace_creation_query(&self) -> String {
        let mut query = format!(
            "CREATE KEYSPACE IF NOT EXISTS \"{keyspace}\" WITH REPLICATION = {replication}",
            keyspace = self.keyspace,
            replication = self.construct_replication_string()
        );
        // Only emit the clause when the user asked for it. A default of 'eventual' would
        // fail on every server without the `strongly-consistent-tables` feature and break
        // all existing eventually-consistent runs.
        if let Some(consistency) = &self.consistency {
            query += &format!(" AND consistency = '{consistency}'");
        }
        query += ";";
        query
    }

    fn construct_compaction_string(&self) -> Option<String> {
        (!self.compaction_opts.is_empty()).then(|| {
            let options_str = self
                .compaction_opts
                .iter()
                .map(|(key, value)| format!("'{key}': '{value}'"))
                .collect::<Vec<_>>()
                .join(", ");

            format!(" AND compaction = {{{options_str}}}")
        })
    }

    // For now the types can be either 'counter' or 'blob'. Once we introduce the USER command,
    // we will allow the user to define some other types to use.
    fn construct_table_creation_query_with(
        &self,
        table_name: &'static str,
        column_type: &'static str,
        column_names: &[String],
    ) -> String {
        // Note that for now we hardcode the columns.
        // In the future, `-col` option will be supported, that lets the user define column names as well as the number of columns.
        let mut result = format!("CREATE TABLE IF NOT EXISTS {table_name} (key blob");
        for column in column_names {
            result += &format!(", \"{column}\" {column_type}");
        }
        result += ", PRIMARY KEY (key))";
        result += " WITH compression = {";
        if let Some(compression) = &self.compression {
            result += &format!("'sstable_compression': '{compression}'");
        }
        result += "}";
        if let Some(compaction_str) = self.construct_compaction_string() {
            result += &compaction_str;
        }
        result += ";";
        result
    }

    pub fn construct_table_creation_query(&self, column_names: &[String]) -> String {
        self.construct_table_creation_query_with("standard1", "blob", column_names)
    }

    pub fn construct_counter_table_creation_query(&self, column_names: &[String]) -> String {
        self.construct_table_creation_query_with("counter1", "counter", column_names)
    }
}

struct SchemaParamHandles {
    replication_factor: SimpleParamHandle<u64>,
    replication_strategy: SimpleParamHandle<String>,
    replication_opts: MultiParamAcceptsArbitraryHandle,
    keyspace: SimpleParamHandle<String>,
    compaction_strategy: SimpleParamHandle<String>,
    compaction_opts: MultiParamAcceptsArbitraryHandle,
    compression: SimpleParamHandle<String>,
}

fn prepare_parser() -> (ParamsParser, SchemaParamHandles) {
    let mut parser = ParamsParser::new(SchemaOption::CLI_STRING);

    let replication_strategy = parser.simple_subparam(
        "strategy=",
        Some("SimpleStrategy"),
        "The replication strategy to use",
        false,
    );
    let replication_factor =
        parser.simple_subparam("factor=", Some("1"), "The number of replicas", false);
    // Multiparameter with two predefined parameters: `strategy` and `factor`.
    // Arbitrary keys are passed straight through into the CQL replication map, with the
    // single exception of `consistency`, which is a cql-stress extension hoisted into the
    // top-level `consistency` keyspace property. See `SchemaOption::from_handles`.
    let replication = parser.multi_param(
        "replication",
        &[&replication_strategy, &replication_factor],
        "Define the replication strategy and any parameters. \
         The `consistency` key is a cql-stress extension: it is lifted out of the \
         replication map into the keyspace-level `consistency` property \
         (global|eventual); omitted from the DDL entirely when not given",
        false,
    );
    let keyspace = parser.simple_param(
        "keyspace=",
        Some("keyspace1"),
        "The keyspace name to use",
        false,
    );
    let compaction_strategy =
        parser.simple_subparam("strategy=", None, "The compaction strategy to use", false);
    let compaction = parser.multi_param(
        "compaction",
        &[&compaction_strategy],
        "Define the compaction strategy and any parameters",
        false,
    );
    let compression = parser.simple_param(
        "compression=",
        None,
        "Specify the compression to use for sstable, default:no compression",
        false,
    );

    // $ ./cassandra-stress help -schema
    // Usage: -schema [replication(?)] [keyspace=?] [compaction(?)] [compression=?]
    parser.group(&[&replication, &keyspace, &compaction, &compression]);

    (
        parser,
        SchemaParamHandles {
            replication_factor,
            replication_strategy,
            replication_opts: replication,
            keyspace,
            compaction_strategy,
            compaction_opts: compaction,
            compression,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{prepare_parser, SchemaOption};

    #[test]
    fn schema_param_good_test() {
        let args = vec![
            "replication(factor=3,key1=value1,strategy=MyStrategy,key2=value2)",
            "keyspace=my_keyspace",
            "compaction(key1=value1)",
        ];

        let (parser, handles) = prepare_parser();
        assert!(parser.parse(args).is_ok());

        let params = SchemaOption::from_handles(handles).unwrap();

        assert_eq!(4, params.replication_opts.len());
        assert_eq!(
            Some("3"),
            params
                .replication_opts
                .get("replication_factor")
                .map(String::as_str)
        );
        assert_eq!(
            Some("MyStrategy"),
            params.replication_opts.get("class").map(String::as_str)
        );
        assert_eq!(
            Some("value1"),
            params.replication_opts.get("key1").map(String::as_str)
        );
        assert_eq!(
            Some("value2"),
            params.replication_opts.get("key2").map(String::as_str)
        );
        assert_eq!("my_keyspace", params.keyspace);
        assert_eq!(1, params.compaction_opts.len());
        assert_eq!(
            Some("value1"),
            params.compaction_opts.get("key1").map(String::as_str)
        );
        assert_eq!(None, params.compression);
        assert_eq!(None, params.consistency);
    }

    fn parse_schema(args: Vec<&str>) -> anyhow::Result<SchemaOption> {
        let (parser, handles) = prepare_parser();
        parser.parse(args)?;
        SchemaOption::from_handles(handles)
    }

    /// `consistency` must be hoisted out of the replication map and emitted as a
    /// top-level keyspace clause - it is not a valid key inside `REPLICATION = {...}`.
    #[test]
    fn schema_consistency_is_hoisted_out_of_replication_map_test() {
        let params = parse_schema(vec![
            "replication(strategy=NetworkTopologyStrategy,replication_factor=3,consistency=global)",
        ])
        .unwrap();

        assert_eq!(Some("global"), params.consistency.as_deref());
        assert!(params.wants_strong_consistency());
        assert_eq!(None, params.replication_opts.get("consistency"));
        assert_eq!(2, params.replication_opts.len());

        let query = params.construct_keyspace_creation_query();
        assert!(
            query.ends_with(" AND consistency = 'global';"),
            "unexpected DDL: {query}"
        );
        assert!(!query.contains("'consistency'"), "unexpected DDL: {query}");
    }

    /// Backward compatibility: without `consistency`, the DDL must be byte-for-byte
    /// what master emits. Emitting `consistency = 'eventual'` would fail on any server
    /// lacking the `strongly-consistent-tables` experimental feature.
    #[test]
    fn schema_without_consistency_emits_unchanged_ddl_test() {
        let params = parse_schema(vec!["replication(strategy=SimpleStrategy,factor=1)"]).unwrap();

        assert_eq!(None, params.consistency);
        assert!(!params.wants_strong_consistency());
        assert_eq!(
            "CREATE KEYSPACE IF NOT EXISTS \"keyspace1\" WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'};",
            // `replication_opts` is a HashMap, so normalise the map ordering before
            // comparing against the expected literal.
            sort_replication_map(&params.construct_keyspace_creation_query()),
        );
    }

    #[test]
    fn schema_consistency_local_is_rejected_test() {
        let err = parse_schema(vec!["replication(consistency=local)"]).unwrap_err();
        assert_eq!("Local consistency is not supported yet", err.to_string());
    }

    #[test]
    fn schema_consistency_unknown_value_is_rejected_test() {
        let err = parse_schema(vec!["replication(consistency=strong)"]).unwrap_err();
        assert!(
            err.to_string().contains("Invalid keyspace consistency"),
            "unexpected error: {err}"
        );
    }

    /// Sorts the entries of the `REPLICATION = {...}` map so that a query built from a
    /// `HashMap` can be compared against a fixed literal.
    fn sort_replication_map(query: &str) -> String {
        let (prefix, rest) = query.split_once('{').unwrap();
        let (map, suffix) = rest.split_once('}').unwrap();
        let mut entries = map.split(", ").collect::<Vec<_>>();
        entries.sort_unstable();
        format!("{prefix}{{{}}}{suffix}", entries.join(", "))
    }
}
