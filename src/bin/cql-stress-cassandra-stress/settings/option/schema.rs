use crate::settings::{
    param::{MultiParamAcceptsArbitraryHandle, ParamsParser, SimpleParamHandle},
    ParsePayload,
};
use anyhow::Result;
use std::collections::HashMap;

pub struct SchemaOption {
    pub replication_opts: HashMap<String, String>,
    pub keyspace: String,
    pub compaction_opts: HashMap<String, String>,
    pub compression: Option<String>,
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
        Ok(Self::from_handles(handles))
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
    }

    fn from_handles(handles: SchemaParamHandles) -> Self {
        let replication_strategy = handles.replication_strategy.get().unwrap();
        let replication_factor = handles.replication_factor.get().unwrap();
        let mut replication_opts = handles.replication_opts.get_arbitrary().unwrap();
        let keyspace = handles.keyspace.get().unwrap();
        let compaction_strategy = handles.compaction_strategy.get();
        let mut compaction_opts = handles.compaction_opts.get_arbitrary().unwrap();
        let compression = handles.compression.get();

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

        Self {
            replication_opts,
            keyspace,
            compaction_opts,
            compression,
        }
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
        format!(
            "CREATE KEYSPACE IF NOT EXISTS \"{keyspace}\" WITH REPLICATION = {replication};",
            keyspace = self.keyspace,
            replication = self.construct_replication_string()
        )
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
    let replication = parser.multi_param(
        "replication",
        &[&replication_strategy, &replication_factor],
        "Define the replication strategy and any parameters",
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

        let params = SchemaOption::from_handles(handles);

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
    }
}
