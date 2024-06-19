use std::{collections::HashMap, fs::File, sync::Arc};

use anyhow::{Context, Result};
use scylla::prepared_statement::PreparedStatement;
use scylla::statement::{Consistency, SerialConsistency};
use scylla::Session;
use serde::{Deserialize, Serialize};

use crate::java_generate::distribution::DistributionFactory;
use crate::settings::param::types::RatioMap;
use crate::settings::{
    param::{types::Parsable, ParamsParser, SimpleParamHandle},
    ParsePayload,
};

use super::{
    common::{CommonParamHandles, ConsistencyLevel, SerialConsistencyLevel},
    Command, CommandParams,
};

#[derive(Deserialize, Serialize, PartialEq, Debug)]
#[serde(deny_unknown_fields)]
pub struct UserProfile {
    pub keyspace: String,
    pub keyspace_definition: Option<String>,
    pub table: String,
    pub table_definition: Option<String>,
    pub queries: HashMap<String, QueryDefinitionYaml>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueryDefinitionYaml {
    pub cql: String,
    pub consistency_level: Option<String>,
    pub serial_consistency_level: Option<String>,
}

impl QueryDefinitionYaml {
    fn into_query_definition(self) -> Result<QueryDefinition> {
        let cql = self.cql;
        let consistency = self
            .consistency_level
            .map(|c| ConsistencyLevel::parse(&c))
            .transpose()?;
        let serial_consistency = self
            .serial_consistency_level
            .map(|sc| SerialConsistencyLevel::parse(&sc))
            .transpose()?;

        Ok(QueryDefinition {
            cql,
            consistency,
            serial_consistency,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryDefinition {
    pub cql: String,
    pub consistency: Option<Consistency>,
    pub serial_consistency: Option<SerialConsistency>,
}

impl QueryDefinition {
    pub async fn to_prepared_statement(&self, session: &Arc<Session>) -> Result<PreparedStatement> {
        let mut statement = session
            .prepare(&*self.cql)
            .await
            .with_context(|| format!("Failed to prepare statement: {}", self.cql))?;

        if let Some(consistency) = self.consistency {
            statement.set_consistency(consistency);
        }
        if self.serial_consistency.is_some() {
            statement.set_serial_consistency(self.serial_consistency);
        }

        Ok(statement)
    }
}

pub const PREDEFINED_INSERT_OPERATION: &str = "insert";

impl Parsable for UserProfile {
    type Parsed = Self;

    fn parse(s: &str) -> Result<Self::Parsed> {
        let yaml =
            File::open(s).with_context(|| format!("Invalid profile yaml filepath: {}", s))?;
        let profile: UserProfile =
            serde_yaml::from_reader(yaml).context("Failed to parse profile yaml file")?;
        anyhow::ensure!(
            !profile.queries.is_empty(),
            "'queries' map cannot be empty. Please define at least one query."
        );
        anyhow::ensure!(
            !profile.queries.contains_key(PREDEFINED_INSERT_OPERATION),
            "'{PREDEFINED_INSERT_OPERATION}' is a reserved name for the operation. See help message for user command for more details."
        );
        Ok(profile)
    }
}

/// Weight with which operation/query will be sampled.
pub type OpWeight = f64;

pub struct UserParams {
    pub keyspace: String,
    pub keyspace_definition: Option<String>,
    pub table: String,
    pub table_definition: Option<String>,
    // Maps a query name to query definition and a ratio with which
    // this query will be sampled.
    pub queries_payload: HashMap<String, (QueryDefinition, OpWeight)>,
    pub clustering: Arc<dyn DistributionFactory>,
    pub insert_operation_weight: Option<OpWeight>,
}

impl UserParams {
    pub fn parse(cmd: &Command, payload: &mut ParsePayload) -> Result<CommandParams> {
        let args = payload.remove(cmd.show()).unwrap_or_default();
        let (parser, common_handles, user_handles) = prepare_parser(cmd.show());
        parser.parse(args)?;
        Ok(CommandParams {
            common: super::common::parse_with_handles(common_handles),
            counter: None,
            mixed: None,
            user: Some(Self::parse_with_handles(user_handles)?),
        })
    }

    pub fn print_help(command_str: &str) {
        let (parser, _, _) = prepare_parser(command_str);
        parser.print_help();
    }

    pub async fn create_schema(&self, session: &Session) -> Result<()> {
        if let Some(keyspace_definition) = &self.keyspace_definition {
            session
                .query(keyspace_definition.as_str(), ())
                .await
                .context("Failed to create keyspace based on user profile")?;
        }
        session.use_keyspace(&self.keyspace, true).await?;

        if let Some(table_definition) = &self.table_definition {
            session
                .query(table_definition.as_str(), ())
                .await
                .context("Failed to create table based on user profile")?;
        }

        Ok(())
    }

    fn parse_with_handles(handles: UserParamHandles) -> Result<Self> {
        // 'profile' is a required parameter. This unwrap is safe since parsing was successful.
        let UserProfile {
            keyspace,
            keyspace_definition,
            table,
            table_definition,
            mut queries,
        } = handles.profile.get().unwrap();
        let mut queries_ratio = handles.ratio.get().unwrap();
        let clustering: Arc<dyn DistributionFactory> = handles.clustering.get().unwrap().into();

        // Handle the `insert` operation separately. This operation is not defined in the yaml file.
        // Its behaviour is predefined by the tool.
        let insert_operation_weight = queries_ratio.remove(PREDEFINED_INSERT_OPERATION);

        let queries_payload = queries_ratio
            .into_iter()
            .map(
                |(query_name, weight)| -> Result<(String, (QueryDefinition, OpWeight))> {
                    let query_def = queries
                        .remove(&query_name)
                        .ok_or_else(|| {
                            anyhow::anyhow!("Unrecognized query name in ratio map: {}", query_name)
                        })?
                        .into_query_definition()
                        .context("Failed to parse query definition")?;

                    Ok((query_name, (query_def, weight)))
                },
            )
            .collect::<Result<HashMap<_, _>, _>>()?;

        Ok(Self {
            keyspace,
            keyspace_definition,
            table,
            table_definition,
            queries_payload,
            clustering,
            insert_operation_weight,
        })
    }
}

struct UserParamHandles {
    profile: SimpleParamHandle<UserProfile>,
    ratio: SimpleParamHandle<RatioMap>,
    clustering: SimpleParamHandle<Box<dyn DistributionFactory>>,
}

fn prepare_parser(cmd: &str) -> (ParamsParser, CommonParamHandles, UserParamHandles) {
    let mut parser = ParamsParser::new(cmd);

    let (mut groups, common_handles) = super::common::add_common_param_groups(&mut parser);

    let profile = parser.simple_param(
        "profile=",
        None,
        "Specify the path to a yaml cql3 profile",
        true,
    );
    let ratio = parser.simple_param(
        "ops",
        None,
        "Specify the ratios for inserts/queries to perform; e.g. ops(insert=2,<query1>=1) will perform 2 inserts for each query1. 
        'insert' is a reserved name for an operation, thus query with such name cannot be defined in a profile yaml.",
        true
    );
    let clustering = parser.simple_param(
        "clustering=",
        Some("GAUSSIAN(1..10)"),
        "Distribution clustering runs of operations of the same kind",
        false,
    );

    for group in groups.iter_mut() {
        group.push(Box::new(profile.clone()));
        group.push(Box::new(ratio.clone()));
        group.push(Box::new(clustering.clone()));
        parser.group_iter(group.iter().map(|e| e.as_ref()));
    }

    (
        parser,
        common_handles,
        UserParamHandles {
            profile,
            ratio,
            clustering,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use scylla::statement::{Consistency, SerialConsistency};

    use crate::settings::{
        command::user::{prepare_parser, QueryDefinition, UserParams, UserProfile},
        param::types::Parsable,
    };

    fn build_file_path(filename: &str) -> String {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("src/bin/cql-stress-cassandra-stress/settings/command/test_user_profile_yamls");
        d.push(filename);
        d.into_os_string().into_string().unwrap()
    }

    #[test]
    fn minimal_user_profile_yaml_contents_test() {
        let yaml_filepath = build_file_path("minimal_profile.yaml");

        let profile = UserProfile::parse(&yaml_filepath).unwrap();
        assert_eq!("foo", profile.keyspace);
        assert_eq!("bar", profile.table);
        assert_eq!(None, profile.keyspace_definition);
        assert_eq!(None, profile.table_definition);

        assert_eq!(1, profile.queries.len());
        let query = profile.queries.get("baz").unwrap();
        assert_eq!("select c1 from standard1 where pkey = ?", query.cql);
        assert_eq!(None, query.consistency_level);
        assert_eq!(None, query.serial_consistency_level);
    }

    #[test]
    fn empty_queries_profile_yaml_contents_test() {
        let yaml_filepath = build_file_path("empty_queries_profile.yaml");

        let profile = UserProfile::parse(&yaml_filepath);
        assert!(profile.is_err());
    }

    #[test]
    fn missing_keyspace_profile_yaml_contents_test() {
        let yaml_filepath = build_file_path("missing_keyspace_profile.yaml");

        let profile = UserProfile::parse(&yaml_filepath);
        assert!(profile.is_err());
    }

    #[test]
    fn unknown_field_profile_yaml_contents_test() {
        let yaml_filepath = build_file_path("unknown_field_profile.yaml");

        let profile = UserProfile::parse(&yaml_filepath);
        assert!(profile.is_err());
    }

    #[test]
    fn unknown_query_field_profile_yaml_contents_test() {
        let yaml_filepath = build_file_path("unknown_query_field_profile.yaml");

        let profile = UserProfile::parse(&yaml_filepath);
        assert!(profile.is_err());
    }

    #[test]
    fn full_profile_yaml_contents_test() {
        let yaml_filepath = build_file_path("full_profile.yaml");

        let profile = UserProfile::parse(&yaml_filepath).unwrap();
        assert_eq!("keyspace2", profile.keyspace);
        assert!(profile.keyspace_definition.is_some());
        assert_eq!("standard1", profile.table);
        assert!(profile.table_definition.is_some());

        assert_eq!(2, profile.queries.len());

        let ins_query = profile.queries.get("ins").unwrap();
        assert_eq!(
            "insert into standard1 (pkey, ckey, c1) values (?, ?, ?)",
            ins_query.cql
        );
        assert_eq!(Some("local_one".to_string()), ins_query.consistency_level);
        assert_eq!(
            Some("local_serial".to_string()),
            ins_query.serial_consistency_level
        );

        let read_query = profile.queries.get("read").unwrap();
        assert_eq!("select c1 from standard1 where pkey = ?", read_query.cql);
        assert_eq!(Some("quorum".to_string()), read_query.consistency_level);
        assert_eq!(
            Some("serial".to_string()),
            read_query.serial_consistency_level
        );
    }

    #[test]
    fn full_profile_yaml_default_values() {
        let yaml_filepath = build_file_path("full_profile.yaml");
        // full_profile.yaml defines two queries: 'ins' and 'read'.
        // Parsing should fail since 'foo' is provided via ops() parameter.
        let profile_arg = format!("profile={yaml_filepath}");
        let args = vec![&profile_arg, "ops(ins=1,read=2)"];

        let (parser, _common_handles, user_handles) = prepare_parser("user");
        parser.parse(args).unwrap();

        let user = UserParams::parse_with_handles(user_handles).unwrap();

        assert_eq!(
            "GAUSSIAN(1..10,mean=5.5,stdev=1.5)",
            format!("{}", user.clustering)
        );
        assert_eq!(2, user.queries_payload.len());

        let ins = user.queries_payload.get("ins").unwrap();
        assert_eq!(
            &(
                QueryDefinition {
                    cql: "insert into standard1 (pkey, ckey, c1) values (?, ?, ?)".to_owned(),
                    consistency: Some(Consistency::LocalOne),
                    serial_consistency: Some(SerialConsistency::LocalSerial)
                },
                1.0
            ),
            ins
        );

        let read = user.queries_payload.get("read").unwrap();
        assert_eq!(
            &(
                QueryDefinition {
                    cql: "select c1 from standard1 where pkey = ?".to_owned(),
                    consistency: Some(Consistency::Quorum),
                    serial_consistency: Some(SerialConsistency::Serial)
                },
                2.0
            ),
            read
        );
    }

    #[test]
    fn full_profile_yaml_unknown_query() {
        let yaml_filepath = build_file_path("full_profile.yaml");
        // full_profile.yaml defines two queries: 'ins' and 'read'.
        // Parsing should fail since 'foo' is provided via ops() parameter.
        let profile_arg = format!("profile={yaml_filepath}");
        let args = vec![&profile_arg, "ops(ins=1,foo=2)"];

        let (parser, _common_handles, user_handles) = prepare_parser("user");
        parser.parse(args).unwrap();
        assert!(UserParams::parse_with_handles(user_handles).is_err());
    }
}
