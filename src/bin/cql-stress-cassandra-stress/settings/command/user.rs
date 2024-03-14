use std::{collections::HashMap, fs::File};

use anyhow::{Context, Result};
use scylla::statement::{Consistency, SerialConsistency};
use serde::{Deserialize, Serialize};

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

pub struct QueryDefinition {
    pub cql: String,
    pub consistency: Option<Consistency>,
    pub serial_consistency: Option<SerialConsistency>,
}

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
        Ok(profile)
    }
}

pub struct UserParams {
    pub keyspace: String,
    pub keyspace_definition: Option<String>,
    pub table: String,
    pub table_definition: Option<String>,
    pub queries: HashMap<String, QueryDefinition>,
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

    fn parse_with_handles(handles: UserParamHandles) -> Result<Self> {
        // 'profile' is a required parameter. This unwrap is safe since parsing was successful.
        let UserProfile {
            keyspace,
            keyspace_definition,
            table,
            table_definition,
            queries,
        } = handles.profile.get().unwrap();

        let queries = queries
            .into_iter()
            .map(|(query_name, query_def)| {
                let query_def = query_def.into_query_definition();
                match query_def {
                    Ok(query_def) => Ok((query_name, query_def)),
                    Err(e) => Err(e.context("Failed to parse query definition")),
                }
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        Ok(Self {
            keyspace,
            keyspace_definition,
            table,
            table_definition,
            queries,
        })
    }
}

struct UserParamHandles {
    profile: SimpleParamHandle<UserProfile>,
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

    for group in groups.iter_mut() {
        group.push(Box::new(profile.clone()));
        parser.group_iter(group.iter().map(|e| e.as_ref()));
    }

    (parser, common_handles, UserParamHandles { profile })
}
