use std::{collections::HashMap, fs::File};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::settings::param::types::Parsable;

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
