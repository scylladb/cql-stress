mod java_generate;
mod operation;
mod settings;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate async_trait;

use crate::settings::parse_cassandra_stress_args;
use anyhow::Result;
use std::env;

use settings::CassandraStressParsingResult;

#[tokio::main]
async fn main() -> Result<()> {
    // Cassandra-stress CLI is case-insensitive.
    let payload = match parse_cassandra_stress_args(env::args().map(|arg| arg.to_lowercase())) {
        // Special commands: help, print, version
        Ok(CassandraStressParsingResult::SpecialCommand) => return Ok(()),
        Ok(CassandraStressParsingResult::Workload(payload)) => payload,
        Err(e) => {
            // For some reason cassandra-stress writes all parsing-related
            // error messages to stdout. We will follow the same approach.
            println!("\n{:?}", e);
            return Err(anyhow::anyhow!("Failed to parse CLI arguments."));
        }
    };

    payload.print_settings();

    Ok(())
}
