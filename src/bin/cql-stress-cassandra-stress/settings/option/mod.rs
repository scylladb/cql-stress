mod node;
mod rate;
mod schema;

use anyhow::Result;

pub use node::NodeOption;
pub use rate::RateOption;
pub use rate::ThreadsInfo;
pub use schema::SchemaOption;

pub struct Options;

impl Options {
    fn help_messages() -> impl Iterator<Item = (&'static str, &'static str)> {
        [
            (NodeOption::CLI_STRING, NodeOption::description()),
            (RateOption::CLI_STRING, RateOption::description()),
            (SchemaOption::CLI_STRING, SchemaOption::description()),
        ]
        .into_iter()
    }

    pub fn print_generic_help() {
        println!("---Options---");
        for (option, description) in Self::help_messages() {
            println!("{:<20} : {}", option, description);
        }
    }

    pub fn print_help(option_str: &str) -> Result<()> {
        match option_str {
            NodeOption::CLI_STRING => NodeOption::print_help(),
            RateOption::CLI_STRING => RateOption::print_help(),
            SchemaOption::CLI_STRING => SchemaOption::print_help(),
            _ => return Err(anyhow::anyhow!("Invalid option provided to command help")),
        }

        Ok(())
    }
}
