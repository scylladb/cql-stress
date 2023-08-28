mod node;

use anyhow::Result;

pub use node::NodeOption;

pub struct Options;

impl Options {
    fn help_messages() -> impl Iterator<Item = (&'static str, &'static str)> {
        [(NodeOption::CLI_STRING, NodeOption::description())].into_iter()
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
            _ => return Err(anyhow::anyhow!("Invalid option provided to command help")),
        }

        Ok(())
    }
}
