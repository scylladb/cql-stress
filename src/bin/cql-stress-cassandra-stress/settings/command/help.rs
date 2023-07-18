use anyhow::Result;

use crate::settings::command::Command;

use super::{CommandParams, ParsePayload};

pub fn print_help() {
    println!("Usage:      cassandra-stress <command> [options]");
    println!("Help usage: cassandra-stress help <command|option>");
    println!();
    Command::print_generic_help();
    // TODO: Add corresponding call for supported options once we introduce them.
}

fn print_command_help(command_str: &str) -> Result<CommandParams> {
    match Command::parse(command_str) {
        Ok(cmd) => {
            cmd.print_help();
            Ok(CommandParams::Special)
        }
        Err(_) => Err(anyhow::anyhow!(
            "Invalid command or option provided to command help"
        )),
    }
}

pub fn parse_help_command(payload: &mut ParsePayload) -> Result<CommandParams> {
    let params = payload.remove("help").unwrap();
    if params.is_empty() && payload.is_empty() {
        print_help();
        return Ok(CommandParams::Special);
    }

    anyhow::ensure!(
        params.len() + payload.len() == 1,
        "Invalid command/option provided to help",
    );

    if !params.is_empty() {
        return print_command_help(params[0]);
    }

    todo!("Implement help for options.");
}
