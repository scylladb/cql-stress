use anyhow::Result;
use std::{collections::HashMap, path::PathBuf};

#[derive(Clone, Debug)]
pub struct LogOption {
    pub hdr_file: Option<PathBuf>,
    pub interval: u64,
}

impl Default for LogOption {
    fn default() -> Self {
        Self {
            hdr_file: None,
            interval: 1,
        }
    }
}

impl LogOption {
    pub const CLI_STRING: &'static str = "-log";

    pub fn description() -> &'static str {
        "Specify logging options"
    }

    pub fn print_help() {
        println!("Usage: {} <param list>", Self::CLI_STRING);
        println!("    hdrfile=<filename>   - Log HDR Histogram data to the specified file");
        println!(
            "    interval=<seconds>   - Set the interval between logs in seconds (default: 1)"
        );
    }

    pub fn print_settings(&self) {
        println!("Log:");
        if let Some(path) = &self.hdr_file {
            println!("  HDR Histogram file: {}", path.display());
        }
        println!("  Log interval: {} seconds", self.interval);
    }

    pub fn parse(args: &[&str]) -> Result<Self> {
        println!("Parsing -log with args: {:?}", args);
        if args.is_empty() {
            return Ok(Self::default());
        }

        let mut hdr_file = None;
        let mut interval = 1;

        for arg in args {
            if let Some(path) = arg.strip_prefix("hdrfile=") {
                let path = PathBuf::from(path);
                hdr_file = Some(path);
            } else if let Some(interval_str) = arg.strip_prefix("interval=") {
                interval = interval_str
                    .parse::<u64>()
                    .map_err(|_| anyhow::anyhow!("Invalid interval value: {:?}", interval_str))?;
            } else {
                return Err(anyhow::anyhow!("Unknown parameter: {:?}", arg));
            }
        }

        Ok(Self { hdr_file, interval })
    }

    pub fn from_args(args_map: &mut HashMap<String, Vec<&str>>) -> Result<Self> {
        match args_map.remove(Self::CLI_STRING) {
            // Remove "-log" from the map
            Some(params) => Self::parse(&params),
            None => Ok(Self::default()),
        }
    }
}
