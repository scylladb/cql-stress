use anyhow::Result;
use std::{path::PathBuf, time::Duration};

use crate::settings::{
    param::{types::IntervalMillisOrSeconds, ParamsParser, SimpleParamHandle},
    ParsePayload,
};

#[derive(Clone, Debug)]
pub struct LogOption {
    pub hdr_file: Option<PathBuf>,
    pub interval: Duration,
}

impl Default for LogOption {
    fn default() -> Self {
        Self {
            hdr_file: None,
            interval: Duration::from_secs(1),
        }
    }
}

impl LogOption {
    pub const CLI_STRING: &'static str = "-log";

    pub fn description() -> &'static str {
        "Specify logging options"
    }

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
        println!("Log:");
        if let Some(path) = &self.hdr_file {
            println!("  HDR Histogram file: {}", path.display());
        }
        println!("  Log interval: {:?}", self.interval);
    }

    fn from_handles(handles: LogParamHandles) -> Result<Self> {
        let hdr_file = handles.hdr_file.get().map(PathBuf::from);
        let interval = handles.interval.get().unwrap_or(Duration::from_secs(1));

        Ok(Self { hdr_file, interval })
    }
}

struct LogParamHandles {
    pub hdr_file: SimpleParamHandle<String>,
    pub interval: SimpleParamHandle<IntervalMillisOrSeconds>,
}

fn prepare_parser() -> (ParamsParser, LogParamHandles) {
    let mut parser = ParamsParser::new(LogOption::CLI_STRING);

    let hdr_file = parser.simple_param(
        "hdrfile=",
        None,
        "Log HDR Histogram data to the specified file",
        false,
    );

    let interval = parser.simple_param(
        "interval=",
        Some("1s"),
        "Set the interval between logs in seconds or milliseconds",
        false,
    );

    parser.group(&[&hdr_file, &interval]);

    (parser, LogParamHandles { hdr_file, interval })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::prepare_parser;

    #[test]
    fn log_default_params_test() {
        let args = vec![];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = super::LogOption::from_handles(handles).unwrap();
        assert_eq!(None, params.hdr_file);
        assert_eq!(Duration::from_secs(1), params.interval);
    }

    #[test]
    fn log_good_params_test() {
        let args = vec!["hdrfile=test.hdr", "interval=500ms"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = super::LogOption::from_handles(handles).unwrap();
        assert!(params.hdr_file.is_some());
        assert_eq!("test.hdr", params.hdr_file.unwrap().to_str().unwrap());
        assert_eq!(Duration::from_millis(500), params.interval);
    }

    #[test]
    fn log_seconds_interval_test() {
        let args = vec!["interval=5s"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = super::LogOption::from_handles(handles).unwrap();
        assert_eq!(Duration::from_secs(5), params.interval);
    }

    #[test]
    fn log_plain_interval_test() {
        let args = vec!["interval=10"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = super::LogOption::from_handles(handles).unwrap();
        assert_eq!(Duration::from_secs(10), params.interval);
    }

    #[test]
    fn log_bad_interval_test() {
        let args = vec!["interval=foo"];
        let (parser, _) = prepare_parser();

        // Should fail with an invalid interval format
        assert!(parser.parse(args).is_err());
    }
}
