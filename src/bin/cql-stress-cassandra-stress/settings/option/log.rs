use anyhow::Result;
use std::{path::PathBuf, time::Duration};

use crate::settings::{
    param::{
        types::{FlagNumericOrBool, IntervalMillisOrSeconds},
        ParamsParser, SimpleParamHandle,
    },
    ParsePayload,
};

#[derive(Clone, Debug)]
pub struct LogOption {
    pub hdr_file: Option<PathBuf>,
    pub interval: Duration,
    /// Tally operations per coordinator host and print the distribution in the summary.
    ///
    /// This is the acceptance evidence for leader-aware routing: on a strongly consistent
    /// keyspace at `cl=quorum` the distribution must follow the leader distribution across
    /// tablets, not spread uniformly over all replicas.
    ///
    /// Off by default, and the per-coordinator map is not even touched when disabled.
    pub coordinators: bool,
}

impl Default for LogOption {
    fn default() -> Self {
        Self {
            hdr_file: None,
            interval: Duration::from_secs(1),
            coordinators: false,
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
        println!("  Coordinator accounting: {}", self.coordinators);
    }

    fn from_handles(handles: LogParamHandles) -> Result<Self> {
        let hdr_file = handles.hdr_file.get().map(PathBuf::from);
        let interval = handles.interval.get().unwrap_or(Duration::from_secs(1));
        let coordinators = handles.coordinators.get().unwrap_or(false);

        Ok(Self {
            hdr_file,
            interval,
            coordinators,
        })
    }
}

struct LogParamHandles {
    pub hdr_file: SimpleParamHandle<String>,
    pub interval: SimpleParamHandle<IntervalMillisOrSeconds>,
    pub coordinators: SimpleParamHandle<FlagNumericOrBool>,
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

    let coordinators = parser.simple_param(
        "coordinators=",
        None,
        "Tally operations per coordinator host and print the distribution in the summary. \
         Used to verify that requests are concentrated on tablet leaders",
        false,
    );

    parser.group(&[&hdr_file, &interval, &coordinators]);

    (
        parser,
        LogParamHandles {
            hdr_file,
            interval,
            coordinators,
        },
    )
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
        assert!(!params.coordinators);
    }

    #[test]
    fn log_coordinators_flag_test() {
        let (parser, handles) = prepare_parser();
        assert!(parser.parse(vec!["coordinators=true"]).is_ok());
        assert!(
            super::LogOption::from_handles(handles)
                .unwrap()
                .coordinators
        );

        let (parser, handles) = prepare_parser();
        assert!(parser.parse(vec!["coordinators=false"]).is_ok());
        assert!(
            !super::LogOption::from_handles(handles)
                .unwrap()
                .coordinators
        );
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
