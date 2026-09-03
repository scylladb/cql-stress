use anyhow::{Context, Result};

use crate::settings::{
    param::{types::NonEmptyString, ParamsParser, SimpleParamHandle},
    ParsePayload,
};

pub const DEFAULT_RETRIES_PER_OPERATION: usize = 9;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorsOption {
    pub ignore: bool,
    pub retries: usize,
}

impl Default for ErrorsOption {
    fn default() -> Self {
        Self {
            ignore: false,
            retries: DEFAULT_RETRIES_PER_OPERATION,
        }
    }
}

impl ErrorsOption {
    pub const CLI_STRING: &'static str = "-errors";

    pub fn description() -> &'static str {
        "How to handle operation errors"
    }

    pub fn parse(cl_args: &mut ParsePayload) -> Result<Self> {
        let params = cl_args.remove(Self::CLI_STRING).unwrap_or_default();
        let (parser, handles) = prepare_parser();
        parser
            .parse(params)
            .context("Failed to parse -errors option parameters")?;
        Self::from_handles(handles)
    }

    pub fn print_help() {
        let (parser, _) = prepare_parser();
        parser.print_help();
    }

    pub fn print_settings(&self) {
        println!("Errors:");
        println!("  Ignore: {}", self.ignore);
        println!("  Retries: {}", self.retries);
    }

    fn from_handles(handles: ErrorsParamHandles) -> Result<Self> {
        let ignore = handles.ignore.get().unwrap_or(false);
        let fail_fast = handles.fail_fast.get().unwrap_or(false);
        anyhow::ensure!(
            !(ignore && fail_fast),
            "ignore and fail-fast are mutually exclusive"
        );

        let retries = handles
            .retries
            .get()
            .unwrap_or(DEFAULT_RETRIES_PER_OPERATION);

        warn_ignored("skip-read-validation", handles.skip_read_validation.get());
        warn_ignored(
            "skip-unsupported-columns",
            handles.skip_unsupported_columns.get(),
        );
        warn_ignored("delay-policy", handles.delay_policy.get());
        warn_ignored("min-delay-ms", handles.min_delay_ms.get());
        warn_ignored("max-delay-ms", handles.max_delay_ms.get());

        Ok(Self { ignore, retries })
    }
}

fn warn_ignored<T>(name: &str, supplied: Option<T>) {
    if supplied.is_some() {
        tracing::warn!("-errors {name} is not supported and will be ignored");
    }
}

struct ErrorsParamHandles {
    ignore: SimpleParamHandle<bool>,
    fail_fast: SimpleParamHandle<bool>,
    retries: SimpleParamHandle<usize>,
    skip_read_validation: SimpleParamHandle<bool>,
    skip_unsupported_columns: SimpleParamHandle<bool>,
    delay_policy: SimpleParamHandle<NonEmptyString>,
    min_delay_ms: SimpleParamHandle<usize>,
    max_delay_ms: SimpleParamHandle<usize>,
}

fn prepare_parser() -> (ParamsParser, ErrorsParamHandles) {
    let mut parser = ParamsParser::new(ErrorsOption::CLI_STRING);

    let ignore = parser.simple_param::<bool>(
        "ignore",
        None,
        "Do not stop the benchmark when an operation keeps failing",
        false,
    );

    let fail_fast = parser.simple_param::<bool>(
        "fail-fast",
        None,
        "Stop the benchmark when an operation keeps failing (default)",
        false,
    );

    let default_retries = DEFAULT_RETRIES_PER_OPERATION.to_string();
    let retries = parser.simple_param::<usize>(
        "retries=",
        Some(&default_retries),
        "Number of times to retry a failed operation before giving up on it",
        false,
    );

    let skip_read_validation = parser.simple_param::<bool>(
        "skip-read-validation",
        None,
        "Do not validate the rows that a read returns (unsupported)",
        false,
    );

    let skip_unsupported_columns = parser.simple_param::<bool>(
        "skip-unsupported-columns",
        None,
        "Skip the columns of a type the driver cannot handle (unsupported)",
        false,
    );

    let delay_policy = parser.simple_param::<NonEmptyString>(
        "delay-policy=",
        None,
        "Backoff policy between the retries of an operation (unsupported)",
        false,
    );

    let min_delay_ms = parser.simple_param::<usize>(
        "min-delay-ms=",
        None,
        "Shortest backoff between the retries of an operation (unsupported)",
        false,
    );

    let max_delay_ms = parser.simple_param::<usize>(
        "max-delay-ms=",
        None,
        "Longest backoff between the retries of an operation (unsupported)",
        false,
    );

    parser.group(&[
        &ignore,
        &fail_fast,
        &retries,
        &skip_read_validation,
        &skip_unsupported_columns,
        &delay_policy,
        &min_delay_ms,
        &max_delay_ms,
    ]);

    (
        parser,
        ErrorsParamHandles {
            ignore,
            fail_fast,
            retries,
            skip_read_validation,
            skip_unsupported_columns,
            delay_policy,
            min_delay_ms,
            max_delay_ms,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{prepare_parser, ErrorsOption};

    fn parse(args: Vec<&str>) -> anyhow::Result<ErrorsOption> {
        let (parser, handles) = prepare_parser();
        parser.parse(args)?;
        ErrorsOption::from_handles(handles)
    }

    #[test]
    fn errors_default_params_test() {
        assert_eq!(ErrorsOption::default(), parse(vec![]).unwrap());
    }

    #[test]
    fn errors_ignore_test() {
        let params = parse(vec!["ignore"]).unwrap();
        assert!(params.ignore);
        assert_eq!(9, params.retries);
    }

    #[test]
    fn errors_retries_test() {
        let params = parse(vec!["retries=20"]).unwrap();
        assert!(!params.ignore);
        assert_eq!(20, params.retries);
    }

    #[test]
    fn errors_ignore_and_retries_test() {
        let params = parse(vec!["ignore", "retries=3"]).unwrap();
        assert!(params.ignore);
        assert_eq!(3, params.retries);
    }

    #[test]
    fn errors_fail_fast_test() {
        let params = parse(vec!["fail-fast"]).unwrap();
        assert!(!params.ignore);
    }

    #[test]
    fn errors_fail_fast_conflicts_with_ignore_test() {
        assert!(parse(vec!["ignore", "fail-fast"]).is_err());
    }

    #[test]
    fn errors_unsupported_suboptions_are_accepted_test() {
        let args = vec![
            "ignore",
            "skip-read-validation",
            "skip-unsupported-columns",
            "delay-policy=fixed",
            "min-delay-ms=10",
            "max-delay-ms=1000",
        ];
        let params = parse(args).unwrap();
        assert!(params.ignore);
        assert_eq!(9, params.retries);
    }

    #[test]
    fn errors_bad_retries_test() {
        assert!(parse(vec!["retries=foo"]).is_err());
    }

    #[test]
    fn errors_empty_delay_policy_test() {
        assert!(parse(vec!["delay-policy="]).is_err());
    }

    #[test]
    fn errors_ignore_with_value_test() {
        assert!(parse(vec!["ignore=true"]).is_err());
    }

    #[test]
    fn errors_unknown_param_test() {
        assert!(parse(vec!["foo"]).is_err());
    }
}
