use crate::settings::{
    param::{types::Rate, ParamsParser, SimpleParamHandle},
    ParsePayload,
};
use anyhow::Result;

pub struct RateOption {
    pub threads_info: ThreadsInfo,
}

#[derive(PartialEq, Debug)]
pub enum ThreadsInfo {
    Fixed {
        threads: u64,
        throttle: Option<u64>,
        co_fixed: bool,
    },
    Auto {
        min_threads: u64,
        max_threads: u64,
        auto: bool,
    },
}

impl ThreadsInfo {
    fn print_settings(&self) {
        match &self {
            Self::Fixed {
                threads,
                throttle,
                co_fixed,
            } => {
                println!("  Thread count: {}", threads);
                if let Some(throttle) = throttle {
                    println!("  OpsPer Sec: {}", throttle);
                }
                println!("  Coordinated-Omission-Fixed latencies: {}", co_fixed);
            }
            Self::Auto {
                min_threads,
                max_threads,
                auto,
            } => {
                println!("  Min threads: {}", min_threads);
                println!("  Max threads: {}", max_threads);
                println!("  auto: {}", auto);
            }
        }
    }
}

impl RateOption {
    pub const CLI_STRING: &str = "-rate";

    pub fn description() -> &'static str {
        "Thread count, rate limit or automatic mode (default is auto)"
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
        println!("Rate:");
        self.threads_info.print_settings();
    }

    fn from_handles(handles: RateParamHandles) -> Result<Self> {
        let threads = handles.threads.get();
        let throttle = handles.throttle.get();
        let co_fixed = handles.co_fixed.get().is_some();
        let min_threads = handles.threads_gte.get();
        let max_threads = handles.threads_lte.get();
        let auto = handles.auto.get().is_some();

        let threads_info = match (min_threads, max_threads) {
            (Some(min_threads), Some(max_threads)) => ThreadsInfo::Auto {
                min_threads,
                max_threads,
                auto,
            },
            _ => ThreadsInfo::Fixed {
                // SAFETY: The parameters are grouped in a way that this won't ever panic
                // when entering this branch.
                threads: threads.unwrap(),
                throttle,
                co_fixed,
            },
        };

        Ok(Self { threads_info })
    }
}

struct RateParamHandles {
    pub threads: SimpleParamHandle<u64>,
    pub throttle: SimpleParamHandle<Rate>,
    pub co_fixed: SimpleParamHandle<bool>,
    pub threads_gte: SimpleParamHandle<u64>,
    pub threads_lte: SimpleParamHandle<u64>,
    pub auto: SimpleParamHandle<bool>,
}

fn prepare_parser() -> (ParamsParser, RateParamHandles) {
    let mut parser = ParamsParser::new(RateOption::CLI_STRING);

    let threads = parser.simple_param("threads=", None, "run this many clients concurrently", true);
    let throttle = parser.simple_param(
        "throttle=",
        None,
        "throttle operations per second across all clients to a maximum rate (or less) with no implied schedule",
        false,
    );
    let co_fixed = parser.simple_param(
        "fixed",
        None,
        "display coordinated-omission-fixed latencies",
        false,
    );
    let threads_gte = parser.simple_param(
        "threads>=",
        Some("4"),
        "run at least this many clients concurrently",
        false,
    );
    let threads_lte = parser.simple_param(
        "threads<=",
        Some("1000"),
        "run at most this many clients concurrently",
        false,
    );
    let auto = parser.simple_param(
        "auto",
        None,
        "stop increasing threads once throughput saturates",
        false,
    );

    // $ ./cassandra-stress help -rate
    // Usage: -rate threads=? [throttle=?] [fixed]
    //  OR
    // Usage: -rate [threads>=?] [threads<=?] [auto]
    parser.group(&[&threads, &throttle, &co_fixed]);
    parser.group(&[&threads_gte, &threads_lte, &auto]);

    (
        parser,
        RateParamHandles {
            threads,
            throttle,
            co_fixed,
            threads_gte,
            threads_lte,
            auto,
        },
    )
}

#[cfg(test)]
mod tests {
    use crate::settings::option::{rate::ThreadsInfo, RateOption};

    use super::prepare_parser;

    #[test]
    fn rate_good_params_group_one_test() {
        let args = vec!["threads=100", "throttle=15/s"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = RateOption::from_handles(handles).unwrap();
        assert_eq!(
            ThreadsInfo::Fixed {
                threads: 100,
                throttle: Some(15),
                co_fixed: false
            },
            params.threads_info
        );
    }

    #[test]
    fn rate_good_params_group_two_test() {
        let args = vec!["threads<=200", "auto"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = RateOption::from_handles(handles).unwrap();
        assert_eq!(
            ThreadsInfo::Auto {
                min_threads: 4,
                max_threads: 200,
                auto: true
            },
            params.threads_info
        )
    }

    #[test]
    fn rate_bad_params_test() {
        let args = vec!["threads<=200", "auto", "fixed=10/s"];
        let (parser, _) = prepare_parser();

        assert!(parser.parse(args).is_err());
    }
}
