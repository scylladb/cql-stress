const DATA: &str = include_str!("args_test.in");

use crate::args::{parse_scylla_bench_args, ParseResult};

fn parse(args: &str) -> Option<ParseResult> {
    parse_scylla_bench_args(args.split_ascii_whitespace(), false)
}

fn parse_ok(args: &str) -> Box<crate::args::ScyllaBenchArgs> {
    match parse(args).expect("expected successful parse") {
        ParseResult::Config(cfg) => cfg,
        ParseResult::VersionDisplayed => panic!("unexpected version display"),
    }
}

#[test]
fn test_example_sets() {
    let mut success_count = 0;
    let mut failure_count = 0;

    for (i, s) in DATA.lines().enumerate() {
        let s = s.trim();
        if s.is_empty() || s.starts_with('#') {
            continue;
        }
        match parse_scylla_bench_args(s.split_ascii_whitespace(), false) {
            Some(ParseResult::Config(_)) => success_count += 1,
            Some(ParseResult::VersionDisplayed) => success_count += 1, // Treat as success
            None => {
                eprintln!("  line {}: {}", i + 1, s);
                failure_count += 1;
            }
        }
    }

    println!("Successes: {success_count}, failures: {failure_count}");
    assert_eq!(failure_count, 0);
}

#[test]
fn test_rack_with_dc_aware_policy() {
    let args = parse_ok(
        "scylla-bench -workload=sequential -mode=write \
         -host-selection-policy=dc-aware:dc1 -rack=rack1",
    );
    assert_eq!(args.datacenter.as_deref(), Some("dc1"));
    assert_eq!(args.rack.as_deref(), Some("rack1"));
}

#[test]
fn test_rack_without_dc_aware_policy() {
    // --rack requires a dc-aware:<dc> host-selection-policy
    assert!(
        parse("scylla-bench -workload=sequential -mode=write -rack=rack1").is_none(),
        "rack without dc-aware policy should be rejected"
    );
}

#[test]
fn test_dc_aware_empty_datacenter() {
    // dc-aware: with no datacenter name should be rejected
    assert!(
        parse(
            "scylla-bench -workload=sequential -mode=write \
             -host-selection-policy=dc-aware:"
        )
        .is_none(),
        "dc-aware: with empty datacenter should be rejected"
    );
}
