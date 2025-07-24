const DATA: &str = include_str!("args_test.in");

use crate::args::{parse_scylla_bench_args, ParseResult};

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
