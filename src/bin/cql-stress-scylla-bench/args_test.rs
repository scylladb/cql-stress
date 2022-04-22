const DATA: &str = include_str!("args_test.in");

use crate::args::parse_scylla_bench_args;

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
            Some(_) => success_count += 1,
            None => {
                eprintln!("  line {}: {}\n", i + 1, s);
                failure_count += 1;
            }
        }
    }

    println!("Successes: {}, failures: {}", success_count, failure_count);
    assert_eq!(failure_count, 0);
}
