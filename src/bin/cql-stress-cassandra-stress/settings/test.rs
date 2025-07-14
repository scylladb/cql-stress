use super::{parse_cassandra_stress_args, repair_params};

const DATA_GOOD: &str = include_str!("cs_args_good_test.in");
const DATA_BAD: &str = include_str!("cs_args_bad_test.in");

#[test]
fn cs_args_good_test() {
    let mut success: u32 = 0;
    let mut failure: u32 = 0;

    for (i, input) in DATA_GOOD.lines().enumerate() {
        let input = input.trim();
        if input.is_empty() || input.starts_with('#') {
            continue;
        }
        match parse_cassandra_stress_args(input.split_ascii_whitespace()) {
            Err(_) => {
                eprintln!("Error on line {}: {}", i + 1, input);
                failure += 1;
            }
            _ => success += 1,
        }

        println!("Success count: {success}, Failure count: {failure}");
        assert_eq!(failure, 0);
    }
}

#[test]
fn cs_args_bad_test() {
    let mut success: u32 = 0;
    let mut failure: u32 = 0;

    for (i, input) in DATA_BAD.lines().enumerate() {
        let input = input.trim();
        if input.is_empty() || input.starts_with('#') {
            continue;
        }
        match parse_cassandra_stress_args(input.split_ascii_whitespace()) {
            Err(_) => failure += 1,
            _ => {
                eprintln!("Should have failed on line {}: {}", i + 1, input);
                success += 1;
            }
        }

        println!("Success count: {success} , Failure count: {failure}");
        assert_eq!(success, 0);
    }
}

#[test]
fn repair_params_test() {
    let args = [
        "write",
        "-schema replication ( factor = 3 , foo = bar )\t \tkeyspace=k ",
        " compression = someCompressionAlgorithm",
    ];

    let result = repair_params(args.iter());
    assert_eq!(
        vec![
            "write",
            "-schema",
            "replication(factor=3,foo=bar)",
            "keyspace=k",
            "compression=someCompressionAlgorithm"
        ],
        result
    );
}
