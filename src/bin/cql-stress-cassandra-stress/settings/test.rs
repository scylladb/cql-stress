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

/// A real profile from the integration-test fixtures, so the test exercises the same yaml
/// shape users write. Its keyspace deliberately differs from the `-schema keyspace=` default.
#[cfg(all(feature = "user-profile", feature = "strong-consistency"))]
const TEST_PROFILE: &str = "tools/util/profiles/cqlstress_text_profile.yaml";
#[cfg(all(feature = "user-profile", feature = "strong-consistency"))]
const TEST_PROFILE_KEYSPACE: &str = "cqlstress_text_keyspace";

#[cfg(all(feature = "user-profile", feature = "strong-consistency"))]
fn parse_workload(args: &str) -> super::CassandraStressSettings {
    match parse_cassandra_stress_args(args.split_ascii_whitespace()).unwrap() {
        super::CassandraStressParsingResult::Workload(settings) => *settings,
        super::CassandraStressParsingResult::SpecialCommand => {
            panic!("expected a workload, got a special command: {args}")
        }
    }
}

/// The strong-consistency checks must follow the keyspace the operations really hit. A user
/// profile brings its own, and `-schema keyspace=` is not consulted at all in user mode - so
/// reading the schema option there would inspect a keyspace the run never touches.
#[test]
#[cfg(all(feature = "user-profile", feature = "strong-consistency"))]
fn workload_keyspace_follows_the_user_profile_test() {
    let settings = parse_workload(&format!(
        "cassandra-stress user profile={TEST_PROFILE} ops(test_query=1) n=10 \
         -schema keyspace=unrelated_keyspace"
    ));

    assert_eq!(TEST_PROFILE_KEYSPACE, settings.workload_keyspace());
}

/// Every other command uses the `-schema keyspace=` value.
#[test]
#[cfg(feature = "strong-consistency")]
fn workload_keyspace_follows_the_schema_option_test() {
    let settings = match parse_cassandra_stress_args(
        "cassandra-stress write n=10 -schema keyspace=my_keyspace".split_ascii_whitespace(),
    )
    .unwrap()
    {
        super::CassandraStressParsingResult::Workload(settings) => *settings,
        super::CassandraStressParsingResult::SpecialCommand => panic!("expected a workload"),
    };

    assert_eq!("my_keyspace", settings.workload_keyspace());
}

/// `consistency=` rides on the `-schema` keyspace creation query, which a user run never
/// executes - the profile's own `keyspace_definition` is used instead. Accepting it silently
/// would read like a request that was honoured while the run measured an eventually
/// consistent keyspace.
#[test]
#[cfg(all(feature = "user-profile", feature = "strong-consistency"))]
fn schema_consistency_is_rejected_with_the_user_command_test() {
    let result = parse_cassandra_stress_args(
        format!(
            "cassandra-stress user profile={TEST_PROFILE} ops(test_query=1) n=10 \
             -schema replication(strategy=NetworkTopologyStrategy,consistency=global)"
        )
        .split_ascii_whitespace(),
    );
    let error = match result {
        Ok(_) => panic!("consistency= should be rejected with the 'user' command"),
        Err(error) => error.to_string(),
    };

    assert!(
        error.contains("has no effect with the 'user' command"),
        "unexpected error: {error}"
    );
    assert!(
        error.contains("keyspace_definition"),
        "the error should say where to put it instead: {error}"
    );
}

/// The same flag stays accepted for the commands whose keyspace cql-stress does create.
#[test]
#[cfg(feature = "strong-consistency")]
fn schema_consistency_is_accepted_for_write_test() {
    assert!(parse_cassandra_stress_args(
        "cassandra-stress write n=10 \
         -schema replication(strategy=NetworkTopologyStrategy,consistency=global)"
            .split_ascii_whitespace(),
    )
    .is_ok());
}

/// The diagnostic's wording is the whole point of it, and it is the message a user reads
/// when they are already confused. A single-node probe reports the mixed case exactly
/// backwards, so that row in particular is worth pinning.
#[test]
#[cfg(feature = "strong-consistency")]
fn summarise_v2_probe_test() {
    use super::summarise_v2_probe;

    let all = summarise_v2_probe(&["a", "b"], &[], &[], 2, 2);
    assert!(all.contains("all of them do (a, b)"), "{all}");
    assert!(
        all.contains("the keyspace itself is what is not strongly consistent"),
        "{all}"
    );

    let none = summarise_v2_probe(&[], &["a", "b"], &[], 2, 2);
    assert!(none.contains("none of them do (a, b)"), "{none}");
    assert!(none.contains("TABLETS_ROUTING_V1"), "{none}");

    // The case a first-node-only probe gets backwards: it would have reported either
    // "this server can route to leaders" or "no node can", depending on the list order.
    let mixed = summarise_v2_probe(&["a"], &["b"], &[], 2, 2);
    assert!(mixed.contains("some do (a) and some do not (b)"), "{mixed}");
    assert!(mixed.contains("part-way through enabling"), "{mixed}");

    let unreachable = summarise_v2_probe(&[], &[], &[String::from("a (refused)")], 1, 1);
    assert!(
        unreachable.contains("none of them could be reached"),
        "{unreachable}"
    );
    assert!(unreachable.contains("a (refused)"), "{unreachable}");

    // One unreachable node must not discard the answers the others gave.
    let partial = summarise_v2_probe(&["a"], &[], &[String::from("b (timeout)")], 2, 2);
    assert!(partial.contains("all of them do (a)"), "{partial}");
    assert!(
        partial.contains("could not be asked: b (timeout)"),
        "{partial}"
    );

    // A long -node list is capped, and the message says so rather than implying the
    // unprobed nodes were found to agree.
    let capped = summarise_v2_probe(&["a"], &[], &[], 1, 20);
    assert!(capped.contains("first 1 of 20, 19 not probed"), "{capped}");
}

/// `tools/test_cs_strong_consistency.py` tells "this server cannot do leader-aware routing"
/// (skip the suite) from "the binary is broken" (fail the job) by matching this code. Pin it
/// here so rewording the failure breaks a fast unit test rather than silently turning the
/// integration job green by skipping everything.
#[test]
#[cfg(feature = "strong-consistency")]
fn strong_consistency_failure_carries_its_diagnostic_code_test() {
    let message = super::strong_consistency_failure_message(
        "keyspace1",
        "Eventual",
        "CREATE KEYSPACE ...",
        "\ndiagnosis here",
    );

    assert!(
        message.contains(super::STRONG_CONSISTENCY_UNAVAILABLE_CODE),
        "the integration probe matches on this code: {message}"
    );
    assert!(message.contains("keyspace1"), "{message}");
    assert!(message.contains("diagnosis here"), "{message}");
}

/// A build without the `strong-consistency` feature cannot read back the consistency mode the
/// driver negotiated, so it has no way to tell a leader-routed run from one that merely looks
/// like one. Accepting `consistency=` there would emit the DDL and then report numbers nobody
/// can vouch for - the precise failure the whole check exists to prevent - so it is refused at
/// parse time, and the error has to say how to get a binary that can.
#[test]
#[cfg(not(feature = "strong-consistency"))]
fn schema_consistency_is_refused_without_the_feature_test() {
    let result = parse_cassandra_stress_args(
        "cassandra-stress write n=10 \
         -schema replication(strategy=NetworkTopologyStrategy,consistency=global)"
            .split_ascii_whitespace(),
    );
    let error = match result {
        Ok(_) => panic!("consistency= should be refused without the strong-consistency feature"),
        Err(error) => error.to_string(),
    };

    assert!(
        error.contains("strong-consistency"),
        "the error should name the feature: {error}"
    );
}
