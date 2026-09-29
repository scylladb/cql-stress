## Test Writing

### Unit test location
Put unit tests in a `#[cfg(test)]` module at the end of the file under test.
Put table-driven CLI parser tests in a separate module with `.in` fixture files.
Examples are `settings/test.rs` in cassandra-stress and `args_test.rs` in scylla-bench.
Use `#[test]` for sync tests and `#[tokio::test]` for async tests.
Shared test helpers, such as `new_test_session()`, go in `src/test_util.rs`.

### Single-threaded DB tests
Run Rust tests with `-- --test-threads=1`. Tests that use the database share one Scylla node.
Parallel runs make these tests flaky.
Before you run the tests, start Scylla with `docker compose -f docker/scylla-test/compose.yml up -d --wait`.
`SCYLLA_URI` sets a different node address. The default is `127.0.0.1:9042`.

### Python integration tests
Integration tests are pytest tests in `tools/`.
Put the test logic in a `tools/test_cs_*.py` or `tools/test_hdr_logging.py` module, in a `run()` or `run_<case>()` function.
`tools/test_with_scylla.py` is the Rust test runner. It is not a test module.
Add a `test_*` function in `tools/cql-stress-cassandra-stress-ci.py` that calls it with the shared fixtures.
Put shared helpers in `tools/util/`.
