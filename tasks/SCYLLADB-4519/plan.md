# SCYLLADB-4519 — implementation plan

**Spec:** `tasks/SCYLLADB-4519/spec.md`

A decision during the build that changes what the spec states updates the
spec in the same commit. A line in its `Decisions` section records the
decision when the Design section does not state the reason. This paragraph
stays in every plan built from a spec.

## Rules

- Each task below is one commit on `feat/sc-verify-m1`, with its boxes checked.
  The `(Tn)` tag maps it to the task list in the tracking repo.
- Verify = the `## Commands` section of `CLAUDE.md`, with
  `export RUSTFLAGS="-D warnings --cfg scylla_unstable"`: the Lint block, then
  `cargo test --features "user-profile,strong-consistency" -- --test-threads=1`
  against `docker compose -f docker/scylla-test/compose.yml --profile strong-consistency up -d --wait scylla-sc-test`.
  A task that touches cassandra-stress also runs
  `pytest -s ./tools/cql-stress-cassandra-stress-ci.py -k strong_consistency`.
- Baseline before task 1, on f990890: 135 Rust tests pass, 5 SC integration tests pass.
- A move is a move: `git mv` where a whole file moves, no edits beyond paths and
  `pub`. The cassandra-stress golden files `cs_args_*_test.in` are never edited.
- New binary code lives in `src/bin/cql-stress-sc-verify/`. Unit tests sit at the
  end of the file under test. Integration logic goes in `tools/test_cs_sc_verify.py`
  as `run_<case>()`, called from `test_strong_consistency_sc_verify_<case>()` in
  `tools/cql-stress-cassandra-stress-ci.py`, so the CI job `-k strong_consistency`
  runs it.
- Errors: `anyhow` with `.context()`. Diagnostics go through `tracing`; `println!`
  is for the user and for `SCV` lines.

## Task 1 — move the protocol-extension probe into the library (T1)

**Files:**
- Move: `src/bin/cql-stress-cassandra-stress/settings/protocol_extensions.rs` → `src/strong_consistency/protocol_extensions.rs`
- Create: `src/strong_consistency.rs`
- Modify: `src/lib.rs`, `src/bin/cql-stress-cassandra-stress/settings/mod.rs:9-10,45-46`

**Internals:** `pub mod strong_consistency` in `lib.rs`, gated on the feature;
`pub use protocol_extensions::fetch_protocol_features`.

- [x] Move the file; point the binary's `use` at `cql_stress::strong_consistency::fetch_protocol_features`.
- [x] Run verify; the SC pytest still passes 5/5.
- [x] Commit `refactor: move the protocol extension probe into the library [SCYLLADB-4519]`.

## Task 2 — move the SC keyspace check into the library (T1)

**Files:**
- Modify: `src/strong_consistency.rs`, `src/bin/cql-stress-cassandra-stress/settings/mod.rs:175-235,380-555`, `.../settings/test.rs:165-235`

**Internals:** moved from `settings/mod.rs`: `STRONG_CONSISTENCY_UNAVAILABLE_CODE`,
`strong_consistency_failure_message`, `summarise_v2_probe`, the probe loop of
`diagnose_missing_strong_consistency` as `diagnose_v2(nodes: &[String], tls: bool) -> String`;
new `keyspace_consistency_mode(session, keyspace)` (refresh plus lookup) and
`unavailable_error(keyspace, reported_mode, ddl, nodes, tls) -> anyhow::Error`.
`verify_consistency_mode` stays in the binary and calls them. The `tls` flag keeps
today's "not asked, TLS" message, so the spec's `unavailable_error` gains `tls: bool`.
Update the spec's Module API in this commit.

- [x] Move `summarise_v2_probe_test` and `strong_consistency_failure_carries_its_diagnostic_code_test` to the end of `src/strong_consistency.rs`; they fail to compile until the code moves.
- [x] Move the code; the binary keeps its CL check and datacenter warning.
- [x] Run verify and the SC pytest; the failure text is unchanged (the pytest matches the code).
- [x] Commit `refactor: move the strong consistency keyspace check into the library [SCYLLADB-4519]`.

## Task 3 — move the java distributions into the library (T2)

**Files:**
- Move: `src/bin/cql-stress-cassandra-stress/java_generate/{mod.rs,faster_random.rs,distribution/}` → `src/java_generate/`
- Keep: `src/bin/cql-stress-cassandra-stress/java_generate/distribution/enumerated.rs`
- Create: `src/bin/cql-stress-cassandra-stress/java_generate/mod.rs` (re-exports)
- Modify: `src/lib.rs`

**Internals:** the `Random` wrapper, `faster_random` and the distributions move
together, because both of the latter use `Random`'s private methods.
`EnumeratedDistribution` stays in the binary, because `-mixed ratio(...)` adds inherent
parsers to it through the `OperationRatio` alias, which only compiles in the crate
that defines the type. The binary's `java_generate::distribution` is an inline module
that re-exports the library's and declares `enumerated`, so every `crate::java_generate`
path in the binary stays unchanged. The tests in the moved files move with them.

- [x] Move the files; fix the `crate::`/`super::` paths only.
- [x] Run verify; the total test count is unchanged and the library gains the moved tests.
- [x] Commit `refactor: move the java distributions into the library [SCYLLADB-4519]`.

## Task 4 — distribution and population parsers in the library (T2)

**Files:**
- Modify: `src/java_generate/distribution/mod.rs`, `src/bin/cql-stress-cassandra-stress/settings/param/types.rs:265-290`

**Internals:** `pub fn parse_distribution(s: &str) -> Result<Box<dyn DistributionFactory>>`
(the body of today's `impl Parsable for Box<dyn DistributionFactory>`, which now calls it);
`pub fn parse_population(s: &str) -> Result<Box<dyn DistributionFactory>>` for
`seq=a..b` (inclusive, i64, with the k/m/b suffixes of the library's `parse_long`) or `dist=<distribution>`.

- [x] Write tests: `seq=0..1023` yields 0, 1, …, 1023, 0; `dist=uniform(1..10)` parses; `seq=5..1`, `foo=1` and `dist=bogus(1)` fail.
- [x] Run them and confirm the failure.
- [x] Add both functions; route the `Parsable` impl through `parse_distribution`.
- [x] Run verify; `cs_args_good_test.in` and `cs_args_bad_test.in` pass unchanged.
- [x] Commit `feat: parse populations in the library [SCYLLADB-4519]`.

**Checkpoint A** (human review): Rust tests and the SC pytest are green; cassandra-stress output is unchanged.

## Task 5 — binary skeleton and CLI (T3)

**Files:**
- Modify: `Cargo.toml` (the `[[bin]]` entry with `required-features = ["strong-consistency"]`; the feature adds `dep:clap`, optional `clap` 4 with `derive`; `serde`/`serde_yaml` join in task 6, which uses them)
- Create: `src/bin/cql-stress-sc-verify/main.rs`, `.../cli.rs`

**Internals:** `struct Cli` (clap derive) holding every flag of the spec's
Command block, with spec defaults; `enum Mode { Verify, Bulk, Both }`;
`enum Checker { Off }`. `-n` is accepted only with `--mode bulk`. The connection
flags are `--nodes`, `--user`, `--password` and TLS: `--ssl` turns it on,
`--ssl-ca <pem>` verifies the server, and `--ssl-cert <pem>` with `--ssl-key <pem>`
gives a client certificate. `fn tls_context(&Cli) -> Result<Option<SslContext>>`
uses `openssl`, which is already a dependency. The spec's Command block gets these
exact flags in this commit (user decision: TLS is in M1). `main` parses the flags,
builds the TLS context and prints the settings; usage errors exit 2. `parse_duration`
(`ms`/`s`/`m`/`h`) is ten lines in `cli.rs`, not a new crate. One of `--duration` or
`-n` is required.

- [x] Write a `tls_context` test: no `--ssl` → `None`; `--ssl-cert` without `--ssl-key` → error.
- [x] Write `cli.rs` tests: the defaults; `-n` with `--mode verify` is an error; `--checker on` is an error.
- [x] Run them and confirm the failure. (The code came first here; two deliberate breaks, a changed default and a dropped `-n` rule, each failed a test.)
- [x] Write the skeleton.
- [x] Run verify; also `cargo build` with no features and confirm the binary is skipped.
- [x] Commit `feat: add the cql-stress-sc-verify binary skeleton [SCYLLADB-4519]`.

## Task 6 — profile and DDL (T3)

**Files:** Create: `src/bin/cql-stress-sc-verify/profile.rs`

**Internals:** `struct Profile` (serde), `enum CellType { Int, Bigint, Text, Blob }`;
`Profile::load(path)`, `validate()` (1..8 cells; `text_size` ≥ 21 and `blob_size` ≥ 8
so any u64 wid fits; keyspace and table names must be lower-case identifiers because
they go into DDL verbatim; unknown keys rejected), `keyspace_ddl()`, `table_ddl(table)`,
`bulk_table_name()`. `Cargo.toml`: the feature adds `dep:serde`, `dep:serde_yaml`.
`main` loads the profile (failure → exit 2) and prints the DDL.

- [x] Write tests: the default profile gives the spec's DDL text exactly; 0 or 9 cells fail; an unknown type fails.
- [x] Run them and confirm the failure.
- [x] Write `profile.rs`.
- [x] Run verify.
- [x] Commit `feat: load the sc-verify profile and build its DDL [SCYLLADB-4519]`.

## Task 7 — schema create, diff and start-up checks (T3)

**Files:** Create: `src/bin/cql-stress-sc-verify/startup.rs`; Modify: `main.rs`; Create: `tools/test_cs_sc_verify.py`; Modify: `tools/cql-stress-cassandra-stress-ci.py`

**Internals:** `fn diff(profile_cols, live_cols) -> Vec<String>` (pure, `"c1: table has text, profile says int"`);
`async fn startup(session, profile) -> Result<(), Exit2>`: run `IF NOT EXISTS` DDL,
call `keyspace_consistency_mode`/`unavailable_error`, read `system_schema.columns`, diff.
`main` maps a start-up failure to exit 2. Order: keyspace DDL, then the SC check, and only
then the table DDL and the diff, so no table goes into a keyspace that is not strongly
consistent. `connect(cli, tls)` builds the session from `--nodes`, the credentials and
TLS. The None/Eventual wording of the mode (four lines) is repeated from cassandra-stress
rather than moved into the library. The pytest also covers an eventually consistent
keyspace (exit 2, the diagnostic code, no table created) and a second start over an
existing schema.

- [x] Write tests: `diff` on equal, type-changed, missing and extra columns; pytest `run_schema`: an empty keyspace gets created; an altered table exits 2 with the diff.
- [x] Run them and confirm the failure. (The unit tests failed to compile first; the pytest was written after the code and asserts the exact diff line and code.)
- [x] Write the code.
- [x] Run verify and the new pytest.
- [x] Commit `feat: create and check the sc-verify schema at start-up [SCYLLADB-4519]`.

## Task 8 — checked session settings (T4, high risk first)

**Files:** Create: `src/bin/cql-stress-sc-verify/ops.rs`

**Internals:** `fn checked_profile(cl, request_timeout) -> ExecutionProfile` with
`FallthroughRetryPolicy`, `speculative_execution_policy(None)`, the given CL and timeout;
`fn prepare_checked(session, query)` marks each statement not idempotent. `connect` sets
the profile as the checked session's default; `main` prepares the checked read
(`Profile::read_query`) at start-up. Driver check (scylla 1.9 `client/execution.rs`): the
only path that moves to another node without the retry policy is a failure to get a
connection, before anything is sent.

- [x] Write a test that reads `get_retry_policy`, `get_speculative_execution_policy` and the consistency back from the profile, and checks `is_idempotent()` on a prepared statement.
- [x] Run it and confirm the failure. (It failed to compile; after the code, turning on speculative execution and marking statements idempotent each failed it.)
- [x] Write the code. If the driver cannot express any of it, stop and ask.
- [x] Run verify.
- [x] Commit `feat: build the non-retrying checked execution profile [SCYLLADB-4519]`.

## Task 9 — cell encoders (T4)

**Files:** Modify: `src/bin/cql-stress-sc-verify/ops.rs`

**Internals:** `fn encode(cell: CellType, wid: u64, size: usize) -> CqlValue`;
`fn decode(cell, size, &CqlValue) -> Result<u64>` is strict: the value must equal
`encode` of the wid it claims, and 0 is never a wid. `int` refuses wid ≥ 2^31, `bigint`
wid ≥ 2^63. Until task 13 gives them a caller, `mod ops` carries `#[allow(dead_code)]`.

- [x] Write tests: round trip for each type; text length = `text_size`; blob is 8 bytes little-endian plus filler; garbage does not decode.
- [x] Run them and confirm the failure.
- [x] Write the code.
- [x] Run verify.
- [x] Commit `feat: encode write ids into cells [SCYLLADB-4519]`.

## Task 10 — outcome classes (T4)

**Files:** Modify: `src/bin/cql-stress-sc-verify/ops.rs`

**Internals:** `enum Failure { Indeterminate, Fail, WorkloadError, ReadFailed }` (a success
is not an error, so it is not a class); `enum OpKind { Read, Write }`;
`fn classify(op: OpKind, err: &ExecutionError, unavailable_is_fail: bool) -> Failure` per the
spec's outcome table. Workload errors: `SyntaxError`, `Invalid`, `Unauthorized`,
`AuthenticationError`, `ConfigError`, `AlreadyExists`, `BadQuery`, request serialization.
Any other write failure is indeterminate (`Unavailable` is `Fail` with the flag); any other
read failure is not recorded.

- [x] Write table tests: write timeout, unknown-outcome server error, client timeout, broken connection, `Unavailable` with and without the flag, `InvalidRequest`, and a failed read.
- [x] Run them and confirm the failure.
- [x] Write the code.
- [x] Run verify.
- [x] Commit `feat: classify checked operation outcomes [SCYLLADB-4519]`.

## Task 11 — invariants engine (T5)

**Files:** Create: `src/bin/cql-stress-sc-verify/invariants.rs`

**Internals:** `struct RowState` (per wid: cell mask, start, `WriteStatus { InFlight,
Ok(end), Indeterminate, Fail(end) }`; per cell: `ack` and `obs` floors as `Option<u64>`,
where `Some` means seen); `begin_write(mask, start) -> wid` (wids from 1; an empty mask
panics); `end_write(wid, WriteEnd)`; `snapshot() -> Floors`, taken before the read's start
time; `check_read(&Floors, seen) -> Vec<Violation>` (no start-time parameter: the floors carry
the timing); `end_read(seen)`, which skips INV-0 values; `status(wid)`. `Seen { Null,
Wid, Undecodable }`, `Violation { kind: Inv, cell }`. The module doc states the locking
contract for task 13.

- [x] Write tests: for each of INV-0..3, one timeline that fires and one legal one; an indeterminate write is never older; an op ending between `snapshot` and the read's start does not count; the F12 shape with non-overlapping writes fires INV-3. Added from the advisor's review: a wid issued after the snapshot; F12 with overlapping writes fires nothing (the known gap); INV-3 against an in-flight or indeterminate `u`; INV-0 values do not move the floors; equal times are not "before"; an empty write panics; write status.
- [x] Run them and confirm the failure. (They failed to compile; after the code, ten guarded breaks, one per rule, each failed a test.)
- [x] Write the code.
- [x] Run verify.
- [x] Commit `feat: check reads with streaming invariants [SCYLLADB-4519]`.

## Task 12 — keys and history file (T6)

**Files:** Create: `src/bin/cql-stress-sc-verify/history.rs`, `.../keys.rs`, `.../history_test.jsonl` (the golden file sits next to the code, like the repo's other fixtures); Modify: `Cargo.toml` (optional `serde_json` in the feature; it was a build dependency only)

**Internals:** `struct RowKey { pk, gen, ck }`; `struct GenMinter` (`start_ms × 2^20 + n`;
`next(now_ms) -> Option<i64>` returns `None` when a millisecond's 2^20 are used up and
the clock has not moved on; `new` refuses a clock before 2004, where gens could approach the
bulk range). No pk pool type: the `seq` population already wraps around and is thread-safe.
`enum OpRecord { Write { client, wid, mask, start_ns, status }, Read { client, start_ns,
end_ns, seen } }`; `struct CheckFile` with `create`, `append_row(key, cells, ops) -> key`,
`rows`, `finish`. It writes the `# key …` line, then call and return lines, with no return
for an indeterminate write; `gen` is a string; an undecodable cell is `-1`.

- [x] Write tests: gen ≥ 2^60 and strictly increasing across a re-base; pk wraps; one known row written equals the golden file byte for byte.
- [x] Run them and confirm the failure. (They failed to compile; after the code, five guarded breaks, covering an indeterminate write's return line, undecodable as null, `gen` unquoted, a non-strict re-base and no 2004 guard, each failed a test.)
- [x] Write the code.
- [x] Run verify.
- [x] Commit `feat: mint row keys and write v2 history files [SCYLLADB-4519]`.

## Task 13a — one checked operation (T6)

**Files:** Modify: `src/bin/cql-stress-sc-verify/ops.rs`, `.../main.rs`

**Internals:** `fn write_query(profile, mask, ttl)`: a full-row `INSERT` for all cells, a
partial `UPDATE` otherwise, with `USING TTL n` when `ttl > 0`; `struct Statements` prepared
once at start-up (the read and one write per non-empty mask, up to 255 with 8 cells);
`write(session, key, mask, wid, unavailable_is_fail)` and `read(session, key) -> Vec<Seen>`
(no row → all `Null`; strict `decode`), both returning `OpError { class: Failure, message }`.
`main` prepares the `Statements` at start-up instead of only the read.

- [x] Write tests: the four query shapes; against an ordinary keyspace (CI runs cargo tests
  on the plain node): absent → all null, INSERT then partial UPDATE read back as the
  expected wids, a foreign value → `Undecodable`.
- [x] Run them and confirm the failure. (They failed to compile; after the code, binding the
  UPDATE's key values first failed the DB test.)
- [x] Write the code.
- [x] Run verify.
- [x] Commit `feat: execute checked reads and writes [SCYLLADB-4519]`.

## Task 13 — the slot loop (T6; 13b in the log)

**Files:** Create: `src/bin/cql-stress-sc-verify/slot.rs`; Modify: `main.rs`

**Internals:** a shared `Checked` (session, `Statements`, settings, pk distribution,
`Mutex<GenMinter>`, process clock, stop flag, `ExitCode` where the highest code wins,
workload-error counter). `run_slot(i)`: a ticker at phase `i × interval / slots`
(`MissedTickBehavior::Skip`) advances a `watch` tick counter; the first row is checked
absent (start-up check 4, else exit 2); then rounds until stopped. A round: `Mutex<Round>`
(`RowState`, `OpRecord`s, a `Budget` that refills lazily on a new tick and starts empty, so
a new row waits for the next tick, counters, max gap) and `clients_per_row` client futures
(`join_all` = drain). A client marks the tick seen, then under the lock checks
`should_stop` and takes a start; it waits on `changed()` when the budget is empty. A write
takes its start time, `begin_write`, sends, takes its end time, then locks and records. A
read snapshots, unlocks, takes its start time, sends, takes its end time, then locks,
checks and records. The sweep is client `clients_per_row`, retried with backoff. Sealed rows
go over an mpsc channel. `main` (`--mode verify`) stops at `--duration`, awaits the slots
(a panic → exit 3), and exits with the highest code (a violation → 1). `--ttl` must cover 10
of the longest rounds. `history` keeps its dead-code allow until task 14.

- [x] Write tests: `should_stop` for each `stop_reason`; the burst budget gives 16 starts per tick.
- [x] Run them and confirm the failure. (They failed to compile; after the code, five guarded breaks each failed a test: an accumulating budget, a new row bursting at once, `end` before the row's own limits, a full mask as a partial update, exit codes overwritten.)
- [x] Write the code; `--mode verify` runs slots until `--duration`.
- [x] Remove the temporary `#[allow(dead_code)]` on the modules in `main.rs` (all except `history`, whose `CheckFile` is used from task 14).
- [x] Run verify; a 10 s compose run exits 0. (128 rows of 41 operations with `--ops-per-gen 40`, 0 violations, every sweep ok; the schema pytest now runs real slots.)
- [x] Commit `feat: run checked slots in bursts [SCYLLADB-4519]`.

## Task 14 — rows.jsonl and check files on disk (T6)

**Files:** Modify: `slot.rs`, `history.rs`, `main.rs`; Modify: `tools/test_cs_sc_verify.py`, `tools/cql-stress-cassandra-stress-ci.py`

**Internals:** `struct Recorder` in `history.rs` (`new(dir, cells, check_rows,
check_age)`, `record(&SealedRow) -> Option<archive dir>`, `finish`): creates `sealed/` and
`archive/`, continues the `sealed/<seq>` numbering after a restart, appends to
`rows.jsonl` (flushed per row; `gen` and `file` as strings; `verdict` is `violation` or
`unchecked`), rotates on `--check-rows`/`--check-age`. A row with a violation closes its
check file at once and copies it to `archive/<seq>/<seq>.jsonl`. `main` records every
sealed row; a failure to record is exit 3. The pytest helper passes `--history-dir`.

- [x] Write pytest `run_verify_quiet`: 60 s, `--checker off`; every row has ≥ 150 ops, `verdict` `unchecked`, 0 violations.
- [x] Run it and confirm the failure. (The Recorder unit tests failed to compile first; the pytest was written after the code.)
- [x] Write the code.
- [x] Run verify and the pytest. (60 s on the SC node: 160 rows, 128 full at 201 operations, 12.9 s each, read share 0.503, 0 violations.)
- [x] Commit `feat: record sealed rows and check files [SCYLLADB-4519]`.

## Task 15 — report and SCV lines (T7)

**Files:** Create: `src/bin/cql-stress-sc-verify/report.rs`; Modify: `main.rs`, `slot.rs`

**Internals:** `enum Scv` (serde, tagged `t`): `Start`, `Violation` (printed at seal with its
archive and the detection time `wall_ms`), `Stats(IntervalStats)`, `End`; `gen` and
`gen_base` as strings, `cell` as `c<n>`. `struct Stats` shared by the slots: hdrhistograms
in microseconds (read, write, scheduling delay; per interval and total; successful operations
only), interval counters for ops, writes, indeterminate writes. The slot ticker records how
late each tick ran. `struct Report` (totals) is written to `report.json` atomically (a temp
file, then rename) every `--report-interval` and at exit with `exit`. `main`'s consumer is a
`select!` over sealed rows and the report interval.

- [x] Write tests: each line's JSON shape; `gen_base` is a string.
- [x] Run them and confirm the failure. (They failed to compile.)
- [x] Write the code.
- [x] Run verify; the compose run prints `start`, `stats` and `end`. (12 s: 512 checked ops/s = 32 slots × 16, p99 about 5 ms, scheduling delay p99 1.9 ms, `report.json` with `exit: 0`.)
- [x] Commit `feat: report sc-verify progress as SCV lines [SCYLLADB-4519]`.

## Task 16 — stale-read fault flag (T8)

**Files:** Modify: `slot.rs` (it needs the row's memory), `cli.rs`; Modify: `tools/test_cs_sc_verify.py`, `tools/cql-stress-cassandra-stress-ci.py`

**Internals:** hidden `--fault-stale-reads <p>` (default 0): with probability p, a successful
read is replaced by what the row's first recorded read saw, before it is checked and
recorded, so the history holds what the "read returned".

- [x] Write pytest `run_stale_reads`: with the flag, `SCV violation` of INV-1 or INV-2 within one row, exit 1.
- [x] Run it and confirm the failure.
- [x] Write the code.
- [x] Run verify and the pytest. (15 s at p = 0.2: 58 of 64 rows violated, archived, exit 1, `SCV end` exit 1.)
- [x] Commit `test: inject stale reads to prove the invariants fire [SCYLLADB-4519]`.

**Checkpoint B** (human review): the 60 s compose run is clean; the fault flag gives exit 1.

## Task 17 — bulk mode (T9)

**Files:** Create: `src/bin/cql-stress-sc-verify/bulk.rs`; Modify: `main.rs`; Modify: `tools/test_cs_sc_verify.py`, `tools/cql-stress-cassandra-stress-ci.py`

**Internals:** `bulk_gen(pk)` (splitmix64 into `[0, 2^40)`); `pk_for(dist, op_id)` seeds a
per-worker distribution with the operation id, so a retry hits the same pk and a `seq`
preload covers its range exactly once; `BulkFactory`/`BulkOperation` on `run.rs` (`make_runnable!`):
`write` (INSERT of random values at the profile's sizes, CL from `--consistency`), `read`
(by full key, a miss counted; `--bulk-read-consistency`), `mixed`; `-n` → `Break` at that
operation id; `--bulk-retries` → `max_retries_per_op`; `ignore_errors` (bulk is load).
`BulkStats` (ops, misses, failed attempts). `main` splits into `run_verify` and `run_bulk`;
`SCV start` gets optional `pop`/`gen_base`/`bulk_pop`; `report.json` gets `bulk_ops`,
`bulk_misses`, `bulk_errors`; the `stats` lines carry `bulk_ops_s`. `--mode bulk` uses the
start-up session; `--mode both` (task 18) gives bulk its own.

- [x] Write tests: `bulk_gen < 2^40` over many pks; pytest `run_bulk`: write `-n` then read the same range → 0 misses.
- [x] Run them and confirm the failure. (The unit tests failed to compile; the pytest failed until `--mode bulk` existed.)
- [x] Write the code.
- [x] Run verify and the pytest. (2000 written, 2000 read with 0 misses, a never-written range 100/100 misses; a 5 s mixed run about 1700 ops/s on 16 threads.)
- [x] Commit `feat: add sc-verify bulk mode [SCYLLADB-4519]`.

## Task 18 — `--mode both` (T9)

**Files:** Modify: `main.rs`; Modify: `tools/test_cs_sc_verify.py`, `tools/cql-stress-cassandra-stress-ci.py`

- [x] Write pytest `run_both`: 60 s; every checked row is clean and bulk ops are > 0.
- [x] Run it and confirm the failure. (exit 2: not implemented; then, after the first version, 0 bulk operations, caught by the `bulk_ops_s > 0` assertion: the dropped controller.)
- [x] Run slots and bulk together in one process, each on its own session. (`run_checked(with_bulk)`: bulk connects its own session; its `RunController` is kept alive for the whole run, because dropping it stops the run at once; `stats` lines carry both rates; `report.json` the bulk totals.)
- [x] Run verify and the pytest. (20 s: 512 checked + about 950 bulk ops/s, 64 rows, 0 violations; checked p99 rises from about 5 to 18–26 ms on this single local node, to be measured on a real cluster in the calibration run.)
- [x] Commit `feat: run checked and bulk load together [SCYLLADB-4519]`.

## Task 19 — CI and image (T10)

**Files:** Modify: `.github/workflows/rust.yml` (the strong-consistency build uploads `cql-stress-sc-verify` too, and the SC job puts it on PATH); `Dockerfile` only if the feature build skips the new binary

- [x] Build the image locally with `--build-arg CARGO_BUILD_FEATURES=strong-consistency`; `docker run --rm <image> -c "cql-stress-sc-verify --help"` works.
- [x] Make the CI change. (Dockerfile: one `COPY` with `cql-stress-sc-verif[y]`, a pattern allowed to match nothing next to a source that matches; `rust.yml`: the SC build and upload, and the SC test job's PATH.)
- [x] Run verify. (Local SC image: 6.5 min build, all three binaries, `version` shows the branch SHA, a 5 s `--mode both` run inside it exits 0.)
- [x] Commit `ci: build and test cql-stress-sc-verify in the strong consistency job [SCYLLADB-4519]`.
- [ ] Ask the user, then `buildx` for amd64 and arm64 and push `aleksbykov/cql-stress:sc-verify-m1-<short-sha>`; `docker manifest inspect` shows both.

**Checkpoint C** (human review): the PR is ready to open, linking the spec.

# Milestone 2 (branch `feat/sc-verify-m2`)

The same rules apply. Tasks 20–27 map to T21–T24 of the tracking list. New code goes in
`checker.rs` and `readback.rs` under `src/bin/cql-stress-sc-verify/`. Child-process tests
use small fake checker scripts written into a temp dir; they need no Scylla.

## Amendments from the review before code (advisor, 2026-10-06)

- Task 20: the start-up probe gets its own short timeout (a timeout counts as "cannot
  run"), and runs only for `verify` and `both`; `--mode bulk` never spawns a checker. The
  pytest helper passes `--checker off` until task 22 brings the binary in. Every fake checker
  exits 2 on empty input.
- Task 21: `GOMEMLIMIT` is the byte count (Go does not read `4G`); `RLIMIT_DATA` is 1.25 ×
  `--checker-mem`, so the Go GC can act before the hard limit kills the child.
- Task 22: the queue is a bounded `tokio::sync::mpsc` of (file, pending row lines);
  `try_send` failing *is* "queue full → skipped". Workers send verdicts back on a second
  channel, a third `select!` arm in `main`. The recorder stays single-owner. A check file
  holding a row with an invariant violation is never deleted, whatever the checker says.
  Tests use short `--checker-timeout`/`--checker-deadline` values, and add `--queue-max 1`
  with a hanging checker → `SCV skipped` and `skipped` verdicts. CI: the
  `strong-consistency-tests` job copies `porcupine_checker` out of the pinned PV image,
  whose tag is defined once, in the Dockerfile's `CHECKER_IMAGE` default.
- Task 26: `COPY --from` does not expand an `ARG`; the shape is `FROM ${CHECKER_IMAGE} AS
  checker` then `COPY --from=checker`. It is tried on a scratch Dockerfile first, including
  the ordinary image, which must not get the checker.

## Task 20 — M2 command line and the checker start-up check (T21)

**Files:** Modify: `cli.rs`, `main.rs`

**Internals:** `Checker { On, Off }` with `On` the default; `--checker-bin`,
`--checker-workers`, `--checker-timeout`, `--checker-mem` (`4G`, `512M`), `--checker-deadline`,
`--queue-max`, `--canary-every`, `--key-timeout`, `--max-viz`, `--readback on|off`,
`--readback-sample`, `--readback-concurrency`; zero values rejected as in M1. With
`--checker on`, start-up runs `<checker-bin>` on an empty v2 history and needs exit 2
("no input") rather than a spawn error; otherwise exit 2.

- [x] Write tests: the M2 defaults; `--checker-mem 4G` parses as bytes; bad sizes and zeros fail.
- [x] Run them and confirm the failure. (Compile errors; the probe's tests use fake checker scripts: good, wrong exit, hanging, missing.)
- [x] Write the code.
- [x] Run verify (the pytests pass `--checker off` until task 22). (Manual: no checker → exit 2 with the reason; the real checker → runs; `--mode bulk` never probes. `Cargo.toml`: the feature turns on `tokio/process` and `tokio/io-util`.)
- [x] Commit `feat: add the sc-verify milestone 2 options [SCYLLADB-4519]`.

## Task 21 — run one check file in a child process (T21)

**Files:** Create: `src/bin/cql-stress-sc-verify/checker.rs`; Modify: `Cargo.toml` (optional `libc` in the feature)

**Internals:** `async fn check_file(cfg, file, rows, archive_dir) -> FileVerdicts` (`--key-timeout` is passed as milliseconds; the child's stderr goes to `archive_dir/checker.stderr`; `mod checker` keeps a dead-code allow until task 22): spawns the
checker with stdin from the file, `GOMEMLIMIT`, and a `pre_exec` that sets `nice 10` and
`RLIMIT_DATA`; reads stdout line by line into `key → ok|illegal|unknown`; kills it after
`--checker-timeout`; a key without a line is `unknown`. `FileVerdicts { per_key, killed }`.

- [x] Write tests with fake checkers (shell scripts): all ok; one illegal; a checker that
      prints one line and then hangs (killed at the timeout, the rest `unknown`); one that
      crashes; and one that checks its own `nice` value and `GOMEMLIMIT`.
- [x] Run them and confirm the failure. (They failed to compile; after the code, dropping `nice` or the data limit each failed the limits test.)
- [x] Write the code.
- [x] Run verify.
- [x] Commit `feat: check a history file in a limited child process [SCYLLADB-4519]`.

## Task 22 — the checker queue (T21)

**Files:** Modify: `history.rs` (the recorder hands closed files over and writes rows'
lines once their verdict is final), `checker.rs`, `main.rs`, `report.rs`; Modify:
`tools/test_cs_sc_verify.py`, `tools/cql-stress-cassandra-stress-ci.py`

**Internals:** a bounded queue (`--queue-max`) of closed files with their pending rows;
`--checker-workers` worker tasks; per file: verdicts → the rows' `rows.jsonl` lines
(severity order), all ok → delete the sealed file, otherwise copy it into `archive/<seq>/`
(the child writes `result.json` and visualizations there); `SCV checked`, `SCV skipped`; a
full queue → archive, `skipped`; at the end the queue drains until `--checker-deadline`, and
what is left is `skipped`. `stats` gains `queue`; `report.json` gains checker totals.

- [x] Write the pytest first: `--checker on` with the checker built from porcupine_validator
      `feat/checker-v2`: 30 s quiet → every row `ok`, `SCV checked` lines, no `sealed/`
      files left; with a hanging fake checker → rows `unknown` and checked ops/s within 5%
      of a `--checker off` run.
- [x] Run it and confirm the failure. (Rows came out `unchecked`. Added from the review: `--queue-max 1` with a hanging checker → `skipped`. Unit tests: the queue returns every file; a full queue refuses at once; the recorder hands closed files over and writes their lines later.)
- [x] Write the code.
- [x] Remove the dead-code allow on `mod checker`.
- [x] Run verify and the pytests. (Real checker: 96 rows `ok`, every file deleted; hanging checker: 64 rows `unknown`, ops/s within 5% of `--checker off`; queue full: 256 rows `skipped`, files archived. CI: the SC job copies `porcupine_checker` out of the image the Dockerfile pins as `ARG CHECKER_IMAGE`, which is public on Docker Hub.)
- [x] Commit `feat: check sealed rows with porcupine_checker [SCYLLADB-4519]`.

## Task 23 — canaries (T22)

**Files:** Modify: `checker.rs`, `history.rs`, `main.rs`, `report.rs`; Modify: the pytest files

**Internals:** `history::make_canary(text)` (text-level: the first row that has a read, as key
0, its earliest read's first cell set to the row's highest wid + 1, every other line
unchanged); `main`'s `Checking` struct (queue, pending files, pending canaries, file count)
with `submit`, `verdicts`, `drain`; canaries are queued under `CANARY_SEQ + n` into
`sealed/canary-<n>.jsonl`, checked into `archive/canary-<n>/`;
queued like any file; its result is never counted in `rows.jsonl`; not `illegal` →
`SCV canary` with the result, `verifier-broken`, exit 1.

- [x] Write tests: the canary file differs from the row's history only in that cell; pytest
      `--canary-every 1 --check-age 10s` for 60 s → every canary `illegal`; with a fake
      checker that answers `ok` to everything → exit 1, `verifier-broken`.
- [x] Run them and confirm the failure. (The unit test failed to compile first; the pytests came after the code. Accepting a canary answered `ok` makes the broken-checker test fail.)
- [x] Write the code.
- [x] Run verify and the pytests. (60 s, canary every file: 160 rows `ok`, 5 canaries `illegal`; the lenient checker: 2 canaries `ok` → `verifier-broken`, exit 1, kept in `archive/canary-*`.)
- [x] Commit `feat: prove the checker still rejects a corrupted history [SCYLLADB-4519]`.

## Task 24 — the expected state at seal (T23)

**Files:** Modify: `invariants.rs` (`RowState::expected`), `slot.rs`, `history.rs`

**Internals:** `fn expected(&self, sweep: Option<&[Seen]>) -> Expected { cells: Vec<Vec<Option<u64>>>,
max_wid, burned }` per §12.1: sweep ok → the value the sweep saw plus every indeterminate
write to the cell; sweep incomplete → every ok write to the cell that ended at or after
`ack_floor`, every indeterminate write, and null if no write to the cell was acknowledged.
The recorder appends it to `expected.jsonl`.

- [x] Write tests: sweep ok with and without indeterminate writes; sweep incomplete with an
      acknowledged write, with only indeterminate writes, and with a burned wid.
- [x] Run them and confirm the failure. (Compile error; after the code, dropping the indeterminate writes or the null each failed a test. `read`/`sweep` now return what they saw; `SealedRow.expected`; the recorder appends `expected.jsonl`, whose line format the recorder test pins.)
- [x] Write the code.
- [x] Run verify.
- [x] Commit `feat: record each row's expected final state [SCYLLADB-4519]`.

## Task 25 — the read-back (T23)

**Files:** Create: `src/bin/cql-stress-sc-verify/readback.rs`; Modify: `main.rs`, `report.rs`; Modify: the pytest files

**Internals:** `fn judge(expected, observed: &[Seen]) -> Vec<CellResult>` (pure, §12.1 table);
after the checker drains, every retired row (or `--readback-sample` of them; with `--ttl`, only
rows younger than it) is read with QUORUM, `--readback-concurrency` at a time, with
`--sweep-retries` retries; failing cells → `readback.jsonl`; `indet_landed` counts cells that
hold an indeterminate write; `SCV readback`; `lost` or `phantom` → exit 1.

- [x] Write tests: the decision table (in the set → ok; null or an issued wid outside the
      set → lost; never issued, burned or undecodable → phantom; absent row = all null);
      pytest: a row deleted behind the tool's back → `lost`, exit 1.
- [x] Run them and confirm the failure. (The unit tests failed to compile; the deleted-row pytest failed with exit 0 and no `SCV readback`. A `>`/`>=` slip at the highest wid fails `judge_test`. `Expected` gains `indeterminate`. The pytest deletes through `cqlsh` in the container: with tablets-routing-v2 negotiated, the Python driver's writes are rejected.)
- [x] Write the code.
- [x] Run verify and the pytests. (Quiet 60 s: 160 rows read back `ok`; deleted row: 1 `lost`, its cells null against their expected wids, exit 1.)
- [x] Commit `feat: read every row back at the end of the run [SCYLLADB-4519]`.

## Task 26 — bundle the checker into the image (T24)

**Files:** Modify: `Dockerfile`

**Internals:** with `CARGO_BUILD_FEATURES=strong-consistency`, the image gets
`/usr/local/bin/porcupine_checker` from `CHECKER_IMAGE` (an `ARG`; the pinned
porcupine_validator `v2-<sha>` image), for both architectures; the ordinary image does not.

- [x] Build both images locally (the stage trick was tried on a scratch Dockerfile first: no features → no checker, `strong-consistency` or `user-profile,strong-consistency` → the checker; the real ordinary image has no checker, exit 127); `docker run <sc image> -c "porcupine_checker < /dev/null"`
      exits 2 ("no input"); a 30 s `--mode both` run inside the image checks rows. (Defaults plus `--canary-every 1 --check-age 5s`: 3 files, 96 rows `ok`, 3 canaries `illegal`, read-back 96/96 `ok`, `sealed/` empty, exit 0. The pinned checker image is amd64 only, so an arm64 build of this image waits for an arm64 checker image.)
- [x] Commit `build: bundle porcupine_checker into the strong consistency image [SCYLLADB-4519]`.

## Task 27 — the milestone 2 integration list (T24)

- [x] On compose, the §20.5 list: 60 s with `--canary-every 1 --check-age 10s` → all rows
      `ok`, read-back all `ok`, canaries `illegal`; a hanging checker → `unknown` and
      unchanged throughput; a tiny `--checker-mem` → `unknown`; a deleted row → `lost`;
      plus the M1 tests unchanged. (17 SC pytests: the canary run now also reads back ok; `--mode both` uses the real checker and wants every row `ok`; new: `--checker-mem 1M` → the Go runtime cannot start, rows `unknown`. Rust tests also pass on the plain node, as in CI. The first full run failed only because the host disk was 99% full and ScyllaDB rejected writes; after pruning the Docker build cache, all green.)

**Checkpoint F** (human review): the §20.5 integration list is green; the CS M2 PR is ready.

# Positive controls (branch `feat/sc-verify-m2`)

The same rules apply. Tasks 28–31 map to the tracking phase 7. All local runs use the
ScyllaDB 2026.4 nightly.

## Task 28 — the mutation matrix

**Files:** Create: `tools/sc_verify_mutate.py`; Modify: `tools/test_cs_sc_verify.py`,
`tools/cql-stress-cassandra-stress-ci.py`

**Internals:** parse a check file into rows (`# key` headers, call/return lines); per class,
find the first eligible ops and rewrite one read's cell; write the mutated file; the
pytest runs `porcupine_checker` on each and compares per-row results with the expected.

- [x] Write the pytest first (it fails: no mutator). (ModuleNotFoundError.)
- [x] Write the mutator; every class has a docstring proof of why it is illegal (or legal). (On a 50-row file of a quiet run: stale, backwards, future, phantom 50/50 rows illegal; torn 47/47 (3 rows without a full write); identity, drop-read, unack-write, shift 50/50 ok. The checker prints "visualization written to" on stderr, so the test fails on any other stderr line.)
- [x] Map each class to its invariant unit test; add any that is missing. (stale → `inv1_read_your_writes_test`, backwards → `inv2_never_goes_backwards_test`, torn → `inv3_no_torn_row_test`, phantom and future → `inv0_no_phantom_value_test`: online, a write issued after the read is checked is indistinguishable from one never issued. None missing.)
- [x] Run verify; commit `test: show the checker rejects each kind of anomaly [SCYLLADB-4519]`.

## Task 29 — replayed writes

**Files:** Modify: `cli.rs`, `slot.rs`, `tools/test_cs_sc_verify.py`,
`tools/cql-stress-cassandra-stress-ci.py`

**Internals:** the round keeps its acknowledged writes `(wid, mask, end)`; after an
acknowledged write, with probability `--fault-replay-writes`, it resends the newest older
one superseded on some cell, unrecorded, result ignored.

- [x] Write the pytest first (it fails: unknown flag): violations > 0, `checker_illegal` > 0,
      exit 1, read-back `lost == phantom == 0`. (Exit 2, "unexpected argument".)
- [x] Write the code; a unit test for choosing the write to replay. (A write is surely superseded only by a later write called after it returned that shares a cell; overlapping writes are not. The recording moved into a sync `record_write`, so no lock guard lives across the replay's `await`.)
- [x] Run verify; commit `test: add a replayed-write fault the verifier must catch [SCYLLADB-4519]`. (3 runs out of 3 pass on nightly 20261006; one run: 64 rows, 39 hit by a replay, 711 violations, `checker_illegal` 39 = exactly those rows, the other 25 `ok`, read-back 64/64 `ok`.)

## Task 30 — the eventually consistent mode

**Files:** Modify: `cli.rs`, `profile.rs`, `startup.rs`, `ops.rs`, `tools/test_cs_sc_verify.py`

**Internals:** `--unsafe-eventual` (hidden); `CheckedConsistency::One` only with it; the
keyspace DDL without `consistency = 'global'`; start-up requires a keyspace that is not
strongly consistent.

- [x] Write tests first: `--consistency one` rejected without the flag; the DDL without
      `consistency`; a pytest on the compose node: an eventually consistent keyspace is
      created and a short run at ONE is clean (one node cannot be stale). (The unit tests failed to compile. The pytest also checks that a normal run refuses that keyspace and the mode refuses a strongly consistent one, both exit 2.)
- [x] Write the code. (The bulk writes follow `--consistency`, so they run at ONE too; the start-up line names the keyspace eventually consistent in this mode.)
- [x] Run verify; commit `test: add an eventually consistent mode for the verifier's controls [SCYLLADB-4519]`.

## Task 31 — local cluster runs

On a 3-node RF 3 nightly cluster: the replayed-write fault on SC; the eventually consistent
control (hinted handoff off, a node stopped ~30 s under load, long rows) and its SC
negative control. Results in the tracking repo `results/`.

Done on nightly 20261006 (tracking `results/2026-10-07-controls/README.md`):
- replay on SC, 320 rows: 203 flagged by the invariants, 205 judged illegal by porcupine (all 203 + 2), read-back clean, exit 1;
- eventually consistent at ONE, `scv-n3` stopped 45 s: 274 rows illegal (35 by the invariants), read-back 166 lost (all overlapping the outage), exit 1; with long rows: 112 illegal, 7 lost;
- the SC negative control under the same stop: 608 rows ok, read-back clean, exit 0.
