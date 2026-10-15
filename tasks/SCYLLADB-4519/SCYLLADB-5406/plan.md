# SCYLLADB-5406 — implementation plan

**Spec:** `tasks/SCYLLADB-4519/SCYLLADB-5406/spec.md`

A decision during the build that changes what the spec states updates the
spec in the same commit. A line in its `Decisions` section records the
decision when the Design section does not state the reason. This paragraph
stays in every plan built from a spec.

## Rules

- Branch `feat/sc-verify-m2`. The verifier builds only with the
  `strong-consistency` feature and `RUSTFLAGS="--cfg scylla_unstable"`.
- `cql-stress-cassandra-stress` is not touched: its port-range parser stays
  where it is, and the verifier gets its own copy with the same checks and
  messages.
- Errors with `anyhow` and `.context(...)`
  (`docs/standards/backend/rust-conventions.md`). Unit tests sit in the
  `#[cfg(test)]` module at the end of the file. Tests that need Scylla read
  `SCYLLA_URI` (default `127.0.0.1:9042`) and run with `--test-threads=1`
  (`docs/standards/testing/test-writing.md`).
- A setup failure goes through `exit_setup_failure` (exit 2), as the other
  start-up checks do.
- Verify sequence: section `Commands` of `CLAUDE.md`, with Scylla up through
  `docker compose -f docker/scylla-test/compose.yml up -d --wait`.

## Task 1 — The four flags, and the sessions use them

**Files:**
- Modify: `src/bin/cql-stress-sc-verify/cli.rs` (the `Cli` struct, section
  `// Connection.`; `parse_checked`; the parsers near `ratio`; the tests)
- Modify: `src/bin/cql-stress-sc-verify/startup.rs` (`connect`)

**Internals:**
- `Cli` fields, after `consistency`:
  - `connections_per_shard: usize`, `#[arg(long, default_value_t = 1,
    conflicts_with = "connections_per_host")]` (clap ignores a default when
    it checks conflicts);
  - `connections_per_host: Option<usize>`, `#[arg(long)]`;
  - `shard_aware_port_range: RangeInclusive<u16>`, `#[arg(long, default_value
    = "49152..65535", value_parser = port_range, conflicts_with =
    "connections_per_host")]`;
  - `tcp_reuse_address: bool`, `#[arg(long, default_value_t = false, action =
    clap::ArgAction::Set)]`, so it takes `true` or `false`.
- `fn port_range(s: &str) -> Result<RangeInclusive<u16>>`: the checks of
  cassandra-stress `ShardAwarePortRangeParam::parse`
  (`src/bin/cql-stress-cassandra-stress/settings/param/types.rs`): the shape
  `^[0-9]+\.\.[0-9]+$`, `Invalid port range start: {start}` / `end`, and
  `ShardAwarePortRange::new(start..=end)` with the context `Invalid port range
  {s}; must be a non-empty sub-range of 1024..65535`. It returns the range.
- `parse_checked`: `("--connections-per-shard", cli.connections_per_shard ==
  0)` and `("--connections-per-host", cli.connections_per_host == Some(0))` in
  the "must be greater than 0" list.
- `impl Cli { pub fn pool_size(&self) -> PoolSize }`: `PerHost(n)` when
  `connections_per_host` is set, else `PerShard(connections_per_shard)`.
- `connect`: `builder.pool_size(cli.pool_size())` and
  `.tcp_reuse_address(cli.tcp_reuse_address)`; for a per-shard pool also
  `.shard_aware_local_port_range(ShardAwarePortRange::new(range)?)`. Both the
  checked and the bulk session come from `connect`, so both get the pool.
  (Moved here from Task 2 during the build: without a caller, `pool_size`
  is dead code and `-D warnings` fails the task's own verify.)

- [x] Write the failing tests.
  - `defaults_follow_the_spec_test`: `connections_per_shard == 1`,
    `connections_per_host == None`, `shard_aware_port_range == 49152..=65535`,
    `!tcp_reuse_address`.
  - `rejected_combinations_test` adds
    `--connections-per-shard 2 --connections-per-host 2`,
    `--connections-per-host 2 --shard-aware-port-range 2000..3000`,
    `--connections-per-shard 0`, `--connections-per-host 0`,
    `--shard-aware-port-range 1000..2000`, `--shard-aware-port-range
    3000..2000`, `--shard-aware-port-range 2000`, and
    `--tcp-reuse-address maybe`.
  - A new `connection_flags_test`: `--connections-per-shard 4
    --shard-aware-port-range 1024..65535 --tcp-reuse-address true` parses to
    `PerShard(4)` and the range; `--connections-per-host 8` parses to
    `PerHost(8)`.
- [x] Run them and confirm the failure: `cargo test --features
  "strong-consistency" --bin cql-stress-sc-verify cli::`.
- [x] Write the smallest change that passes them.
- [x] Run the verify sequence from the `Commands` section of `CLAUDE.md`.
- [x] Commit, with the boxes of this task checked:
  `feat(sc-verify): connection pool flags for every session [SCYLLADB-5406]`.

## Task 2 — The pool must fit its ports

**Files:**
- Modify: `src/bin/cql-stress-sc-verify/startup.rs` (new functions, the
  tests)
- Modify: `src/bin/cql-stress-sc-verify/main.rs` (right after the first
  `startup::connect`, before `startup::startup`)

**Internals:**
- `pub fn shard_counts(session: &Session) -> Vec<usize>`: from
  `session.get_cluster_state().get_nodes_info()`, `sharder().nr_shards` of
  every node that has one. Nodes without shard information are left out:
  their connections use no port of the range.
- `pub fn check_pool(cli: &Cli, sessions: usize, shards: &[usize]) ->
  Result<()>`: nothing for a per-host pool. For a per-shard pool, with P the
  number of ports in the range: without reuse, `sessions × Σ shards × n ≤ P`;
  with reuse, `sessions × max(shards) × n ≤ P`. The error names the numbers
  and both fixes, as in the spec's message.
- `main`: `let sessions = if cli.mode == Mode::Both { 2 } else { 1 };` then
  `startup::check_pool(&cli, sessions, &startup::shard_counts(&session))`,
  failing through `exit_setup_failure`. It runs before the schema is
  touched.

- [x] Write the failing tests.
  - `check_pool_test` (unit, no cluster): 1 session × [14; 6] × 195 fits the
    default range; 2 sessions × [14; 6] × 100 = 16,800 fails, and the
    message holds `16800` and `16384`; the same with `--tcp-reuse-address
    true` fits (2 × 14 × 100); a per-host pool of 10,000 passes; a 10-port
    range `1024..1033` with 2 sessions, shards [4, 4] and n 1 needs 16 ports
    without reuse (fails) and 8 with it (fits).
  - `pool_connects_test` (needs Scylla): `connect` with
    `--connections-per-shard 2` builds a session; `shard_counts` returns one
    entry ≥ 1; `check_pool` passes for it.
- [x] Run them and confirm the failure: `cargo test --features
  "strong-consistency" --bin cql-stress-sc-verify startup:: --
  --test-threads=1`.
- [x] Write the smallest change that passes them.
- [x] Run the verify sequence from the `Commands` section of `CLAUDE.md`.
- [x] Commit, with the boxes of this task checked:
  `feat(sc-verify): check that the pool fits its ports [SCYLLADB-5406]`.

## Task 3 — The `start` line records the pool

**Files:**
- Modify: `src/bin/cql-stress-sc-verify/report.rs` (`Scv::Start`, a new
  struct, `scv_lines_test`)
- Modify: `src/bin/cql-stress-sc-verify/cli.rs` (`impl Cli`)
- Modify: `src/bin/cql-stress-sc-verify/main.rs` (both `Scv::Start` sites:
  `run_checked` and `run_bulk`)

**Internals:**
- `report.rs`: `#[derive(Serialize)] pub struct Connections { pool: &'static
  str, pool_size: usize, shard_aware_port_range: Option<String>,
  tcp_reuse_address: bool }`, added to `Scv::Start` as `#[serde(flatten)]
  connections: Connections` after `bulk_pop`, so the old keys keep their
  order and `null` is written for a per-host range.
- `impl Cli { pub fn connections(&self) -> Connections }`: `"per-shard"` or
  `"per-host"`, the size, `Some("a..b")` for a per-shard pool, the reuse
  flag.

- [ ] Write the failing test: `scv_lines_test` expects
  `SCV {"t":"start","mode":"verify","pop":"seq=0..2047","slots":32,"gen_base":"1878307305715400704","pool":"per-shard","pool_size":1,"shard_aware_port_range":"49152..65535","tcp_reuse_address":false}`
  and, for bulk,
  `SCV {"t":"start","mode":"bulk","slots":0,"bulk_pop":"seq=1..10","pool":"per-host","pool_size":8,"shard_aware_port_range":null,"tcp_reuse_address":true}`.
- [ ] Run it and confirm the failure: `cargo test --features
  "strong-consistency" --bin cql-stress-sc-verify report::`.
- [ ] Write the smallest change that passes it.
- [ ] Run the verify sequence from the `Commands` section of `CLAUDE.md`.
- [ ] Run the binary against the local node with `--mode verify --duration
  5s --connections-per-shard 2` and check the `start` line; with
  `--shard-aware-port-range 60000..60009 --connections-per-shard 8` check
  exit 2 and the message. (The driver starts filling the pool before the
  check, so a test of the failure uses a small range, never a huge pool.)
- [ ] Commit, with the boxes of this task checked:
  `feat(sc-verify): record the connection pool in the start line [SCYLLADB-5406]`.
