# SCYLLADB-5659 — implementation plan

**Spec:** `tasks/SCYLLADB-4519/SCYLLADB-5659/spec.md`

A decision during the build that changes what the spec states updates the
spec in the same commit. A line in its `Decisions` section records the
decision when the Design section does not state the reason. This paragraph
stays in every plan built from a spec.

## Rules

- **Branch and PR:** `feat/sc-verify-m2`, PR #206. No new PR. The verifier
  builds only with the `strong-consistency` feature and
  `RUSTFLAGS="--cfg scylla_unstable"`.
- **Tracking spec:** `docs/SPEC-sc-verify.md` in the sc-verify tracking
  repository already states the change (`7c40cc5`). A build decision that
  changes it updates it before the code commit.
- **Code conventions:**
  - Errors with `anyhow` and `.context(...)`. Diagnostics with
    `tracing::warn!`; `println!` only for user-facing output
    (`docs/standards/backend/rust-conventions.md`).
  - Unit tests sit in the `#[cfg(test)]` module at the end of the file.
    Integration logic is `run_<case>()` in `tools/test_cs_sc_verify.py`,
    called from a `test_*` function in `tools/cql-stress-cassandra-stress-ci.py`
    (`docs/standards/testing/test-writing.md`).
- **Verify sequence:** section `Commands` of `CLAUDE.md`, with the
  strong-consistency compose node up. The tracking repository's
  `work/verify-cs.sh` runs it all, the SC pytests included, and prints
  `VERIFY_OK`.

## Task 1 — Correct the parent spec

The subtask changes two things the parent design states. Correct them
before the code.

**Files:**
- Modify: `tasks/SCYLLADB-4519/spec.md`, section `Milestone 2`:
  - **the flowchart:** the queue-full branch becomes "delete, or archive with
    `--keep-skipped on`; rows skipped";
  - **`Constraints`:** `nice 10` and `RLIMIT_DATA` are set by the tool right
    after a `posix_spawn` start;
  - **`Failure behaviour`:** the `Queue full` row;
  - **`Contracts`:** `--keep-skipped off` in the flag block, and the
    `SCV skipped` line with the optional `archive`.

- [x] Edit the five places. Name SCYLLADB-5659 once, in the constraint.
- [x] Commit `docs(sdlc): correct the parent design for SCYLLADB-5659 [SCYLLADB-5659]`.

## Task 2 — Start checkers without fork

**Files:**
- Modify: `src/bin/cql-stress-sc-verify/checker.rs`:
  - `probe`, lines 20–35;
  - `check_file`, lines 100–150;
  - `the_child_is_limited_test`, lines 453–484.
- Test: `src/bin/cql-stress-sc-verify/checker.rs`, `mod tests`.

**Internals:**
- **`probe` and `check_file`:** call `command.process_group(0)` in place of
  their `pre_exec` blocks. A comment on each command says that a
  `pre_exec`, uid, gid or chroot makes the standard library fork the whole
  tool for every check file (SCYLLADB-5659).
- **`fn limit_child(pid: i32, data_limit: u64) -> std::io::Result<()>`:**
  - `libc::setpriority(PRIO_PROCESS, pid, 10)`, then
    `libc::prlimit64(pid, RLIMIT_DATA, …)` at `data_limit` for both soft
    and hard;
  - `ESRCH` from either means the child already exited, so return
    `Ok(())`.
- **`check_file`:** calls `limit_child` right after `spawn()`. Any other
  error goes to one `tracing::warn!` per run, through a
  `static WARNED: std::sync::Once`. The check goes on.

- [x] Write the failing test `the_checker_starts_without_fork_test`:
  - register `libc::pthread_atfork` once (a `std::sync::Once`), with a
    prepare handler that bumps a `static FORKS: AtomicUsize`;
  - run `probe` and `check_file` with a fake checker;
  - assert `FORKS` did not change.

  glibc runs the handler on `fork()` and not on `posix_spawn`. If
  `libc 0.2.186` lacks `pthread_atfork` for Linux, declare it in an
  `extern "C"` block in the test module.
- [x] Run it and confirm the failure: `pre_exec` forks.
- [x] Change `the_child_is_limited_test`: its fake checker sleeps 0.2 s
  before it reads `nice` and `ulimit -d`, because the limits now arrive just
  after the start. Run it and confirm it still passes on the old code.
- [x] Write `limit_child` and swap `pre_exec` for `process_group(0)` in
  `probe` and `check_file`. Both tests pass.
- [x] Add `limit_child_ignores_an_exited_child_test`: spawn `/bin/true`,
  wait for it, call `limit_child` on its pid, expect `Ok(())`.
- [x] Check the syscall once by hand, with the release binary and the
  compose node: `strace -f -e trace=clone,clone3` over a short `--mode verify`
  run shows `clone3` with `CLONE_VM|CLONE_VFORK` for the checker children.
  Put the line in the PR body.
- [x] Run the verify sequence.
- [x] Commit `fix(sc-verify): start checkers without forking the tool [SCYLLADB-5659]`,
  with the boxes of this task checked.

## Task 3 — `--keep-skipped on|off`, default off

**Files:**
- Modify: `src/bin/cql-stress-sc-verify/cli.rs`: the flag after
  `--queue-max`, and `defaults_follow_the_spec_test`.
- Modify: `src/bin/cql-stress-sc-verify/report.rs`: `Scv::Skipped`, lines
  71–74, and its line test.
- Modify: `src/bin/cql-stress-sc-verify/verdicts.rs`: `skip`, lines 300–322.
- Test: `tools/test_cs_sc_verify.py`: `run_queue_full`, and the new
  `run_queue_full_keep`.
- Test: `tools/cql-stress-cassandra-stress-ci.py`:
  `test_strong_consistency_sc_verify_queue_full_keep`.

**Internals:**
- **`cli.rs`:** `enum KeepSkipped { On, Off }`, like `Checker`, and
  `Cli::keep_skipped`, with `#[arg(long, value_enum, default_value_t = KeepSkipped::Off)]`.
  Its doc says the rows are recorded either way.
- **`Scv::Skipped`:** gains `archive: Option<&'a str>`, with
  `#[serde(skip_serializing_if = "Option::is_none")]`.
- **`Verdicts::skip`:**
  - with `On`, `self.recorder.archive(seq, path)`, and the line carries
    `Some(&dir)`;
  - with `Off`, `self.recorder.discard(seq, path)`, and the line carries
    `None`;
  - an error from either prints `error: …` and raises exit 3, as the archive
    move does today.

- [x] Write the failing tests:
  - `defaults_follow_the_spec_test` expects `KeepSkipped::Off`;
  - the `report.rs` line test adds
    `SCV {"t":"skipped","file":"41","rows":50,"archive":"archive/41"}`;
  - `run_queue_full` asserts that no skipped file is left in `archive/` or
    `sealed/` and that no `SCV skipped` line has `archive`. Its rows are
    still `skipped`;
  - `run_queue_full_keep` runs the same shape with `--keep-skipped on` and
    asserts each skipped file is in the `archive/<seq>/` that its `SCV
    skipped` line names.
- [x] Run them and confirm the failures.
- [x] Write the flag, the field and the two branches of `skip`.
- [x] Run the verify sequence.
- [x] Commit `feat(sc-verify): delete skipped check files unless --keep-skipped on [SCYLLADB-5659]`,
  with the boxes of this task checked.

## Task 4 — Soak check on the compose node

The spec's risk: the drift may have a second cause. Compare the tool before
and after on one shape that makes many cheap files.

**Files:**
- Create: none. The numbers go to the PR body and to the tracking
  repository's status.

- [x] Build both binaries: the parent of Task 2's commit, and the branch head.
- [x] Run each for 10 minutes against the compose node:
  `--mode verify --slots 64 --clients-per-row 8 --burst-interval 100ms
  --ops-per-gen 100 --check-rows 5 --checker-workers 4 --readback off`.
- [x] Per minute, record:
  - checked files/s and the maximum `queue`;
  - `sched_delay_p99_ms`, from the `SCV` lines;
  - the tool's kernel CPU, from `/proc/<pid>/stat` field 15 sampled each
    minute.
- [x] Compare the two runs:
  - **pass:** after the change, checked files/s and `sched_delay_p99_ms`
    stay level over the 10 minutes, and kernel CPU per checked file is lower;
  - **if `sched_delay_p99_ms` still grows:** say so in the PR body and file
    the leftover drift as its own issue.

**Result (2026-10-24):** both runs held 18.1–18.4 checked files/s, an empty
queue and `sched_delay_p99_ms` at 2.0 ms for the whole 10 minutes, at 9,230
ops/s. The tool's kernel CPU per checked file went from 5.64 ms to 4.30 ms.
The tool held only 10–21 MB here, against 4–6 GiB on the AWS loader, so the
loader-scale drift can't show on this node; the SCT run with the new image
checks it.

## Task 5 — PR #206 and the image

- [ ] Add a SCYLLADB-5659 section to the PR #206 body:
  - the cause, with the measurement table;
  - both changes;
  - the strace line and the soak numbers;
  - `closes SCYLLADB-5659`.
- [ ] Build and push the image `aleksbykov/cql-stress:sc-verify-m2-<sha>`
  from the branch head, as for `556435e`. Check:
  - `--keep-skipped` is in `--help`;
  - the bundled checker exits 2 on empty input;
  - the 4h job's L1 and L2 commands parse.
- [ ] Report the image tag for the SCT pin. The SCT change itself waits for
  the user.
