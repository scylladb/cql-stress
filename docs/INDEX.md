# Documentation Index

Read this file at the start of any task. It indexes the standards of this
repository and the project documentation.

## Standards

The conventions the team decided on. Follow them when you write code. When a
standard conflicts with the task, ask the user.

### Global standards

Located in `docs/standards/global/`.

#### Coding Style (`standards/global/coding-style.md`)
Format Rust code with default rustfmt settings. Keep Clippy clean with
`-D warnings` for every feature set that CI checks. Keep the fmt, check, and
Clippy steps of the Lint block in `CLAUDE.md` in line with CI.

#### Git Commits (`standards/global/git-commits.md`)
Write commit subjects as conventional commits, `type(scope): summary`. Use
the types `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `ci`, and
`build`. Use the `deps` scope for dependency updates.

#### Build Flags (`standards/global/build-flags.md`)
Repeat `--cfg scylla_unstable` wherever `RUSTFLAGS` is set, because the
variable replaces the value in `.cargo/config.toml`.

### Backend standards

Located in `docs/standards/backend/`.

#### Rust Conventions (`standards/backend/rust-conventions.md`)
Return `anyhow::Result` and make errors with `anyhow!`, `ensure!`, and
`bail!`. Keep the scylla-bench messages the same as the Go tool. Add
`.context()` to fallible calls in new code. Use `tracing` for diagnostics and
`println!` only for user-facing output.

### Testing standards

Located in `docs/standards/testing/`.

#### Test Writing (`standards/testing/test-writing.md`)
Put unit tests in a `#[cfg(test)]` module at the end of the file under test.
Put table-driven CLI parser tests in a separate module with `.in` fixtures.
Run Rust tests with `--test-threads=1` against one Scylla node. Write the
integration test logic as `run()` or `run_<case>()` functions in
`tools/test_cs_*.py`. Call each one from a `test_*` function in
`tools/cql-stress-cassandra-stress-ci.py`.

### Infra standards

Located in `docs/standards/infra/`.

#### CI and Dependencies (`standards/infra/ci-and-dependencies.md`)
Pin third-party GitHub Actions to commit SHAs with a version comment. Renovate
updates crates and Actions. Update Docker images by hand. Give a reason in
`Cargo.toml` for each hand-pinned version.

## Updating this documentation

- Update a standard when a team convention changes, through
  `/qatools-sdlc:standards-update`.
- Update this index when you add, remove, or change a file.
