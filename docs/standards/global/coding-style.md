## Coding Style

### Rustfmt
Format all Rust code with the default rustfmt settings. The repository has no `rustfmt.toml`.
Run `cargo fmt --all` before you commit. CI runs the same check and fails on a diff.

### Clippy clean
Clippy must pass with `-D warnings` for every feature set that CI checks.
The Lint block in the `## Commands` section of `CLAUDE.md` has the fmt, check, and Clippy steps from `.github/workflows/rust.yml`.
When you add a lint step to CI, add it to that block too.
