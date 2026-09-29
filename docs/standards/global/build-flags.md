## Build Flags

### scylla_unstable cfg
The `strong-consistency` feature needs `--cfg scylla_unstable`. `.cargo/config.toml` sets it.
A `RUSTFLAGS` environment variable replaces that setting. It does not add to it.
When you set `RUSTFLAGS` anywhere, also add `--cfg scylla_unstable`.
This applies to `Dockerfile`, `.github/workflows/rust.yml`, `.github/workflows/release.yml`, and local commands.

```bash
RUSTFLAGS="--cfg fetch_extended_version_info --cfg scylla_unstable" cargo build --profile dist
```
