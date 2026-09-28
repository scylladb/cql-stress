## Rust Conventions

### Errors with anyhow
Return `anyhow::Result<T>` from fallible functions. Do not add custom error enums or `thiserror`.
Make errors with `anyhow::anyhow!`, `anyhow::ensure!`, or `anyhow::bail!`.

```rust
anyhow::ensure!(bit_size != 0 && bit_size <= 64, "Invalid bit size {}", bit_size);
```

### Compatible error messages
The scylla-bench frontend copies the messages and flags of the Go scylla-bench tool.
Do not change the text of these messages. Users and scripts compare them with the Go tool output.

### Error context
In new code, add `.context("...")` or `.with_context(|| ...)` to a fallible driver, file, or parse call.
The context tells what the program tried to do.

```rust
let statement = session.prepare(query).await.context("Failed to prepare statement")?;
```

### Logging
Use `tracing` macros (`debug!`, `info!`, `warn!`, `error!`) for diagnostics.
Use `println!` and `eprintln!` only for user-facing output: results, summaries, help, and CLI errors.
`RUST_LOG` sets the log level. The default level is `warn`.
