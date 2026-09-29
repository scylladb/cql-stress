## Git Commits

### Conventional commits
Write the commit subject as `type(scope): summary`. The scope is optional.
Use one of these types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `ci`, `build`.
Use the `deps` scope for dependency updates.
Write the summary in lower case and in the imperative mood. Do not end it with a period.

```text
feat: add shardAwarePortRange and tcpReuseAddress to -mode
fix: default to NetworkTopologyStrategy so keyspace creation works
```
