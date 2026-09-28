## CI and Dependencies

### Pinned GitHub Actions
Pin every third-party GitHub Action to a full commit SHA. Add the version tag as a comment.
Reusable workflows from `scylladb/github-automation` use `@main`.

```yaml
- uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
```

### Dependency updates
Renovate (`renovate.json`) updates crates and GitHub Actions.
Update Docker images by hand: the `Dockerfile` base images and the Scylla image in `docker/scylla-test/compose.yml`.
Change a crate or Action version by hand only when a change needs it. Give the reason in the commit body.
When you pin a version, add a comment in `Cargo.toml` that gives the reason and the upstream issue.
