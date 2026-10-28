# SCYLLADB-4519 — a verified load for strongly consistent tables

## Problem

ScyllaDB's strongly consistent (SC) tables promise linearizability: every read
returns the latest completed write, in an order that respects real time.
cql-stress can drive SC keyspaces today, but only for throughput. Its writes go
to keys that no run reads back and compares. A run that serves a stale read,
loses an acknowledged write or applies half of a write still finishes green
with good numbers.

The existing SC checks in cql-stress prove only that a run is leader-routed.
They never check that the data is correct.

## Who it affects

- The SC test coverage of ScyllaDB master (epic SCYLLADB-4477).
- SCT longevity jobs with nemesis on SC keyspaces, which use cql-stress as the
  load and have no correctness verdict.
- ScyllaDB developers, who learn of an SC correctness bug only after it
  reaches users.

## Evidence

SCYLLADB-4519, Goal:

> A new verifiable workload that runs alongside the regular load and nemeses,
> producing two verdicts per run: "no acknowledged write was lost" over the
> whole run, and "every operation in the sampled windows is explainable by
> some valid sequential order".

SCYLLADB-4519, Scope 2:

> Critical constraints: no internal retries, no speculative execution, no
> client-side timestamps, and request timeouts smaller than the window
> margins. A timed-out operation must be recorded as indeterminate, never
> dropped — that is the class of bug the whole effort targets.

`tools/test_cs_strong_consistency.py`, the module docstring of the current SC
tests:

> What is being guarded here is not that a run produces numbers, but that a run
> which has silently lost strong consistency cannot produce numbers at all.

Design: [sc-verify spec](https://scylladb.atlassian.net/wiki/pages/viewpage.action?pageId=479887448)
(Confluence 479887448), §1.

## What good looks like

- A cql-stress run against an SC keyspace records every checked operation and
  gives each row a verdict. A stale read, a lost acknowledged write or a torn
  row makes the run exit non-zero, with the row's history on disk.
- A test-only fault that serves stale reads is caught within one row. A
  deliberately invalid history fails the check.
- The tool refuses to start, with exit 2, against a cluster or schema that
  could make a verdict meaningless: no leader routing, or a table that differs
  from the profile.
- The behaviour and output of `cql-stress-cassandra-stress` do not change.

## Out of scope

- The SCT stress thread, events, Argus table and job. These live in
  scylla-cluster-tests.
- The `porcupine_checker` model changes. These live in porcupine_validator.
- Multi-partition or transactional guarantees, LWT and counters.
- Nemeses that change data outside the workload (restore, truncate).
- The later phases of the spec (§17), for example learning which timed-out
  writes landed.
