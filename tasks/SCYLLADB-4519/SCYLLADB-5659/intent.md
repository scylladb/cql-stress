# SCYLLADB-5659 — the checker falls behind on a busy loader and skipped files fill its disk

## Problem

On a loader that seals many cheap rows, `cql-stress-sc-verify` gets its check
files checked more and more slowly as a long run goes on. The checker workers
sit idle while the queue stays full, so the share of rows the checker proves
drops from all of them to about a tenth within three hours. Meanwhile the
tool's own scheduling delay grows past the burst interval, and the loader's
kernel CPU stays far above the checker's.

A check file the checker can't take is kept on disk, unchecked, with no limit.
At a high file rate over a long run these files fill the loader's disk. The
tool then can't write its history and stops early with exit 3, and SCT can't
pack the history to copy it off the loader.

## Who it affects

QA engineers who run the verified load in SCT. The 4h job
`test-cases/features/strong-consistency/longevity-sc-verify-100gb-4h.yaml` hit
it on loader L1 (8 clients per row, 256 slots, 16 checker workers). Any run
with many slots of cheap rows on one loader is exposed. The check and research
jobs in `configurations/sc_verify/` run the same tool.

## Evidence

Jira: SCYLLADB-5659, a subtask of SCYLLADB-4519. Run report:
`results/2026-10-23-4h-95a8ee10/README.md` in the sc-verify tracking
repository.

### Run 95a8ee10 (Argus, 2026-10-23), loader L1

The tool log, from the SCT runner:

```
[2026-10-23 09:39:34.039] SCV {"t":"start","mode":"verify","pop":"seq=0..1023","slots":256,"gen_base":"1879832918723919872","pool":"per-shard","pool_size":16,"shard_aware_port_range":"49152..65535","tcp_reuse_address":false}
[2026-10-23 09:44:34.254] SCV {"t":"stats","wall_ms":1792748673765,"verified_ops_s":64651.73080557615,"failed_ops_s":0.0,"bulk_ops_s":0.0,"read_p99_ms":75.071,"write_p99_ms":75.135,"indet_pct":0.0,"queue":10,"sched_delay_p99_ms":31.039}
[2026-10-23 10:26:40.511] SCV {"t":"skipped","file":"267934","rows":5}
[2026-10-23 11:36:14.096] SCV {"t":"stats","wall_ms":1792755373770,"verified_ops_s":64188.5156049762,"failed_ops_s":0.0,"bulk_ops_s":0.0,"read_p99_ms":32.095,"write_p99_ms":32.751,"indet_pct":0.0,"queue":254,"sched_delay_p99_ms":51.743}
[2026-10-23 12:42:54.162] SCV {"t":"stats","wall_ms":1792759373796,"verified_ops_s":63874.06394775411,"failed_ops_s":0.0,"bulk_ops_s":0.0,"read_p99_ms":49.791,"write_p99_ms":49.983,"indet_pct":0.0,"queue":255,"sched_delay_p99_ms":91.775}
[2026-10-23 13:14:41.849] error: failed to record a sealed row: Failed to write check file /history/sealed/1408938.jsonl: No space left on device (os error 28)
[2026-10-23 13:14:41.949] error: failed to close the history files: Failed to write check file /history/sealed/1408940.jsonl: No space left on device (os error 28)
[2026-10-23 13:17:47.239] SCV {"t":"readback","rows":7044967,"ok":7044967,"lost":0,"phantom":0,"incomplete":0,"indet_total":0,"indet_landed":0}
[2026-10-23 13:17:47.239] Sealed 7044967 rows, 711529144 recorded operations, 0 violations
[2026-10-23 13:17:47.239] SCV {"t":"end","exit":3}
```

The `SCV checked` and `SCV skipped` lines, counted per 10 minutes, with the
loader's CPU from node_exporter on the run's monitor
(`rate(node_cpu_seconds_total[5m])`, summed by mode; the checker runs at
`nice 10`, so its CPU is `nice`):

| Time | Files checked/s | Files skipped/s | Queue (max) | Checker (`nice`) | Kernel (`system`) | Tool (`user`) | Memory used | Disk used |
|---|---|---|---|---|---|---|---|---|
| 09:40–09:55 | 127.8 | 0.0 | 20 | 3.0 cores | 16.1 cores | 1.9 cores | 4.0 GiB | 3.2 GB |
| 10:15–10:20 | 65.9 | 1.3 | 253 | 2.1 | 12.0 | 1.4 | 4.4 GiB | 3.4 GB |
| 11:15–11:20 | 25.6 | 98.2 | 256 | 0.4 | 8.9 | 2.1 | 5.2 GiB | 13.2 GB |
| 12:55–13:00 | 10.8 | 113.4 | 256 | 0.2 | 6.3 | 1.7 | 6.4 GiB | 81.3 GB |

The cluster was not the limit: average reactor utilization 42–55% and
96–134k CQL requests/s over the run.

The history directory on loader 1 after the run:

```
/dev/root        96G   96G     0 100% /
0	/home/ubuntu/sc-verify-l1-c0-20261023_093931-9eddf33f-9dfe-42ba-98a8-cb437b930cf7/readback.jsonl
4,0K	/home/ubuntu/sc-verify-l1-c0-20261023_093931-9eddf33f-9dfe-42ba-98a8-cb437b930cf7/report.json
16K	/home/ubuntu/sc-verify-l1-c0-20261023_093931-9eddf33f-9dfe-42ba-98a8-cb437b930cf7/sealed
773M	/home/ubuntu/sc-verify-l1-c0-20261023_093931-9eddf33f-9dfe-42ba-98a8-cb437b930cf7/expected.jsonl
1,9G	/home/ubuntu/sc-verify-l1-c0-20261023_093931-9eddf33f-9dfe-42ba-98a8-cb437b930cf7/rows.jsonl
91G	/home/ubuntu/sc-verify-l1-c0-20261023_093931-9eddf33f-9dfe-42ba-98a8-cb437b930cf7/archive
sealed files: 1
archive entries: 875334
```

SCT's copy of the history (`sct.log`, `sdcm/stress/sc_verify_thread.py`):

```
gzip: stdout: No space left on device
tar: /home/ubuntu/sc-verify-l1-c0-20261023_093931-9eddf33f-9dfe-42ba-98a8-cb437b930cf7.tar.gz: Wrote only 2048 of 10240 bytes
tar: Child returned status 1
tar: Error is not recoverable: exiting now
```

Totals over the run: 533,607 files checked (2,668,030 rows ok, 5 unknown,
0 illegal) and 875,333 files skipped (4.38 M rows).

### The same drift in earlier runs

The tool's `sched_delay_p99_ms` on a loader that checks every file:

- Run 478276a9 (2026-10-23), L1 at 128 slots: 2.5 ms → 6.4 ms in the first
  13 minutes.
- Run ae3d3762 (2026-10-20), L1: 1.0 ms → 7.8 ms over the hour.

### How a file reaches the checker today

Each check file is checked in a new child process, started with `pre_exec`
(`src/bin/cql-stress-sc-verify/checker.rs`):

```rust
    // SAFETY: only async-signal-safe calls (setpgid, nice, setrlimit) between fork and exec.
    unsafe {
        command.pre_exec(move || {
            // Its own process group, so a kill reaches anything it started too.
            libc::setpgid(0, 0);
            libc::nice(10);
            let limit = libc::rlimit {
                rlim_cur: data_limit,
                rlim_max: data_limit,
            };
            if libc::setrlimit(libc::RLIMIT_DATA, &limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
```

A `pre_exec` hook makes the Rust standard library start the child with
`fork()` instead of `posix_spawn`. That this per-file `fork()` of the
multi-GB tool is what slows the checker down is suspected, not proven.

A file the queue can't take is archived unchecked, every time
(`src/bin/cql-stress-sc-verify/verdicts.rs`):

```rust
    /// Archives a check file unchecked: the queue was full, or the run ended first. Its rows
    /// are `skipped`, or `violation` when an invariant fired.
    fn skip(&mut self, checked: &Checked, closed: ClosedFile) {
```

and the tracking spec requires it (`docs/SPEC-sc-verify.md`, §11.3):

```
- **Queue.** The queue holds at most `--queue-max` files (default 256). If it is full, the file goes straight to `archive/`, and its rows are `skipped`. Nothing is dropped silently.
```

## What good looks like

- Over a 4-hour run, while files wait in the queue, the rate at which the
  checker finishes files stays level. How many files get checked is bound
  by the checker's CPU, not by the tool's cost of starting a check.
- The tool's `sched_delay_p99_ms` stays flat over the run, with no hourly
  growth.
- A long run at any file rate can't fill the loader's disk with unchecked
  files. The run still reports how many rows went unchecked, and a skipped
  row is still never a pass.

## Out of scope

- The checker's own speed on hard rows, such as L2's 16 clients per row at
  about 4 core-seconds per file. That is the checker's algorithm, in
  porcupine_validator.
- The SCT side of the same run:
  - the event flood from ScyllaDB's `transfer_snapshot() not implemented`
    (SCYLLADB-2565, SCYLLADB-4753);
  - the stress event that never ended after the history copy failed;
  - the job's slots and disk sizes.
- ScyllaDB itself.
