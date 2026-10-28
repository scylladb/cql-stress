//! The full check: porcupine_checker run as a child process on each check file (spec §11.3).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// How long the start-up probe waits for the checker.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Start-up: the checker must run, and refuse empty input with exit 2 ("no input"), as
/// porcupine_checker does. Anything else, a hang included, means it cannot check the run.
pub async fn probe(bin: &Path, timeout: Duration) -> Result<()> {
    let mut command = Command::new(bin);
    // Its own process group, set by posix_spawn: see check_file for why there is no pre_exec.
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .with_context(|| format!("Cannot run the checker {}", bin.display()))?;
    // A probe that hangs is killed with everything it started when this guard drops.
    let mut group = ProcessGroup(child.id().map(|pid| pid as i32));
    let status = tokio::time::timeout(timeout, child.wait())
        .await
        .with_context(|| {
            format!(
                "The checker {} did not answer in {timeout:?}",
                bin.display()
            )
        })?
        .with_context(|| format!("The checker {} failed", bin.display()))?;
    group.0 = None;
    anyhow::ensure!(
        status.code() == Some(2),
        "The checker {} answered empty input with {status}, not exit 2: is it porcupine_checker?",
        bin.display()
    );
    Ok(())
}

/// How each check file is checked.
#[derive(Debug, Clone)]
pub struct CheckerConfig {
    pub bin: PathBuf,
    /// The child is killed after this long.
    pub timeout: Duration,
    /// Memory for the child, in bytes.
    pub mem: u64,
    pub key_timeout: Duration,
    pub max_viz: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowResult {
    Ok,
    Illegal,
    Unknown,
}

/// The verdicts of one check file, `per_key[key]` for each row in it.
#[derive(Debug)]
pub struct FileVerdicts {
    pub per_key: Vec<RowResult>,
    /// The child ran out of time and was killed.
    pub killed: bool,
    /// The checker started; `false` when it could not run at all, which is no answer.
    pub ran: bool,
}

#[derive(Deserialize)]
struct Line {
    key: Option<usize>,
    result: Option<String>,
}

/// Checks the `rows` rows of `file` in a porcupine_checker child, which writes its artifacts
/// into `archive_dir`.
///
/// The child runs at `nice 10`, with `GOMEMLIMIT` at `mem` and `RLIMIT_DATA` a quarter above
/// it, so the Go GC can act before the hard limit; never `RLIMIT_AS`, which the Go runtime's
/// up-front address-space reservation would hit. It writes a line per row as soon as the row
/// is decided, so when it is killed (`timeout`) or dies, the rows it finished keep their
/// verdict and the others are `unknown`.
pub async fn check_file(
    cfg: &CheckerConfig,
    file: &Path,
    rows: usize,
    archive_dir: &Path,
) -> Result<FileVerdicts> {
    std::fs::create_dir_all(archive_dir)
        .with_context(|| format!("Failed to create {}", archive_dir.display()))?;
    let stdin =
        std::fs::File::open(file).with_context(|| format!("Failed to open {}", file.display()))?;
    let stderr_path = archive_dir.join("checker.stderr");
    let stderr = std::fs::File::create(&stderr_path)
        .with_context(|| format!("Failed to create {}", stderr_path.display()))?;
    let data_limit = cfg.mem.saturating_mul(5) / 4;
    let mut command = Command::new(&cfg.bin);
    command
        .arg("--output-dir")
        .arg(archive_dir)
        .arg("--key-timeout")
        .arg(format!("{}ms", cfg.key_timeout.as_millis()))
        .arg("--max-viz")
        .arg(cfg.max_viz.to_string())
        .env("GOMEMLIMIT", cfg.mem.to_string())
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(stderr)
        // Its own process group, so a kill reaches anything it started too.
        .process_group(0)
        .kill_on_drop(true);
    // No pre_exec, uid, gid or chroot on this command: any of them makes the standard library
    // fork() the whole tool for every check file instead of using posix_spawn. The fork copies
    // the tool's page tables and stalls its threads, which starved the checker on a busy loader
    // (SCYLLADB-5659). The priority and the data limit are set from outside, just below.
    let mut child = command
        .spawn()
        .with_context(|| format!("Cannot run the checker {}", cfg.bin.display()))?;
    if let Some(pid) = child.id() {
        if let Err(err) = limit_child(pid as i32, data_limit) {
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| {
                tracing::warn!("cannot set the checker's priority and data limit: {err}")
            });
        }
    }
    let mut group = ProcessGroup(child.id().map(|pid| pid as i32));

    let mut per_key = vec![RowResult::Unknown; rows];
    let mut lines = BufReader::new(child.stdout.take().expect("stdout is piped")).lines();
    let read_all = async {
        while let Ok(Some(text)) = lines.next_line().await {
            let Ok(Line {
                key: Some(key),
                result: Some(result),
            }) = serde_json::from_str::<Line>(&text)
            else {
                continue; // the summary, or a line this tool does not read
            };
            if let Some(slot) = per_key.get_mut(key) {
                *slot = match result.as_str() {
                    "ok" => RowResult::Ok,
                    "illegal" => RowResult::Illegal,
                    _ => RowResult::Unknown,
                };
            }
        }
    };
    // The reading and the wait share the time limit: a child that closes its stdout and
    // keeps running must not hold a worker beyond --checker-timeout either.
    let finished = tokio::time::timeout(cfg.timeout, async {
        read_all.await;
        let _ = child.wait().await;
    })
    .await;
    let killed = finished.is_err();
    if killed {
        group.kill();
        let _ = child.wait().await;
    }
    // Exited on its own: never signal its group id again, it may be reused.
    group.0 = None;
    Ok(FileVerdicts {
        per_key,
        killed,
        ran: true,
    })
}

/// Puts a started checker child at `nice 10` and caps its data segment at `data_limit` bytes.
/// A child that already exited (`ESRCH`) needs neither.
fn limit_child(pid: i32, data_limit: u64) -> std::io::Result<()> {
    let check = |rc: libc::c_int| match rc {
        0 => Ok(()),
        _ => match std::io::Error::last_os_error() {
            err if err.raw_os_error() == Some(libc::ESRCH) => Ok(()),
            err => Err(err),
        },
    };
    let limit = libc::rlimit64 {
        rlim_cur: data_limit,
        rlim_max: data_limit,
    };
    // SAFETY: plain syscalls on a child this process started.
    let priority = check(unsafe { libc::setpriority(libc::PRIO_PROCESS, pid as libc::id_t, 10) });
    // SAFETY: as above; `limit` outlives the call, and the old limit is not asked for.
    let data =
        check(unsafe { libc::prlimit64(pid, libc::RLIMIT_DATA, &limit, std::ptr::null_mut()) });
    priority.and(data)
}

/// The process group of a running checker child; dropping it (a worker aborted mid-check)
/// kills the whole group, the child and anything it started.
struct ProcessGroup(Option<i32>);

impl ProcessGroup {
    fn kill(&mut self) {
        if let Some(pgid) = self.0.take() {
            // SAFETY: plain syscall on a group this process created.
            unsafe {
                libc::killpg(pgid, libc::SIGKILL);
            }
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        self.kill();
    }
}

/// One check file for a worker.
#[derive(Debug)]
pub struct Job {
    pub seq: u64,
    pub path: PathBuf,
    pub rows: usize,
    pub archive_dir: PathBuf,
}

/// The bounded queue of check files and the workers that check them (spec §11.3).
///
/// `submit` never waits: a full queue hands the job back, and the caller archives the file
/// unchecked. Workers send each file's verdicts back; `next` yields them.
pub struct CheckQueue {
    jobs: Option<mpsc::Sender<Job>>,
    results: mpsc::UnboundedReceiver<(u64, FileVerdicts)>,
    workers: Vec<JoinHandle<()>>,
}

impl CheckQueue {
    pub fn start(cfg: CheckerConfig, workers: usize, capacity: usize) -> Self {
        let (jobs, queue) = mpsc::channel::<Job>(capacity);
        let queue = Arc::new(tokio::sync::Mutex::new(queue));
        let (results_tx, results) = mpsc::unbounded_channel();
        let cfg = Arc::new(cfg);
        let workers = (0..workers)
            .map(|_| {
                let (queue, results_tx, cfg) = (queue.clone(), results_tx.clone(), cfg.clone());
                tokio::spawn(async move {
                    loop {
                        let Some(job) = queue.lock().await.recv().await else {
                            break;
                        };
                        let verdicts = check_file(&cfg, &job.path, job.rows, &job.archive_dir)
                            .await
                            .unwrap_or_else(|err| {
                                tracing::error!("checking {} failed: {err:#}", job.path.display());
                                FileVerdicts {
                                    per_key: vec![RowResult::Unknown; job.rows],
                                    killed: false,
                                    ran: false,
                                }
                            });
                        if results_tx.send((job.seq, verdicts)).is_err() {
                            break;
                        }
                    }
                })
            })
            .collect();
        Self {
            jobs: Some(jobs),
            results,
            workers,
        }
    }

    /// Queues a file; a full (or closed) queue hands the job back at once.
    pub fn submit(&self, job: Job) -> std::result::Result<(), Job> {
        match &self.jobs {
            Some(jobs) => jobs.try_send(job).map_err(|err| match err {
                mpsc::error::TrySendError::Full(job) | mpsc::error::TrySendError::Closed(job) => {
                    job
                }
            }),
            None => Err(job),
        }
    }

    /// Files waiting for a worker.
    pub fn waiting(&self) -> usize {
        self.jobs
            .as_ref()
            .map_or(0, |jobs| jobs.max_capacity() - jobs.capacity())
    }

    /// The next checked file; `None` once the queue is closed and every worker is done.
    pub async fn next(&mut self) -> Option<(u64, FileVerdicts)> {
        self.results.recv().await
    }

    /// No more files: the workers finish what is queued, then stop.
    pub fn close(&mut self) {
        self.jobs = None;
    }

    /// Stops the workers now; their children are killed.
    /// Stops the workers now and waits until they are gone, so their children are killed
    /// before the caller moves the files those children were writing.
    pub async fn abort(&mut self) {
        for worker in &self.workers {
            worker.abort();
        }
        for worker in self.workers.drain(..) {
            let _ = worker.await;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    /// A fake checker: a shell script with `body`, in a fresh temp dir. Like the real one, it
    /// exits 2 on empty input unless the body says otherwise.
    pub(crate) fn fake_checker(name: &str, body: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("sc-verify-fake-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("porcupine_checker");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    fn config(bin: PathBuf, timeout: Duration) -> CheckerConfig {
        CheckerConfig {
            bin,
            timeout,
            mem: 512 << 20,
            key_timeout: Duration::from_secs(7),
            max_viz: 2,
        }
    }

    /// A check file of `rows` one-read rows, and an empty archive directory for it.
    fn check_file_fixture(name: &str, rows: usize) -> (PathBuf, PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("sc-verify-check-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("0.jsonl");
        let mut content = String::new();
        for key in 0..rows {
            content.push_str(&format!(
                "{{\"id\":{key},\"client_id\":0,\"kind\":\"call\",\"op\":\"read\",\"key\":{key},\"time_ns\":1}}\n"
            ));
        }
        std::fs::write(&file, content).unwrap();
        (file, dir.join("archive"))
    }

    const OK_0: &str = r#"echo '{"key":0,"result":"ok","ops":1,"ms":0}'"#;
    const OK_1: &str = r#"echo '{"key":1,"result":"ok","ops":1,"ms":0}'"#;
    const ILLEGAL_1: &str = r#"echo '{"key":1,"result":"illegal","ops":1,"ms":0}'"#;
    const SUMMARY: &str = r#"echo '{"summary":true}'"#;

    #[tokio::test]
    async fn check_file_reads_one_verdict_per_row_test() {
        let (file, archive) = check_file_fixture("ok", 2);
        let all_ok = fake_checker(
            "all-ok",
            &format!("cat >/dev/null\n{OK_0}\n{OK_1}\n{SUMMARY}\nexit 0"),
        );
        let verdicts = check_file(&config(all_ok, Duration::from_secs(5)), &file, 2, &archive)
            .await
            .unwrap();
        assert_eq!(verdicts.per_key, [RowResult::Ok, RowResult::Ok]);
        assert!(!verdicts.killed);

        let illegal = fake_checker(
            "illegal",
            &format!("cat >/dev/null\n{OK_0}\n{ILLEGAL_1}\n{SUMMARY}\nexit 1"),
        );
        let verdicts = check_file(&config(illegal, Duration::from_secs(5)), &file, 2, &archive)
            .await
            .unwrap();
        assert_eq!(verdicts.per_key, [RowResult::Ok, RowResult::Illegal]);
    }

    /// A killed or crashed checker keeps the rows it finished; the others are unknown.
    #[tokio::test]
    async fn rows_without_a_line_are_unknown_test() {
        let (file, archive) = check_file_fixture("hang", 2);
        let hangs = fake_checker("hangs", &format!("cat >/dev/null\n{OK_0}\nsleep 30"));
        let start = std::time::Instant::now();
        let verdicts = check_file(
            &config(hangs, Duration::from_millis(500)),
            &file,
            2,
            &archive,
        )
        .await
        .unwrap();
        assert_eq!(verdicts.per_key, [RowResult::Ok, RowResult::Unknown]);
        assert!(verdicts.killed);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "killed at --checker-timeout"
        );

        // Closing stdout ends the reading, not the child: the wait is under the timeout too.
        let quiet = fake_checker(
            "quiet",
            &format!("cat >/dev/null\n{OK_0}\nexec >&-\nsleep 30"),
        );
        let start = std::time::Instant::now();
        let verdicts = check_file(
            &config(quiet, Duration::from_millis(500)),
            &file,
            2,
            &archive,
        )
        .await
        .unwrap();
        assert_eq!(verdicts.per_key, [RowResult::Ok, RowResult::Unknown]);
        assert!(verdicts.killed && start.elapsed() < Duration::from_secs(5));

        // The whole process group goes, not just the checker: its own children too.
        let forks = fake_checker(
            "forks",
            &format!("cat >/dev/null\n{OK_0}\nsleep 30 &\necho $! > \"$2/child.pid\"\nwait"),
        );
        check_file(
            &config(forks, Duration::from_millis(500)),
            &file,
            2,
            &archive,
        )
        .await
        .unwrap();
        let pid: i32 = std::fs::read_to_string(archive.join("child.pid"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let alive = unsafe { libc::kill(pid, 0) } == 0
            && !std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .unwrap_or_default()
                .contains(") Z ");
        assert!(!alive, "the checker's child {pid} survived the kill");

        let crashes = fake_checker("crashes", &format!("cat >/dev/null\n{OK_0}\nkill -9 $$"));
        let verdicts = check_file(&config(crashes, Duration::from_secs(5)), &file, 2, &archive)
            .await
            .unwrap();
        assert_eq!(verdicts.per_key, [RowResult::Ok, RowResult::Unknown]);
    }

    /// The child runs at lower priority, with its memory limited, and gets the right flags.
    #[tokio::test]
    async fn the_child_is_limited_test() {
        let (file, archive) = check_file_fixture("env", 1);
        // $2 is the --output-dir. The tool sets the priority and the data limit just after
        // the start, so the fake reads them a moment later.
        let env = fake_checker(
            "env",
            "cat >/dev/null\nsleep 0.2\necho \"$(nice) $GOMEMLIMIT $(ulimit -d) $*\" > \"$2/env\"",
        );
        check_file(&config(env, Duration::from_secs(5)), &file, 1, &archive)
            .await
            .unwrap();
        let seen = std::fs::read_to_string(archive.join("env")).unwrap();
        let fields: Vec<&str> = seen.split_whitespace().collect();
        assert!(fields[0].parse::<i32>().unwrap() >= 10, "nice 10: {seen}");
        assert_eq!(
            fields[1],
            (512u64 << 20).to_string(),
            "GOMEMLIMIT in bytes: {seen}"
        );
        assert_eq!(
            fields[2],
            ((512u64 << 20) * 5 / 4 / 1024).to_string(),
            "RLIMIT_DATA, kB: {seen}"
        );
        assert_eq!(
            fields[3..].join(" "),
            format!(
                "--output-dir {} --key-timeout 7000ms --max-viz 2",
                archive.display()
            )
        );
    }

    fn job(file: &Path, seq: u64) -> Job {
        Job {
            seq,
            path: file.to_owned(),
            rows: 2,
            archive_dir: file.parent().unwrap().join(format!("archive-{seq}")),
        }
    }

    #[tokio::test]
    async fn the_queue_returns_every_file_test() {
        let (file, _) = check_file_fixture("queue-ok", 2);
        let all_ok = fake_checker(
            "queue-ok",
            &format!("cat >/dev/null\n{OK_0}\n{OK_1}\nexit 0"),
        );
        let mut queue = CheckQueue::start(config(all_ok, Duration::from_secs(5)), 2, 4);
        for seq in 0..3 {
            queue.submit(job(&file, seq)).unwrap();
        }
        let mut seen = Vec::new();
        for _ in 0..3 {
            let (seq, verdicts) = queue.next().await.unwrap();
            assert_eq!(verdicts.per_key, [RowResult::Ok, RowResult::Ok]);
            seen.push(seq);
        }
        seen.sort();
        assert_eq!(seen, [0, 1, 2]);
        queue.close();
        assert!(queue.next().await.is_none(), "closed and drained");
    }

    /// A full queue refuses at once: the load never waits for the checker.
    #[tokio::test]
    async fn a_full_queue_refuses_test() {
        let (file, _) = check_file_fixture("queue-full", 2);
        let hangs = fake_checker("queue-full", "cat >/dev/null\nsleep 30");
        let mut queue = CheckQueue::start(config(hangs, Duration::from_secs(60)), 1, 1);
        queue.submit(job(&file, 0)).unwrap();
        // Let the worker take the first file and hang on it.
        tokio::time::sleep(Duration::from_millis(300)).await;
        queue.submit(job(&file, 1)).unwrap();
        assert_eq!(queue.waiting(), 1);
        let refused = queue.submit(job(&file, 2)).unwrap_err();
        assert_eq!(refused.seq, 2, "the refused job comes back");
        queue.abort().await;
    }

    #[tokio::test]
    async fn probe_test() {
        let timeout = Duration::from_millis(500);
        let good = fake_checker(
            "probe-good",
            "cat >/dev/null; echo 'error: no input data' >&2; exit 2",
        );
        assert!(probe(&good, timeout).await.is_ok());

        let wrong = fake_checker("probe-wrong", "exit 0");
        assert!(
            probe(&wrong, timeout).await.is_err(),
            "empty input must be refused"
        );

        let hangs = fake_checker("probe-hangs", "sleep 30");
        let start = std::time::Instant::now();
        assert!(probe(&hangs, timeout).await.is_err());
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the probe has its own timeout"
        );

        assert!(probe(Path::new("/nonexistent/porcupine_checker"), timeout)
            .await
            .is_err());
    }

    /// Bumped by glibc on every fork() of this process; posix_spawn never runs it.
    static FORKS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    extern "C" fn count_fork() {
        FORKS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// Starting a checker must not fork the tool: a fork copies the tool's page tables and
    /// stalls its threads, which starved the checker on a busy loader (SCYLLADB-5659).
    #[tokio::test]
    async fn the_checker_starts_without_fork_test() {
        static REGISTER: std::sync::Once = std::sync::Once::new();
        // SAFETY: the handler only bumps an atomic counter.
        REGISTER.call_once(|| unsafe {
            assert_eq!(libc::pthread_atfork(Some(count_fork), None, None), 0);
        });
        let before = FORKS.load(std::sync::atomic::Ordering::SeqCst);

        let answers = fake_checker("no-fork-probe", "cat >/dev/null; exit 2");
        probe(&answers, Duration::from_secs(5)).await.unwrap();
        let (file, archive) = check_file_fixture("no-fork", 1);
        let ok = fake_checker(
            "no-fork",
            &format!("cat >/dev/null\n{OK_0}\n{SUMMARY}\nexit 0"),
        );
        check_file(&config(ok, Duration::from_secs(5)), &file, 1, &archive)
            .await
            .unwrap();

        assert_eq!(
            FORKS.load(std::sync::atomic::Ordering::SeqCst),
            before,
            "starting a checker forked the tool"
        );
    }

    /// A checker that exits before its limits are set is no error: nothing is left to limit.
    #[test]
    fn limit_child_ignores_an_exited_child_test() {
        let mut child = std::process::Command::new("/bin/true").spawn().unwrap();
        let pid = child.id() as i32;
        child.wait().unwrap();
        assert!(limit_child(pid, 1 << 30).is_ok());
    }
}
