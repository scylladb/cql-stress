//! The checked stream: slots that work one fresh row at a time in bursts (spec §8).
//!
//! A slot mints a fresh row, lets its clients fire bursts of overlapping operations at it,
//! stops starting new ones once a limit is reached, drains, sweeps and seals the row, then
//! mints the next one. Each operation keeps the timing rule of the invariants module: a
//! write takes its start time before it is issued and its end time before it locks the row;
//! a read copies the floors, releases the lock, and only then takes its start time.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cql_stress::java_generate::distribution::Distribution;
use rand::{random_bool, random_range};
use scylla::client::session::Session;
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;

use crate::cli::Cli;
use crate::history::OpRecord;
use crate::invariants::{Expected, Floors, RowState, Seen, Violation, WriteEnd};
use crate::keys::{GenMinter, RowKey};
use crate::ops::{Failure, OpError, Statements};
use crate::report::Stats;

/// Exit codes, raised from anywhere; the highest wins: 3 (the tool or profile is broken)
/// over 2 (set-up failure) over 1 (a violation). A run whose tool failed cannot vouch for
/// its verdicts, which is worse than a finding.
#[derive(Debug, Default)]
pub struct ExitCode(AtomicU8);

impl ExitCode {
    pub fn raise(&self, code: u8) {
        self.0.fetch_max(code, Ordering::Relaxed);
    }

    pub fn get(&self) -> u8 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Starts left in the current burst. A new tick resets it to `burst_ops`; unused starts do
/// not carry over.
#[derive(Debug)]
struct Budget {
    tick: u64,
    left: u64,
}

impl Budget {
    /// A budget that waits for the tick after `tick`.
    fn new(tick: u64) -> Self {
        Self { tick, left: 0 }
    }

    fn take(&mut self, tick: u64, burst_ops: u64) -> bool {
        // Only a newer tick refills: a client may have read the tick just before another one
        // moved the budget on.
        if tick > self.tick {
            self.tick = tick;
            self.left = burst_ops;
        }
        if self.left == 0 {
            return false;
        }
        self.left -= 1;
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    Ops,
    Time,
    Indeterminate,
    End,
}

impl StopReason {
    pub fn as_str(self) -> &'static str {
        match self {
            StopReason::Ops => "ops",
            StopReason::Time => "time",
            StopReason::Indeterminate => "indeterminate",
            StopReason::End => "end",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Limits {
    ops: u64,
    time: Duration,
    indeterminate: u64,
}

/// Why a row stops starting operations, if it does; checked after every operation ends.
fn should_stop(
    limits: &Limits,
    started: u64,
    elapsed: Duration,
    indeterminate: u64,
    run_ended: bool,
) -> Option<StopReason> {
    if indeterminate >= limits.indeterminate {
        Some(StopReason::Indeterminate)
    } else if started >= limits.ops {
        Some(StopReason::Ops)
    } else if elapsed >= limits.time {
        Some(StopReason::Time)
    } else if run_ended {
        Some(StopReason::End)
    } else {
        None
    }
}

/// The cells a write sets: all of them with probability `insert_ratio` (or when there is
/// one cell), else a random non-empty proper subset (spec §7.2).
fn write_mask(cells: usize, insert_ratio: f64) -> u8 {
    let all = ((1u16 << cells) - 1) as u8;
    if cells == 1 || random_bool(insert_ratio) {
        all
    } else {
        random_range(1..all)
    }
}

/// A violation and when the read that exposed it ended, in unix ms.
#[derive(Debug, Clone, Copy)]
pub struct Detected {
    pub violation: Violation,
    pub wall_ms: u64,
}

/// A sealed row, ready to be recorded.
#[derive(Debug, Clone)]
pub struct SealedRow {
    pub key: RowKey,
    pub slot: usize,
    pub wall_start_ms: u64,
    pub wall_end_ms: u64,
    pub ops: Vec<OpRecord>,
    pub reads: u64,
    pub writes_ok: u64,
    pub writes_indet: u64,
    pub errors: u64,
    pub max_gap_ms: u64,
    pub stop_reason: StopReason,
    pub violations: Vec<Detected>,
    pub sweep_ok: bool,
    /// The values each cell may still hold, for the read-back.
    pub expected: Expected,
}

/// Everything the slots share.
pub struct Checked {
    pub session: Arc<Session>,
    pub statements: Statements,
    pub cli: Cli,
    pub cells: usize,
    pub exit: ExitCode,
    pub stats: Stats,
    pks: Box<dyn Distribution>,
    gens: Mutex<GenMinter>,
    clock: Instant,
    stopped: AtomicBool,
    workload_errors: AtomicU64,
}

impl Checked {
    pub fn new(
        session: Arc<Session>,
        statements: Statements,
        cli: Cli,
        cells: usize,
        pks: Box<dyn Distribution>,
        gens: GenMinter,
    ) -> Self {
        Self {
            session,
            statements,
            cli,
            cells,
            exit: ExitCode::default(),
            stats: Stats::default(),
            pks,
            gens: Mutex::new(gens),
            clock: Instant::now(),
            stopped: AtomicBool::new(false),
            workload_errors: AtomicU64::new(0),
        }
    }

    /// Ends the run: every slot finishes its current row and stops.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }

    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    fn now_ns(&self) -> u64 {
        self.clock.elapsed().as_nanos() as u64
    }

    fn limits(&self) -> Limits {
        Limits {
            ops: self.cli.ops_per_gen,
            time: self.cli.max_gen_duration,
            indeterminate: self.cli.max_indeterminate,
        }
    }

    /// Counts an error caused by the tool or the profile; enough of them end the run, exit 3.
    fn workload_error(&self, error: &OpError) {
        let count = self.workload_errors.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::error!("workload error {count}: {}", error.message);
        if count >= self.cli.max_workload_errors {
            // Operations still in flight keep failing after the stop; each count is unique, so
            // exactly one caller crosses the limit and prints the reason.
            if count == self.cli.max_workload_errors {
                eprintln!(
                    "error: {count} errors caused by the tool or the profile; the last: {}",
                    error.message
                );
            }
            self.exit.raise(3);
            self.stop();
        }
    }

    pub async fn mint(&self) -> RowKey {
        let gen = loop {
            if let Some(gen) = self.gens.lock().unwrap().next(unix_ms()) {
                break gen;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        RowKey {
            pk: self.pks.next_i64(),
            gen,
            ck: 0,
        }
    }
}

/// What a round knows about its row while it is live.
struct Round {
    state: RowState,
    ops: Vec<OpRecord>,
    budget: Budget,
    started: u64,
    reads: u64,
    writes_ok: u64,
    writes_indet: u64,
    /// Failed operations other than indeterminate writes, which `writes_indet` counts: failed
    /// reads, `fail` writes and workload errors.
    errors: u64,
    violations: Vec<Detected>,
    last_ok_ns: u64,
    max_gap_ns: u64,
    stop: Option<StopReason>,
    /// What the row's first recorded read saw; `--fault-stale-reads` serves it again.
    first_seen: Option<Vec<Seen>>,
    /// The row's acknowledged writes, for `--fault-replay-writes`.
    acked: Vec<Acked>,
}

/// An acknowledged write of a row.
#[derive(Clone, Copy, Debug)]
struct Acked {
    wid: u64,
    mask: u8,
    call_ns: u64,
    end_ns: u64,
}

/// The newest acknowledged write that a later one surely superseded: one called after it
/// returned and sharing a cell with it. Sending anything else again may change nothing.
fn superseded(acked: &[Acked]) -> Option<&Acked> {
    acked
        .iter()
        .enumerate()
        .rev()
        .find(|(i, write)| {
            acked[i + 1..]
                .iter()
                .any(|later| later.call_ns > write.end_ns && later.mask & write.mask != 0)
        })
        .map(|(_, write)| write)
}

/// A write that returned: its id, the cells it set, and when it was called and returned.
#[derive(Clone, Copy, Debug)]
struct WriteOp {
    wid: u64,
    mask: u8,
    start_ns: u64,
    end_ns: u64,
}

impl Round {
    /// A fresh row's round, whose budget waits for the tick after `tick`.
    fn new(cells: usize, tick: u64, start_ns: u64) -> Self {
        Self {
            state: RowState::new(cells),
            ops: Vec::new(),
            budget: Budget::new(tick),
            started: 0,
            reads: 0,
            writes_ok: 0,
            writes_indet: 0,
            errors: 0,
            violations: Vec::new(),
            last_ok_ns: start_ns,
            max_gap_ns: 0,
            stop: None,
            first_seen: None,
            acked: Vec::new(),
        }
    }

    fn succeeded(&mut self, end_ns: u64) {
        self.max_gap_ns = self.max_gap_ns.max(end_ns.saturating_sub(self.last_ok_ns));
        self.last_ok_ns = self.last_ok_ns.max(end_ns);
    }

    /// Records how a write by `client` ended (spec §9). A workload error is counted and
    /// burned but never recorded.
    fn end_write(&mut self, client: usize, op: WriteOp, outcome: Result<(), Failure>) {
        let WriteOp {
            wid,
            mask,
            start_ns,
            end_ns,
        } = op;
        match outcome {
            Ok(()) => {
                self.state.end_write(wid, WriteEnd::Ok(end_ns));
                self.writes_ok += 1;
                self.succeeded(end_ns);
                self.acked.push(Acked {
                    wid,
                    mask,
                    call_ns: start_ns,
                    end_ns,
                });
            }
            Err(Failure::Indeterminate) => {
                self.state.end_write(wid, WriteEnd::Indeterminate);
                self.writes_indet += 1;
            }
            Err(Failure::Fail) => {
                self.state.end_write(wid, WriteEnd::Fail(end_ns));
                self.errors += 1;
            }
            Err(Failure::WorkloadError | Failure::ReadFailed) => {
                // Rejected outright: never recorded. Burned, so a cell showing it is a
                // phantom.
                self.state.end_write(wid, WriteEnd::Fail(end_ns));
                self.errors += 1;
                return;
            }
        }
        let status = self.state.status(wid);
        self.ops.push(OpRecord::Write {
            client,
            wid,
            mask,
            start_ns,
            status,
        });
    }

    /// Records a read by `client` that saw `seen`, or failed (`None`): a failed read is
    /// counted and never recorded (spec §9). Returns what the read saw, served stale for the
    /// share `fault`, and the violations it exposed against `floors`.
    fn end_read(
        &mut self,
        client: usize,
        floors: &Floors,
        start_ns: u64,
        end_ns: u64,
        seen: Option<Vec<Seen>>,
        fault: f64,
    ) -> Option<(Vec<Seen>, Vec<Violation>)> {
        let Some(mut seen) = seen else {
            self.errors += 1;
            return None;
        };
        match &self.first_seen {
            None => self.first_seen = Some(seen.clone()),
            Some(first) if fault > 0.0 && random_bool(fault) => seen = first.clone(),
            Some(_) => {}
        }
        let violations = self.state.check_read(floors, &seen);
        self.state.end_read(&seen);
        self.reads += 1;
        self.succeeded(end_ns);
        self.ops.push(OpRecord::Read {
            client,
            start_ns,
            end_ns,
            seen: seen.clone(),
        });
        Some((seen, violations))
    }
}

/// Runs slot `slot`, starting with the row `first` (checked absent at start-up), until the run
/// is stopped, sending every sealed row to `sealed`.
pub async fn run_slot(
    checked: Arc<Checked>,
    slot: usize,
    first: RowKey,
    sealed: mpsc::UnboundedSender<SealedRow>,
) {
    let cli = &checked.cli;
    // Slots tick at evenly spread phases, so the bursts of one loader interleave.
    let offset = cli.burst_interval * slot as u32 / cli.slots as u32;
    let (ticks_tx, ticks) = watch::channel(0u64);
    let interval = cli.burst_interval;
    let ticker = {
        let checked = checked.clone();
        tokio::spawn(async move {
            let start = tokio::time::Instant::now() + offset;
            let mut timer = tokio::time::interval_at(start, interval);
            // A stalled loader skips the bursts it missed rather than firing them back to back.
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                let due = timer.tick().await;
                checked.stats.sched_delay(due.elapsed());
                ticks_tx.send_modify(|tick| *tick += 1);
            }
        })
    };

    let mut next = Some(first);
    while !checked.is_stopped() {
        let key = match next.take() {
            Some(key) => key,
            None => checked.mint().await,
        };
        let row = run_round(&checked, slot, key, ticks.clone()).await;
        if sealed.send(row).is_err() {
            break;
        }
    }
    ticker.abort();
}

/// Start-up check 4 (spec §15.3): a freshly minted row must not exist yet.
pub async fn fresh_row_is_absent(checked: &Checked, key: &RowKey) -> bool {
    for _ in 0..checked.cli.sweep_retries.max(1) {
        match checked.statements.read(&checked.session, key).await {
            Ok(seen) if seen.iter().all(|cell| *cell == Seen::Null) => return true,
            Ok(seen) => {
                eprintln!(
                    "error: the fresh row pk {} gen {} already holds {seen:?}; \
                     row keys are not unique",
                    key.pk, key.gen
                );
                return false;
            }
            Err(error) => tracing::warn!("fresh-row check failed: {}", error.message),
        }
        tokio::time::sleep(checked.cli.sweep_backoff).await;
    }
    eprintln!("error: could not read a fresh row to check that it is absent");
    false
}

async fn run_round(
    checked: &Checked,
    slot: usize,
    key: RowKey,
    ticks: watch::Receiver<u64>,
) -> SealedRow {
    let wall_start_ms = unix_ms();
    let row_start = Instant::now();
    let start_ns = checked.now_ns();
    let round = Mutex::new(Round::new(checked.cells, *ticks.borrow(), start_ns));

    let clients = (0..checked.cli.clients_per_row)
        .map(|client| run_client(checked, &round, &key, client, ticks.clone(), row_start));
    futures::future::join_all(clients).await;
    // Drained: every operation of the row has returned.
    let sweep_seen = sweep(checked, &round, &key).await;

    let round = round.into_inner().unwrap();
    let expected = round.state.expected(sweep_seen.as_deref());
    let end_ns = checked.now_ns();
    SealedRow {
        key,
        slot,
        wall_start_ms,
        wall_end_ms: unix_ms(),
        ops: round.ops,
        reads: round.reads,
        writes_ok: round.writes_ok,
        writes_indet: round.writes_indet,
        errors: round.errors,
        max_gap_ms: round
            .max_gap_ns
            .max(end_ns.saturating_sub(round.last_ok_ns))
            / 1_000_000,
        stop_reason: round.stop.unwrap_or(StopReason::End),
        violations: round.violations,
        sweep_ok: sweep_seen.is_some(),
        expected,
    }
}

async fn run_client(
    checked: &Checked,
    round: &Mutex<Round>,
    key: &RowKey,
    client: usize,
    mut ticks: watch::Receiver<u64>,
    row_start: Instant,
) {
    let limits = checked.limits();
    loop {
        // Mark the current tick seen before looking at the budget, so a tick that lands
        // between the look and the wait still wakes this client.
        let tick = *ticks.borrow_and_update();
        let may_start = {
            let mut round = round.lock().unwrap();
            if round.stop.is_none() {
                round.stop = should_stop(
                    &limits,
                    round.started,
                    row_start.elapsed(),
                    round.writes_indet,
                    checked.is_stopped(),
                );
            }
            if round.stop.is_some() {
                return;
            }
            let may_start = round.budget.take(tick, checked.cli.burst_ops);
            if may_start {
                round.started += 1;
            }
            may_start
        };
        if !may_start {
            if ticks.changed().await.is_err() {
                return;
            }
            continue;
        }

        if random_bool(checked.cli.read_ratio) {
            read(checked, round, key, client, checked.cli.fault_stale_reads).await;
        } else {
            write(checked, round, key, client).await;
        }
    }
}

async fn write(checked: &Checked, round: &Mutex<Round>, key: &RowKey, client: usize) {
    let mask = write_mask(checked.cells, checked.cli.insert_ratio);
    let start_ns = checked.now_ns();
    let wid = round.lock().unwrap().state.begin_write(mask, start_ns);
    let result = checked
        .statements
        .write(
            &checked.session,
            key,
            mask,
            wid,
            checked.cli.unavailable_is_fail,
        )
        .await;
    let end_ns = checked.now_ns();

    let op = WriteOp {
        wid,
        mask,
        start_ns,
        end_ns,
    };
    let replay = record_write(checked, round, client, op, result);
    // `--fault-replay-writes`: the older write again, unrecorded, whatever it returns.
    if let Some(old) = replay {
        let _ = checked
            .statements
            .write(
                &checked.session,
                key,
                old.mask,
                old.wid,
                checked.cli.unavailable_is_fail,
            )
            .await;
    }
}

/// Records a write that returned, and picks the write `--fault-replay-writes` sends again.
fn record_write(
    checked: &Checked,
    round: &Mutex<Round>,
    client: usize,
    op: WriteOp,
    result: Result<(), OpError>,
) -> Option<Acked> {
    let mut round = round.lock().unwrap();
    round.end_write(
        client,
        op,
        result.as_ref().map(|_| ()).map_err(|error| error.class),
    );
    match result {
        Ok(()) => {
            checked
                .stats
                .write(Duration::from_nanos(op.end_ns - op.start_ns));
            let fault = checked.cli.fault_replay_writes;
            if fault > 0.0 && random_bool(fault) {
                return superseded(&round.acked).copied();
            }
        }
        Err(error) => match error.class {
            Failure::Indeterminate => checked.stats.write_indeterminate(),
            Failure::Fail => checked.stats.failed(),
            Failure::WorkloadError | Failure::ReadFailed => {
                checked.stats.failed();
                drop(round);
                checked.workload_error(&error);
            }
        },
    }
    None
}

/// One read, recorded and checked. Returns what it saw; `None` when it failed and was not
/// recorded. `fault` is the share served stale by `--fault-stale-reads`.
async fn read(
    checked: &Checked,
    round: &Mutex<Round>,
    key: &RowKey,
    client: usize,
    fault: f64,
) -> Option<Vec<Seen>> {
    let floors = round.lock().unwrap().state.snapshot();
    let start_ns = checked.now_ns();
    let result = checked.statements.read(&checked.session, key).await;
    let end_ns = checked.now_ns();

    let (seen, error) = match result {
        Ok(seen) => (Some(seen), None),
        Err(error) => (None, Some(error)),
    };
    let mut round = round.lock().unwrap();
    match round.end_read(client, &floors, start_ns, end_ns, seen, fault) {
        Some((seen, violations)) => {
            checked.stats.read(Duration::from_nanos(end_ns - start_ns));
            let wall_ms = unix_ms();
            for violation in violations {
                // The SCV line follows when the row seals and its evidence is archived.
                println!(
                    "VIOLATION {} pk {} gen {} cell c{}",
                    violation.kind, key.pk, key.gen, violation.cell
                );
                round.violations.push(Detected { violation, wall_ms });
            }
            Some(seen)
        }
        None => {
            checked.stats.failed();
            drop(round);
            if let Some(error) = error.filter(|error| error.class == Failure::WorkloadError) {
                checked.workload_error(&error);
            }
            None
        }
    }
}

/// The one final read of a drained row, the only place a retry is allowed: nothing else
/// runs on the row any more, and only the successful attempt is recorded. Returns what it
/// saw; `None` when every attempt failed (`sweep: incomplete`). The stale-read fault never
/// applies: the row's expected state is built from what the sweep saw.
async fn sweep(checked: &Checked, round: &Mutex<Round>, key: &RowKey) -> Option<Vec<Seen>> {
    let client = checked.cli.clients_per_row;
    for attempt in 0..checked.cli.sweep_retries.max(1) {
        if attempt > 0 {
            tokio::time::sleep(checked.cli.sweep_backoff).await;
        }
        if let Some(seen) = read(checked, round, key, client, 0.0).await {
            return Some(seen);
        }
    }
    None
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_gives_burst_ops_starts_per_tick_test() {
        let mut budget = Budget::new(7);
        assert!(!budget.take(7, 16), "a new row waits for the next tick");
        for _ in 0..16 {
            assert!(budget.take(8, 16));
        }
        assert!(
            !budget.take(8, 16),
            "the budget is empty until the next tick"
        );
        // Unused starts do not carry over: a tick resets the budget rather than adding to it.
        assert!(budget.take(9, 16));
        let mut taken = 1;
        while budget.take(10, 16) {
            taken += 1;
        }
        assert_eq!(
            taken, 17,
            "one left from tick 9 is gone; tick 10 gives a full 16"
        );
    }

    /// A client that read the tick before another client moved the budget on must not take
    /// it back to the older tick, which would refill it and double the burst.
    #[test]
    fn budget_never_goes_back_to_an_older_tick_test() {
        let mut budget = Budget::new(1);
        let taken = [2, 1]
            .iter()
            .cycle()
            .take(64)
            .filter(|&&tick| budget.take(tick, 16))
            .count();
        assert_eq!(
            taken, 16,
            "one burst, whatever order the clients saw the ticks in"
        );
    }

    #[test]
    fn should_stop_test() {
        let limits = Limits {
            ops: 200,
            time: Duration::from_secs(30),
            indeterminate: 8,
        };
        let check = |started, secs, indet, global| {
            should_stop(&limits, started, Duration::from_secs(secs), indet, global)
        };
        assert_eq!(check(199, 29, 7, false), None);
        assert_eq!(check(200, 0, 0, false), Some(StopReason::Ops));
        assert_eq!(check(10, 30, 0, false), Some(StopReason::Time));
        assert_eq!(check(10, 0, 8, false), Some(StopReason::Indeterminate));
        assert_eq!(check(10, 0, 0, true), Some(StopReason::End));
        // The row's own limit names the reason even when the run ends at the same moment.
        assert_eq!(check(10, 0, 8, true), Some(StopReason::Indeterminate));
        assert_eq!(StopReason::Indeterminate.as_str(), "indeterminate");
    }

    #[test]
    fn a_replayed_write_is_one_a_later_write_superseded_test() {
        let acked = |wid, mask, call_ns, end_ns| Acked {
            wid,
            mask,
            call_ns,
            end_ns,
        };
        // 3 was called after 1 returned and shares cell 0 with it; 2 shares no cell with 3.
        let writes = [
            acked(1, 0b011, 0, 10),
            acked(2, 0b100, 5, 20),
            acked(3, 0b001, 30, 40),
        ];
        assert_eq!(superseded(&writes).map(|w| w.wid), Some(1));
        // Overlapping writes: 2 may be applied before 1, so 1 is not surely superseded.
        let overlapping = [acked(1, 0b001, 0, 10), acked(2, 0b001, 5, 20)];
        assert_eq!(superseded(&overlapping).map(|w| w.wid), None);
        assert_eq!(superseded(&writes[..1]).map(|w| w.wid), None);
        // The newest candidate wins.
        let many = [
            acked(1, 0b001, 0, 10),
            acked(2, 0b001, 20, 30),
            acked(3, 0b001, 40, 50),
        ];
        assert_eq!(superseded(&many).map(|w| w.wid), Some(2));
    }

    #[test]
    fn write_masks_test() {
        for _ in 0..1000 {
            let mask = write_mask(3, 0.0);
            assert!(mask != 0 && mask != 0b111, "a partial update: {mask:#b}");
            assert_eq!(write_mask(3, 1.0), 0b111);
            assert_eq!(write_mask(1, 0.0), 0b1, "one cell: always a full insert");
        }
    }

    /// How each outcome class of a write becomes a history record and counters (spec §9):
    /// a workload error burns the wid like `fail` but is never recorded.
    #[test]
    fn a_write_outcome_is_recorded_by_its_class_test() {
        use crate::invariants::WriteStatus;
        // Outcome; the status of the wid; recorded; writes_ok, writes_indet, errors.
        let cases = [
            (Ok(()), WriteStatus::Ok(20), true, (1, 0, 0)),
            (
                Err(Failure::Indeterminate),
                WriteStatus::Indeterminate,
                true,
                (0, 1, 0),
            ),
            (Err(Failure::Fail), WriteStatus::Fail(20), true, (0, 0, 1)),
            (
                Err(Failure::WorkloadError),
                WriteStatus::Fail(20),
                false,
                (0, 0, 1),
            ),
        ];
        for (outcome, status, recorded, counters) in cases {
            let mut round = Round::new(2, 0, 0);
            let wid = round.state.begin_write(0b01, 10);
            let op = WriteOp {
                wid,
                mask: 0b01,
                start_ns: 10,
                end_ns: 20,
            };
            round.end_write(3, op, outcome);
            assert_eq!(round.state.status(wid), status, "{outcome:?}");
            let ops: Vec<_> = round
                .ops
                .iter()
                .map(|op| match op {
                    OpRecord::Write {
                        client,
                        wid,
                        mask,
                        start_ns,
                        status,
                    } => (*client, *wid, *mask, *start_ns, *status),
                    OpRecord::Read { .. } => panic!("a write recorded a read"),
                })
                .collect();
            let expected: Vec<_> = recorded
                .then_some((3, wid, 0b01, 10, status))
                .into_iter()
                .collect();
            assert_eq!(ops, expected, "{outcome:?}");
            assert_eq!(
                (round.writes_ok, round.writes_indet, round.errors),
                counters,
                "{outcome:?}"
            );
            assert_eq!(round.acked.len() as u64, counters.0, "{outcome:?}");
        }
    }

    #[test]
    fn a_read_outcome_is_recorded_test() {
        let mut round = Round::new(1, 0, 0);
        let floors = round.state.snapshot();
        // Wid 1 was never issued: INV-0.
        let (seen, violations) = round
            .end_read(2, &floors, 30, 40, Some(vec![Seen::Wid(1)]), 0.0)
            .unwrap();
        assert_eq!(seen, [Seen::Wid(1)]);
        assert_eq!(
            violations,
            [Violation {
                kind: crate::invariants::Inv::Inv0,
                cell: 0
            }]
        );
        assert!(matches!(
            &round.ops[..],
            [OpRecord::Read { client: 2, start_ns: 30, end_ns: 40, seen }] if seen == &[Seen::Wid(1)]
        ));
        assert_eq!((round.reads, round.errors, round.last_ok_ns), (1, 0, 40));

        // A failed read is counted, never recorded.
        assert!(round.end_read(2, &floors, 50, 60, None, 0.0).is_none());
        assert_eq!((round.reads, round.errors, round.ops.len()), (1, 1, 1));

        // `--fault-stale-reads`: a later read is served what the first one saw.
        let (seen, _) = round
            .end_read(2, &floors, 70, 80, Some(vec![Seen::Null]), 1.0)
            .unwrap();
        assert_eq!(seen, [Seen::Wid(1)]);
    }

    /// The `max_indeterminate`-th indeterminate write seals the row early.
    #[test]
    fn indeterminate_writes_stop_the_row_test() {
        let limits = Limits {
            ops: 200,
            time: Duration::from_secs(30),
            indeterminate: 3,
        };
        let mut round = Round::new(1, 0, 0);
        for n in 0..limits.indeterminate {
            let stop = should_stop(&limits, n, Duration::ZERO, round.writes_indet, false);
            assert_eq!(stop, None, "after {n} indeterminate writes");
            let wid = round.state.begin_write(1, n);
            let op = WriteOp {
                wid,
                mask: 1,
                start_ns: n,
                end_ns: n + 1,
            };
            round.end_write(0, op, Err(Failure::Indeterminate));
        }
        assert_eq!(
            should_stop(&limits, 3, Duration::ZERO, round.writes_indet, false),
            Some(StopReason::Indeterminate)
        );
    }

    #[test]
    fn the_highest_exit_code_wins_test() {
        let exit = ExitCode::default();
        exit.raise(1);
        exit.raise(3);
        exit.raise(2);
        assert_eq!(exit.get(), 3);
    }
}
