//! The four streaming invariants (spec §10), checked on every recorded read of a live row.
//!
//! The engine is pure; the slot loop keeps one [`RowState`] per live row behind a lock and
//! must keep the timing rule of spec §10.1, which the engine cannot enforce:
//! - an operation that ends takes its end time *before* it locks the state, then checks, then
//!   updates it ([`RowState::end_write`], [`RowState::end_read`]);
//! - a read calls [`RowState::snapshot`], releases the lock, and only *then* takes its start
//!   time and sends the request.
//!
//! So everything in a read's snapshot ended before the read started. An operation that ends
//! between the snapshot and the start is merely missing from it: less sensitivity, never a
//! false alarm. The start and end times of individual writes are facts once known, so
//! [`RowState::check_read`] may look them up at check time.
//!
//! Times are nanoseconds on the process's monotonic clock. "`a` ended before `t`" means
//! `a`'s end < `t`; a write without an end (in flight or indeterminate) never ended before
//! anything, which is what keeps timeouts from raising false alarms.

use std::fmt;

/// One cell as a read returned it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seen {
    Null,
    Wid(u64),
    /// Not the encoding of any write id.
    Undecodable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inv {
    Inv0,
    Inv1,
    Inv2,
    Inv3,
}

impl fmt::Display for Inv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = match self {
            Inv::Inv0 => 0,
            Inv::Inv1 => 1,
            Inv::Inv2 => 2,
            Inv::Inv3 => 3,
        };
        write!(f, "INV-{n}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Violation {
    pub kind: Inv,
    pub cell: usize,
}

/// How a write ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteEnd {
    Ok(u64),
    /// Timed out or lost: it may land at any time after it started.
    Indeterminate,
    /// Certainly not applied (`--unavailable-is-fail`): its wid is burned.
    Fail(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStatus {
    InFlight,
    Ok(u64),
    Indeterminate,
    Fail(u64),
}

#[derive(Debug)]
struct Write {
    mask: u8,
    start: u64,
    status: WriteStatus,
}

impl Write {
    fn ended_before(&self, t: u64) -> bool {
        matches!(self.status, WriteStatus::Ok(end) if end < t)
    }
}

/// The floors a read compares against, copied before the read starts. `Some(t)`: some
/// operation has been seen, and `t` is the latest start of such a write.
#[derive(Debug, Clone)]
pub struct Floors {
    /// Per cell: the latest start of a write to it that returned ok.
    ack: Vec<Option<u64>>,
    /// Per cell: the latest start of a write that a finished read observed in it.
    obs: Vec<Option<u64>>,
}

/// A retired row's expected final state (spec §12.1): per cell, the values it may hold
/// (`None` = null); the highest wid issued; the burned wids; the indeterminate wids, whose
/// landing the read-back counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expected {
    pub cells: Vec<Vec<Option<u64>>>,
    pub max_wid: u64,
    pub burned: Vec<u64>,
    pub indeterminate: Vec<u64>,
}

/// What the tool remembers about one live row (spec §10.1).
#[derive(Debug)]
pub struct RowState {
    /// `writes[wid - 1]`: write ids count from 1.
    writes: Vec<Write>,
    floors: Floors,
}

impl RowState {
    pub fn new(cells: usize) -> Self {
        assert!((1..=8).contains(&cells), "a row has 1 to 8 cells");
        Self {
            writes: Vec::new(),
            floors: Floors {
                ack: vec![None; cells],
                obs: vec![None; cells],
            },
        }
    }

    /// Issues the next write id, for a write to the cells in `mask` that starts at `start`.
    pub fn begin_write(&mut self, mask: u8, start: u64) -> u64 {
        assert!(
            mask != 0 && u32::from(mask) < 1 << self.cells(),
            "a write sets a non-empty subset of the row's cells, not {mask:#b}"
        );
        self.writes.push(Write {
            mask,
            start,
            status: WriteStatus::InFlight,
        });
        self.writes.len() as u64
    }

    pub fn end_write(&mut self, wid: u64, end: WriteEnd) {
        let write = &mut self.writes[wid as usize - 1];
        write.status = match end {
            WriteEnd::Ok(t) => WriteStatus::Ok(t),
            WriteEnd::Indeterminate => WriteStatus::Indeterminate,
            WriteEnd::Fail(t) => WriteStatus::Fail(t),
        };
        if let WriteEnd::Ok(_) = end {
            for cell in cells_of(write.mask) {
                raise(&mut self.floors.ack[cell], write.start);
            }
        }
    }

    pub fn status(&self, wid: u64) -> WriteStatus {
        self.writes[wid as usize - 1].status
    }

    pub fn snapshot(&self) -> Floors {
        self.floors.clone()
    }

    /// The violations of a read that saw `seen`, against the floors it copied before it
    /// started.
    pub fn check_read(&self, floors: &Floors, seen: &[Seen]) -> Vec<Violation> {
        let mut violations = Vec::new();
        let mut fire = |kind, cell| violations.push(Violation { kind, cell });
        for (cell, &value) in seen.iter().enumerate() {
            let (ack, obs) = (floors.ack[cell], floors.obs[cell]);
            let wid = match value {
                Seen::Null => {
                    if ack.is_some() {
                        fire(Inv::Inv1, cell);
                    }
                    if obs.is_some() {
                        fire(Inv::Inv2, cell);
                    }
                    continue;
                }
                Seen::Undecodable => {
                    fire(Inv::Inv0, cell);
                    continue;
                }
                Seen::Wid(wid) => wid,
            };
            let Some(write) = self.valid_write(wid) else {
                fire(Inv::Inv0, cell);
                continue;
            };
            if ack.is_some_and(|floor| write.ended_before(floor)) {
                fire(Inv::Inv1, cell);
            }
            if obs.is_some_and(|floor| write.ended_before(floor)) {
                fire(Inv::Inv2, cell);
            }
            let torn = cells_of(write.mask)
                .filter(|&d| d != cell)
                .any(|d| match seen[d] {
                    Seen::Null => true,
                    Seen::Wid(u) => self
                        .valid_write(u)
                        .is_some_and(|other| other.ended_before(write.start)),
                    Seen::Undecodable => false,
                });
            if torn {
                fire(Inv::Inv3, cell);
            }
        }
        violations
    }

    /// Records what a finished read observed. Values that fail INV-0 are skipped.
    pub fn end_read(&mut self, seen: &[Seen]) {
        for (cell, &value) in seen.iter().enumerate() {
            if let Seen::Wid(wid) = value {
                if let Some(start) = self.valid_write(wid).map(|write| write.start) {
                    raise(&mut self.floors.obs[cell], start);
                }
            }
        }
    }

    /// The values each cell may still hold once the row is retired (spec §12.1), for the
    /// read-back: `sweep` is what the sweep saw, `None` when it never succeeded.
    pub fn expected(&self, sweep: Option<&[Seen]>) -> Expected {
        let cells = (0..self.cells())
            .map(|cell| {
                let wrote = |write: &&Write| write.mask & (1 << cell) != 0;
                let mut set: Vec<Option<u64>> = Vec::new();
                match sweep {
                    Some(seen) => match seen[cell] {
                        Seen::Null => set.push(None),
                        Seen::Wid(wid) => set.push(Some(wid)),
                        // Already INV-0; no value of this tool can match it.
                        Seen::Undecodable => {}
                    },
                    None => {
                        let floor = self.floors.ack[cell];
                        if floor.is_none() {
                            set.push(None);
                        }
                        set.extend(self.wids().filter(|(_, w)| wrote(w)).filter_map(
                            |(wid, write)| match write.status {
                                WriteStatus::Ok(end) if floor.is_some_and(|f| end >= f) => {
                                    Some(Some(wid))
                                }
                                _ => None,
                            },
                        ));
                    }
                }
                // An indeterminate write may still land after anything else.
                set.extend(
                    self.wids()
                        .filter(|(_, w)| wrote(w) && w.status == WriteStatus::Indeterminate)
                        .map(|(wid, _)| Some(wid)),
                );
                set.sort();
                set.dedup();
                set
            })
            .collect();
        Expected {
            cells,
            max_wid: self.writes.len() as u64,
            burned: self
                .wids()
                .filter(|(_, w)| matches!(w.status, WriteStatus::Fail(_)))
                .map(|(wid, _)| wid)
                .collect(),
            indeterminate: self
                .wids()
                .filter(|(_, w)| w.status == WriteStatus::Indeterminate)
                .map(|(wid, _)| wid)
                .collect(),
        }
    }

    fn wids(&self) -> impl Iterator<Item = (u64, &Write)> {
        self.writes
            .iter()
            .enumerate()
            .map(|(i, write)| (i as u64 + 1, write))
    }

    fn cells(&self) -> usize {
        self.floors.ack.len()
    }

    /// The write `wid` names, unless it was never issued or is burned (INV-0).
    fn valid_write(&self, wid: u64) -> Option<&Write> {
        let write = self
            .writes
            .get(usize::try_from(wid).ok()?.checked_sub(1)?)?;
        (!matches!(write.status, WriteStatus::Fail(_))).then_some(write)
    }
}

fn cells_of(mask: u8) -> impl Iterator<Item = usize> {
    (0..8).filter(move |cell| mask & (1 << cell) != 0)
}

fn raise(floor: &mut Option<u64>, t: u64) {
    *floor = Some(floor.map_or(t, |old| old.max(t)));
}

#[cfg(test)]
mod tests {
    use super::*;

    const C0: u8 = 0b001;
    const C1: u8 = 0b010;
    const ALL: u8 = 0b111;

    /// A finished write: begin at `start`, ok at `end`.
    fn ok_write(row: &mut RowState, mask: u8, start: u64, end: u64) -> u64 {
        let wid = row.begin_write(mask, start);
        row.end_write(wid, WriteEnd::Ok(end));
        wid
    }

    /// A finished read seeing `seen`: snapshot, check, end. Returns its violations.
    fn read(row: &mut RowState, seen: &[Seen]) -> Vec<Violation> {
        let floors = row.snapshot();
        let violations = row.check_read(&floors, seen);
        row.end_read(seen);
        violations
    }

    fn fired(violations: &[Violation], kind: Inv, cell: usize) -> bool {
        violations.contains(&Violation { kind, cell })
    }

    use Seen::{Null, Undecodable, Wid};

    #[test]
    fn a_fresh_row_reads_absent_test() {
        let mut row = RowState::new(3);
        assert!(read(&mut row, &[Null, Null, Null]).is_empty());
    }

    #[test]
    fn inv0_no_phantom_value_test() {
        let mut row = RowState::new(3);
        let w1 = ok_write(&mut row, ALL, 10, 20);
        assert!(read(&mut row, &[Wid(w1), Wid(w1), Wid(w1)]).is_empty());

        let v = read(&mut row, &[Wid(w1 + 1), Undecodable, Wid(w1)]);
        assert!(fired(&v, Inv::Inv0, 0), "never issued: {v:?}");
        assert!(fired(&v, Inv::Inv0, 1), "does not decode: {v:?}");
        assert!(!v.iter().any(|v| v.cell == 2), "{v:?}");

        let burned = row.begin_write(C0, 30);
        row.end_write(burned, WriteEnd::Fail(40));
        assert!(fired(
            &read(&mut row, &[Wid(burned), Wid(w1), Wid(w1)]),
            Inv::Inv0,
            0
        ));
    }

    #[test]
    fn inv1_read_your_writes_test() {
        let mut row = RowState::new(3);
        let w1 = ok_write(&mut row, C0, 10, 20);
        // Legal: the latest acknowledged write.
        assert!(read(&mut row, &[Wid(w1), Null, Null]).is_empty());
        // Null after an acknowledged write to the cell.
        assert!(fired(&read(&mut row, &[Null, Null, Null]), Inv::Inv1, 0));

        let w2 = ok_write(&mut row, C0, 30, 40);
        assert!(read(&mut row, &[Wid(w2), Null, Null]).is_empty());

        // a ended (20) before b started (30), and b ended before this read began.
        let mut fresh = RowState::new(3);
        let a = ok_write(&mut fresh, C0, 10, 20);
        let b = ok_write(&mut fresh, C0, 30, 40);
        let v = read(&mut fresh, &[Wid(a), Null, Null]);
        assert!(fired(&v, Inv::Inv1, 0), "{v:?}");
        assert!(read(&mut fresh, &[Wid(b), Null, Null]).is_empty());
    }

    #[test]
    fn inv2_never_goes_backwards_test() {
        let mut row = RowState::new(3);
        // Two writes, still in flight: the reads can see either, in either order...
        let a = row.begin_write(C0, 10);
        let b = row.begin_write(C0, 30);
        assert!(read(&mut row, &[Wid(b), Null, Null]).is_empty());
        // ...until a is known to have ended before b started.
        row.end_write(a, WriteEnd::Ok(20));
        let v = read(&mut row, &[Wid(a), Null, Null]);
        assert!(fired(&v, Inv::Inv2, 0), "{v:?}");
        // Seen set once, a cell never reads null again.
        assert!(fired(&read(&mut row, &[Null, Null, Null]), Inv::Inv2, 0));
        row.end_write(b, WriteEnd::Ok(50));
        assert!(read(&mut row, &[Wid(b), Null, Null]).is_empty());
    }

    #[test]
    fn inv3_no_torn_row_test() {
        let mut row = RowState::new(3);
        let all = ok_write(&mut row, ALL, 10, 20);
        let both = row.begin_write(C0 | C1, 30);
        // Legal: both cells of `both` together.
        assert!(read(&mut row, &[Wid(both), Wid(both), Wid(all)]).is_empty());
        // `both` set c1 too, so c1 cannot be null...
        let mut torn = RowState::new(3);
        let w = torn.begin_write(C0 | C1, 10);
        assert!(fired(&read(&mut torn, &[Wid(w), Null, Null]), Inv::Inv3, 0));
        // ...nor hold a write that ended before `both` started.
        let v = read(&mut row, &[Wid(both), Wid(all), Wid(all)]);
        assert!(fired(&v, Inv::Inv3, 0), "{v:?}");
    }

    /// The F12 shape (ScyllaDB commit 19c73fe010): A sets X; B sets X and Y; a read sees
    /// X = A and Y = B. When A ended before B started, INV-3 catches it.
    #[test]
    fn f12_with_ordered_writes_test() {
        let mut row = RowState::new(2);
        let a = ok_write(&mut row, C0, 10, 20);
        let b = row.begin_write(C0 | C1, 30);
        let v = read(&mut row, &[Wid(a), Wid(b)]);
        assert!(fired(&v, Inv::Inv3, 1), "{v:?}");
    }

    /// The known gap (spec §10.2): when A and B overlap, each read alone is explainable,
    /// and only the full Porcupine check of milestone 2 can tell. No invariant may fire.
    #[test]
    fn f12_with_overlapping_writes_is_not_an_invariant_test() {
        let mut row = RowState::new(2);
        let a = row.begin_write(C0, 10);
        let b = row.begin_write(C0 | C1, 15);
        assert!(read(&mut row, &[Wid(a), Wid(b)]).is_empty());
        row.end_write(a, WriteEnd::Ok(20));
        row.end_write(b, WriteEnd::Ok(25));
    }

    #[test]
    fn indeterminate_and_in_flight_writes_are_never_older_test() {
        let mut row = RowState::new(2);
        let lost = row.begin_write(C0, 10);
        row.end_write(lost, WriteEnd::Indeterminate);
        let later = ok_write(&mut row, C0, 30, 40);
        // An indeterminate write may land at any time after it started, even last.
        assert!(read(&mut row, &[Wid(lost), Null]).is_empty());
        assert!(read(&mut row, &[Wid(later), Null]).is_empty());

        // INV-3 against an in-flight or indeterminate `u`.
        let mut row = RowState::new(2);
        let u = row.begin_write(C1, 10);
        let w = row.begin_write(C0 | C1, 30);
        assert!(read(&mut row, &[Wid(w), Wid(u)]).is_empty());
        row.end_write(u, WriteEnd::Indeterminate);
        assert!(read(&mut row, &[Wid(w), Wid(u)]).is_empty());
    }

    /// The timing rule (spec §10.1): an operation that ends between a read's snapshot and the
    /// read's start must not count against that read.
    #[test]
    fn an_op_ending_after_the_snapshot_does_not_count_test() {
        let mut row = RowState::new(1);
        let old = ok_write(&mut row, C0, 10, 20);
        let new = row.begin_write(C0, 30);
        let floors = row.snapshot(); // read: copy the floors first...
        row.end_write(new, WriteEnd::Ok(40)); // ...`new` ends...
                                              // ...then the read takes its start time and sees the older value: legal, since
                                              // the read may have been ordered before `new` from its point of view.
        assert!(row.check_read(&floors, &[Wid(old)]).is_empty());
        // A read that copies the floors after `new` ended must see it.
        let v = read(&mut row, &[Wid(old)]);
        assert!(fired(&v, Inv::Inv1, 0), "{v:?}");
    }

    #[test]
    fn a_wid_issued_after_the_snapshot_is_legal_test() {
        let mut row = RowState::new(1);
        ok_write(&mut row, C0, 10, 20);
        let floors = row.snapshot();
        let new = row.begin_write(C0, 30);
        assert!(row.check_read(&floors, &[Wid(new)]).is_empty());
    }

    #[test]
    fn inv0_values_do_not_move_the_floors_test() {
        let mut row = RowState::new(1);
        assert!(fired(&read(&mut row, &[Wid(99)]), Inv::Inv0, 0));
        assert!(fired(&read(&mut row, &[Undecodable]), Inv::Inv0, 0));
        // Had either moved obs, this null would fire INV-2.
        assert!(read(&mut row, &[Null]).is_empty());
    }

    #[test]
    fn equal_times_are_not_before_test() {
        let mut row = RowState::new(1);
        let a = ok_write(&mut row, C0, 10, 30);
        let b = ok_write(&mut row, C0, 30, 40); // starts exactly when a ends
        assert!(
            read(&mut row, &[Wid(a)]).is_empty(),
            "end == start is not before"
        );
        assert!(read(&mut row, &[Wid(b)]).is_empty());
    }

    /// Spec §12.1, sweep ok: what the sweep saw, plus every indeterminate write to the cell,
    /// which may still land after the sweep.
    #[test]
    fn expected_after_a_sweep_test() {
        let mut row = RowState::new(2);
        ok_write(&mut row, C0, 10, 20);
        let w2 = ok_write(&mut row, C0, 30, 40);
        let late = row.begin_write(C0, 50);
        row.end_write(late, WriteEnd::Indeterminate);
        let expected = row.expected(Some(&[Wid(w2), Null]));
        assert_eq!(expected.cells, [vec![Some(w2), Some(late)], vec![None]]);
        assert_eq!((expected.max_wid, expected.burned.len()), (3, 0));
        assert_eq!(expected.indeterminate, [late]);
    }

    /// Spec §12.1, sweep incomplete: every ok write that ended at or after the cell's ack floor
    /// (the latest start of an ok write), every indeterminate write, and null while no write to
    /// the cell was acknowledged.
    #[test]
    fn expected_without_a_sweep_test() {
        let mut row = RowState::new(3);
        ok_write(&mut row, C0, 10, 20); // ended before the floor (30): overwritten
        let w2 = ok_write(&mut row, C0, 30, 40); // sets the floor
        let w3 = ok_write(&mut row, C0, 25, 50); // overlaps w2: either may be last
        let lost = row.begin_write(C1, 60); // c1: never acknowledged
        row.end_write(lost, WriteEnd::Indeterminate);
        let burned = row.begin_write(0b100, 70);
        row.end_write(burned, WriteEnd::Fail(80));
        let expected = row.expected(None);
        assert_eq!(
            expected.cells,
            [vec![Some(w2), Some(w3)], vec![None, Some(lost)], vec![None]]
        );
        assert_eq!(expected.max_wid, 5);
        assert_eq!(expected.burned, [burned]);
        assert_eq!(expected.indeterminate, [lost]);
    }

    #[test]
    #[should_panic]
    fn an_empty_write_is_a_bug_test() {
        RowState::new(2).begin_write(0, 10);
    }

    #[test]
    fn write_status_test() {
        let mut row = RowState::new(1);
        let a = row.begin_write(C0, 10);
        assert_eq!(row.status(a), WriteStatus::InFlight);
        row.end_write(a, WriteEnd::Indeterminate);
        assert_eq!(row.status(a), WriteStatus::Indeterminate);
        let b = ok_write(&mut row, C0, 20, 30);
        assert_eq!(row.status(b), WriteStatus::Ok(30));
        let c = row.begin_write(C0, 40);
        row.end_write(c, WriteEnd::Fail(50));
        assert_eq!(row.status(c), WriteStatus::Fail(50));
        assert_eq!((a, b, c), (1, 2, 3), "wids count from 1");
    }
}
