//! The durability read-back (spec §12): every retired row read once more at the end of the
//! run, each cell judged against the values it may still hold.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};
use futures::StreamExt;
use rand::random_bool;
use serde::Serialize;

use crate::invariants::{Expected, Seen};
use crate::keys::RowKey;
use crate::slot::{unix_ms, Checked};

/// A retired row, for the read-back.
#[derive(Debug, Clone)]
pub struct Retired {
    pub key: RowKey,
    pub expected: Expected,
    /// When the row started, in unix ms: its first write can be this old.
    pub start_ms: u64,
}

/// The read-back's totals (spec §14.4 `SCV readback`); `ok`, `lost`, `phantom` and
/// `incomplete` count rows.
#[derive(Debug, Default, Clone, Copy)]
pub struct Summary {
    pub rows: u64,
    pub ok: u64,
    pub lost: u64,
    pub phantom: u64,
    pub incomplete: u64,
    /// Rows older than `--ttl`, which may have expired, and were not read.
    pub expired: u64,
    pub indet_total: u64,
    pub indet_landed: u64,
}

/// One line of `readback.jsonl`: a failing cell, or a row that could not be read.
#[derive(Serialize)]
struct Line<'a> {
    pk: i64,
    gen: String,
    ck: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    cell: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expected: Option<&'a [Option<u64>]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    observed: Option<serde_json::Value>,
    result: &'static str,
}

/// Reads every retired row back after the run (spec §12.2): `--readback-concurrency` at a
/// time, retried like a sweep, so a busy cluster does not make a row look lost.
pub async fn read_back(checked: &Checked, rows: Vec<Retired>, dir: &Path) -> Result<Summary> {
    let path = dir.join("readback.jsonl");
    let mut out = BufWriter::new(
        File::create(&path).with_context(|| format!("Failed to create {}", path.display()))?,
    );
    let mut summary = Summary::default();
    let ttl_ms = u64::from(checked.cli.ttl) * 1000;
    let sample = checked.cli.readback_sample;
    let rows: Vec<Retired> = rows
        .into_iter()
        .filter(|_| sample >= 1.0 || random_bool(sample))
        .collect();

    // Expiry is judged per row just before it is read: the read-back itself takes time.
    let mut reads = futures::stream::iter(rows)
        .map(|row| async move {
            if may_have_expired(row.start_ms, unix_ms(), ttl_ms) {
                return (row, Read::Expired);
            }
            let read = match read_row(checked, &row.key).await {
                Some(seen) => Read::Seen(seen),
                None => Read::Unreadable,
            };
            (row, read)
        })
        .buffer_unordered(checked.cli.readback_concurrency);
    while let Some((row, read)) = reads.next().await {
        let seen = match read {
            Read::Expired => {
                summary.expired += 1;
                continue;
            }
            Read::Unreadable => None,
            Read::Seen(seen) => Some(seen),
        };
        summary.rows += 1;
        let line = |cell, expected, observed, result| Line {
            pk: row.key.pk,
            gen: row.key.gen.to_string(),
            ck: row.key.ck,
            cell,
            expected,
            observed,
            result,
        };
        let Some(seen) = seen else {
            summary.incomplete += 1;
            write_line(&mut out, &line(None, None, None, "incomplete"))?;
            continue;
        };
        // Only rows actually read can show whether their indeterminate writes landed.
        summary.indet_total += row.expected.indeterminate.len() as u64;
        summary.indet_landed += landed(&row.expected, &seen) as u64;
        let cells = judge(&row.expected, &seen);
        match row_result(&cells) {
            RowOutcome::Ok => summary.ok += 1,
            RowOutcome::Lost => summary.lost += 1,
            RowOutcome::Phantom => summary.phantom += 1,
        }
        for (cell, result) in cells.iter().enumerate() {
            let result = match result {
                CellResult::Ok => continue,
                CellResult::Lost => "lost",
                CellResult::Phantom => "phantom",
            };
            let observed = match seen[cell] {
                Seen::Null => serde_json::Value::Null,
                Seen::Wid(wid) => wid.into(),
                Seen::Undecodable => (-1).into(),
            };
            write_line(
                &mut out,
                &line(
                    Some(format!("c{cell}")),
                    Some(&row.expected.cells[cell]),
                    Some(observed),
                    result,
                ),
            )?;
        }
    }
    out.flush().context("Failed to write readback.jsonl")?;
    Ok(summary)
}

enum Read {
    Seen(Vec<Seen>),
    Unreadable,
    Expired,
}

/// Whether a row's cells may have expired under `--ttl` (`ttl_ms`; 0 is no TTL). A cell may
/// have been written as early as the row's start, and the margin, a tenth of the TTL, is at
/// least one longest round under the TTL guard (spec §7.4) and covers clock skew between the
/// loader and the nodes: an expired cell must never be judged lost.
fn may_have_expired(start_ms: u64, now_ms: u64, ttl_ms: u64) -> bool {
    ttl_ms > 0 && now_ms.saturating_sub(start_ms) + ttl_ms / 10 >= ttl_ms
}

async fn read_row(checked: &Checked, key: &RowKey) -> Option<Vec<Seen>> {
    for attempt in 0..checked.cli.sweep_retries.max(1) {
        if attempt > 0 {
            tokio::time::sleep(checked.cli.sweep_backoff).await;
        }
        match checked.statements.read(&checked.session, key).await {
            Ok(seen) => return Some(seen),
            Err(error) => tracing::debug!("read-back of pk {} failed: {}", key.pk, error.message),
        }
    }
    None
}

fn write_line(out: &mut BufWriter<File>, line: &Line) -> Result<()> {
    serde_json::to_writer(&mut *out, line).context("Failed to write readback.jsonl")?;
    out.write_all(b"\n")
        .context("Failed to write readback.jsonl")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellResult {
    /// The value is one the cell may hold.
    Ok,
    /// Null, or a wid this row issued, though not one the cell may still hold: an
    /// acknowledged write is missing.
    Lost,
    /// A value no write of this row produced: never issued, burned, or not decodable.
    Phantom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowOutcome {
    Ok,
    Lost,
    Phantom,
}

/// Judges each cell of a row read back against its expected state (spec §12.1). An absent
/// row reads as all null.
pub fn judge(expected: &Expected, observed: &[Seen]) -> Vec<CellResult> {
    expected
        .cells
        .iter()
        .zip(observed)
        .map(|(may_hold, seen)| {
            let value = match seen {
                Seen::Null => None,
                Seen::Wid(wid) => Some(*wid),
                Seen::Undecodable => return CellResult::Phantom,
            };
            if may_hold.contains(&value) {
                return CellResult::Ok;
            }
            match value {
                Some(wid) if wid == 0 || wid > expected.max_wid => CellResult::Phantom,
                Some(wid) if expected.burned.contains(&wid) => CellResult::Phantom,
                _ => CellResult::Lost,
            }
        })
        .collect()
}

/// A row is as bad as its worst cell; a phantom is worse than a loss.
pub fn row_result(cells: &[CellResult]) -> RowOutcome {
    if cells.contains(&CellResult::Phantom) {
        RowOutcome::Phantom
    } else if cells.contains(&CellResult::Lost) {
        RowOutcome::Lost
    } else {
        RowOutcome::Ok
    }
}

/// How many of the row's indeterminate writes the read-back shows landed. A lower bound:
/// a landed write that something newer overwrote leaves no trace (spec §12.3).
pub fn landed(expected: &Expected, observed: &[Seen]) -> usize {
    expected
        .indeterminate
        .iter()
        .filter(|wid| observed.contains(&Seen::Wid(**wid)))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invariants::Seen::{Null, Undecodable, Wid};

    fn expected(cells: Vec<Vec<Option<u64>>>) -> Expected {
        Expected {
            cells,
            max_wid: 6,
            burned: vec![4],
            indeterminate: vec![5],
        }
    }

    /// The decision table of spec §12.1.
    #[test]
    fn judge_test() {
        use CellResult::{Lost, Ok, Phantom};
        let e = expected(vec![
            vec![Some(3), Some(5)],
            vec![None],
            vec![None, Some(2)],
        ]);
        assert_eq!(judge(&e, &[Wid(3), Null, Null]), [Ok, Ok, Ok]);
        assert_eq!(
            judge(&e, &[Wid(5), Null, Wid(2)]),
            [Ok, Ok, Ok],
            "a late indeterminate write"
        );
        // A real wid of this row, but one that must have been overwritten.
        assert_eq!(judge(&e, &[Wid(1), Null, Null])[0], Lost);
        // An acknowledged cell that came back empty.
        assert_eq!(judge(&e, &[Null, Null, Null])[0], Lost);
        // The highest wid issued is still a real one.
        assert_eq!(judge(&e, &[Wid(6), Null, Null])[0], Lost);
        // No write produced these: never issued, burned, not decodable.
        assert_eq!(judge(&e, &[Wid(7), Null, Null])[0], Phantom);
        assert_eq!(judge(&e, &[Wid(4), Null, Null])[0], Phantom);
        assert_eq!(judge(&e, &[Undecodable, Null, Null])[0], Phantom);
        assert_eq!(
            judge(&e, &[Wid(3), Wid(2), Null])[1],
            Lost,
            "c1 was never written"
        );
    }

    /// A cell may have been written as early as the row's start; the margin covers a tenth
    /// of the TTL, at least one longest round under the TTL guard, plus clock skew.
    #[test]
    fn ttl_expiry_counts_from_the_row_start_test() {
        let hour = 3_600_000;
        assert!(!may_have_expired(0, u64::MAX, 0), "no TTL, nothing expires");
        assert!(!may_have_expired(1_000_000, 1_000_000 + 3_239_000, hour));
        assert!(may_have_expired(1_000_000, 1_000_000 + 3_240_000, hour));
        assert!(may_have_expired(1_000_000, 1_000_000 + 2 * hour, hour));
    }

    #[test]
    fn row_result_test() {
        use CellResult::{Lost, Ok, Phantom};
        assert_eq!(row_result(&[Ok, Ok]), RowOutcome::Ok);
        assert_eq!(row_result(&[Ok, Lost]), RowOutcome::Lost);
        assert_eq!(
            row_result(&[Lost, Phantom]),
            RowOutcome::Phantom,
            "phantom is worse"
        );
    }

    #[test]
    fn landed_indeterminate_writes_test() {
        let e = expected(vec![vec![Some(3), Some(5)], vec![None, Some(5)]]);
        assert_eq!(
            landed(&e, &[Wid(5), Wid(5)]),
            1,
            "one write, seen in two cells"
        );
        assert_eq!(landed(&e, &[Wid(3), Null]), 0);
    }
}
