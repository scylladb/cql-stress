//! Row keys of the checked stream (spec §7.3).
//!
//! `pk` comes from `--pop` (a library population, which wraps around on its own) and `ck` is
//! always 0 in phase 1. `gen` makes every round a fresh row: `start_unix_ms × 2^20 + n`, with
//! one counter `n` per process. It is unique within the process and across restarts, as long
//! as the clock moves forward, and it is at least 2^60, so no checked row is ever a bulk or
//! preload row (those use gen < 2^40).

use anyhow::Result;

const PER_MILLISECOND: u64 = 1 << 20;

/// The primary key of a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowKey {
    pub pk: i64,
    pub gen: i64,
    pub ck: i32,
}

#[derive(Debug)]
pub struct GenMinter {
    base_ms: u64,
    n: u64,
}

impl GenMinter {
    pub fn new(now_ms: u64) -> Result<Self> {
        anyhow::ensure!(
            now_ms >= 1 << 40,
            "The clock reads {now_ms} ms since 1970, before 2004: checked row keys would not \
             stay apart from bulk rows"
        );
        Ok(Self {
            base_ms: now_ms,
            n: 0,
        })
    }

    /// The gen base, for the `SCV start` line.
    pub fn base(&self) -> i64 {
        (self.base_ms * PER_MILLISECOND) as i64
    }

    /// The next gen; `None` once this millisecond's 2^20 are used up and the clock has not
    /// moved past it yet: wait a millisecond and ask again.
    pub fn next(&mut self, now_ms: u64) -> Option<i64> {
        if self.n == PER_MILLISECOND {
            if now_ms <= self.base_ms {
                return None;
            }
            self.base_ms = now_ms;
            self.n = 0;
        }
        let gen = self.base_ms * PER_MILLISECOND + self.n;
        self.n += 1;
        Some(gen as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-10-06 in unix milliseconds.
    const NOW_MS: u64 = 1_791_288_000_000;

    #[test]
    fn gens_are_large_and_increasing_test() {
        let mut minter = GenMinter::new(NOW_MS).unwrap();
        let first = minter.next(NOW_MS).unwrap();
        assert_eq!(first, (NOW_MS << 20) as i64);
        assert!(
            first >= 1 << 60,
            "checked rows can never collide with bulk rows (gen < 2^40)"
        );
        assert_eq!(minter.next(NOW_MS).unwrap(), first + 1);
    }

    #[test]
    fn exhausted_millisecond_rebases_test() {
        let mut minter = GenMinter::new(NOW_MS).unwrap();
        let mut last = 0;
        for _ in 0..1 << 20 {
            last = minter.next(NOW_MS).unwrap();
        }
        assert_eq!(
            minter.next(NOW_MS),
            None,
            "2^20 gens per millisecond at most"
        );
        let rebased = minter.next(NOW_MS + 1).unwrap();
        assert_eq!(rebased, ((NOW_MS + 1) << 20) as i64);
        assert!(rebased > last);
    }

    #[test]
    fn a_clock_before_2004_is_refused_test() {
        assert!(GenMinter::new((1 << 40) - 1).is_err());
    }
}
