use std::convert::TryInto;
use std::time::Duration;

use anyhow::Result;

/// Equivalent to Go's strconv.ParseBool.
pub fn parse_bool(s: &str) -> Result<bool> {
    match s {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(false),
        _ => Err(anyhow::anyhow!(
            "Invalid string representation of bool value: {}",
            s
        )),
    }
}

/// Similar to Go's strconv.ParseInt, however it always
/// detects the base automatically.
///
/// Ref: https://pkg.go.dev/strconv#ParseInt
pub fn parse_int(mut s: &str, bit_size: u32) -> Result<i64> {
    anyhow::ensure!(
        bit_size != 0 && bit_size <= 64,
        "Invalid bit size {}",
        bit_size
    );

    // Detect the sign
    let negative = s.starts_with('-');
    s = s.strip_prefix(&['-', '+']).unwrap_or(s);

    if negative {
        let max_value = 1u128 << (bit_size - 1);
        let v = parse_int_inner(s, max_value)? as i64;
        Ok(-v)
    } else {
        let max_value = (1u128 << (bit_size - 1)) - 1;
        Ok(parse_int_inner(s, max_value)? as i64)
    }
}

/// Similar to Go's strconv.ParseUint, however it always
/// detects the base automatically.
///
/// Ref: https://pkg.go.dev/strconv#ParseUint
pub fn parse_uint(s: &str, bit_size: u32) -> Result<u64> {
    anyhow::ensure!(
        bit_size != 0 && bit_size <= 64,
        "Invalid bit size {}",
        bit_size
    );

    let max_value = (1u128 << bit_size) - 1;
    Ok(parse_int_inner(s, max_value)? as u64)
}

fn parse_int_inner(mut s: &str, max_value: u128) -> Result<u128> {
    // The literal may be interspersed with underscores.
    // Underscores cannot happen at beginning and at the end,
    // and they cannot touch each other.
    let good_underscores = s.split('_').all(|part| !part.is_empty());
    anyhow::ensure!(
        good_underscores,
        "The literal has bad placement of underscores",
    );

    // Detect the base
    let base = match s.get(..2) {
        Some("0x") => 16,
        Some("0o") => 8,
        Some("0b") => 2,
        _ => 10,
    };
    if base != 10 {
        s = &s[2..];
    }

    // Parse the value
    let mut ret = 0u128;

    for c in s.chars() {
        // Ignore underscores
        if c == '_' {
            continue;
        }

        let d = match c.to_ascii_lowercase() {
            '0'..='9' => (c as u32) - ('0' as u32),
            'a'..='f' => (c as u32) - ('a' as u32) + 10,
            _ => base, // Just to trigger the next error check
        };
        anyhow::ensure!(d < base, "Invalid digit of base {}: {}", base, c);

        ret *= base as u128;
        ret += d as u128;
        anyhow::ensure!(ret <= max_value, "Literal out of representable range");
    }

    Ok(ret)
}

static UNIT_MULTIPLICANDS: &[(&str, f64)] = &[
    ("ns", 1.0),
    ("us", 1_000.0),
    ("\u{00B5}s", 1_000.0), // U+00B5 = micro symbol
    ("\u{03BC}s", 1_000.0), // U+03BC = Greek letter mu
    ("ms", 1_000_000.0),
    ("s", 1_000_000_000.0),
    ("m", 60.0 * 1_000_000_000.0),
    ("h", 60.0 * 60.0 * 1_000_000_000.0),
];

/// Reimplementation of Go's time.ParseDuration.
///
/// A duration string is a sequence of decimal numbers, each with optional
/// fraction and a unit suffix. Everything is preceded with an optional
/// plus (`+`) sign. A unitless zero (`0`) is accepted as a special case
/// Some examples: 20s, 1h20min, 100ms, 0.
///
/// Unlike the original Go implementation, this function does not support
/// negative durations, as they are not representable by std::time::Duration
/// and are not useful for the scylla-bench frontend.
///
/// Ref: https://pkg.go.dev/time#ParseDuration
pub fn parse_duration(mut s: &str) -> Result<Duration> {
    let original = s;
    let mut nanos = 0u128;

    // We don't support negative durations! We don't need them, and Rust's duration
    // does not permit negative durations either.
    if s.starts_with('-') {
        return Err(anyhow::anyhow!("Negative durations are not supported"));
    } else if let Some(stripped) = s.strip_prefix('+') {
        s = stripped;
    }

    // Special case for unitless 0
    if s == "0" {
        return Ok(Duration::ZERO);
    }

    if s.is_empty() {
        return Err(anyhow::anyhow!("Invalid duration: {}", original));
    }

    while !s.is_empty() {
        // Consume a number (possibly floating point)
        let number_end = s
            .find(|c: char| c != '.' && !c.is_ascii_digit())
            .unwrap_or(s.len());
        let (number_s, rest) = s.split_at(number_end);
        s = rest;

        let number = number_s.parse::<f64>()?;

        // Consume a unit
        let unit_end = s
            .find(|c: char| c == '.' || c.is_ascii_digit())
            .unwrap_or(s.len());
        let (unit, rest) = s.split_at(unit_end);
        s = rest;

        let unit_multiplicand = UNIT_MULTIPLICANDS
            .iter()
            .find_map(|(uname, mult)| (&unit == uname).then(|| mult))
            .ok_or_else(|| anyhow::anyhow!("Invalid duration unit: {}", unit))?;

        // Converting floats to ints when the float is too big to fit is UB,
        // therefore before converting we check if the currently parsed part
        // itself would overflow Duration
        const MAX_DURATION_NANOS: u128 = (1 << 32) * 1_000_000_000 - 1;
        let multiplied_number = number * unit_multiplicand;
        anyhow::ensure!(
            multiplied_number <= MAX_DURATION_NANOS as f64,
            "Duration out of representable range"
        );

        nanos = nanos
            .checked_add(multiplied_number as u128) // Assume it's OK to convert
            .ok_or_else(|| anyhow::anyhow!("Duration out of representable range"))?;
    }

    // Rust's API does not permit constructing durations from u128 nanoseconds, only u64
    // Therefore, we need to split into seconds and nanoseconds and then combine.

    const NANOS_PER_SEC: u128 = 1_000_000_000;

    let seconds: u64 = (nanos / NANOS_PER_SEC)
        .try_into()
        .map_err(|_| anyhow::anyhow!("Duration out of representable range"))?;
    let nanos = (nanos % NANOS_PER_SEC) as u32;

    Ok(Duration::new(seconds, nanos))
}

// TODO: Comment
pub fn format_duration(d: Duration) -> String {
    use std::fmt::Write;

    if d == Duration::ZERO {
        return "0".to_string();
    }

    // Not sure how much capacity is needed
    let mut s = String::with_capacity(16);

    if d >= Duration::from_secs(1) {
        let minutes = (d.as_secs() / 60) % 60;
        let hours = d.as_secs() / 3600;

        if hours > 0 {
            write!(s, "{}h", hours).unwrap();
        }
        if minutes > 0 {
            write!(s, "{}m", minutes).unwrap();
        }

        // Print one digit of precision
        let secs = d.as_secs_f64() % 60.0;
        write!(s, "{:.1}s", secs).unwrap();
    } else {
        // Diverge a bit from scylla-bench: we will keep at most 3 digits of precision, always
        let total_nanos = d.subsec_nanos();
        let mut first_digit = 1_000_000_000;
        let mut first_offset = 9;
        while first_digit > 1 && total_nanos < first_digit {
            first_digit /= 10;
            first_offset -= 1;
        }

        let round_unit = std::cmp::max(first_digit / 100, 1);
        let total_nanos = total_nanos - (total_nanos % round_unit);

        if first_digit >= 1_000_000 {
            let prec = 8 - first_offset;
            write!(s, "{:.*}ms", prec, total_nanos as f64 / 1_000_000.0).unwrap();
        } else if first_digit >= 1_000 {
            let prec = 5 - first_offset;
            write!(s, "{:.*}μs", prec, total_nanos as f64 / 1_000.0).unwrap();
        } else {
            write!(s, "{}ns", total_nanos).unwrap();
        }
    }

    s
}

// TODO: Comment
pub fn quote_string(s: &str) -> String {
    use std::fmt::Write;

    // Go escapes literals differently than Rust.
    // Here is a simplified and unoptimized version of what Go does.
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');

    for c in s.chars() {
        if c == '\\' || c == '"' {
            out.push('\\');
            out.push(c);
            continue;
        }

        // The actual condition for being "printable" here is much
        // more sophisticated, but Rust's stdlib doesn't provide much
        // utilities for classifying Unicode chars, so instead here is
        // a conservative condition which will escape more chars
        // than necessary, but for our case this should be sufficient.
        let printable = c.is_ascii_graphic() || c == ' ' || c.is_alphanumeric();

        if printable {
            out.push(c);
            continue;
        }

        // Special case escapes
        let special_escape = match c {
            '\u{7}' => Some('a'),
            '\u{8}' => Some('b'),
            '\u{C}' => Some('d'),
            '\u{A}' => Some('n'),
            '\u{D}' => Some('r'),
            '\u{9}' => Some('t'),
            '\u{B}' => Some('v'),
            _ => None,
        };

        if let Some(esc) = special_escape {
            out.push('\\');
            out.push(esc);
            continue;
        }

        // Otherwise, escape as unicode in the Go syntax
        if c < ' ' {
            out.push_str("\\x");
            write!(out, "{:02x}", c as u32).unwrap();
        } else if (c as u32) < 0x10000 {
            out.push_str("\\u");
            write!(out, "{:04x}", c as u32).unwrap();
        } else {
            out.push_str("\\U");
            write!(out, "{:08x}", c as u32).unwrap();
        }
    }

    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;

    // Returns true if parsing succeeded and returned the expected value
    fn parse_expecting_success<T: Eq + Debug>(
        raw: &str,
        expected: &T,
        parse: impl Fn(&str) -> Result<T>,
    ) -> bool {
        match (parse)(raw) {
            Err(err) => {
                println!("Subtest failed: failed to parse {}: {}", raw, err);
                false
            }
            Ok(actual) if &actual != expected => {
                println!(
                    "Subtest failed: {} parsed as {:?}, but expected {:?}",
                    raw, actual, expected,
                );
                false
            }
            Ok(_) => true,
        }
    }

    // Returns true if parsing failed, as expected
    fn parse_expecting_failure<T: Debug>(raw: &str, parse: impl Fn(&str) -> Result<T>) -> bool {
        match (parse)(raw) {
            Ok(actual) => {
                println!(
                    "Subtest failed: unexpectedly parsed {} as {:?}",
                    raw, actual,
                );
                false
            }
            Err(_) => true,
        }
    }

    #[test]
    fn test_parse_uint_good() {
        use std::{u32, u64};

        let tests_32: &[(&str, u32)] = &[
            // Zeros
            ("0", 0),
            ("0x0", 0x0),
            ("0o0", 0o0),
            ("0b0", 0b0),
            // Non-zeros
            ("123", 123),
            ("0x123", 0x123),
            ("0o123", 0o123),
            ("0b101", 0b101),
            // Limits
            (&format!("{}", u32::MAX), u32::MAX),
            (&format!("0x{:x}", u32::MAX), u32::MAX),
            (&format!("0o{:o}", u32::MAX), u32::MAX),
            (&format!("0b{:b}", u32::MAX), u32::MAX),
            // Separators
            ("12_34", 1234),
            ("1_2_3_4", 1234),
            ("0x_1234", 0x1234),
            ("0o_1234", 0o1234),
            ("0b_1010", 0b1010),
        ];
        let tests_64: &[(&str, u64)] = &[
            // Out of range for u32
            ("0x1_0000_0000", 1u64 << 32),
            // Limits
            (&format!("{}", u64::MAX), u64::MAX),
            (&format!("0x{:x}", u64::MAX), u64::MAX),
            (&format!("0o{:o}", u64::MAX), u64::MAX),
            (&format!("0b{:b}", u64::MAX), u64::MAX),
        ];

        let it_32 = tests_32.iter().map(|(s, i)| (s, *i as u64, 32));
        let it_64 = tests_64.iter().map(|(s, i)| (s, *i as u64, 64));

        let mut succeeded = true;
        for (s, expected, bits) in it_32.chain(it_64) {
            succeeded &= parse_expecting_success(s, &expected, |s| parse_uint(s, bits));
        }

        if !succeeded {
            panic!("Test failed");
        }
    }

    #[test]
    fn test_parse_uint_bad() {
        use std::u64;

        let tests_32: &[&str] = &[
            // Invalid characters
            "abcd",
            "0xabcdefghijklmn",
            "0o123456789",
            "0b123",
            "-123",
            // Bad separators
            "_0",
            "0_",
            "1__0",
            "_0x123",
            // Out of range
            "123456789123456789123456789",
            "0x1_0000_0000",
            &format!("{}", u64::MAX),
            &format!("0x{:x}", u64::MAX),
            &format!("0o{:o}", u64::MAX),
            &format!("0b{:b}", u64::MAX),
        ];
        let tests_64: &[&str] = &[
            // Out of range
            "123456789123456789123456789",
        ];

        let it_32 = tests_32.iter().map(|s| (s, 32));
        let it_64 = tests_64.iter().map(|s| (s, 64));

        let mut succeeded = true;
        for (s, bits) in it_32.chain(it_64) {
            succeeded &= parse_expecting_failure(s, |s| parse_uint(s, bits));
        }

        if !succeeded {
            panic!("Test failed");
        }
    }

    #[test]
    fn test_parse_int_good() {
        use std::i32;

        let tests_32: &[(&str, i32)] = &[
            // Zero
            ("0", 0),
            // Signs
            ("123", 123),
            ("+123", 123),
            ("-123", -123),
            ("0x123", 0x123),
            ("+0x123", 0x123),
            ("-0x123", -0x123),
            // Limits
            (&format!("{}", i32::MAX), i32::MAX),
            (&format!("0x{:x}", i32::MAX), i32::MAX),
            (&format!("0o{:o}", i32::MAX), i32::MAX),
            (&format!("0b{:b}", i32::MAX), i32::MAX),
            (&format!("{}", i32::MIN), i32::MIN),
        ];
        let tests_64: &[(&str, i64)] = &[
            // Out of range for i32
            (&format!("{}", i32::MAX as i64 + 1), i32::MAX as i64 + 1),
            (&format!("{}", i32::MIN as i64 - 1), i32::MIN as i64 - 1),
        ];

        let it_32 = tests_32.iter().map(|(s, i)| (s, *i as i64, 32));
        let it_64 = tests_64.iter().map(|(s, i)| (s, *i as i64, 64));

        let mut succeeded = true;
        for (s, expected, bits) in it_32.chain(it_64) {
            succeeded &= parse_expecting_success(s, &expected, |s| parse_int(s, bits));
        }

        if !succeeded {
            panic!("Test failed");
        }
    }

    #[test]
    fn test_parse_int_bad() {
        let tests_32: &[&str] = &[
            // Out of range for i32
            &format!("{}", i32::MAX as i64 + 1),
            &format!("{}", i32::MIN as i64 - 1),
        ];
        let tests_64: &[&str] = &[
            // Out of range
            "123456789123456789123456789",
            "-123456789123456789123456789",
        ];

        let it_32 = tests_32.iter().map(|s| (s, 32));
        let it_64 = tests_64.iter().map(|s| (s, 64));

        let mut succeeded = true;
        for (s, bits) in it_32.chain(it_64) {
            succeeded &= parse_expecting_failure(s, |s| parse_int(s, bits));
        }

        if !succeeded {
            panic!("Test failed");
        }
    }

    #[test]
    fn test_parse_duration_good() {
        let h = Duration::from_secs(60 * 60);
        let m = Duration::from_secs(60);
        let s = Duration::from_secs(1);
        let ms = Duration::from_millis(1);
        let us = Duration::from_micros(1);
        let ns = Duration::from_nanos(1);

        let tests: &[(&str, Duration)] = &[
            // Unitless zero
            ("0", Duration::ZERO),
            // All units
            ("24h", 24 * h),
            ("30m", 30 * m),
            ("1s", 1 * s),
            ("100ms", 100 * ms),
            ("10\u{00B5}s", 10 * us),
            ("15\u{03BC}s", 15 * us),
            ("20us", 20 * us),
            ("123ns", 123 * ns),
            // Optional plus sign
            ("+24h", 24 * h),
            ("+0", Duration::ZERO),
            // Fractions
            ("1.5h", 1 * h + 30 * m),
            ("1.2s", 1 * s + 200 * ms),
            (".5h", 30 * m),
            // Multiple units
            ("1h20m", 1 * h + 20 * m),
            ("5s200ms50us", 5 * s + 200 * ms + 50 * us),
        ];

        let mut succeeded = true;
        for (s, expected) in tests.iter() {
            succeeded &= parse_expecting_success(s, expected, parse_duration);
        }

        if !succeeded {
            panic!("Test failed");
        }
    }

    #[test]
    fn test_parse_duration_bad() {
        // rustfmt insists on putting multiple test cases into a single line
        // and moving comments at the end of the previous line, which is
        // something that I definitely don't want, hence rustfmt::skip here
        #[rustfmt::skip]
        let tests: &[&str] = &[
            // Negative duration
            "-100ms",
            // Invalid suffixes
            "100days",
            "1min",
            // Non-decimal numbers
            "0x123ms",
            "0o123ms",
            "0b101ms",
            // Whitespace
            " 10ms",
            "10ms ",
            "1h 10m",
        ];

        let mut succeeded = true;
        for s in tests.iter() {
            succeeded &= parse_expecting_failure(s, parse_duration);
        }

        if !succeeded {
            panic!("Test failed");
        }
    }
}
