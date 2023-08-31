use std::{num::NonZeroUsize, time::Duration};

use anyhow::{Context, Result};
use cql_stress::distribution::{parse_description, SyntaxFlavor};

use crate::java_generate::distribution::{fixed::FixedDistributionFactory, DistributionFactory};

pub trait Parsable: Sized {
    type Parsed;

    fn parse(s: &str) -> Result<Self::Parsed>;

    // Used only to print the same help message as cassandra-stress does for boolean flags.
    fn is_bool() -> bool {
        false
    }
}

/// Simple macro for checking if value `s` matches the regex `regex_str`.
/// Returns error if the value didn't match.
macro_rules! ensure_regex {
    ($s:ident, $regex_str:expr) => {
        lazy_static! {
            static ref RGX: regex::Regex = regex::Regex::new($regex_str).unwrap();
        }
        anyhow::ensure!(
            RGX.is_match($s),
            "Invalid value {}; must match pattern {}",
            $s,
            $regex_str
        )
    };
}

// Implementation of Parsable for common types.

impl Parsable for u64 {
    type Parsed = u64;

    fn parse(s: &str) -> Result<Self::Parsed> {
        ensure_regex!(s, r"^[0-9]+$");
        s.parse::<u64>()
            .with_context(|| format!("Invalid u64 value: {s}"))
    }
}

impl Parsable for NonZeroUsize {
    type Parsed = NonZeroUsize;

    fn parse(s: &str) -> Result<Self::Parsed> {
        s.parse::<NonZeroUsize>()
            .with_context(|| format!("Invalid non-zero usize value: {s}"))
    }
}

impl Parsable for f64 {
    type Parsed = f64;

    fn parse(s: &str) -> Result<Self::Parsed> {
        ensure_regex!(s, r"[0-9]+(\.[0-9]+)?");
        s.parse::<f64>()
            .with_context(|| format!("Invalid f64 argument: {s}"))
    }
}

pub struct UnitInterval;
impl Parsable for UnitInterval {
    type Parsed = f64;

    fn parse(s: &str) -> Result<Self::Parsed> {
        ensure_regex!(s, r"^0\.[0-9]+$");
        s.parse::<f64>()
            .with_context(|| format!("Invalid f64 argument: {s}"))
    }
}

impl Parsable for bool {
    type Parsed = bool;

    fn parse(s: &str) -> Result<Self::Parsed> {
        anyhow::ensure!(
            s.is_empty(),
            "Invalid value {}. Boolean flag cannot have any value.",
            s
        );

        Ok(true)
    }

    fn is_bool() -> bool {
        true
    }
}

impl Parsable for String {
    type Parsed = String;

    fn parse(s: &str) -> Result<Self::Parsed> {
        Ok(s.to_owned())
    }
}

impl Parsable for Duration {
    type Parsed = Duration;

    fn parse(s: &str) -> Result<Self::Parsed> {
        ensure_regex!(s, r"^[0-9]+[smh]$");

        let parse_duration_unit = |unit: char| -> Result<u64> {
            match unit {
                's' => Ok(1),
                'm' => Ok(60),
                'h' => Ok(60 * 60),
                _ => anyhow::bail!("Invalid duration unit: {unit}"),
            }
        };

        let multiplier = parse_duration_unit(
            s.chars()
                .last()
                .ok_or_else(|| anyhow::anyhow!("Invalid argument: {}", s))?,
        )?;
        let value_str = &s[0..s.len() - 1];
        let value = value_str
            .parse::<u64>()
            .with_context(|| format!("Invalid u64 value: {}", value_str))?;
        Ok(Duration::from_secs(value * multiplier))
    }
}

#[derive(Debug, PartialEq, Eq)]
/// Wrapper over the parameter's value matching pattern "[0-9]+[bmk]?".
/// [bmk] suffix denotes the multiplier. One of billion, million or thousand.
pub struct Count;

impl Parsable for Count {
    type Parsed = u64;

    fn parse(s: &str) -> Result<Self::Parsed> {
        ensure_regex!(s, r"^[0-9]+[bmk]?$");

        let parse_operation_count_unit = |unit: char| -> Result<u64> {
            match unit {
                'k' => Ok(1_000),
                'm' => Ok(1_000_000),
                'b' => Ok(1_000_000_000),
                _ => anyhow::bail!("Invalid operation count unit: {unit}"),
            }
        };

        let last = s
            .chars()
            .last()
            .ok_or_else(|| anyhow::anyhow!("Invalid argument: {}", s))?;
        let mut multiplier = 1;
        let mut number_slice = s;
        if last.is_alphabetic() {
            multiplier = parse_operation_count_unit(last)?;
            number_slice = &s[0..s.len() - 1];
        }
        let value = number_slice
            .parse::<u64>()
            .with_context(|| format!("Invalid u64 value: {}", number_slice))?;
        Ok(value * multiplier)
    }
}

pub struct CommaDelimitedList;

impl Parsable for CommaDelimitedList {
    type Parsed = Vec<String>;

    fn parse(s: &str) -> Result<Self::Parsed> {
        ensure_regex!(s, r"^[^=,]+(,[^=,]+)*$");
        Ok(s.split(',').map(|e| e.to_owned()).collect())
    }
}

pub struct Rate;

impl Parsable for Rate {
    type Parsed = u64;

    fn parse(s: &str) -> Result<Self::Parsed> {
        ensure_regex!(s, r"^[0-9]+/s$");

        let value_slice = &s[..s.len() - 2];
        let value = value_slice
            .parse::<u64>()
            .with_context(|| format!("Invalid u64 value: {value_slice}"))?;
        Ok(value)
    }
}

impl Parsable for Box<dyn DistributionFactory> {
    type Parsed = Self;

    fn parse(s: &str) -> Result<Self::Parsed> {
        let description = parse_description(s, SyntaxFlavor::Classic)?;

        anyhow::ensure!(
            !description.inverted,
            "Inverted distributions are not yet supported!"
        );

        match description.name {
            "fixed" => FixedDistributionFactory::parse_from_description(description),
            _ => Err(anyhow::anyhow!(
                "Invalid distribution name: {}",
                description.name
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::java_generate::distribution::DistributionFactory;

    use super::Parsable;

    type DistributionTestType = Box<dyn DistributionFactory>;

    #[test]
    fn distribution_param_fixed_test() {
        let good_test_cases = &["fixed(45)", "fixed(100000)"];
        for input in good_test_cases {
            assert!(DistributionTestType::parse(input).is_ok());
        }

        let bad_test_cases = &[
            "fixed(45,50)",
            "fixed(45",
            "fixed45",
            "fixed(45..50)",
            "fixed(100.1234)",
            "fixed40)",
        ];

        for input in bad_test_cases {
            assert!(DistributionTestType::parse(input).is_err());
        }
    }
}
