use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use anyhow::{Context, Result};
use cql_stress::distribution::{parse_description, SyntaxFlavor};
use scylla::client::{Compression, PoolSize};

use crate::java_generate::distribution::{
    fixed::FixedDistributionFactory, normal::NormalDistributionFactory,
    sequence::SeqDistributionFactory, uniform::UniformDistributionFactory, DistributionFactory,
};

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

impl Parsable for NonZeroU32 {
    type Parsed = NonZeroU32;

    fn parse(s: &str) -> Result<Self::Parsed> {
        s.parse::<NonZeroU32>()
            .with_context(|| format!("Invalid non-zero u32 value: {s}"))
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
        let s = &s.to_lowercase();
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
        let s: &str = &s.to_lowercase();
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
        let s = &s.to_lowercase();
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
        let s = &s.to_lowercase();
        let description = parse_description(s, SyntaxFlavor::Classic)?;

        anyhow::ensure!(
            !description.inverted,
            "Inverted distributions are not yet supported!"
        );

        match description.name {
            "fixed" => FixedDistributionFactory::parse_from_description(description),
            "seq" => SeqDistributionFactory::parse_from_description(description),
            "uniform" => UniformDistributionFactory::parse_from_description(description),
            "gaussian" | "gauss" | "norm" | "normal" => {
                NormalDistributionFactory::parse_from_description(description)
            }
            _ => Err(anyhow::anyhow!(
                "Invalid distribution name: {}",
                description.name
            )),
        }
    }
}

/// A range syntax (where value1 and value2 parse to type T) is "value1..value2".
pub struct Range<T: Parsable>(PhantomData<T>);

impl<T: Parsable> Parsable for Range<T> {
    type Parsed = (T::Parsed, T::Parsed);

    fn parse(s: &str) -> Result<Self::Parsed> {
        let (from, to) = match s.split_once("..") {
            Some((from_str, to_str)) => {
                let from = T::parse(from_str)?;
                let to = T::parse(to_str)?;
                (from, to)
            }
            None => {
                return Err(anyhow::anyhow!(
                    "Invalid range value: Expected syntax is value1..value2"
                ));
            }
        };

        Ok((from, to))
    }
}

impl Parsable for Option<Compression> {
    type Parsed = Option<Compression>;

    fn parse(s: &str) -> Result<Self::Parsed> {
        match s {
            "none" => Ok(None),
            "lz4" => Ok(Some(Compression::Lz4)),
            "snappy" => Ok(Some(Compression::Snappy)),
            _ => Err(anyhow::anyhow!("Invalid compression algorithm: {}. Valid compression algorithms: none, lz4, snappy.", s))
        }
    }
}

pub struct ConnectionsPerHost;

impl Parsable for ConnectionsPerHost {
    type Parsed = PoolSize;

    fn parse(s: &str) -> Result<Self::Parsed> {
        let value = <NonZeroUsize as Parsable>::parse(s)?;
        Ok(PoolSize::PerHost(value))
    }
}

pub struct ConnectionsPerShard;

impl Parsable for ConnectionsPerShard {
    type Parsed = PoolSize;

    fn parse(s: &str) -> Result<Self::Parsed> {
        let value = <NonZeroUsize as Parsable>::parse(s)?;
        Ok(PoolSize::PerShard(value))
    }
}

/// A ratio map which should match the following pattern:
/// (<item1>=<f64>,<item2>=<f64>,...,<item_n>=<f64>)
///
/// Requirements:
/// - Items have to be unique -> "(foo=1,foo=2)" parsing should fail
/// - User needs to specify at least one item's ratio -> "()" parsing should fail
/// - Weights cannot be negative -> "(foo=-1)" parsing should fail
/// - Weights cannot sum up to 0 -> "(foo=0,bar=0)" parsing should fail
///
/// Last 3 requirements are introduced so creating a [rand_distr::WeightedIndex] with
/// [rand_distr::WeightedIndex::new] from iterator of f64 values does not fail.
pub struct RatioMap;

impl RatioMap {
    fn parse_item_weight(s: &str) -> Result<(&str, f64)> {
        let (item, weight) = {
            let mut iter = s.split('=').fuse();
            match (iter.next(), iter.next(), iter.next()) {
                (Some(cmd), Some(w), None) => (cmd, w),
                _ => anyhow::bail!("Item weight specification should match pattern <item>=<f64>"),
            }
        };

        let weight = weight.parse::<f64>()?;
        anyhow::ensure!(weight >= 0f64, "Item weight cannot be negative: {}", weight);

        Ok((item, weight))
    }

    fn do_parse(s: &str) -> Result<HashMap<String, f64>> {
        // Remove wrapping parentheses.
        let arg = {
            let mut chars = s.chars();
            anyhow::ensure!(
                chars.next() == Some('(') && chars.next_back() == Some(')'),
                "List of item weights should be wrapped with parentheses",
            );
            chars.as_str()
        };

        // A set to ensure that items are unique.
        let mut item_set = HashSet::<&str>::new();
        // Verify that sum of weights is non-zero.
        let mut sum = 0f64;
        let weights_map = arg
            .split(',')
            .map(|s| -> Result<(String, f64)> {
                let (item, weight) = Self::parse_item_weight(s)?;
                anyhow::ensure!(
                    !item_set.contains(item),
                    "'{}' item has been specified more than once",
                    item
                );
                sum += weight;
                item_set.insert(item);
                Ok((item.to_owned(), weight))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        anyhow::ensure!(!weights_map.is_empty(), "Ratio map is empty.");
        anyhow::ensure!(sum > 0f64, "Weights cannot sum up to 0.");

        Ok(weights_map)
    }
}

impl Parsable for RatioMap {
    type Parsed = HashMap<String, f64>;

    fn parse(s: &str) -> Result<Self::Parsed> {
        Self::do_parse(s).with_context(|| format!("Invalid ratio specification: {}", s))
    }
}

/// Parses an interval value with optional millisecond or second suffix.
/// Valid formats: "123" (seconds), "123s" (seconds), "123ms" (milliseconds)
pub struct IntervalMillisOrSeconds;

impl Parsable for IntervalMillisOrSeconds {
    type Parsed = std::time::Duration;

    fn parse(s: &str) -> Result<Self::Parsed> {
        ensure_regex!(s, r"^[0-9]+(ms|s|)$");

        if s.ends_with("ms") {
            // Parse milliseconds
            let ms_str = &s[0..s.len() - 2];
            let ms = ms_str
                .parse::<u64>()
                .with_context(|| format!("Invalid millisecond value: {}", ms_str))?;
            Ok(Duration::from_millis(ms))
        } else {
            // Parse seconds (either with "s" suffix or without suffix)
            let sec_str = if s.ends_with('s') {
                &s[0..s.len() - 1]
            } else {
                s
            };
            let sec = sec_str
                .parse::<u64>()
                .with_context(|| format!("Invalid second value: {}", sec_str))?;
            Ok(Duration::from_secs(sec))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        java_generate::distribution::DistributionFactory, settings::param::types::RatioMap,
    };

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

    #[test]
    fn distribution_param_seq_test() {
        let good_test_cases = &["seq(45..50)", "seq(1..100000)"];
        for input in good_test_cases {
            assert!(DistributionTestType::parse(input).is_ok());
        }

        let bad_test_cases = &[
            "seq(2..1)",
            "seq(2..2)",
            "seq(45",
            "seq45..50",
            "seq(45)",
            "seq(100.1234)",
            "seq40)",
        ];

        for input in bad_test_cases {
            assert!(DistributionTestType::parse(input).is_err());
        }
    }

    #[test]
    fn distribution_param_uniform_test() {
        let good_test_cases = &["uniform(45..50)", "uniform(1..100000)", "uniform(2..2)"];
        for input in good_test_cases {
            assert!(DistributionTestType::parse(input).is_ok());
        }

        let bad_test_cases = &[
            "uniform(2..1)",
            "uniform(1..20,50)",
            "uniform(45",
            "uniform45..50",
            "uniform(45)",
            "uniform(100.1234)",
            "uniform40)",
        ];

        for input in bad_test_cases {
            assert!(DistributionTestType::parse(input).is_err());
        }
    }

    #[test]
    fn distribution_param_gaussian_test() {
        let good_test_cases = &[
            "gaussian(1..10)",
            "gauss(1..10)",
            "normal(1..10)",
            "norm(1..10)",
            "gaussian(1..10,5)",
            "gaussian(1..10,5,5)",
        ];
        for input in good_test_cases {
            assert!(DistributionTestType::parse(input).is_ok());
        }

        let bad_test_cases = &[
            "gaussian(2..1)",
            "gaussian(1..20,50,50,50)",
            "gaussian(45",
            "gaussian45..50",
            "gaussian(45)",
            "gaussian(100.1234)",
            "gaussian40)",
        ];
        for input in bad_test_cases {
            assert!(DistributionTestType::parse(input).is_err());
        }
    }

    #[test]
    fn ratio_map_param_test() {
        let good_test_cases = ["(foo=1)", "(foo=1.2,bar=21,baz=0.5)", "(foo=1,bar=0)"];
        for input in good_test_cases {
            assert!(RatioMap::parse(input).is_ok())
        }

        let bad_test_cases = [
            "()",
            "(foo=1=2)",
            "(foo=1,foo=2)",
            "(foo=bar)",
            "(foo=1",
            "foo=1)",
            "(foo=0,bar=0)",
            "(foo=-1.2)",
        ];
        for input in bad_test_cases {
            assert!(RatioMap::parse(input).is_err())
        }
    }
}
