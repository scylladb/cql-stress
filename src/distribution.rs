use anyhow::{Context, Result};

pub trait Factory {
    type Distribution;

    fn create(name: &str, args: &[&str], inverted: bool) -> Result<Self::Distribution>;
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum SyntaxFlavor {
    Classic,
    ClassicOrShort,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Description<'a> {
    pub name: &'a str,
    pub args: Vec<&'a str>,
    pub inverted: bool,
}

impl<'a> Description<'a> {
    pub fn check_argument_count(&self, expected: usize) -> Result<()> {
        anyhow::ensure!(
            self.args.len() == expected,
            "Expected {} arguments, but got {}",
            expected,
            self.args.len(),
        );
        Ok(())
    }
}

// Parses the description of a distribution.
//
// Both cassandra-stress and scylla-bench share a common format of distribution
// descriptions. The description consists of a name and an argument list.
// Arguments are enclosed in parentheses and separated with commas, with the
// exception of the first two arguments which usually represent a range
// and are separated by two dots.
//
// Here are some examples:
//
//   distributionname(arg1)
//   distributionname(arg1..arg2)
//   distributionname(arg1..arg2,arg3)
//   distributionname(arg1..arg2,arg3,arg4)
//   ... and so on
//
// Optionally, a distribution can be "inverted" - in that case, it will
// be preceded with a tilde (~):
//
//   ~distributionname(arg1..arg2, arg3)
//
// In addition to that, scylla-bench supports a "bash-friendly" variant
// of the above syntax which does not use parentheses, but instead separates
// the distribution name from the arguments with a colon:
//
//   distributionname:arg1..arg2,arg3
//
// This function is only responsible for decomposing the description string
// into the distribution name and arguments, represented as strings. Both
// cassandra-stress and scylla-bench accept different distributions and parse
// their arguments in a slightly different way, so it's the responsiblity
// of the frontends to further interpret the decomposed description.
pub fn parse_description(s: &str, flavor: SyntaxFlavor) -> Result<Description> {
    let mut s = s.trim();

    let inverted = match s.strip_prefix('~') {
        Some(stripped) => {
            s = stripped;
            true
        }
        None => false,
    };

    let (name, args_subslice) = decompose_name_and_args(s, flavor)
        .context("Could not decompose into distribution name and argument list")?;

    let args = decompose_args(args_subslice).context("Could not the parse argument list")?;

    Ok(Description {
        name,
        args,
        inverted,
    })
}

// Decomposes given string into (distribution name, args slice).
fn decompose_name_and_args(s: &str, flavor: SyntaxFlavor) -> Result<(&str, &str)> {
    if let Some((name, args_subslice)) = s.split_once('(') {
        let args_subslice = args_subslice.strip_suffix(')').ok_or_else(|| {
            anyhow::anyhow!("Missing closing parenthesis ')' in the distribution parameter list")
        })?;
        return Ok((name.trim(), args_subslice));
    } else if flavor == SyntaxFlavor::ClassicOrShort {
        if let Some((name, args_subslice)) = s.split_once(':') {
            return Ok((name.trim(), args_subslice));
        }
    }

    Err(match flavor {
        SyntaxFlavor::Classic => anyhow::anyhow!("Missing opening parenthesis '('"),
        SyntaxFlavor::ClassicOrShort => {
            anyhow::anyhow!("Missing opening parenthesis '(' or colon ':'")
        }
    })
}

// Decomposes the argument list into separate arguments.
fn decompose_args(s: &str) -> Result<Vec<&str>> {
    if let Some((first, after_dots)) = s.split_once("..") {
        let mut v = vec![first.trim()];
        v.extend(after_dots.split(',').map(|s| s.trim()));
        Ok(v)
    } else if !s.is_empty() {
        // No "..", assume it's only one argument
        // Make sure that there are no commas in the string
        anyhow::ensure!(
            !s.contains(','),
            "The first two parameters must be separated by double dots '..', not a comma ','"
        );
        Ok(vec![s.trim()])
    } else {
        // Empty string - so no args
        Ok(vec![])
    }
}

// Parses a 64-bit integer which is a part of a distribution description,
// in a format accepted both by c-s and s-b.
//
// c-s accepts signed integers and s-b unsigned, so the function is generic
// over the integer type and accepts both i64 and u64.
//
// The number may end with a one letter suffix which serves as a multiplier
// for the number: 'k' - thousands, 'm' - millions, 'b' - billions.
// The suffix is case-insensitive.
//
// NOTE: Actually, s-b does not support the b, m, k suffixes, however
// there is a TODO with a note to implement it.
pub fn parse_long<I: ParsableNumber>(s: &str) -> Result<I> {
    let s = s.trim();
    let last_char = s.chars().next_back().map(|c| c.to_ascii_lowercase());

    let mult: Option<I> = match last_char {
        Some('b') => Some(I::from_u32(1_000_000_000)),
        Some('m') => Some(I::from_u32(1_000_000)),
        Some('k') => Some(I::from_u32(1_000)),
        _ => None,
    };
    match mult {
        Some(mult) => {
            let s = &s[..s.len() - 1];
            let num = I::from_str(s)?;
            let adjusted = mult.checked_mul(num)?;
            Ok(adjusted)
        }
        None => Ok(I::from_str(s)?),
    }
}

// Unfortunately, Rust's stdlib does not provide a trait for checked_mul,
// therefore we define this trait for i64 and u64.
pub trait ParsableNumber: Sized {
    fn from_u32(num: u32) -> Self;
    fn checked_mul(&self, other: Self) -> Result<Self>;
    fn from_str(s: &str) -> Result<Self>;
}

macro_rules! impl_parsable_number {
    ($typ:tt) => {
        impl ParsableNumber for $typ {
            fn from_u32(num: u32) -> Self {
                num as $typ
            }
            fn checked_mul(&self, other: Self) -> Result<Self> {
                $typ::checked_mul(*self, other).ok_or_else(|| {
                    anyhow::anyhow!("Multiplication of {} * {} is out of range", self, other)
                })
            }
            fn from_str(s: &str) -> Result<Self> {
                Ok(<$typ as std::str::FromStr>::from_str(s)?)
            }
        }
    };
}

impl_parsable_number!(i64);
impl_parsable_number!(u64);

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_classic(s: &str) -> Result<Description> {
        parse_description(s, SyntaxFlavor::Classic)
    }

    fn parse_modern(s: &str) -> Result<Description> {
        parse_description(s, SyntaxFlavor::ClassicOrShort)
    }

    #[test]
    fn test_distribution() {
        assert_eq!(
            parse_classic("  dist()").unwrap(),
            Description {
                name: "dist",
                args: vec![],
                inverted: false,
            }
        );
        assert_eq!(
            parse_classic("dist(1)  ").unwrap(),
            Description {
                name: "dist",
                args: vec!["1"],
                inverted: false,
            }
        );
        assert_eq!(
            parse_classic("dist( 1 .. 2 )").unwrap(),
            Description {
                name: "dist",
                args: vec!["1", "2"],
                inverted: false,
            }
        );
        assert_eq!(
            parse_classic("dist ( 1 .. 2 , 3 )").unwrap(),
            Description {
                name: "dist",
                args: vec!["1", "2", "3"],
                inverted: false,
            }
        );

        assert_eq!(
            parse_modern("dist:").unwrap(),
            Description {
                name: "dist",
                args: vec![],
                inverted: false,
            }
        );
        assert_eq!(
            parse_modern("dist:1").unwrap(),
            Description {
                name: "dist",
                args: vec!["1"],
                inverted: false,
            }
        );
        assert_eq!(
            parse_modern("dist:1..2").unwrap(),
            Description {
                name: "dist",
                args: vec!["1", "2"],
                inverted: false,
            }
        );
        assert_eq!(
            parse_modern("dist:1..2,3").unwrap(),
            Description {
                name: "dist",
                args: vec!["1", "2", "3"],
                inverted: false,
            }
        );

        assert_eq!(
            parse_modern("~ dist:1").unwrap(),
            Description {
                name: "dist",
                args: vec!["1"],
                inverted: true,
            }
        );

        assert!(parse_modern("dist").is_err()); // No argument list
        assert!(parse_modern("dist(1..2,3").is_err()); // Missing closing parenthesis
        assert!(parse_modern("dist(1,2)").is_err()); // Missing dots
        assert!(parse_classic("dist:1").is_err()); // Semicolon not supported by classic
    }

    #[test]
    fn test_parse_long_signed() {
        let goods: &[(&str, i64)] = &[
            ("123", 123),
            ("321", 321),
            ("12k", 12_000),
            ("12K", 12_000),
            ("34m", 34_000_000),
            ("34M", 34_000_000),
            ("56b", 56_000_000_000),
            ("56B", 56_000_000_000),
            ("-123", -123),
            ("-321", -321),
            ("-12k", -12_000),
            ("-12K", -12_000),
            ("-34m", -34_000_000),
            ("-34M", -34_000_000),
            ("-56b", -56_000_000_000),
            ("-56B", -56_000_000_000),
        ];

        for (s, expected) in goods {
            println!("Parsing: {}", s);
            let value: i64 = parse_long(s).unwrap();
            assert_eq!(value, *expected);
        }

        let bads: &[&str] = &[
            "abc",
            "0x123", // <- Only decimal numbers are supported
            "0b123",
            "0o123",
            "123x",
            "1 2 3",
            "999999999999999999999999999999999999999999999999999999",
            "99999999999b", // <- Will overflow after adjusting for the suffix
            &format!("{}", u64::MAX), // <- Out of range of i64, but in range of u64
        ];

        for s in bads {
            println!("Parsing: {}", s);
            parse_long::<i64>(s).unwrap_err();
        }
    }

    #[test]
    fn test_parse_long_unsigned() {
        let goods: &[(&str, u64)] = &[
            ("123", 123),
            ("321", 321),
            ("12k", 12_000),
            ("12K", 12_000),
            ("34m", 34_000_000),
            ("34M", 34_000_000),
            ("56b", 56_000_000_000),
            ("56B", 56_000_000_000),
            (&format!("{}", u64::MAX), u64::MAX),
        ];

        for (s, expected) in goods {
            println!("Parsing: {}", s);
            let value: u64 = parse_long(s).unwrap();
            assert_eq!(value, *expected);
        }

        let bads: &[&str] = &[
            "-123", // <- Negative numbers are not supported
            "abc",
            "0x123", // <- Only decimal numbers are supported
            "0b123",
            "0o123",
            "123x",
            "1 2 3",
            "999999999999999999999999999999999999999999999999999999",
            "99999999999b", // <- Will overflow after adjusting for the suffix
        ];

        for s in bads {
            println!("Parsing: {}", s);
            parse_long::<u64>(s).unwrap_err();
        }
    }
}
