//! Facilitates parsing flags in the format compatible with Go's "flag" package.
//!
//! Link to the Go package: https://pkg.go.dev/flag

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::rc::Rc;
use std::time::Duration;

use anyhow::Result;

pub trait GoValue: Sized + 'static {
    fn parse(s: &str) -> Result<Self>;
    fn to_string(&self) -> String;
    fn is_zero_value(&self) -> bool {
        false
    }
    fn is_bool_flag() -> bool {
        false
    }
    fn default_name() -> &'static str {
        "value"
    }
}

impl GoValue for bool {
    fn parse(s: &str) -> Result<Self> {
        super::strconv::parse_bool(s)
    }

    fn to_string(&self) -> String {
        if *self {
            "true".to_string()
        } else {
            "false".to_string()
        }
    }

    fn is_zero_value(&self) -> bool {
        self == &false
    }

    fn is_bool_flag() -> bool {
        true
    }

    fn default_name() -> &'static str {
        // Empty string is intended
        ""
    }
}

impl GoValue for i64 {
    fn parse(s: &str) -> Result<Self> {
        super::strconv::parse_int(s, 64)
    }

    fn to_string(&self) -> String {
        format!("{self}")
    }

    fn is_zero_value(&self) -> bool {
        self == &0
    }

    fn default_name() -> &'static str {
        "int"
    }
}

impl GoValue for u64 {
    fn parse(s: &str) -> Result<Self> {
        super::strconv::parse_uint(s, 64)
    }

    fn to_string(&self) -> String {
        format!("{self}")
    }

    fn is_zero_value(&self) -> bool {
        self == &0
    }

    fn default_name() -> &'static str {
        "uint"
    }
}

impl GoValue for String {
    fn parse(s: &str) -> Result<Self> {
        Ok(s.to_string())
    }

    fn to_string(&self) -> String {
        super::strconv::quote_string(self)
    }

    fn is_zero_value(&self) -> bool {
        self.is_empty()
    }

    fn default_name() -> &'static str {
        "string"
    }
}

impl GoValue for Duration {
    fn parse(s: &str) -> Result<Self> {
        super::strconv::parse_duration(s)
    }

    fn to_string(&self) -> String {
        super::strconv::format_duration(*self)
    }

    fn is_zero_value(&self) -> bool {
        self == &Duration::ZERO
    }

    fn default_name() -> &'static str {
        "duration"
    }
}

struct Flag {
    desc: &'static str,
    default: Option<String>,
    is_bool_flag: bool,
    default_name: &'static str,
    cell: Rc<dyn GenericFlagCell>,
}

impl Flag {
    // Detects a `quoted` name in the description and returns
    // a pair: the quoted name, and the description without the quote.
    // If there is no quoted name then an appropriate name will be deduced
    // based on the type.
    // TODO: This could be done during preparation?
    fn unquote_usage(&self) -> (&str, Cow<'_, str>) {
        // Try to extract the quoted name
        let parts: Vec<_> = self.desc.splitn(3, '`').collect();
        if let &[left, name, right] = parts.as_slice() {
            return (name, Cow::Owned(format!("{left}{name}{right}")));
        }

        // No explicit name, so use the type instead
        (self.default_name, Cow::Borrowed(self.desc))
    }
}

struct GoValueFlagCell<T: GoValue> {
    value: RefCell<Option<T>>,
}

trait GenericFlagCell {
    fn parse(&self, s: &str) -> Result<()>;
}

trait TypedFlagCell<T: GoValue>: GenericFlagCell {
    fn take(&self) -> Option<T>;
}

impl<T: GoValue> GenericFlagCell for GoValueFlagCell<T> {
    fn parse(&self, s: &str) -> Result<()> {
        let t = T::parse(s)?;
        *self.value.borrow_mut() = Some(t);
        Ok(())
    }
}

impl<T: GoValue> TypedFlagCell<T> for GoValueFlagCell<T> {
    fn take(&self) -> Option<T> {
        self.value.borrow_mut().take()
    }
}

/// Represents a handle to a value which will be parsed by Parser.
pub struct FlagValue<T: GoValue> {
    r: Rc<dyn TypedFlagCell<T>>,
}

impl<T: GoValue> FlagValue<T> {
    fn new(r: Rc<dyn TypedFlagCell<T>>) -> Self {
        Self { r }
    }

    /// Returns the value of the flag parsed by the associated Parser.
    /// If flags weren't parsed yet, this will be set to the flag's
    /// default value.
    pub fn get(self) -> T {
        // This object is the only external reference to this value
        // and we consume ourselves after that, so take + unwrap is okay
        self.r.take().unwrap()
    }
}

type FlagMap = HashMap<&'static str, Flag>;

/// Accumulates a description of flags and builds a parser
/// and a flag set description.
pub struct ParserBuilder {
    flags: FlagMap,
}

impl ParserBuilder {
    /// Creates an initially empty set of flags.
    pub fn new() -> Self {
        Self {
            flags: FlagMap::new(),
        }
    }

    /// Builds a parser and flag set description.
    pub fn build(self) -> (Parser, FlagSetDescription) {
        let flags = Rc::new(self.flags);
        let parser = Parser {
            flags: Rc::clone(&flags),
        };
        let desc = FlagSetDescription { flags };
        (parser, desc)
    }

    /// Defines a boolean flag.
    pub fn bool_var(
        &mut self,
        name: &'static str,
        default: bool,
        desc: &'static str,
    ) -> FlagValue<bool> {
        self.add_flag(name, default, desc)
    }

    /// Defines a string flag.
    pub fn string_var(
        &mut self,
        name: &'static str,
        default: impl ToString,
        desc: &'static str,
    ) -> FlagValue<String> {
        self.add_flag(name, default.to_string(), desc)
    }

    /// Defines a signed 64-bit integer flag.
    pub fn i64_var(
        &mut self,
        name: &'static str,
        default: i64,
        desc: &'static str,
    ) -> FlagValue<i64> {
        self.add_flag(name, default, desc)
    }

    /// Defines an unsigned 64-bit integer flag.
    pub fn u64_var(
        &mut self,
        name: &'static str,
        default: u64,
        desc: &'static str,
    ) -> FlagValue<u64> {
        self.add_flag(name, default, desc)
    }

    /// Defines a duration flag.
    pub fn duration_var(
        &mut self,
        name: &'static str,
        default: Duration,
        desc: &'static str,
    ) -> FlagValue<Duration> {
        self.add_flag(name, default, desc)
    }

    /// Defines a flag with custom type.
    pub fn var<T: GoValue>(
        &mut self,
        name: &'static str,
        default: T,
        desc: &'static str,
    ) -> FlagValue<T> {
        self.add_flag(name, default, desc)
    }

    fn add_flag<T: GoValue>(
        &mut self,
        name: &'static str,
        default: T,
        desc: &'static str,
    ) -> FlagValue<T> {
        if name.is_empty() {
            panic!("Flag name must not be empty");
        }
        if name.starts_with('-') {
            panic!("Flag name must not start with a dash");
        }
        if name.starts_with('=') {
            panic!("Flag name must not start with an equality sign");
        }

        let default_s = if !default.is_zero_value() {
            Some(default.to_string())
        } else {
            None
        };

        let cell = Rc::new(GoValueFlagCell {
            value: RefCell::new(Some(default)),
        });

        let flag = Flag {
            desc,
            default: default_s,
            is_bool_flag: T::is_bool_flag(),
            default_name: T::default_name(),
            cell: Rc::clone(&cell) as Rc<dyn GenericFlagCell>,
        };

        if self.flags.insert(name, flag).is_some() {
            panic!("Flag {name} was defined more than once");
        }

        FlagValue::new(cell)
    }
}

pub struct Parser {
    flags: Rc<FlagMap>,
}

impl Parser {
    /// Parses the configured flags.
    ///
    /// Each flag must have one of the following forms:
    /// -name=value
    /// -name value  (non-boolean flags only)
    /// -name        (boolean flags only)
    ///
    /// A flag may start with one or two dashes.
    ///
    /// A double dash ("--") in non-value position terminates the parsing process.
    ///
    /// When parsing completes, FlagValues associated with this Parser
    /// will have its inner values appropriately set.
    pub fn parse_args<I, S>(self, mut args: I) -> Result<()>
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
    {
        let mut parsed_flags = HashSet::new();

        while let Some(arg) = args.next() {
            let arg = arg.as_ref();

            // Double dash stops processing the flags
            if arg == "--" {
                break;
            }

            // Trim one or two dashes at the beginning
            let original_arg = arg;
            let arg = arg
                .strip_prefix("--")
                .or_else(|| arg.strip_prefix('-'))
                .ok_or_else(|| anyhow::anyhow!("Expected an option, but got {}", arg))?;

            anyhow::ensure!(
                !arg.is_empty() && !arg.starts_with('-') && !arg.starts_with('='),
                "Invalid flag parameter: {}",
                original_arg,
            );

            // Get the name of the flag, and - if it has form '-name=value' - its value
            let (name, value_after_eq) = match arg.split_once('=') {
                Some((name, value)) => (name, Some(value)),
                None => (arg, None),
            };

            // Ensure that the flag was not parsed already
            // TODO: Is this what golang really does?
            anyhow::ensure!(
                parsed_flags.insert(name.to_owned()),
                "The flag {} was provided twice",
                name,
            );

            // Get the flag object
            let flag = self
                .flags
                .get(&name)
                .ok_or_else(|| anyhow::anyhow!("Unknown flag: {name}"))?;

            match value_after_eq {
                // The current option had `-name=value` form, so we already have the value
                Some(value) => flag.cell.parse(value)?,

                // Special case for booleans - `-name` just means setting it to true
                None if flag.is_bool_flag => flag.cell.parse("1")?,

                // Otherwise, we must get the value from the next argument
                None => {
                    let arg = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("Value is missing for flag {name}"))?;
                    flag.cell.parse(arg.as_ref())?
                }
            };
        }

        Ok(())
    }
}

pub struct FlagSetDescription {
    flags: Rc<FlagMap>,
}

impl FlagSetDescription {
    /// Prints the help message with information about the flag usage.
    pub fn print_help(&self, write: &mut impl Write, program_name: &str) -> Result<()> {
        writeln!(write, "Usage of {program_name}:")?;
        let mut flag_names: Vec<&str> = self.flags.keys().copied().collect();
        flag_names.sort_unstable();

        for fname in flag_names {
            let flag = self.flags.get(&fname).unwrap();
            let mut s = String::new();
            s.push_str("  -");
            s.push_str(fname);

            let (name, usage) = flag.unquote_usage();
            if !name.is_empty() {
                s.push(' ');
                s.push_str(name);
            }

            if fname.len() == 1 {
                // Short name flag, print description in the same line
                s.push('\t');
            } else {
                // Insert newline and indent a bit more
                s.push_str("\n    \t");
            }

            // usage of "indented" was intended
            let indented_usage = usage.replace('\n', "\n    \t");
            s.push_str(&indented_usage);

            // The "isZeroValue" check is made while the flag is defined,
            // flag.default will just be None in this case
            if let Some(default) = &flag.default {
                s.push_str(" (default ");
                s.push_str(default);
                s.push(')');
            }

            writeln!(write, "{s}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_single_flag_parser<T, MkParser>(mkparser: MkParser) -> impl Fn(&[&str]) -> Result<T>
    where
        T: GoValue,
        MkParser: Fn(&mut ParserBuilder) -> FlagValue<T>,
    {
        move |args| {
            let mut set = ParserBuilder::new();
            let value = mkparser(&mut set);
            let (parser, _) = set.build();
            parser.parse_args(args.iter())?;
            Ok(value.get())
        }
    }

    #[test]
    fn test_string_var() {
        const DEFAULT_VALUE: &str = "<default value>";
        let parse =
            make_single_flag_parser(|set| set.string_var("var", DEFAULT_VALUE, "string flag"));

        // Successful cases
        assert_eq!(parse(&[]).unwrap(), DEFAULT_VALUE);
        assert_eq!(parse(&["-var=thing"]).unwrap(), "thing");
        assert_eq!(parse(&["--var=thing"]).unwrap(), "thing");
        assert_eq!(parse(&["-var", "thing"]).unwrap(), "thing");
        assert_eq!(parse(&["--var", "thing"]).unwrap(), "thing");

        // Invalid syntax for non-boolean flags
        assert!(parse(&["-var"]).is_err());
        assert!(parse(&["--var"]).is_err());
    }

    #[test]
    #[allow(clippy::bool_assert_comparison)]
    fn test_boolean_var() {
        const DEFAULT_VALUE: bool = false;
        let parse = make_single_flag_parser(|set| set.bool_var("var", DEFAULT_VALUE, "bool flag"));

        // Successful cases
        assert_eq!(parse(&[]).unwrap(), DEFAULT_VALUE);
        assert_eq!(parse(&["-var=true"]).unwrap(), true);
        assert_eq!(parse(&["-var=false"]).unwrap(), false);
        assert_eq!(parse(&["-var"]).unwrap(), true);

        // Invalid syntax for a boolean flag
        assert!(parse(&["-var=123"]).is_err());
    }

    #[test]
    fn test_i64_var() {
        const DEFAULT_VALUE: i64 = 0;
        let parse = make_single_flag_parser(|set| set.i64_var("var", DEFAULT_VALUE, "bool flag"));

        // Successful cases
        assert_eq!(parse(&[]).unwrap(), DEFAULT_VALUE);
        assert_eq!(parse(&["-var=123"]).unwrap(), 123);
        assert_eq!(parse(&["-var=+123"]).unwrap(), 123);
        assert_eq!(parse(&["-var=-123"]).unwrap(), -123);
        assert_eq!(parse(&["-var", "123"]).unwrap(), 123);
        assert_eq!(parse(&["-var", "+123"]).unwrap(), 123);
        assert_eq!(parse(&["-var", "-123"]).unwrap(), -123);

        assert_eq!(parse(&["-var", "0x123"]).unwrap(), 0x123);
        assert_eq!(parse(&["-var", "0b111"]).unwrap(), 0b111);
        assert_eq!(parse(&["-var", "0o123"]).unwrap(), 0o123);

        // Invalid cases for i64 flags
        assert!(parse(&["-var=999999999999999999999999999999999999"]).is_err());
        assert!(parse(&["-var=-999999999999999999999999999999999999"]).is_err());
        assert!(parse(&["-var=thing"]).is_err());
    }

    #[test]
    fn test_custom_var() {
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        enum V {
            One,
            Two,
            Three,
        }

        impl GoValue for V {
            fn parse(s: &str) -> Result<Self> {
                match s {
                    "one" => Ok(V::One),
                    "two" => Ok(V::Two),
                    "three" => Ok(V::Three),
                    _ => Err(anyhow::anyhow!("Wrong value of V: {s}")),
                }
            }
            fn to_string(&self) -> String {
                format!("{self:?}")
            }
        }

        let parse = make_single_flag_parser(|set| set.var("var", V::One, "V flag"));

        // Successful cases
        assert_eq!(parse(&[]).unwrap(), V::One);
        assert_eq!(parse(&["-var=two"]).unwrap(), V::Two);
        assert_eq!(parse(&["-var", "three"]).unwrap(), V::Three);

        // Invalid value
        assert!(parse(&["-var"]).is_err());
        assert!(parse(&["-var=four"]).is_err());
        assert!(parse(&["-var", "four"]).is_err());
    }

    #[test]
    fn test_multiple_flags() {
        #[derive(Debug, PartialEq, Eq, Default)]
        struct Flags {
            pub sflag: String,
            pub iflag: i64,
            pub bflag: bool,
        }

        let parse = |args: &[&str]| -> Result<Flags> {
            let mut set = ParserBuilder::new();
            let sflag = set.string_var("sflag", "", "string flag");
            let iflag = set.i64_var("iflag", 0, "i64 flag");
            let bflag = set.bool_var("bflag", false, "bool flag");

            let (parser, _) = set.build();
            parser.parse_args(args.iter())?;

            let flags = Flags {
                sflag: sflag.get(),
                iflag: iflag.get(),
                bflag: bflag.get(),
            };
            Ok(flags)
        };

        assert_eq!(parse(&[]).unwrap(), Flags::default());
        assert_eq!(
            parse(&["-sflag=thing"]).unwrap(),
            Flags {
                sflag: "thing".to_string(),
                ..Flags::default()
            }
        );
        assert_eq!(
            parse(&["-iflag", "-123"]).unwrap(),
            Flags {
                iflag: -123,
                ..Flags::default()
            }
        );
        assert_eq!(
            parse(&["-sflag", "thing", "-iflag", "-123"]).unwrap(),
            Flags {
                sflag: "thing".to_string(),
                iflag: -123,
                ..Flags::default()
            }
        );
        assert_eq!(
            parse(&["-iflag", "-123", "-sflag", "thing"]).unwrap(),
            Flags {
                sflag: "thing".to_string(),
                iflag: -123,
                ..Flags::default()
            }
        );
        assert_eq!(
            parse(&["-iflag", "-123", "-bflag", "-sflag", "thing"]).unwrap(),
            Flags {
                sflag: "thing".to_string(),
                iflag: -123,
                bflag: true,
            }
        );
    }

    #[test]
    #[allow(clippy::bool_assert_comparison)]
    fn test_stop_parsing_after_double_dash() {
        let parse = make_single_flag_parser(|set| set.bool_var("var", false, "bool flag"));
        assert_eq!(parse(&["--", "-var=true"]).unwrap(), false);
    }
}
