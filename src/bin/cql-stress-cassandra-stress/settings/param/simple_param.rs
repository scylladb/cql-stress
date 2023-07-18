use std::{cell::RefCell, fmt, rc::Rc, str::FromStr};

use regex::Regex;

use super::{regex_is_empty, Param, ParamCell, ParamHandle, ParamMatchResult};

/// Abstraction of simple parameter which is of the following pattern:
/// <prefix><value_pattern>
///
/// For example `n=` is a simple parameter where
/// - prefix := "n="
/// - value_pattern := r"^[0-9]+[bmk]?$"
pub struct SimpleParam {
    value: Option<String>,
    prefix: &'static str,
    value_pattern: Regex,
    default: Option<&'static str>,
    desc: &'static str,
    required: bool,
    supplied_by_user: bool,
    satisfied: bool,
}

impl SimpleParam {
    pub fn new(
        prefix: &'static str,
        value_pattern: &'static str,
        default: Option<&'static str>,
        desc: &'static str,
        required: bool,
    ) -> Self {
        Self {
            value: default.map(|d| d.to_string()),
            prefix,
            value_pattern: Regex::new(value_pattern).unwrap(),
            default,
            desc,
            required,
            supplied_by_user: false,
            satisfied: false,
        }
    }

    /// Retrieves the value (if parsed successfully) and consumes the parameter.
    fn get(self) -> Option<String> {
        if !self.satisfied {
            return None;
        }
        self.value
    }

    fn is_bool_flag(&self) -> bool {
        regex_is_empty(&self.value_pattern)
    }
}

impl Param for SimpleParam {
    fn try_match(&self, arg: &str) -> ParamMatchResult {
        if !arg.starts_with(self.prefix) {
            return ParamMatchResult::NoMatch;
        }

        if self.supplied_by_user {
            return ParamMatchResult::Error(anyhow::anyhow!(
                "{} suboption has been specified more than once",
                self.prefix
            ));
        }

        let arg_val = &arg[self.prefix.len()..];
        if self.value_pattern.is_match(arg_val) {
            return ParamMatchResult::Match;
        }

        ParamMatchResult::Error(anyhow::anyhow!(
            "Invalid value {}; must patch pattern {}",
            arg_val,
            self.value_pattern.as_str()
        ))
    }

    fn parse(&mut self, arg: &str) {
        match self.try_match(arg) {
            ParamMatchResult::Match => {
                let arg_val = &arg[self.prefix.len()..];
                self.supplied_by_user = true;
                self.value = Some(arg_val.to_string());
            }
            _ => panic!("Cannot parse the parameter: {} with argument: {}. Make sure that Param::is_match returns `Match` before calling this method.", self.prefix, arg),
        }
    }

    fn supplied_by_user(&self) -> bool {
        self.supplied_by_user
    }

    fn required(&self) -> bool {
        self.required
    }

    fn set_satisfied(&mut self) {
        self.satisfied = true;
    }

    fn print_usage(&self) {
        if !self.required {
            print!("[");
        }
        print!("{}", self.prefix);
        if !self.is_bool_flag() {
            print!("?");
        }
        if !self.required {
            print!("]");
        }
    }

    fn print_desc(&self) {
        let mut desc = String::from(self.prefix);
        if !self.is_bool_flag() {
            desc.push('?');
        }
        if let Some(default) = self.default {
            desc += &format!(" (default={default})");
        }
        println!("{:<40} {}", desc, self.desc);
    }
}

pub struct SimpleParamHandle {
    cell: Rc<RefCell<SimpleParam>>,
}

impl SimpleParamHandle {
    pub fn new(cell: Rc<RefCell<SimpleParam>>) -> Self {
        Self { cell }
    }

    /// Retrieves the value from underlying parameter.
    /// Consumes both handle and underlying parameter.
    pub fn get(self) -> Option<String> {
        let param_name = self.cell.borrow().prefix;
        match Rc::try_unwrap(self.cell) {
            Ok(cell) => cell.into_inner().get(),
            Err(_) => panic!("Something holds the reference to `{param_name}` param cell. Make sure the parser is consumed with Parser::parse before calling this method."),
        }
    }

    /// Parses the param's String value to type T.
    /// Can cause panic.
    pub fn get_type<T>(self) -> Option<T>
    where
        T: FromStr,
        <T as FromStr>::Err: fmt::Debug,
    {
        self.get().map(|v| v.parse::<T>().unwrap())
    }

    pub fn supplied_by_user(&self) -> bool {
        self.cell.borrow().supplied_by_user()
    }
}

impl ParamHandle for SimpleParamHandle {
    fn cell(&self) -> ParamCell {
        Rc::clone(&self.cell) as ParamCell
    }
}
