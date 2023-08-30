use std::{cell::RefCell, rc::Rc};

use anyhow::{Context, Result};

use super::{types::Parsable, Param, ParamCell, ParamHandle, ParamMatchResult};

/// Abstraction of simple parameter which is of the following pattern:
/// <prefix><value_pattern>
///
/// Parameter is aware of the prefix, and holds it.
/// However, parsing of the parameter's value is delegated to the type
/// that implements [super::types::Parsable].
///
/// For example `n=` is a simple parameter where
/// - prefix := "n="
/// - value_pattern := r"^[0-9]+[bmk]?$". It's provided by [super::types::Count].
pub struct SimpleParam<T: Parsable> {
    value: Option<T::Parsed>,
    prefix: &'static str,
    default: Option<&'static str>,
    desc: &'static str,
    required: bool,
    supplied_by_user: bool,
    satisfied: bool,
}

impl<T: Parsable> SimpleParam<T> {
    pub fn new(
        prefix: &'static str,
        default: Option<&'static str>,
        desc: &'static str,
        required: bool,
    ) -> Self {
        Self {
            // SAFETY: The default value must be successfully parsed.
            value: default.map(|d| T::parse(d).unwrap()),
            prefix,
            default,
            desc,
            required,
            supplied_by_user: false,
            satisfied: false,
        }
    }

    /// Retrieves the value (if parsed successfully) and consumes the parameter.
    fn get(self) -> Option<T::Parsed> {
        if !self.satisfied {
            return None;
        }
        self.value
    }
}

impl<T: Parsable> Param for SimpleParam<T> {
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

        ParamMatchResult::Match
    }

    fn parse(&mut self, arg: &str) -> Result<()> {
        let arg_val = &arg[self.prefix.len()..];
        self.supplied_by_user = true;
        self.value = Some(
            T::parse(arg_val)
                .with_context(|| format!("Failed to parse parameter {}.", self.prefix))?,
        );

        Ok(())
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
        if !T::is_bool() {
            print!("?");
        }
        if !self.required {
            print!("]");
        }
    }

    fn print_desc(&self) {
        let mut desc = String::from(self.prefix);
        if !T::is_bool() {
            desc.push('?');
        }
        if let Some(default) = self.default {
            desc += &format!(" (default={default})");
        }
        println!("{:<40} {}", desc, self.desc);
    }
}

pub struct SimpleParamHandle<T: Parsable> {
    cell: Rc<RefCell<SimpleParam<T>>>,
}

impl<T: Parsable> SimpleParamHandle<T> {
    pub fn new(cell: Rc<RefCell<SimpleParam<T>>>) -> Self {
        Self { cell }
    }

    /// Retrieves the value from underlying parameter.
    /// Consumes both handle and underlying parameter.
    pub fn get(self) -> Option<T::Parsed> {
        let param_name = self.cell.borrow().prefix;
        match Rc::try_unwrap(self.cell) {
            Ok(cell) => cell.into_inner().get(),
            Err(_) => panic!("Something holds the reference to `{param_name}` param cell. Make sure the parser is consumed with Parser::parse before calling this method."),
        }
    }
}

impl<T: Parsable + 'static> ParamHandle for SimpleParamHandle<T> {
    fn cell(&self) -> ParamCell {
        Rc::clone(&self.cell) as ParamCell
    }
}
