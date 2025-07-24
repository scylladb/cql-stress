use std::{cell::RefCell, rc::Rc};

use anyhow::Result;

use super::{types::Parsable, ParamCell, ParamHandle, ParamImpl, TypedParam};

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
    additional_desc: Option<String>,
}

impl<T: Parsable> SimpleParam<T> {
    pub fn new_wrapped(
        prefix: &'static str,
        default: Option<&str>,
        desc: &'static str,
        additional_desc: Option<&str>,
        required: bool,
    ) -> TypedParam<Self> {
        let param = Self {
            // SAFETY: The default value must be successfully parsed.
            value: default.map(|d| T::parse(d).unwrap()),
            additional_desc: additional_desc.map(|desc| desc.to_owned()),
        };

        TypedParam::new(param, prefix, desc, default, required)
    }

    /// Retrieves the value (if parsed successfully) and consumes the parameter.
    fn get(self) -> Option<T::Parsed> {
        self.value
    }
}

impl<T: Parsable> ParamImpl for SimpleParam<T> {
    fn parse(&mut self, _param_name: &'static str, arg_value: &str) -> Result<()> {
        self.value = Some(T::parse(arg_value)?);
        Ok(())
    }

    fn print_usage(&self, param_name: &'static str) {
        print!("{param_name}");
        if !T::is_bool() {
            print!("?");
        }
    }

    fn print_desc(
        &self,
        param_name: &'static str,
        description: &'static str,
        default_value: Option<&str>,
    ) {
        let mut usage = String::from(param_name);
        if !T::is_bool() {
            usage.push('?');
        }
        if let Some(default) = default_value {
            usage += &format!(" (default={default})");
        }
        println!("{usage:<40} {description}");
        if let Some(additional_description) = &self.additional_desc {
            println!("{additional_description}")
        }
    }
}

impl<T: Parsable> TypedParam<SimpleParam<T>> {
    fn get(self) -> Option<T::Parsed> {
        self.satisfied.then_some(self.param.get()?)
    }
}

pub struct SimpleParamHandle<T: Parsable> {
    cell: Rc<RefCell<TypedParam<SimpleParam<T>>>>,
}

impl<T: Parsable> Clone for SimpleParamHandle<T> {
    fn clone(&self) -> Self {
        Self {
            cell: Rc::clone(&self.cell),
        }
    }
}

impl<T: Parsable> SimpleParamHandle<T> {
    pub fn new(cell: Rc<RefCell<TypedParam<SimpleParam<T>>>>) -> Self {
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
