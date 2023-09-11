use std::{cell::RefCell, rc::Rc};

mod multi_param;
mod parser;
mod simple_param;
pub mod types;

use anyhow::Context;
use anyhow::Result;

pub use multi_param::MultiParamAcceptsArbitraryHandle;
pub use multi_param::MultiParamHandle;
pub use parser::ParamsParser;
pub use simple_param::SimpleParamHandle;

/// A specific implementation of the parameter.
pub trait ParamImpl {
    /// Parses the `arg_value'.
    /// Includes `param_name` for building error messages based on the context.
    fn parse(&mut self, param_name: &'static str, arg_value: &str) -> Result<()>;
    /// Ref: check `ParamsGroup`.
    /// Checking whether the group is satisfied happens right after all of the
    /// CLI arguments were successfully consumed. If the group is satisfied,
    /// it will mark all of its parameters as satisfied as well.
    /// Then, before returning any value, the parameter will check if its satisfied.
    /// If it's not, it will return `None`. Note that it's needed in case of parameters
    /// with default values that don't belong to the satisfied group - otherwise, they would return `Some(_)`.
    fn set_satisfied(&mut self);
    /// Prints the usage format of the parameter.
    fn print_usage(&self, param_name: &'static str);
    /// Prints short description of the parameter.
    fn print_desc(&self, param_name: &'static str, description: &'static str);
}

/// A simple wrapper for specific parameters implementations.
///
/// Represents the state and implements the logic that is shared
/// across all types of the parameters. It's introduced to prevent
/// copying boilerplate code such as getters for `required`.
///
/// It allows us to achieve two things which are really important to complex c-s parsing logic:
/// - composition -> traits cannot contain any member variables.
///   For example - `required` flag should be shared by all of the types of parameters -
///   - that's why we extract it to the type wrapping the specific parameter.
/// - accessing type specific methods by the user.
pub struct TypedParam<P: ParamImpl> {
    param: P,
    prefix: &'static str,
    desc: &'static str,
    default: Option<&'static str>,
    required: bool,
    supplied_by_user: bool,
    satisfied: bool,
}

impl<P: ParamImpl> TypedParam<P> {
    fn new(
        param: P,
        prefix: &'static str,
        desc: &'static str,
        default: Option<&'static str>,
        required: bool,
    ) -> Self {
        Self {
            param,
            prefix,
            desc,
            default,
            required,
            supplied_by_user: false,
            satisfied: false,
        }
    }
}

/// A trait representing logic of the generic parameter.
///
/// It is implemented by [TypedParam] generic types.
/// Introduced, so the [TypedParam]s can be used as trait objects (dyn GenericParam).
pub trait GenericParam {
    fn supplied_by_user(&self) -> bool;
    fn required(&self) -> bool;
    fn try_match(&self, arg: &str) -> bool;
    /// Ref: check [parser::ParamsGroup].
    /// Checking whether the group is satisfied happens right after all of the
    /// CLI arguments were successfully consumed. If the group is satisfied,
    /// it will mark all of its parameters as satisfied as well.
    /// Then, before returning any value, the parameter will check if its satisfied.
    /// If it's not, it will return `None`. Note that it's needed in case of parameters
    /// with default values that don't belong to the satisfied group - otherwise, they would return `Some(_)`.
    fn set_satisfied(&mut self);
    fn print_usage(&self);
    fn print_desc(&self);
    fn parse(&mut self, arg: &str) -> Result<()>;
}

impl<P: ParamImpl> GenericParam for TypedParam<P> {
    fn supplied_by_user(&self) -> bool {
        self.supplied_by_user
    }

    fn required(&self) -> bool {
        self.required
    }

    fn try_match(&self, arg: &str) -> bool {
        // Common logic for all types of parameters.
        arg.starts_with(self.prefix)
    }

    fn set_satisfied(&mut self) {
        self.param.set_satisfied()
    }

    fn print_usage(&self) {
        if !self.required {
            print!("[");
        }
        self.param.print_usage(self.prefix);
        if !self.required {
            print!("]");
        }
    }

    fn print_desc(&self) {
        self.param.print_desc(self.prefix, self.desc)
    }

    fn parse(&mut self, arg: &str) -> Result<()> {
        anyhow::ensure!(
            !self.supplied_by_user,
            "{} suboption has been specified more than once",
            self.prefix
        );
        self.supplied_by_user = true;
        let arg_val = &arg[self.prefix.len()..];
        self.param
            .parse(self.prefix, arg_val)
            .with_context(|| format!("Failed to parse parameter {}.", self.prefix))
    }
}

type ParamCell = Rc<RefCell<dyn GenericParam>>;

pub trait ParamHandle {
    fn cell(&self) -> ParamCell;
}
