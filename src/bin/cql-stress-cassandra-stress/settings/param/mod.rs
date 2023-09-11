use std::{cell::RefCell, rc::Rc};

mod multi_param;
mod parser;
mod simple_param;
pub mod types;

use anyhow::Result;

pub use multi_param::MultiParamAcceptsArbitraryHandle;
pub use multi_param::MultiParamHandle;
pub use parser::ParamsParser;
pub use simple_param::SimpleParamHandle;

/// A specific implementation of the parameter.
pub trait ParamImpl {
    /// Checks whether `arg` matches parameter's prefix.
    fn try_match(&self, arg: &str) -> bool;
    /// Parses the `arg` value.
    fn parse(&mut self, arg: &str) -> Result<()>;
    /// Tells whether the parameter was parsed with the user-provided argument.
    fn supplied_by_user(&self) -> bool;
    fn required(&self) -> bool;
    /// Ref: check `ParamsGroup`.
    /// Checking whether the group is satisfied happens right after all of the
    /// CLI arguments were successfully consumed. If the group is satisfied,
    /// it will mark all of its parameters as satisfied as well.
    /// Then, before returning any value, the parameter will check if its satisfied.
    /// If it's not, it will return `None`. Note that it's needed in case of parameters
    /// with default values that don't belong to the satisfied group - otherwise, they would return `Some(_)`.
    fn set_satisfied(&mut self);
    /// Prints the usage format of the parameter.
    fn print_usage(&self);
    /// Prints short description of the parameter.
    fn print_desc(&self);
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
        self.param.supplied_by_user()
    }

    fn required(&self) -> bool {
        self.param.required()
    }

    fn try_match(&self, arg: &str) -> bool {
        self.param.try_match(arg)
    }

    fn set_satisfied(&mut self) {
        self.param.set_satisfied()
    }

    fn print_usage(&self) {
        self.param.print_usage()
    }

    fn print_desc(&self) {
        self.param.print_desc()
    }

    fn parse(&mut self, arg: &str) -> Result<()> {
        self.param.parse(arg)
    }
}

type ParamCell = Rc<RefCell<dyn GenericParam>>;

pub trait ParamHandle {
    fn cell(&self) -> ParamCell;
}
