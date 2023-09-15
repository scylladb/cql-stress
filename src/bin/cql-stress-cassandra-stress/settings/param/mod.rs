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

type ParamCell = Rc<RefCell<dyn ParamImpl>>;

pub trait ParamHandle {
    fn cell(&self) -> ParamCell;
}
