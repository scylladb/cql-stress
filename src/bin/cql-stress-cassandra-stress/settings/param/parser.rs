use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

use super::{
    multi_param::{ArbitraryParamsAcceptance, MultiParam},
    simple_param::{SimpleParam, SimpleParamHandle},
    types::Parsable,
    MultiParamHandle, ParamCell, ParamHandle,
};

/// Some of the parameters are mutually exclusive. For example, we can't do
/// `./cassandra-stress read n=10 n<20`.
/// That's why we arrange the parameters in so called groups.
///
/// Given the output of `./cassandra-stress help read`:
///
/// Usage: read [err<?] [n>?] [n<?] [no-warmup] [truncate=?] [cl=?] [serial-cl=?]
///  OR
/// Usage: read n=? [no-warmup] [truncate=?] [cl=?] [serial-cl=?]
///  OR
/// Usage: read duration=? [no-warmup] [truncate=?] [cl=?] [serial-cl=?]
///
/// We see that there are 3 groups of the parameters - a group can be identified on whether
/// `n=` or `duration=` parameter has been defined.
struct ParamsGroup {
    params: Vec<ParamCell>,
}

impl ParamsGroup {
    fn new(params: Vec<ParamCell>) -> Self {
        Self { params }
    }

    /// Tells whether the group is satisfied with received arguments.
    /// The group is satisfied \iff all of the following requirements are satisfied:
    ///   - number of consumed parameters by the group is equal to the number of CLI arguments
    ///   - every required parameter has been provided by the user
    fn satisfied(&self, args_size: usize) -> bool {
        let consumed_count = self
            .params
            .iter()
            .filter(|p| p.borrow().supplied_by_user())
            .count();
        if consumed_count != args_size {
            return false;
        }
        if self
            .params
            .iter()
            .any(|param| param.borrow().required() && !param.borrow().supplied_by_user())
        {
            return false;
        }
        true
    }

    fn mark_params_as_satisfied(&self) {
        // The group is satisfied - it means that the parameters of this group
        // were successfully parsed and will be returned to the user.
        for param in self.params.iter() {
            param.borrow_mut().set_satisfied();
        }
    }

    fn print_help(&self) {
        let params_size = self.params.len();
        for (i, param) in self.params.iter().enumerate() {
            param.borrow().print_usage();
            if i < params_size - 1 {
                print!(" ");
            }
        }
    }
}

/// Parser lets the user define the parameters (see trait [super::Param]).
/// The parser registers such parameter internally and returns the handle to the user.
/// Once the user calls `Parser::parse` (which consumes the parser), the values of
/// the parsed parameters can be retrieved using previously created handles.
pub struct ParamsParser {
    command_name: String,
    params: Vec<ParamCell>,
    groups: Vec<ParamsGroup>,
}

impl ParamsParser {
    pub fn new(command_name: &str) -> Self {
        Self {
            command_name: command_name.to_owned(),
            params: Vec::new(),
            groups: Vec::new(),
        }
    }

    /// Registers the simple parameter provided by the user and returns the handle.
    /// `value_pattern` has to be a regular expression, otherwise we panic.
    pub fn simple_param<T: Parsable + 'static>(
        &mut self,
        prefix: &'static str,
        default: Option<&'static str>,
        desc: &'static str,
        required: bool,
    ) -> SimpleParamHandle<T> {
        let param = Rc::new(RefCell::new(SimpleParam::new_wrapped(
            prefix, default, desc, None, required,
        )));

        self.params.push(Rc::clone(&param) as ParamCell);
        SimpleParamHandle::new(param)
    }

    /// A sub-parameter of some complex parameter e.g. `MultiParam`.
    /// In contrast to [simple_param] - parser won't add the param to its vector.
    /// This results in the owner of the subparameter
    /// being responsible for displaying the subparameter's help message.
    pub fn simple_subparam<T: Parsable + 'static>(
        &mut self,
        prefix: &'static str,
        default: Option<&'static str>,
        desc: &'static str,
        required: bool,
    ) -> SimpleParamHandle<T> {
        let param = Rc::new(RefCell::new(SimpleParam::new_wrapped(
            prefix, default, desc, None, required,
        )));

        SimpleParamHandle::new(param)
    }

    /// Registers the multi parameter provided by the user and returns the handle.
    /// `subparams` should be created via `*_subparam` parser's API.
    pub fn multi_param<A: ArbitraryParamsAcceptance + 'static>(
        &mut self,
        prefix: &'static str,
        subparams: &[&dyn ParamHandle],
        desc: &'static str,
        required: bool,
    ) -> MultiParamHandle<A> {
        let param = Rc::new(RefCell::new(MultiParam::new_wrapped(
            prefix,
            subparams.iter().map(|handle| handle.cell()).collect(),
            desc,
            required,
        )));

        self.params.push(Rc::clone(&param) as ParamCell);
        MultiParamHandle::new(param)
    }

    /// Creates a new group of the parameters.
    pub fn group(&mut self, params: &[&dyn ParamHandle]) {
        self.groups.push(ParamsGroup::new(
            params.iter().map(|handle| handle.cell()).collect(),
        ))
    }

    // Consume the parser during parsing.
    pub fn parse(mut self, args: Vec<&str>) -> Result<()> {
        if self.groups.is_empty() {
            // User didn't specify any groups. Treat all parameters as a single group.
            self.groups.push(ParamsGroup::new(self.params.clone()));
        }

        let args_size = args.len();
        for arg in args {
            let mut consumed = false;
            for param in self.params.iter() {
                let mut borrowed = param.borrow_mut();
                if borrowed.try_match(arg) {
                    borrowed.parse(arg)?;
                    consumed = true;
                    break;
                }
            }

            anyhow::ensure!(consumed, "Invalid parameter {}", arg);
        }

        // Find satisfied group. If found, mark its parameters as satisfied as well.
        if let Some(satisfied_group) = self.groups.iter().find(|g| g.satisfied(args_size)) {
            satisfied_group.mark_params_as_satisfied();
            return Ok(());
        }

        Err(anyhow::anyhow!(
            "Invalid {} parameters provided, see `help {}` for valid parameters",
            self.command_name.to_uppercase(),
            self.command_name
        ))
    }

    pub fn print_help(&self) {
        let groups_size = self.groups.len();
        println!();
        for (i, group) in self.groups.iter().enumerate() {
            print!("Usage: {} ", self.command_name);
            group.print_help();
            if i < groups_size - 1 {
                print!("\n OR");
            }
            println!();
        }
        println!();

        for param in self.params.iter() {
            print!("  ");
            param.borrow().print_desc();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::settings::param::SimpleParamHandle;

    use super::ParamsParser;

    struct TestHandles {
        count: SimpleParamHandle<u64>,
        foo: SimpleParamHandle<bool>,
        duration: SimpleParamHandle<Duration>,
    }

    fn prepare_parser() -> (ParamsParser, TestHandles) {
        // Create a parser.
        let mut parser = ParamsParser::new("some_parser");

        // Define 3 parameters and get their corresponding handles.
        let count = parser.simple_param("count=", None, "this is count", true);

        // Parameters with empty value_pattern (regex) are boolean flags.
        let foo = parser.simple_param("foo", None, "this is foo", false);
        let duration = parser.simple_param("duration=", Some("10s"), "this is duration", false);

        // Group the parameters. Meaning that if a user defined,
        // for example `count=` and `duration=` at the same time, the parsing should fail.
        parser.group(&[&count, &foo]);
        parser.group(&[&duration]);

        (
            parser,
            TestHandles {
                count,
                foo,
                duration,
            },
        )
    }

    #[test]
    fn parser_success_test() {
        let args = vec!["count=100", "foo"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        // We can now retrieve the parsed values from the handles.
        assert_eq!(Some(100), handles.count.get());
        assert!(handles.foo.get().is_some());

        // Even though `duration` has some default value, it doesn't belong
        // to the same group as `count`. This is why we get `None`.
        assert_eq!(None, handles.duration.get());
    }

    #[test]
    fn parser_fail_test() {
        let args = vec!["count=100", "duration=20s"];

        // We discard the handles, since the parsing will fail anyway.
        let (parser, _) = prepare_parser();

        // It fails because `count` and `duration` are from different groups.
        assert!(parser.parse(args).is_err());
    }
}
