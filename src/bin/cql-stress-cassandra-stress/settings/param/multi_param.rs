use std::{cell::RefCell, collections::HashMap, rc::Rc};

use anyhow::Result;
use regex::Regex;

use super::{ParamCell, ParamHandle, ParamImpl, TypedParam};

lazy_static! {
    // The arbitrary parameters should match pattern `key=value`.
    static ref ARBITRARY_PARAM: Regex = Regex::new(r"^([^=]+)=([^=]+)$").unwrap();
}

/// Multiparameters may or may not accept arbitrary parameters.
/// That's why we introduce the trait responsible for accepting such parameters.
/// [MultiParam] is generic over the types that implement this trait.
/// See [AcceptsArbitraryParams] and [RejectsArbitraryParams].
pub trait ArbitraryParamsAcceptance: Sized + Default {
    fn accepts_arbitrary(&self) -> bool;
    fn try_parse_arbitrary(&mut self, param_name: &str, arg: &str) -> Result<()>;
}

/// [MultiParam<AcceptsArbitraryParams>] will accept all of the arbitrary parameters.
#[derive(Default)]
pub struct AcceptsArbitraryParams {
    map: HashMap<String, String>,
}

impl ArbitraryParamsAcceptance for AcceptsArbitraryParams {
    fn accepts_arbitrary(&self) -> bool {
        true
    }

    fn try_parse_arbitrary(&mut self, param_name: &str, arg: &str) -> Result<()> {
        // Ensure that argument matches pattern "key=value"
        anyhow::ensure!(
            ARBITRARY_PARAM.is_match(arg),
            "Invalid '{}' specification: '{}'",
            param_name,
            arg
        );

        let (key, val) = {
            let mut split = arg.split('=');
            let key = split.next();
            let val = split.next();
            match (key, val, split.next()) {
                (Some(key), Some(val), None) => (key, val),
                _ => anyhow::bail!("Invalid arbitrary parameter: {}", arg),
            }
        };

        anyhow::ensure!(
            !self.map.contains_key(key),
            "{} suboption has been specified more than once",
            key
        );
        self.map.insert(key.to_owned(), val.to_owned());

        Ok(())
    }
}

/// [MultiParam<RejectsArbitraryParams>] rejects all arbitrary params by returning an error.
#[derive(Default)]
pub struct RejectsArbitraryParams;

impl ArbitraryParamsAcceptance for RejectsArbitraryParams {
    fn accepts_arbitrary(&self) -> bool {
        false
    }

    fn try_parse_arbitrary(&mut self, param_name: &str, arg: &str) -> Result<()> {
        Err(anyhow::anyhow!(
            "Cannot accept parameter {}. {} command/option doesn't accept arbitrary parameters.",
            arg,
            param_name
        ))
    }
}

/// Representation of complex parameter - so called multiparameters.
/// Multiparams have some predefined subparameters (subparams field)
/// as well as (if applicable) arbitrary parameters (arbitrary_params field).
///
/// For example take `replication` parameter of `-schema` option:
/// The help message produces:
///
/// replication([strategy=?][factor=?][<option 1..N>=?]): Define the replication strategy and any parameters
///    strategy=? (default=org.apache.cassandra.locator.SimpleStrategy) The replication strategy to use
///    factor=? (default=1)                     The number of replicas
///
/// So in this case replication parameter accepts two (non-required) predefined parameters: `strategy` and `factor`.
/// It also accepts arbitrary parameters (denoted by `[<option 1..N>=?]`).
///
/// This means that parser should accept an exemplary input:
/// replication(foo=bar,factor=3,key=value)
///
/// The multiparameter will delegate parsing of `factor=3` part to its predefined subparameter.
/// `foo=bar` and `key=value` will be stored in the map of arbitrary parameters.
pub struct MultiParam<A: ArbitraryParamsAcceptance> {
    // Pre-defined parameters.
    // User can access them via their corresponding handles.
    subparams: Vec<ParamCell>,
    // Arbitrary parameters of the `key=value` form.
    arbitrary_params: A,
}

impl MultiParam<AcceptsArbitraryParams> {
    /// Retrieves arbitrary subparameters (if parsed successfully) and consumes the parameter.
    pub fn get_arbitrary(self) -> HashMap<String, String> {
        self.arbitrary_params.map
    }
}

impl<A: ArbitraryParamsAcceptance> MultiParam<A> {
    pub fn new_wrapped(
        prefix: &'static str,
        subparams: Vec<ParamCell>,
        desc: &'static str,
        required: bool,
    ) -> TypedParam<Self> {
        let param = Self {
            subparams,
            arbitrary_params: Default::default(),
        };

        TypedParam::new(param, prefix, desc, None, required)
    }

    fn accepts_arbitrary(&self) -> bool {
        self.arbitrary_params.accepts_arbitrary()
    }

    /// Tries to parse one of the predefined multiparameters.
    /// Returns Some(parsing_result) if one of the parameters matched the argument,
    /// and None otherwise.
    fn try_parse_predefined(&self, arg: &str) -> Option<Result<()>> {
        for param in self.subparams.iter() {
            let mut borrowed = param.borrow_mut();
            if borrowed.try_match(arg) {
                return Some(borrowed.parse(arg));
            }
        }

        None
    }
}

impl<A: ArbitraryParamsAcceptance> ParamImpl for MultiParam<A> {
    fn parse(&mut self, param_name: &'static str, arg_value: &str) -> Result<()> {
        // Remove wrapping parenthesis.
        let arg = {
            let mut chars = arg_value.chars();
            anyhow::ensure!(
                chars.next() == Some('(') && chars.next_back() == Some(')'),
                "Invalid '{}' specification: {}",
                param_name,
                arg_value
            );
            chars.as_str()
        };

        // Iterate over comma-delimited sub-parameters.
        for subparam in arg.split(',') {
            // Check if the argument matches on of the predefined subparameters.
            match self.try_parse_predefined(subparam) {
                // Parsing error - return it.
                Some(e @ Err(_)) => return e,
                // Parsing successful, move on to the next parameter.
                Some(Ok(())) => continue,
                // None of the predefined parameters matched, try parsing as arbitrary.
                None => (),
            }

            // If the argument didn't match any of the prefefined sub-parameters,
            // try to parse it as an arbitrary parameter (if applicable).
            self.arbitrary_params
                .try_parse_arbitrary(param_name, subparam)?;
        }

        Ok(())
    }

    fn set_subparams_satisfied(&mut self) {
        for param in self.subparams.iter() {
            param.borrow_mut().set_satisfied();
        }

        // Clear the subparameters so the user can consume them via corresponding handles.
        // Otherwise, retrieving the value by the user would cause panic.
        // Note that SimpleParamHandle::get(), as well as MultiParamHandle::get_arbitrary()
        // use [std::cell::RefCell::try_unwrap] method (and panic on error), since these methods
        // consume both - the handle and the parameter referenced by the handle.
        self.subparams.clear();
    }

    fn print_usage(&self, param_name: &'static str) {
        print!("{}(?)", param_name)
    }

    fn print_desc(
        &self,
        param_name: &'static str,
        description: &'static str,
        _default_value: Option<&str>,
    ) {
        print!("{}(", param_name);
        for param in self.subparams.iter() {
            param.borrow().print_usage();
        }
        if self.accepts_arbitrary() {
            print!("[<option 1..N>=?]");
        }
        println!("): {}", description);
        for param in self.subparams.iter() {
            print!("      ");
            param.borrow().print_desc();
        }
    }
}

impl TypedParam<MultiParam<AcceptsArbitraryParams>> {
    fn get_arbitrary(self) -> Option<HashMap<String, String>> {
        self.satisfied.then_some(self.param.get_arbitrary())
    }
}

pub struct MultiParamHandle<A: ArbitraryParamsAcceptance> {
    cell: Rc<RefCell<TypedParam<MultiParam<A>>>>,
}

pub type MultiParamAcceptsArbitraryHandle = MultiParamHandle<AcceptsArbitraryParams>;

impl MultiParamAcceptsArbitraryHandle {
    pub fn get_arbitrary(self) -> Option<HashMap<String, String>> {
        let param_name = self.cell.borrow().prefix;
        match Rc::try_unwrap(self.cell) {
            Ok(cell) => cell.into_inner().get_arbitrary(),
            Err(_) => panic!("Something holds the reference to `{param_name}` param cell. Make sure the parser is consumed with Parser::parse before calling this method."),
        }
    }
}

impl<A: ArbitraryParamsAcceptance> MultiParamHandle<A> {
    pub fn new(cell: Rc<RefCell<TypedParam<MultiParam<A>>>>) -> Self {
        Self { cell }
    }
}

impl<A: ArbitraryParamsAcceptance + 'static> ParamHandle for MultiParamHandle<A> {
    fn cell(&self) -> ParamCell {
        Rc::clone(&self.cell) as ParamCell
    }
}

#[cfg(test)]
mod tests {
    use crate::settings::param::GenericParam;

    use super::MultiParam;

    #[test]
    fn multi_param_arbitrary_test() {
        let mut multi_param =
            MultiParam::new_wrapped("replication", Vec::new(), "description", false);

        assert!(multi_param
            .parse("replication(foo=bar,key=value,gear=five)")
            .is_ok());
        multi_param.set_satisfied();

        let parsed = multi_param.get_arbitrary().unwrap();
        assert_eq!(&String::from("bar"), parsed.get("foo").unwrap());
        assert_eq!(&String::from("value"), parsed.get("key").unwrap());
        assert_eq!(&String::from("five"), parsed.get("gear").unwrap());
    }
}
