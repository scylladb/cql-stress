use anyhow::Result;

use crate::{
    java_generate::distribution::DistributionFactory,
    settings::{
        param::{types::CommaDelimitedList, ParamsParser, SimpleParamHandle},
        ParsePayload,
    },
};

pub struct ColumnOption {
    pub columns: Vec<String>,
    pub size_distribution: Box<dyn DistributionFactory>,
}

impl ColumnOption {
    pub const CLI_STRING: &str = "-col";

    pub fn description() -> &'static str {
        "Column details such as size distribution, names"
    }

    pub fn parse(cl_args: &mut ParsePayload) -> Result<Self> {
        let params = cl_args.remove(Self::CLI_STRING).unwrap_or_default();
        let (parser, handles) = prepare_parser();
        parser.parse(params)?;
        Ok(Self::from_handles(handles))
    }

    pub fn print_help() {
        let (parser, _) = prepare_parser();
        parser.print_help();
    }

    pub fn print_settings(&self) {
        println!("Column:");
        println!("  Column names: {:?}", self.columns);
        println!("  Size distribution: {}", self.size_distribution);
    }

    fn from_handles(handles: ColumnParamHandles) -> Self {
        let names = handles.names.get();
        let columns_count = handles.columns_count.get();
        let size_distribution = handles.size_distribution.get().unwrap();

        let columns = match names {
            Some(names) => names,
            None => (0..columns_count.unwrap())
                .map(|n| format!("C{n}"))
                .collect(),
        };

        Self {
            columns,
            size_distribution,
        }
    }
}

struct ColumnParamHandles {
    names: SimpleParamHandle<CommaDelimitedList>,
    columns_count: SimpleParamHandle<u64>,
    size_distribution: SimpleParamHandle<Box<dyn DistributionFactory>>,
}

fn prepare_parser() -> (ParamsParser, ColumnParamHandles) {
    let mut parser = ParamsParser::new(ColumnOption::CLI_STRING);

    let names = parser.simple_param("names=", None, "Column names", true);
    let columns_count = parser.simple_param("n=", Some("5"), "Number of columns", false);
    let size_distribution =
        parser.distribution_param("size=", Some("fixed(34)"), "Cell size distribution", false);

    // $ ./cassandra-stress help -col
    // Usage: -col [n=?] [size=DIST(?)]
    //  OR
    // Usage: -col names=? [size=DIST(?)]
    parser.group(&[&names, &size_distribution]);
    parser.group(&[&columns_count, &size_distribution]);

    (
        parser,
        ColumnParamHandles {
            names,
            columns_count,
            size_distribution,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::ColumnOption;

    use super::prepare_parser;

    #[test]
    fn col_default_params_test() {
        let args = vec![];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = ColumnOption::from_handles(handles);
        assert_eq!(&["C0", "C1", "C2", "C3", "C4"], params.columns.as_slice());
    }

    #[test]
    fn col_names_params_test() {
        let args = vec!["names=foo,bar,baz"];
        let (parser, handles) = prepare_parser();

        assert!(parser.parse(args).is_ok());

        let params = ColumnOption::from_handles(handles);
        assert_eq!(&["foo", "bar", "baz"], params.columns.as_slice());
    }

    #[test]
    fn col_bad_params_test() {
        let args = vec!["names=foo,bar,baz", "n=10"];
        let (parser, _) = prepare_parser();

        assert!(parser.parse(args).is_err());
    }
}
