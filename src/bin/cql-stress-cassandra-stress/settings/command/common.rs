use crate::settings::{
    param::{
        types::{Count, Parsable, UnitInterval},
        ParamHandle, ParamsParser, SimpleParamHandle,
    },
    ParsePayload,
};
use anyhow::{Context, Result};
use scylla::statement::{Consistency, SerialConsistency};
use std::{num::NonZeroU32, str::FromStr, time::Duration};
use strum::IntoEnumIterator;
use strum_macros::{AsRefStr, EnumIter, EnumString};

use super::{Command, CommandParams};

#[derive(Clone, Debug, PartialEq)]
pub struct Uncertainty {
    pub target_uncertainty: f64,
    pub min_uncertainty_measurements: u64,
    pub max_uncertainty_measurements: u64,
}

impl Uncertainty {
    pub fn new(
        target_uncertainty: f64,
        min_uncertainty_measurements: u64,
        max_uncertainty_measurements: u64,
    ) -> Self {
        Self {
            target_uncertainty,
            min_uncertainty_measurements,
            max_uncertainty_measurements,
        }
    }

    pub fn print_settings(&self) {
        println!("  Target Uncertainty: {}", self.target_uncertainty);
        println!(
            "  Minimum Uncertainty Measurements: {}",
            self.min_uncertainty_measurements
        );
        println!(
            "  Maximum Uncertainty Measurements: {}",
            self.max_uncertainty_measurements
        );
    }
}

#[derive(Clone, Debug, PartialEq, Eq, AsRefStr, EnumString, EnumIter)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[strum(ascii_case_insensitive)]
pub enum Truncate {
    Never,
    Once,
    Always,
}

impl Truncate {
    fn show(&self) -> &str {
        self.as_ref()
    }
}

impl Parsable for Truncate {
    type Parsed = Truncate;

    fn parse(truncate: &str) -> Result<Self::Parsed> {
        let create_err_msg = || {
            let concat = Self::iter()
                .map(|tr| tr.show().to_owned())
                .collect::<Vec<String>>()
                .join("|");

            format!(
                "Invalid truncate type: {}. Must be one of: {}",
                truncate, concat,
            )
        };

        Self::from_str(truncate).with_context(create_err_msg)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, AsRefStr, EnumString, EnumIter)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[strum(ascii_case_insensitive)]
pub enum ConsistencyLevel {
    One,
    Quorum,
    LocalQuorum,
    EachQuorum,
    All,
    Any,
    Two,
    Three,
    LocalOne,
    Serial,
    LocalSerial,
}

impl ConsistencyLevel {
    fn show(&self) -> &str {
        self.as_ref()
    }

    fn to_scylla_consistency(&self) -> Consistency {
        match self {
            ConsistencyLevel::One => Consistency::One,
            ConsistencyLevel::Quorum => Consistency::Quorum,
            ConsistencyLevel::LocalQuorum => Consistency::LocalQuorum,
            ConsistencyLevel::EachQuorum => Consistency::EachQuorum,
            ConsistencyLevel::All => Consistency::All,
            ConsistencyLevel::Any => Consistency::Any,
            ConsistencyLevel::Two => Consistency::Two,
            ConsistencyLevel::Three => Consistency::Three,
            ConsistencyLevel::LocalOne => Consistency::LocalOne,
            ConsistencyLevel::Serial => Consistency::Serial,
            ConsistencyLevel::LocalSerial => Consistency::LocalSerial,
        }
    }
}

impl Parsable for ConsistencyLevel {
    type Parsed = Consistency;

    fn parse(cl: &str) -> Result<Self::Parsed> {
        let create_err_msg = || {
            let concat = Self::iter()
                .map(|cl| cl.show().to_owned())
                .collect::<Vec<String>>()
                .join("|");

            format!(
                "Invalid consistency level: {}. Must be one of: {}",
                cl, concat
            )
        };

        Self::from_str(cl)
            .with_context(create_err_msg)
            .map(|cl| cl.to_scylla_consistency())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, AsRefStr, EnumString, EnumIter)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[strum(ascii_case_insensitive)]
pub enum SerialConsistencyLevel {
    Serial,
    LocalSerial,
}

impl SerialConsistencyLevel {
    fn show(&self) -> &str {
        self.as_ref()
    }

    fn to_scylla_serial_consistency(&self) -> SerialConsistency {
        match self {
            SerialConsistencyLevel::Serial => SerialConsistency::Serial,
            SerialConsistencyLevel::LocalSerial => SerialConsistency::LocalSerial,
        }
    }
}

impl Parsable for SerialConsistencyLevel {
    type Parsed = SerialConsistency;

    fn parse(serial_cl: &str) -> Result<Self::Parsed> {
        let create_err_msg = || {
            let concat = Self::iter()
                .map(|serial_cl| serial_cl.show().to_owned())
                .collect::<Vec<String>>()
                .join("|");

            format!(
                "Invalid serial consistency level: {}. Must be one of: {}",
                serial_cl, concat
            )
        };

        Self::from_str(serial_cl)
            .with_context(create_err_msg)
            .map(|serial_cl| serial_cl.to_scylla_serial_consistency())
    }
}

pub struct CommonParams {
    pub uncertainty: Option<Uncertainty>,
    pub no_warmup: bool,
    pub truncate: Truncate,
    pub consistency_level: Consistency,
    pub serial_consistency_level: SerialConsistency,
    pub operation_count: Option<u64>,
    pub duration: Option<Duration>,
    pub keysize: NonZeroU32,
}

impl CommonParams {
    pub fn print_settings(&self, command: &Command) {
        println!("Command:");
        println!("  Type: {}", command.show());
        print!("  Count: ");
        match &self.operation_count {
            Some(v) => println!("{v}"),
            None => println!("-1"),
        }
        if let Some(duration) = self.duration {
            println!("  Duration: {} SECONDS", duration.as_secs());
        }
        println!("  No Warmup: {}", self.no_warmup);
        println!("  Consistency Level: {}", self.consistency_level);
        println!(
            "  Serial Consistency Level: {}",
            self.serial_consistency_level
        );
        println!("  Truncate: {}", self.truncate.show());
        if self.uncertainty.is_none() {
            println!("  Target Uncertainty: not applicable");
        } else {
            self.uncertainty.as_ref().unwrap().print_settings();
        }
        println!("  Key Size (bytes): {}", self.keysize);
    }
}

pub struct CommonParamHandles {
    err: SimpleParamHandle<UnitInterval>,
    ngt: SimpleParamHandle<u64>,
    nlt: SimpleParamHandle<u64>,
    no_warmup: SimpleParamHandle<bool>,
    truncate: SimpleParamHandle<Truncate>,
    cl: SimpleParamHandle<ConsistencyLevel>,
    serial_cl: SimpleParamHandle<SerialConsistencyLevel>,
    n: SimpleParamHandle<Count>,
    duration: SimpleParamHandle<Duration>,
    keysize: SimpleParamHandle<NonZeroU32>,
}

pub fn add_common_param_groups(
    parser: &mut ParamsParser,
) -> (Vec<Vec<Box<dyn ParamHandle>>>, CommonParamHandles) {
    let err = parser.simple_param(
        "err<",
        Some("0.02"),
        "Run until the standard error of the mean is below this fraction",
        false,
    );
    let ngt = parser.simple_param(
        "n>",
        Some("30"),
        "Run at least this many iterations before accepting uncertainty convergence",
        false,
    );
    let nlt = parser.simple_param(
        "n<",
        Some("200"),
        "Run at most this many iterations before accepting uncertainty convergence",
        false,
    );
    let no_warmup = parser.simple_param("no-warmup", None, "Do not warmup the process", false);
    let truncate = parser.simple_param(
        "truncate=",
        Some("never"),
        "Truncate the table: never, before performing any work, or before each iteration",
        false,
    );
    let cl = parser.simple_param("cl=", Some("local_one"), "Consistency level to use", false);
    let serial_cl = parser.simple_param(
        "serial-cl=",
        Some("serial"),
        "Serial consistency level to use",
        false,
    );
    let n = parser.simple_param("n=", None, "Number of operations to perform", true);
    let duration = parser.simple_param(
        "duration=",
        None,
        "Time to run in (in seconds, minutes or hours)",
        true,
    );
    let keysize = parser.simple_param("keysize=", Some("10"), "Key size in bytes", false);

    // $ ./cassandra-stress help read
    //
    // Usage: read [err<?] [n>?] [n<?] [no-warmup] [truncate=?] [cl=?] [serial-cl=?] [keysize=?]
    //  OR
    // Usage: read n=? [no-warmup] [truncate=?] [cl=?] [serial-cl=?] [keysize=?]
    //  OR
    // Usage: read duration=? [no-warmup] [truncate=?] [cl=?] [serial-cl=?] [keysize=?]

    let groups: Vec<Vec<Box<dyn ParamHandle>>> = vec![
        vec![
            Box::new(err.clone()),
            Box::new(ngt.clone()),
            Box::new(nlt.clone()),
            Box::new(no_warmup.clone()),
            Box::new(truncate.clone()),
            Box::new(cl.clone()),
            Box::new(serial_cl.clone()),
            Box::new(keysize.clone()),
        ],
        vec![
            Box::new(n.clone()),
            Box::new(no_warmup.clone()),
            Box::new(truncate.clone()),
            Box::new(cl.clone()),
            Box::new(serial_cl.clone()),
            Box::new(keysize.clone()),
        ],
        vec![
            Box::new(duration.clone()),
            Box::new(no_warmup.clone()),
            Box::new(truncate.clone()),
            Box::new(cl.clone()),
            Box::new(serial_cl.clone()),
            Box::new(keysize.clone()),
        ],
    ];

    (
        groups,
        CommonParamHandles {
            err,
            ngt,
            nlt,
            no_warmup,
            truncate,
            cl,
            serial_cl,
            n,
            duration,
            keysize,
        },
    )
}

fn prepare_parser(cmd: &str) -> (ParamsParser, CommonParamHandles) {
    let mut parser = ParamsParser::new(cmd);

    let (groups, handles) = add_common_param_groups(&mut parser);

    for group in groups.iter() {
        parser.group(&group.iter().map(|e| e.as_ref()).collect::<Vec<_>>())
    }

    (parser, handles)
}

pub fn parse_with_handles(handles: CommonParamHandles) -> CommonParams {
    let err = handles.err.get();
    let ngt = handles.ngt.get();
    let nlt = handles.nlt.get();
    let no_warmup = handles.no_warmup.get().is_some();
    let truncate = handles.truncate.get().unwrap();
    let consistency_level = handles.cl.get().unwrap();
    let serial_consistency_level = handles.serial_cl.get().unwrap();
    let operation_count = handles.n.get();
    let duration = handles.duration.get();
    let keysize = handles.keysize.get().unwrap();

    let uncertainty = match (err, ngt, nlt) {
        (Some(err), Some(ngt), Some(nlt)) => Some(Uncertainty::new(err, ngt, nlt)),
        _ => None,
    };

    // Parser's regular expressions ensure that String parsing won't fail.
    CommonParams {
        uncertainty,
        no_warmup,
        truncate,
        consistency_level,
        serial_consistency_level,
        operation_count,
        duration,
        keysize,
    }
}

pub fn parse_common_params(cmd: &Command, payload: &mut ParsePayload) -> Result<CommandParams> {
    let args = payload.remove(cmd.show()).unwrap();
    let (parser, handles) = prepare_parser(cmd.show());
    parser.parse(args)?;
    Ok(CommandParams {
        common: parse_with_handles(handles),
        counter: None,
        mixed: None,
        #[cfg(feature = "user-profile")]
        user: None,
    })
}

pub fn print_help_common(command_str: &str) {
    let (parser, _) = prepare_parser(command_str);
    parser.print_help();
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use scylla::statement::{Consistency, SerialConsistency};

    use crate::settings::command::{
        common::{parse_with_handles, prepare_parser, Truncate},
        Command,
    };

    const CMD: Command = Command::Read;

    #[test]
    fn read_params_parser_with_operation_count_test() {
        let args = vec!["n=10m", "cl=quorum", "no-warmup", "keysize=5"];
        let (parser, handles) = prepare_parser(CMD.show());

        assert!(parser.parse(args).is_ok());

        let params = parse_with_handles(handles);

        assert_eq!(None, params.uncertainty);
        assert!(params.no_warmup);
        assert_eq!(Truncate::Never, params.truncate);
        assert_eq!(Consistency::Quorum, params.consistency_level);
        assert_eq!(SerialConsistency::Serial, params.serial_consistency_level);
        assert_eq!(Some(10_000_000), params.operation_count);
        assert_eq!(None, params.duration);
        assert_eq!(NonZeroU32::new(5).unwrap(), params.keysize);
    }

    #[test]
    fn read_params_parser_with_uncertainty_test() {
        let args = vec!["err<0.02", "n<1000", "no-warmup"];

        let (parser, handles) = prepare_parser(CMD.show());

        assert!(parser.parse(args).is_ok());

        let params = parse_with_handles(handles);

        assert_eq!(
            0.02,
            params.uncertainty.as_ref().unwrap().target_uncertainty
        );
        assert_eq!(
            1000,
            params
                .uncertainty
                .as_ref()
                .unwrap()
                .max_uncertainty_measurements
        );
        assert_eq!(
            30,
            params
                .uncertainty
                .as_ref()
                .unwrap()
                .min_uncertainty_measurements
        );
        assert!(params.no_warmup);
        assert_eq!(Truncate::Never, params.truncate);
        assert_eq!(Consistency::LocalOne, params.consistency_level);
        assert_eq!(SerialConsistency::Serial, params.serial_consistency_level);
        assert_eq!(None, params.operation_count);
        assert_eq!(None, params.duration);
        assert_eq!(NonZeroU32::new(10).unwrap(), params.keysize);
    }

    #[test]
    fn read_params_groups_test() {
        // Here we declare uncertainty parameters (err< and n<) with operation count parameter (n=).
        // These two are mutually exclusive so the parsing should fail.
        let args = vec!["err<0.02", "n<1000", "n=10m"];

        let (parser, _) = prepare_parser(CMD.show());

        assert!(parser.parse(args).is_err());
    }
}
