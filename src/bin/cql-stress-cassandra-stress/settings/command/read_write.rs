use crate::settings::{
    param::{ParamsParser, SimpleParamHandle},
    ParsePayload,
};
use anyhow::{Context, Result};
use scylla::statement::{Consistency, SerialConsistency};
use std::{str::FromStr, time::Duration};
use strum_macros::{AsRefStr, EnumString};

use super::{Command, CommandParams};

#[derive(Clone, Debug, PartialEq)]
pub struct Uncertainty {
    pub target_uncertainty: f32,
    pub min_uncertainty_measurements: u64,
    pub max_uncertainty_measurements: u64,
}

impl Uncertainty {
    pub fn new(
        target_uncertainty: f32,
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

#[derive(Clone, Debug, PartialEq, Eq, AsRefStr, EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[strum(ascii_case_insensitive)]
pub enum Truncate {
    Never,
    Once,
    Always,
}

impl Truncate {
    fn parse(truncate: &str) -> Result<Self> {
        Self::from_str(truncate).with_context(|| format!("Invalid truncate type: {}", truncate))
    }

    fn show(&self) -> &str {
        self.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, AsRefStr, EnumString)]
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
}

impl ConsistencyLevel {
    fn parse(cl: &str) -> Result<Consistency> {
        Self::from_str(cl)
            .with_context(|| format!("Invalid consistency level: {}", cl))
            .map(|cl| cl.to_scylla_consistency())
    }

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
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, AsRefStr, EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[strum(ascii_case_insensitive)]
pub enum SerialConsistencyLevel {
    Serial,
    LocalSerial,
}

impl SerialConsistencyLevel {
    fn parse(serial_cl: &str) -> Result<SerialConsistency> {
        Self::from_str(serial_cl)
            .with_context(|| format!("Invalid serial consistency level: {}", serial_cl))
            .map(|serial_cl| serial_cl.to_scylla_serial_consistency())
    }

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

pub struct ReadWriteParams {
    pub uncertainty: Option<Uncertainty>,
    pub no_warmup: bool,
    pub truncate: Truncate,
    pub consistency_level: Consistency,
    pub serial_consistency_level: SerialConsistency,
    pub operation_count: Option<u64>,
    pub duration: Option<Duration>,
}

impl ReadWriteParams {
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
    }
}

struct ReadWriteParamHandles {
    err: SimpleParamHandle,
    ngt: SimpleParamHandle,
    nlt: SimpleParamHandle,
    no_warmup: SimpleParamHandle,
    truncate: SimpleParamHandle,
    cl: SimpleParamHandle,
    serial_cl: SimpleParamHandle,
    n: SimpleParamHandle,
    duration: SimpleParamHandle,
}

fn parse_operation_count_unit(unit: char) -> Result<u64> {
    match unit {
        'k' => Ok(1_000),
        'm' => Ok(1_000_000),
        'b' => Ok(1_000_000_000),
        _ => Err(anyhow::anyhow!("Invalid operation count unit: {}", unit)),
    }
}

fn parse_operation_count(n: &str) -> Result<u64> {
    let last = n.chars().last().unwrap();
    let mut multiplier = 1;
    let mut number_slice = n;
    if last.is_alphabetic() {
        multiplier = parse_operation_count_unit(last)?;
        number_slice = &n[0..n.len() - 1];
    }
    Ok(number_slice.parse::<u64>().unwrap() * multiplier)
}

fn parse_duration_unit(unit: char) -> Result<u64> {
    match unit {
        's' => Ok(1),
        'm' => Ok(60),
        'h' => Ok(60 * 60),
        _ => Err(anyhow::anyhow!("Invalid duration unit: {}", unit)),
    }
}

fn parse_duration(n: &str) -> Result<Duration> {
    let multiplier = parse_duration_unit(n.chars().last().unwrap())?;
    Ok(Duration::from_secs(
        n[0..n.len() - 1].parse::<u64>().unwrap() * multiplier,
    ))
}

fn prepare_parser(cmd: &str) -> (ParamsParser, ReadWriteParamHandles) {
    let mut parser = ParamsParser::new(cmd);
    let err = parser.simple_param(
        "err<",
        r"^0\.[0-9]+$",
        Some("0.02"),
        "Run until the standard error of the mean is below this fraction",
        false,
    );
    let ngt = parser.simple_param(
        "n>",
        r"^[0-9]+$",
        Some("30"),
        "Run at least this many iterations before accepting uncertainty convergence",
        false,
    );
    let nlt = parser.simple_param(
        "n<",
        r"^[0-9]+$",
        Some("200"),
        "Run at most this many iterations before accepting uncertainty convergence",
        false,
    );
    let no_warmup =
        parser.simple_param("no-warmup", r"^$", None, "Do not warmup the process", false);
    let truncate = parser.simple_param(
        "truncate=",
        r"^(never|once|always)$",
        Some("never"),
        "Truncate the table: never, before performing any work, or before each iteration",
        false,
    );
    let cl = parser.simple_param(
        "cl=",
        r"^(one|quorum|local_quorum|each_quorum|all|any|two|three|local_one|serial|local_serial)$",
        Some("local_one"),
        "Consistency level to use",
        false,
    );
    let serial_cl = parser.simple_param(
        "serial-cl=",
        r"^(serial|local_serial)$",
        Some("serial"),
        "Serial consistency level to use",
        false,
    );
    let n = parser.simple_param(
        "n=",
        r"^[0-9]+[bmk]?$",
        None,
        "Number of operations to perform",
        true,
    );
    let duration = parser.simple_param(
        "duration=",
        r"^[0-9]+[smh]$",
        None,
        "Time to run in (in seconds, minutes or hours)",
        true,
    );

    // $ ./cassandra-stress help read
    //
    // Usage: read [err<?] [n>?] [n<?] [no-warmup] [truncate=?] [cl=?] [serial-cl=?]
    //  OR
    // Usage: read n=? [no-warmup] [truncate=?] [cl=?] [serial-cl=?]
    //  OR
    // Usage: read duration=? [no-warmup] [truncate=?] [cl=?] [serial-cl=?]
    parser.group(vec![
        &err, &ngt, &nlt, &no_warmup, &truncate, &cl, &serial_cl,
    ]);
    parser.group(vec![&n, &no_warmup, &truncate, &cl, &serial_cl]);
    parser.group(vec![&duration, &no_warmup, &truncate, &cl, &serial_cl]);

    (
        parser,
        ReadWriteParamHandles {
            err,
            ngt,
            nlt,
            no_warmup,
            truncate,
            cl,
            serial_cl,
            n,
            duration,
        },
    )
}

fn parse_with_handles(handles: ReadWriteParamHandles) -> ReadWriteParams {
    let err = handles.err.get_type::<f32>();
    let ngt = handles.ngt.get_type::<u64>();
    let nlt = handles.nlt.get_type::<u64>();
    let no_warmup = handles.no_warmup.supplied_by_user();
    let truncate = Truncate::parse(&handles.truncate.get().unwrap()).unwrap();
    let consistency_level = ConsistencyLevel::parse(&handles.cl.get().unwrap()).unwrap();
    let serial_consistency_level =
        SerialConsistencyLevel::parse(&handles.serial_cl.get().unwrap()).unwrap();
    let operation_count = handles.n.get().map(|n| parse_operation_count(&n).unwrap());
    let duration = handles.duration.get().map(|d| parse_duration(&d).unwrap());

    let uncertainty = match (err, ngt, nlt) {
        (Some(err), Some(ngt), Some(nlt)) => Some(Uncertainty::new(err, ngt, nlt)),
        _ => None,
    };

    // Parser's regular expressions ensure that String parsing won't fail.
    ReadWriteParams {
        uncertainty,
        no_warmup,
        truncate,
        consistency_level,
        serial_consistency_level,
        operation_count,
        duration,
    }
}

pub fn parse_read_write_params(cmd: &Command, payload: &mut ParsePayload) -> Result<CommandParams> {
    let args = payload.remove(cmd.show()).unwrap();
    let (parser, handles) = prepare_parser(cmd.show());
    parser.parse(args)?;
    Ok(CommandParams::BasicParams(parse_with_handles(handles)))
}

pub fn print_help_read_write(command_str: &str) {
    let (parser, _) = prepare_parser(command_str);
    parser.print_help();
}

#[cfg(test)]
mod tests {
    use scylla::statement::{Consistency, SerialConsistency};

    use crate::settings::command::{
        read_write::{parse_with_handles, prepare_parser, Truncate},
        Command,
    };

    const CMD: Command = Command::Read;

    #[test]
    fn read_params_parser_with_operation_count_test() {
        let args = vec!["n=10m", "cl=quorum", "no-warmup"];
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
