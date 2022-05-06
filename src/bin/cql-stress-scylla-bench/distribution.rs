use anyhow::{Context, Result};
use rand::distributions::Distribution as RandDistribution;
use rand_pcg::Pcg64Mcg;

use cql_stress::distribution::{parse_description, parse_long, Description, SyntaxFlavor};

pub type RngGen = Pcg64Mcg;

pub trait Distribution: Send + Sync {
    fn get_u64(&self, rng: &mut RngGen) -> u64;
    fn get_f64(&self, rng: &mut RngGen) -> f64 {
        self.get_u64(rng) as f64
    }

    fn describe(&self) -> String;
}

pub fn parse_distribution(desc: &str) -> Result<Box<dyn Distribution>> {
    // If `desc` is a single number, then it's a fixed distribution
    if desc.trim().chars().all(|c| c.is_ascii_digit()) {
        let desc = Description {
            name: "fixed",
            args: vec![desc.trim()],
            inverted: false,
        };
        let fixed = Fixed::parse_from_desc(desc)?;
        return Ok(Box::new(fixed));
    }

    let desc = parse_description(desc, SyntaxFlavor::ClassicOrShort)?;

    anyhow::ensure!(!desc.inverted, "Inverted distributions are not supported");

    match desc.name {
        "fixed" => {
            let fixed =
                Fixed::parse_from_desc(desc).context("Failed to parse fixed distribution")?;
            Ok(Box::new(fixed))
        }
        "uniform" => {
            let uniform =
                Uniform::parse_from_desc(desc).context("Failed to parse uniform distribution")?;
            Ok(Box::new(uniform))
        }
        other => Err(anyhow::anyhow!("Unknown distribution: {}", other)),
    }
}

pub struct Fixed(pub u64);

impl Fixed {
    fn parse_from_desc(desc: Description<'_>) -> Result<Self> {
        desc.check_argument_count(1)?;
        let arg: u64 = parse_long(desc.args[0])?;

        Ok(Self(arg))
    }
}

impl Distribution for Fixed {
    fn get_u64(&self, _: &mut RngGen) -> u64 {
        self.0
    }

    fn describe(&self) -> String {
        format!("Fixed({})", self.0)
    }
}

pub struct Uniform {
    sampler: rand_distr::Uniform<u64>,
    low: u64,
    high: u64,
}

impl Uniform {
    fn parse_from_desc(desc: Description<'_>) -> Result<Self> {
        desc.check_argument_count(2)?;
        let low: u64 = parse_long(desc.args[0])?;
        let high: u64 = parse_long(desc.args[1])?;
        anyhow::ensure!(low <= high, "Invalid number range");
        Ok(Self {
            sampler: rand_distr::Uniform::new_inclusive(low, high),
            low,
            high,
        })
    }
}

impl Distribution for Uniform {
    fn get_u64(&self, rng: &mut RngGen) -> u64 {
        self.sampler.sample(rng)
    }

    fn describe(&self) -> String {
        format!("Uniform(min={}, max={})", self.low, self.high)
    }
}
