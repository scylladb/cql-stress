use std::{
    cell::{RefCell, RefMut},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use thread_local::ThreadLocal;

use crate::distribution::{parse_description, parse_long, SyntaxFlavor};

use self::{
    fixed::FixedDistributionFactory, normal::NormalDistributionFactory,
    sequence::SeqDistributionFactory, uniform::UniformDistributionFactory,
};

use super::Random;

pub mod fixed;
pub mod normal;
pub mod sequence;
pub mod uniform;

/// A distribution that atomically performs the operations.
/// It implies that the distribution can be safely used in a multi-threaded environment.
pub trait Distribution: Send + Sync {
    fn next_i64(&self) -> i64;
    fn next_f64(&self) -> f64;
    fn set_seed(&self, seed: i64);
}

/// A thread_local wrapper for [java_random::Random].
/// Used by distributions to implement `atomic` sampling.
struct ThreadLocalRandom {
    rng: ThreadLocal<RefCell<Random>>,
}

impl ThreadLocalRandom {
    fn new() -> Self {
        Self {
            rng: ThreadLocal::new(),
        }
    }

    fn get(&self) -> RefMut<'_, Random> {
        self.rng
            .get_or(|| {
                RefCell::new(Random::with_seed(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|duration| duration.as_millis() as u64)
                        .unwrap_or_default(),
                ))
            })
            .borrow_mut()
    }
}

pub trait DistributionFactory: Send + Sync + std::fmt::Display {
    fn create(&self) -> Box<dyn Distribution>;
}

/// Parses a distribution the way cassandra-stress writes it, for example `uniform(1..10)`,
/// `gaussian(1..100)`, `fixed(5)` or `seq(1..1000)`.
pub fn parse_distribution(s: &str) -> Result<Box<dyn DistributionFactory>> {
    let s = &s.to_lowercase();
    let description = parse_description(s, SyntaxFlavor::Classic)?;

    anyhow::ensure!(
        !description.inverted,
        "Inverted distributions are not yet supported!"
    );

    match description.name {
        "fixed" => FixedDistributionFactory::parse_from_description(description),
        "seq" => SeqDistributionFactory::parse_from_description(description),
        "uniform" => UniformDistributionFactory::parse_from_description(description),
        "gaussian" | "gauss" | "norm" | "normal" => {
            NormalDistributionFactory::parse_from_description(description)
        }
        _ => Err(anyhow::anyhow!(
            "Invalid distribution name: {}",
            description.name
        )),
    }
}

/// Parses a population the way cassandra-stress `-pop` takes it: `seq=a..b`, inclusive at
/// both ends and with the k/m/b suffixes, or `dist=<distribution>` (see
/// [`parse_distribution`]).
pub fn parse_population(s: &str) -> Result<Box<dyn DistributionFactory>> {
    if let Some(range) = s.strip_prefix("seq=") {
        let (from, to) = range
            .split_once("..")
            .with_context(|| format!("Invalid sequence {range:?}: expected a..b"))?;
        let factory = SeqDistributionFactory::new(parse_long(from)?, parse_long(to)?)?;
        return Ok(Box::new(factory));
    }
    if let Some(distribution) = s.strip_prefix("dist=") {
        return parse_distribution(distribution);
    }
    anyhow::bail!("Invalid population {s:?}: expected seq=a..b or dist=<distribution>")
}

#[cfg(test)]
mod tests {
    use super::{parse_distribution, parse_population};

    #[test]
    fn parse_population_seq_test() {
        let dist = parse_population("seq=0..1023").unwrap().create();
        let drawn: Vec<i64> = (0..1025).map(|_| dist.next_i64()).collect();
        assert_eq!(drawn[..1024], (0..=1023).collect::<Vec<_>>()[..]);
        assert_eq!(drawn[1024], 0, "the sequence wraps around");

        // The k/m/b suffixes cassandra-stress accepts for counts.
        let dist = parse_population("seq=1k..2k").unwrap().create();
        assert_eq!(dist.next_i64(), 1000);
    }

    #[test]
    fn parse_population_dist_test() {
        let dist = parse_population("dist=uniform(1..10)").unwrap().create();
        for _ in 0..100 {
            assert!((1..=10).contains(&dist.next_i64()));
        }
        assert!(parse_population("dist=gaussian(1..100)").is_ok());
        assert!(parse_population("dist=FIXED(5)").is_ok());
    }

    #[test]
    fn parse_population_rejects_test() {
        for bad in [
            "seq=5..1",
            "seq=1",
            "seq=a..b",
            "foo=1",
            "1..10",
            "dist=bogus(1)",
            "",
        ] {
            assert!(parse_population(bad).is_err(), "{bad:?} must not parse");
        }
    }

    #[test]
    fn parse_distribution_test() {
        assert_eq!(
            parse_distribution("seq(3..4)").unwrap().create().next_i64(),
            3
        );
        assert!(parse_distribution("~uniform(1..2)").is_err(), "inverted");
        assert!(parse_distribution("zipf(1..2)").is_err());
    }
}
