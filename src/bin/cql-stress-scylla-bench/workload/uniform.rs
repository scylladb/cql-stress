use std::ops::Range;

use crate::distribution::RngGen;

use anyhow::Result;
use rand::Rng;
use rand_distr::Distribution;

use super::{Workload, WorkloadFactory};

/// Creates workloads which write data uniformly.
pub struct UniformFactory {
    config: UniformConfig,
}

struct Uniform {
    gen: RngGen,
    pk_distribution: rand_distr::Uniform<u64>,
    ck_distribution: rand_distr::Uniform<u64>,
}

/// Defines parameters of a uniform workload.
#[derive(Clone)]
pub struct UniformConfig {
    pub pk_range: Range<u64>,
    pub ck_range: Range<u64>,
}

impl UniformFactory {
    pub fn new(config: UniformConfig) -> Result<UniformFactory> {
        anyhow::ensure!(
            config.pk_range.start < config.pk_range.end,
            "Invalid partition key range",
        );
        anyhow::ensure!(
            config.ck_range.start < config.ck_range.end,
            "Invalid clustering key key range",
        );

        Ok(UniformFactory { config })
    }
}

impl WorkloadFactory for UniformFactory {
    fn create(&self) -> Box<dyn Workload> {
        Box::new(Uniform::new(self.config.clone()))
    }
}

impl Uniform {
    /// Creates a new uniform workload.
    fn new(config: UniformConfig) -> Uniform {
        Uniform {
            pk_distribution: config.pk_range.into(),
            ck_distribution: config.ck_range.into(),
            gen: RngGen::new(rand::thread_rng().gen()),
        }
    }
}

impl Workload for Uniform {
    fn generate_keys(&mut self, ck_count: usize) -> Option<(i64, Vec<i64>)> {
        let pk = self.pk_distribution.sample(&mut self.gen) as i64;
        let cks = self
            .ck_distribution
            .sample_iter(&mut self.gen)
            .map(|x| x as i64)
            .take(ck_count)
            .collect();

        Some((pk, cks))
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_uniform_workload() {
        let check = |config: UniformConfig, rpk: usize, expected: &[(i64, i64)]| {
            let mut seq = Uniform::new(config);
            let mut actual = HashSet::new();

            // Generate 1000 times, hoping that we cover whole range
            for _ in 0..1000 {
                let (pk, cks) = seq.generate_keys(rpk).unwrap();
                for ck in cks {
                    actual.insert((pk, ck));
                }
            }

            let expected: HashSet<_> = expected.iter().cloned().collect();
            assert_eq!(actual, expected);
        };

        check(
            UniformConfig {
                pk_range: (0..3),
                ck_range: (0..3),
            },
            1,
            &[
                (0, 0),
                (0, 1),
                (0, 2),
                (1, 0),
                (1, 1),
                (1, 2),
                (2, 0),
                (2, 1),
                (2, 2),
            ],
        );

        check(
            UniformConfig {
                pk_range: (0..3),
                ck_range: (0..3),
            },
            3,
            &[
                (0, 0),
                (0, 1),
                (0, 2),
                (1, 0),
                (1, 1),
                (1, 2),
                (2, 0),
                (2, 1),
                (2, 2),
            ],
        );
    }
}
