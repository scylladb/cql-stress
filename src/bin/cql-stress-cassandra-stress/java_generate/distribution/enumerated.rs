use anyhow::Result;
use rand_distr::{Distribution, WeightedIndex};

#[derive(Clone)]
pub struct EnumeratedDistribution<T> {
    items: Vec<(T, f64)>,
    dist: WeightedIndex<f64>,
}

impl<T: Copy> EnumeratedDistribution<T> {
    pub fn new(items: Vec<(T, f64)>) -> Result<Self> {
        let dist = WeightedIndex::new(items.iter().map(|w| w.1))?;

        Ok(Self { items, dist })
    }

    pub fn sample(&self) -> T {
        self.items[self.dist.sample(&mut rand::thread_rng())].0
    }
}

impl<T: PartialEq + Eq> EnumeratedDistribution<T> {
    pub fn contains(&self, t: &T) -> bool {
        self.items.iter().any(|(item, _weight)| item == t)
    }
}

impl<T: std::fmt::Display> std::fmt::Display for EnumeratedDistribution<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        let items_str = self
            .items
            .iter()
            .map(|item| format!("{key}={value}", key = item.0, value = item.1))
            .collect::<Vec<_>>()
            .join(",");
        write!(f, "{items_str}}}")
    }
}
