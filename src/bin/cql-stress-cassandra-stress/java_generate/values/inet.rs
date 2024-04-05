use std::net::{IpAddr, Ipv4Addr};

use scylla::frame::response::result::CqlValue;

use crate::java_generate::distribution::Distribution;

use super::{ValueGenerator, ValueGeneratorFactory};

#[derive(Default)]
pub struct Inet;

impl ValueGenerator for Inet {
    fn generate(
        &mut self,
        identity_distribution: &mut dyn Distribution,
        _size_distribution: &mut dyn Distribution,
    ) -> CqlValue {
        let octets = (identity_distribution.next_i64() as i32).to_be_bytes();
        CqlValue::Inet(IpAddr::V4(Ipv4Addr::from(octets)))
    }
}

pub struct InetFactory;

impl ValueGeneratorFactory for InetFactory {
    fn create(&self) -> Box<dyn ValueGenerator> {
        Box::<Inet>::default()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        str::FromStr,
    };

    use crate::java_generate::{
        distribution::fixed::FixedDistribution,
        values::{Generator, GeneratorConfig},
    };

    use super::Inet;

    fn ipv4_from_string(ips: impl IntoIterator<Item = &'static str>) -> Vec<IpAddr> {
        ips.into_iter()
            .map(|ip| IpAddr::V4(Ipv4Addr::from_str(ip).unwrap()))
            .collect()
    }

    #[test]
    fn small_inet_generator_test() {
        let config = GeneratorConfig::new(
            "randomstrC0",
            None,
            Some(Box::new(FixedDistribution::new(5))),
        );
        let inet_gen = Box::<Inet>::default();
        let mut gen = Generator::new(inet_gen, config, String::from("C0"));

        // Values which we test against are generated from c-s.
        gen.set_seed(0);
        let results = (0..5)
            .map(|_| gen.generate().as_inet().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            ipv4_from_string([
                "111.164.74.168",
                "240.188.46.170",
                "233.145.187.186",
                "50.136.51.18",
                "54.211.10.133"
            ]),
            results
        );

        gen.set_seed(0xdeadcafe);
        let results = (0..5)
            .map(|_| gen.generate().as_inet().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            ipv4_from_string([
                "50.210.248.130",
                "85.88.197.197",
                "48.112.102.213",
                "152.233.96.233",
                "58.116.101.95"
            ]),
            results
        );

        gen.set_seed(i64::MIN);
        let results = (0..5)
            .map(|_| gen.generate().as_inet().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            ipv4_from_string([
                "111.164.74.168",
                "240.188.46.170",
                "233.145.187.186",
                "50.136.51.18",
                "54.211.10.133"
            ]),
            results
        );

        gen.set_seed(i64::MAX);
        let results = (0..5)
            .map(|_| gen.generate().as_inet().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            ipv4_from_string([
                "216.73.236.123",
                "58.146.172.102",
                "88.16.209.169",
                "80.126.117.191",
                "166.176.232.127"
            ]),
            results
        );
    }
}
