use anyhow::{Context, Result};
use cql_stress::distribution::Description;

use super::{
    fixed::FixedDistributionFactory, Distribution, DistributionFactory, ThreadLocalRandom,
};

/// Uniform real distribution that uses java.util.Random generator.
/// The distribution samples real numbers from [lower, upper + 1)
/// and integers from [lower, upper].
/// See: https://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/distribution/UniformRealDistribution.html.
pub struct UniformDistribution {
    /// Lower bound of the distribution
    lower: f64,
    /// Upper bound of the distribution
    upper: f64,
    /// java.util.Random
    rng: ThreadLocalRandom,
}

impl UniformDistribution {
    pub fn new(lower: f64, upper: f64) -> Result<Self> {
        anyhow::ensure!(
            lower < upper,
            "Upper bound ({}) for real uniform distribution is not higher than the lower bound ({}).",
            upper,
            lower
        );

        Ok(Self {
            lower,
            upper,
            rng: ThreadLocalRandom::new(),
        })
    }
}

impl Distribution for UniformDistribution {
    fn next_i64(&self) -> i64 {
        (self.next_f64() as i64).clamp(self.lower as i64, self.upper as i64)
    }

    fn next_f64(&self) -> f64 {
        let sample = self.rng.get().next_double();
        // See: https://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/src-html/org/apache/commons/math3/distribution/UniformRealDistribution.html#line.240.
        sample * (self.upper + 1.0) + (1.0 - sample) * self.lower
    }

    fn set_seed(&self, seed: i64) {
        self.rng.get().set_seed(seed as u64)
    }
}

pub struct UniformDistributionFactory {
    min: f64,
    max: f64,
}

impl UniformDistributionFactory {
    pub fn new(min: f64, max: f64) -> Result<Self> {
        anyhow::ensure!(
            min < max,
            "Upper bound ({}) for real uniform distribution is not higher than the lower bound ({}).",
            max,
            min
        );

        Ok(Self { min, max })
    }
}

impl DistributionFactory for UniformDistributionFactory {
    fn create(&self) -> Box<dyn Distribution> {
        Box::new(UniformDistribution::new(self.min, self.max).unwrap())
    }
}

impl UniformDistributionFactory {
    pub fn parse_from_description(desc: Description<'_>) -> Result<Box<dyn DistributionFactory>> {
        let result = || -> Result<Box<dyn DistributionFactory>> {
            desc.check_argument_count(2)?;
            let (min, max) = (desc.args[0].parse::<i64>()?, desc.args[1].parse::<i64>()?);

            if min == max {
                Ok(Box::new(FixedDistributionFactory(min)))
            } else {
                Ok(Box::new(UniformDistributionFactory::new(
                    min as f64, max as f64,
                )?))
            }
        }();

        result.with_context(|| {
            format!(
                "Invalid parameter list for uniform distribution: {:?}",
                desc.args
            )
        })
    }

    pub fn help_description() -> String {
        format!(
            "      {:<36} A uniform distribution over the range [min, max]",
            "UNIFORM(min..max)"
        )
    }
}

impl std::fmt::Display for UniformDistributionFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UNIFORM({}..{})", self.min as i64, self.max as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::UniformDistribution;
    use crate::java_generate::distribution::Distribution;

    fn generate_n_samples_with_seed(n: i64, seed: i64) -> Vec<i64> {
        let random = UniformDistribution::new(1f64, 100_000_000_000f64).unwrap();
        random.set_seed(seed);
        (0..n).map(|_| random.next_i64()).collect::<Vec<i64>>()
    }

    #[test]
    fn uniform_distribution_test() {
        let values_seed_one = generate_n_samples_with_seed(100, 1);
        let values_seed_deadcafe = generate_n_samples_with_seed(100, 0xdeadcafe);
        let values_seed_max_i64 = generate_n_samples_with_seed(100, i64::MAX);
        let values_seed_min_i64 = generate_n_samples_with_seed(100, i64::MIN);

        assert_eq!(
            vec![
                73087819071,
                41008081150,
                20771484131,
                33271705596,
                96775590943,
                611718227,
                96370479703,
                93986538879,
                94719491767,
                93708214890,
                39717434219,
                34751802921,
                29405703201,
                50648362733,
                11596708804,
                77053588008,
                65989270870,
                15674689057,
                37820204533,
                13976268291,
                69494797961,
                80522777148,
                502517600,
                52313515579,
                74398448624,
                14202270322,
                48172830158,
                54455480890,
                57710026138,
                20491354576,
                62336381064,
                18470709028,
                1068441099,
                16104332338,
                17805484664,
                54039706479,
                97383400992,
                24542655359,
                39452090630,
                21760212491,
                43200687990,
                23315579476,
                88990875166,
                3832689332,
                59237924387,
                65517360313,
                11983904231,
                65247670690,
                98432280067,
                20673750737,
                37464950030,
                46334978757,
                33361021118,
                44321245032,
                50413556671,
                99898081477,
                63040486393,
                90958403559,
                50765991607,
                49145120557,
                42878417875,
                30809291521,
                71732240163,
                96241459274,
                20949273347,
                17264882092,
                54895251694,
                55546838209,
                58781582891,
                78919065784,
                69900662244,
                20463555221,
                25490945130,
                77791635952,
                22502238601,
                98310318484,
                80361860602,
                83631494776,
                16310352739,
                63749742986,
                876041732,
                63118988832,
                20797797905,
                88035728491,
                70704422606,
                72337164622,
                50949291968,
                98710213810,
                15324623282,
                71057477515,
                82398087546,
                12367122851,
                61695138675,
                48186235527,
                9975028133,
                61714588987,
                2812037296,
                59143910596,
                7302602022,
                7404792728,
            ],
            values_seed_one
        );

        assert_eq!(
            vec![
                12486620361,
                21142084900,
                68746296323,
                84820191312,
                4458732094,
                49441156878,
                82936298484,
                52254451841,
                54306762695,
                51106228389,
                45991262419,
                34747735324,
                19618453586,
                19951628478,
                80293494828,
                58614635783,
                58940067143,
                18894649642,
                5710036792,
                45989449782,
                87774953562,
                34885927674,
                97805311405,
                70327658577,
                58798671078,
                70963026814,
                79390538597,
                54305584373,
                33188251806,
                57485940961,
                31887522112,
                73085130040,
                49904584953,
                8460885894,
                66988412801,
                46320680914,
                92833353431,
                70831412817,
                56221897161,
                50820396706,
                1881246720,
                14713529269,
                76290218788,
                62434285340,
                73440764970,
                90774155868,
                91420290826,
                40526598909,
                48078126464,
                37299930350,
                60476099555,
                1300256204,
                88865363074,
                53976011817,
                26208520975,
                87276145092,
                12469014803,
                71992178282,
                16772512617,
                29390312335,
                46924244336,
                40816209208,
                65520635843,
                15457226044,
                54964621530,
                20822145099,
                34085692035,
                24157417934,
                79844226854,
                17933730204,
                5894216035,
                57242998100,
                41780103888,
                21540952658,
                2506448757,
                4314667874,
                37459562765,
                10170291480,
                63557322856,
                4309051298,
                72498142760,
                70736899030,
                22576919818,
                25745255843,
                87300546533,
                36679623676,
                5597717875,
                73814802700,
                20468213722,
                87877456716,
                77209919040,
                46559181389,
                90713593849,
                70968589688,
                59604990888,
                4664421199,
                68182655613,
                92540964350,
                54151499044,
                89812312030,
            ],
            values_seed_deadcafe
        );

        assert_eq!(
            vec![
                26894263089,
                1226998193,
                66208448412,
                41643831729,
                82616590048,
                80603725180,
                5678939924,
                8237099186,
                72293220316,
                55930329460,
                59922813403,
                93024561487,
                62171263003,
                23263624315,
                17603670287,
                81134421668,
                23872313061,
                10828668917,
                59629294800,
                94489293230,
                21891574691,
                7957553152,
                74764198709,
                42990268584,
                12839110660,
                86764711418,
                18095417866,
                79638960208,
                39470693766,
                38107241040,
                39002593033,
                84515177597,
                24919022202,
                41523180778,
                60459072232,
                45984834331,
                85524342842,
                2610318837,
                87291623000,
                16594762098,
                84276836641,
                20702569300,
                57452109614,
                95818141134,
                41121333412,
                94720483496,
                27405235041,
                32146255691,
                1238296852,
                35717042673,
                412267035,
                42979339742,
                37903210743,
                30909368801,
                64791446141,
                22351302692,
                32639849351,
                95881543443,
                8161534233,
                43730171100,
                32121144689,
                59163224140,
                13225742893,
                67968068349,
                28634693891,
                46533326261,
                20829114182,
                34931639287,
                48918169974,
                71167534916,
                66823118441,
                51425836696,
                64783256050,
                90142951395,
                60697090212,
                87730998633,
                58184921558,
                57832708081,
                75986889305,
                60405204773,
                83256210715,
                92367933221,
                65146907644,
                9960921125,
                68316969397,
                72613945225,
                94872397218,
                78606979297,
                91978177361,
                2126035,
                21784822945,
                55453935474,
                49201913852,
                92078427447,
                92943729772,
                84412946334,
                95540209820,
                22773995487,
                60824435005,
                23475275797,
            ],
            values_seed_max_i64
        );

        assert_eq!(
            vec![
                73096778738,
                24053641568,
                63741742536,
                55043700512,
                59754527780,
                33321839948,
                38518918475,
                98484154020,
                87918251788,
                94124917949,
                27495396604,
                12889715088,
                14660165765,
                2323812249,
                54673975720,
                96448686068,
                10449068626,
                62514636347,
                41079619550,
                77631229128,
                99072278572,
                48723284704,
                74624140533,
                73315207020,
                81729707141,
                83889035005,
                52669943461,
                89933501162,
                13393984059,
                8306239823,
                97857434015,
                72235711919,
                71503101386,
                14322038531,
                46295781843,
                448560219,
                7149831488,
                34842022980,
                33876965354,
                85935655136,
                97154698886,
                86574588022,
                61258110471,
                17898798453,
                21757041221,
                85448716705,
                967349731,
                69229300696,
                77131296618,
                71268742815,
                21123537493,
                78309248977,
                94533323896,
                1423635511,
                39420355278,
                85379077531,
                78604245082,
                99347195501,
                88310440599,
                17029153025,
                96206891821,
                72429503358,
                67735416125,
                80439541723,
                44142677368,
                46208799029,
                85282746660,
                50183485021,
                99194298042,
                96926990995,
                35310607218,
                4726586920,
                7162362342,
                2910751273,
                48367019011,
                97195012096,
                98911715076,
                76744210302,
                50139735102,
                25552531090,
                30915818725,
                84828050028,
                5208453818,
                1017545454,
                35385296971,
                8673785517,
                85031151527,
                367690236,
                30789316764,
                53160855625,
                91881420184,
                27721002607,
                87426221029,
                60988151352,
                90863920970,
                4449062016,
                64672390104,
                49680376363,
                50670159596,
                52068881990,
            ],
            values_seed_min_i64
        );
    }
}
