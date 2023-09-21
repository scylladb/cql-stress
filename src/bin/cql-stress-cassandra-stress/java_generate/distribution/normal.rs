use anyhow::{Context, Result};
use cql_stress::distribution::Description;

use super::{Distribution, DistributionFactory, ThreadLocalRandom};

/// Normal distribution based on https://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/src-html/org/apache/commons/math3/distribution/NormalDistribution.
struct NormalDistribution {
    min: i64,
    max: i64,
    mean: f64,
    standard_deviation: f64,
    rng: ThreadLocalRandom,
}

impl NormalDistribution {
    fn verify_args(min: i64, max: i64, standard_deviation: f64) -> Result<()> {
        anyhow::ensure!(
            min < max,
            "Upper bound ({}) for normal distribution is not higher than the lower bound ({}).",
            max,
            min
        );
        anyhow::ensure!(
            standard_deviation > 0f64,
            "Standard deviation must be positive"
        );

        Ok(())
    }

    pub fn new(min: i64, max: i64, mean: f64, standard_deviation: f64) -> Result<Self> {
        Self::verify_args(min, max, standard_deviation)?;
        Ok(Self {
            min,
            max,
            mean,
            standard_deviation,
            rng: ThreadLocalRandom::new(),
        })
    }

    fn sample(&self) -> f64 {
        self.standard_deviation * self.rng.get().next_gaussian() + self.mean
    }
}

impl Distribution for NormalDistribution {
    fn next_i64(&self) -> i64 {
        (self.sample() as i64).clamp(self.min, self.max)
    }

    fn next_f64(&self) -> f64 {
        self.sample().clamp(self.min as f64, self.max as f64)
    }

    fn set_seed(&self, seed: i64) {
        self.rng.get().set_seed(seed as u64)
    }
}

pub struct NormalDistributionFactory {
    min: i64,
    max: i64,
    mean: f64,
    standard_deviation: f64,
}

impl NormalDistributionFactory {
    fn new(min: i64, max: i64, mean: f64, standard_deviation: f64) -> Result<Self> {
        NormalDistribution::verify_args(min, max, standard_deviation)?;
        Ok(Self {
            min,
            max,
            mean,
            standard_deviation,
        })
    }
}

impl DistributionFactory for NormalDistributionFactory {
    fn create(&self) -> Box<dyn Distribution> {
        Box::new(
            NormalDistribution::new(self.min, self.max, self.mean, self.standard_deviation)
                .unwrap(),
        )
    }
}

impl NormalDistributionFactory {
    fn do_parse_from_description(desc: &Description<'_>) -> Result<Box<dyn DistributionFactory>> {
        // See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/settings/OptionDistribution.java#L202.
        desc.check_minimum_argument_count(2)?;
        let mut iter = desc.args_fused();

        let (min, max) = (
            iter.next().unwrap().parse::<i64>()?,
            iter.next().unwrap().parse::<i64>()?,
        );

        let (mean, stdev) = match (iter.next(), iter.next(), iter.next()) {
            (Some(mean), Some(stdev), None) => (mean.parse::<f64>()?, stdev.parse::<f64>()?),
            (maybe_stdvrng, None, None) => {
                let stdevs_to_edge = maybe_stdvrng
                    .map(|s| s.parse::<f64>())
                    .unwrap_or(Ok(3f64))?;

                let mean = ((min + max) as f64) / 2f64;
                let stdev = (((max - min) as f64) / 2f64) / stdevs_to_edge;
                (mean, stdev)
            }
            _ => anyhow::bail!("Invalid arguments count"),
        };

        Ok(Box::new(Self::new(min, max, mean, stdev)?))
    }

    pub fn parse_from_description(desc: Description<'_>) -> Result<Box<dyn DistributionFactory>> {
        Self::do_parse_from_description(&desc).with_context(|| {
            format!(
                "Invalid parameter list for normal distribution: {:?}",
                desc.args
            )
        })
    }

    pub fn help_description_two_args() -> String {
        format!(
            "      {:<36} A gaussian/normal distribution, where mean=(min+max)/2, and stdev=(mean-min)/3. Aliases: GAUSS, NORMAL, NORM",
            "GAUSSIAN(min..max)"
        )
    }

    pub fn help_description_three_args() -> String {
        format!(
            "      {:<36} A gaussian/normal distribution, where mean=(min+max)/2, and stdev=(mean-min)/stdvrng. Aliases: GAUSS, NORMAL, NORM",
            "GAUSSIAN(min..max,stdvrng)"
        )
    }

    pub fn help_description_four_args() -> String {
        format!(
            "      {:<36} A gaussian/normal distribution, with explicitly defined mean and stdev. Aliases: GAUSS, NORMAL, NORM",
            "GAUSSIAN(min..max,mean,stdev)"
        )
    }
}

impl std::fmt::Display for NormalDistributionFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GAUSSIAN({}..{},mean={},stdev={})",
            self.min, self.max, self.mean, self.standard_deviation,
        )
    }
}

#[cfg(test)]
mod tests {
    use cql_stress::distribution::Description;

    use super::NormalDistributionFactory;

    #[test]
    fn gaussian_distribution_test() {
        // Gaussian distribution sampling values from 1 to 1_000_000_000.
        let desc = Description {
            name: "GAUSSIAN",
            args: vec!["1", "1000000000"],
            inverted: false,
        };

        let dist = NormalDistributionFactory::parse_from_description(desc)
            .unwrap()
            .create();

        // Values taken from the corresponding Java run.
        dist.set_seed(0);
        let result_seed_0 = (0..100).map(|_| dist.next_i64()).collect::<Vec<_>>();
        assert_eq!(
            vec![
                633755510, 349742319, 846820131, 627295128, 664095755, 219431290, 495451623,
                519207617, 434972160, 392768531, 508743485, 586890346, 362672166, 543453032,
                424502041, 733857897, 545188436, 498824331, 650826450, 642809041, 562055680,
                566278806, 510490962, 656909999, 573517298, 378020045, 498039398, 473772967,
                402956962, 465671637, 433349796, 648552602, 568460106, 304793917, 434915297,
                581690067, 659962542, 625397699, 390340598, 607505389, 447424613, 675815799,
                599297196, 670425161, 107300517, 245821981, 696809545, 501023492, 477168432,
                429632011, 497244750, 579629856, 501611543, 218051107, 423126801, 640291248,
                459682526, 385993674, 180566669, 552558393, 567487039, 464099986, 659375317,
                546791796, 868968059, 491962392, 681878777, 1000000000, 124339143, 437137207,
                697386290, 328488223, 741336700, 605532056, 271080441, 404120674, 604129959,
                446338344, 387077475, 797892250, 318534785, 208581194, 640539623, 696718803,
                652188319, 530850796, 518809580, 171165750, 506420538, 575507100, 512985839,
                445967651, 742138310, 666905300, 626185902, 595727330, 671474833, 258492854,
                599914915, 376367221,
            ],
            result_seed_0
        );

        dist.set_seed(0xdeadcafe);
        let result_seed_deadcafe = (0..100).map(|_| dist.next_i64()).collect::<Vec<_>>();
        assert_eq!(
            vec![
                438095211, 452378507, 576528728, 642147810, 398175114, 498750493, 714262218,
                514666003, 1000000000, 629002086, 408981328, 153696245, 406064646, 407094775,
                717977253, 561986729, 560725724, 288715802, 386358873, 489709555, 640508784,
                443781270, 614526691, 772862353, 737999207, 534865835, 195566262, 635558359,
                349784048, 691456675, 499670334, 356479232, 834868582, 427474781, 566052796,
                532123870, 976079417, 562774098, 721818454, 604911791, 541098056, 571488219,
                631092644, 470017275, 441855896, 115773797, 504282031, 480094325, 664708019,
                516849991, 437153011, 598467753, 392673737, 562890391, 359533518, 412874233,
                364376535, 95046660, 572015728, 339721578, 540487587, 262048139, 376500491,
                299454290, 582590429, 411260325, 389857479, 518087471, 433076115, 268295124,
                457511009, 365050629, 520759538, 430036115, 671765771, 658319266, 360602865,
                376708166, 651528629, 445887700, 458902021, 552712251, 756267620, 467593788,
                587821217, 545230275, 519043306, 410115553, 536508991, 585418089, 516305106,
                656363753, 496767787, 345883468, 481868781, 633231116, 201190644, 622068739,
                225614898, 757238386,
            ],
            result_seed_deadcafe
        );

        dist.set_seed(i64::MIN);
        let result_seed_min_i64 = (0..100).map(|_| dist.next_i64()).collect::<Vec<_>>();
        assert_eq!(
            vec![
                633755510, 349742319, 846820131, 627295128, 664095755, 219431290, 495451623,
                519207617, 434972160, 392768531, 508743485, 586890346, 362672166, 543453032,
                424502041, 733857897, 545188436, 498824331, 650826450, 642809041, 562055680,
                566278806, 510490962, 656909999, 573517298, 378020045, 498039398, 473772967,
                402956962, 465671637, 433349796, 648552602, 568460106, 304793917, 434915297,
                581690067, 659962542, 625397699, 390340598, 607505389, 447424613, 675815799,
                599297196, 670425161, 107300517, 245821981, 696809545, 501023492, 477168432,
                429632011, 497244750, 579629856, 501611543, 218051107, 423126801, 640291248,
                459682526, 385993674, 180566669, 552558393, 567487039, 464099986, 659375317,
                546791796, 868968059, 491962392, 681878777, 1000000000, 124339143, 437137207,
                697386290, 328488223, 741336700, 605532056, 271080441, 404120674, 604129959,
                446338344, 387077475, 797892250, 318534785, 208581194, 640539623, 696718803,
                652188319, 530850796, 518809580, 171165750, 506420538, 575507100, 512985839,
                445967651, 742138310, 666905300, 626185902, 595727330, 671474833, 258492854,
                599914915, 376367221,
            ],
            result_seed_min_i64
        );

        dist.set_seed(i64::MAX);
        let result_seed_max_i64 = (0..100).map(|_| dist.next_i64()).collect::<Vec<_>>();
        assert_eq!(
            vec![
                797555240, 346597182, 581156541, 576148134, 783320014, 575367354, 526414864,
                614532834, 600716941, 278757063, 421429668, 575509845, 454671312, 432042011,
                521605795, 599823152, 761369357, 426017029, 409221249, 584332331, 263384929,
                232745354, 442551690, 680300558, 248195989, 414895779, 880596906, 353891474,
                581451282, 430381169, 514585132, 589674425, 480283257, 599310216, 305730321,
                346493594, 341015904, 249098155, 607406735, 299231511, 483714243, 543042004,
                365244209, 479805766, 216743332, 645173966, 366001536, 565472256, 198930838,
                451149375, 307933764, 400787266, 484238122, 808403438, 846080734, 529331935,
                545495394, 623539726, 544808389, 658049078, 793439971, 780812665, 735686259,
                594369270, 546477721, 377141579, 654350122, 690559100, 256444443, 547078786,
                497376122, 638341771, 593990259, 269681678, 459917512, 552335584, 496761956,
                415631693, 827018819, 789860926, 692555037, 535835392, 831322829, 529628469,
                574254584, 670193057, 325566593, 479952758, 367680437, 776294173, 686661186,
                336454243, 510006376, 493918603, 678592474, 726482792, 565691525, 746082866,
                749748661, 677142245,
            ],
            result_seed_max_i64
        );
    }
}
