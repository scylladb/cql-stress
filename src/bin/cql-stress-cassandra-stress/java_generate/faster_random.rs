use std::num::Wrapping;

use java_random::Random;

/// Implementation of the RNG used in cassandra-stress.
/// See: https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/FasterRandom.java
/// Uses Rust implementation of `java.util.Random` under the hood.
pub struct FasterRandom {
    seed: Wrapping<i64>,
    reseed_counter: i32,
    random: Random,
}

impl Default for FasterRandom {
    fn default() -> Self {
        Self {
            seed: Default::default(),
            reseed_counter: Default::default(),
            random: Random::with_seed(Default::default()),
        }
    }
}

impl FasterRandom {
    pub fn set_seed(&mut self, seed: i64) {
        self.seed = Wrapping(seed);
        self.reseed()
    }

    fn reseed(&mut self) {
        self.reseed_counter = 0;
        self.random.set_seed(self.seed.0 as u64);
        self.seed = Wrapping(self.random.next_long())
    }

    pub fn next_i64(&mut self) -> i64 {
        self.reseed_counter += 1;
        if self.reseed_counter == 32 {
            self.reseed();
        }

        let mut seed = self.seed;
        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;
        self.seed = seed;
        (seed * Wrapping(2685821657736338717i64)).0
    }
}

#[cfg(test)]
mod tests {
    use super::FasterRandom;

    fn generate_n_samples_with_seed(n: i64, seed: i64) -> Vec<i64> {
        let mut random = FasterRandom::default();
        random.set_seed(seed);
        (0..n).map(|_| random.next_i64()).collect::<Vec<i64>>()
    }

    #[test]
    fn faster_random_test() {
        let values_seed_one = generate_n_samples_with_seed(35, 1);
        let values_seed_deadcafe = generate_n_samples_with_seed(35, 0xdeadcafe);
        let values_seed_max_i64 = generate_n_samples_with_seed(35, i64::MAX);
        let values_seed_min_i64 = generate_n_samples_with_seed(35, i64::MIN);

        // Values generated from cassandra-stress.
        // Used Java's implementation of FasterRandom.
        assert_eq!(
            vec![
                -4146445186634961508,
                -5644524092788810841,
                -8780053043296993624,
                2200813764513942921,
                1567451884733067761,
                7223005227944205159,
                12193496966718396,
                5005534657638179243,
                -7205694335490576763,
                8582769701196490522,
                6174200446246690084,
                6396028387432977924,
                8669704779397116818,
                -1808508851209253639,
                -6998012785647266716,
                3533837240073173872,
                -6477883200828596236,
                4862426856553382858,
                3025666695059641260,
                -5455343246528299537,
                -8362485225101916742,
                -1552618824096076797,
                -3727466615225401010,
                -4613746930867200601,
                -80706028209404676,
                5386184468992368308,
                -4888639343045998843,
                438016325225555866,
                6882061418041144310,
                7953175724591314388,
                7321497847876274511,
                -2707597717226415229,
                4138543161181408227,
                -1298744581235174812,
                -8546407443987846798,
            ],
            values_seed_one,
        );

        assert_eq!(
            vec![
                49434433457782990,
                3665564222418438880,
                -3495258991372030743,
                -7009553108860880397,
                55352948507852127,
                -8876220067375671067,
                -7636152647847985300,
                -467967488444381632,
                -6417186081697436306,
                5960123611551334889,
                -4413460666804198623,
                6567306237456924709,
                -3789664448623346774,
                441820498322075830,
                -6532886523714393491,
                -6114288088817287325,
                1539638564596873851,
                2615653902205877656,
                4194493049113980269,
                -169887369022094777,
                1083729844614462438,
                5342172672856700939,
                7414403648881484981,
                -2320570954220608738,
                6637742869339771424,
                -1888853426585576290,
                -6700469362836751013,
                7605924059290135131,
                7221226617009387153,
                -3849169185507391628,
                -9157909435843534633,
                -3394647935814472436,
                -8124792638269280860,
                263354235673168812,
                -3584568987167265934,
            ],
            values_seed_deadcafe
        );

        assert_eq!(
            vec![
                -421565949513610942,
                5927074950217443851,
                -8626510764071457801,
                1058236062391371955,
                -4793764754212643165,
                -6769022956311583952,
                -3735554182468415180,
                1911166576467816642,
                -2444382699771834561,
                -3342970423958198553,
                -917781814688716008,
                5335124397824666293,
                2864132505299939141,
                2391238274121336655,
                -7089590066956813460,
                -5302506553667573811,
                2987618040327936093,
                -5076262057950206633,
                5965318486562403334,
                8853183197025282890,
                4964732549197661560,
                1679486474875888190,
                7544862592299878397,
                -4020352216411377815,
                -3317854620822001345,
                4770010190558301172,
                -1780505689100889721,
                -4094293633678866925,
                -4020801115271220665,
                4516218160410015216,
                7879968266420677552,
                1172507437434563896,
                4760974651451996955,
                103055336402823706,
                -5857173763992897626,
            ],
            values_seed_max_i64
        );

        assert_eq!(
            vec![
                8379940216768492356,
                4541987008376338494,
                2504302794943349549,
                -5024369511516201687,
                6606508406236350050,
                -7362406017382227563,
                5999038292518187100,
                -4931697830750418974,
                5472717832483541595,
                -8897053584723523841,
                -2387069448313750750,
                -6492586444233381993,
                -2443766821015210779,
                1270705599062964649,
                5423136387396038191,
                4371166202184071435,
                3305271144502464079,
                2388833476384382675,
                -8412044345200111129,
                6599049238978116533,
                -550143707874218025,
                3292254248733993849,
                3185457983132449669,
                5245838808725237203,
                2379388432944360983,
                -9223216682178115192,
                7162865966455330361,
                4126776455598920601,
                -2499035158167435398,
                5189091634025675901,
                -6339876780267077086,
                4591757445335432526,
                -121157961670426730,
                3959813452224811408,
                -6602763524395334709,
            ],
            values_seed_min_i64
        );
    }
}