pub mod distribution;
pub mod faster_random;
pub mod values;

/// A structure wrapping [java_random::Random].
/// In addition to methods exposed by [java_random::Random],
/// it exposes [next_gaussian] method as well.
struct Random {
    delegate: java_random::Random,
    next_next_gaussian: Option<f64>,
}

impl Random {
    fn with_seed(seed: u64) -> Self {
        Self {
            delegate: java_random::Random::with_seed(seed),
            next_next_gaussian: None,
        }
    }

    fn set_seed(&mut self, seed: u64) {
        self.delegate.set_seed(seed);
        self.next_next_gaussian = None
    }

    fn next_gaussian(&mut self) -> f64 {
        // See: https://docs.oracle.com/javase/8/docs/api/java/util/Random.html#nextGaussian--.
        if let Some(value) = self.next_next_gaussian.take() {
            return value;
        }

        let mut v1 = 2f64 * self.delegate.next_double() - 1f64;
        let mut v2 = 2f64 * self.delegate.next_double() - 1f64;
        let mut s = v1 * v1 + v2 * v2;

        loop {
            if s != 0f64 && s < 1f64 {
                break;
            }
            v1 = 2f64 * self.delegate.next_double() - 1f64;
            v2 = 2f64 * self.delegate.next_double() - 1f64;
            s = v1 * v1 + v2 * v2;
        }

        let multiplier = rust_strictmath::sqrt(-2f64 * rust_strictmath::log(s) / s);
        self.next_next_gaussian = Some(v2 * multiplier);
        v1 * multiplier
    }

    fn next_long(&mut self) -> i64 {
        self.delegate.next_long()
    }

    fn next_double(&mut self) -> f64 {
        self.delegate.next_double()
    }
}

#[cfg(test)]
mod tests {
    use super::Random;

    #[test]
    fn next_gaussian_test() {
        let mut r = Random::with_seed(0);

        let samples_seed_zero = (0..100).map(|_| r.next_gaussian()).collect::<Vec<_>>();

        assert_eq!(
            vec![
                0.8025330637390305,
                -0.9015460884175122,
                2.080920790428163,
                0.7637707684364894,
                0.9845745328825128,
                -1.6834122587673428,
                -0.027290262907887285,
                0.11524570286202315,
                -0.39016704137993785,
                -0.6433888131264491,
                0.052460907198835775,
                0.5213420769298895,
                -0.8239670026881707,
                0.26071819402835644,
                -0.4529877558422544,
                1.4031473817209366,
                0.27113061707020236,
                -0.007054015349837401,
                0.9049586994113287,
                0.8568542481006806,
                0.3723340814425109,
                0.3976728390023819,
                0.06294576961546386,
                0.9414599976474556,
                0.44110379103508873,
                -0.7318797311599887,
                -0.01176361185227962,
                -0.15736219614735453,
                -0.5822582291186266,
                -0.2059701784999411,
                -0.39990122591137445,
                0.8913156150655253,
                0.41076063425965825,
                -1.1712365002966285,
                -0.3905082189100106,
                0.49014040388330665,
                0.9597752538041666,
                0.7523861952143763,
                -0.657956415573505,
                0.6450323331598297,
                -0.3154523215417022,
                1.054894794114192,
                0.5957831787424875,
                1.0225509680217193,
                -2.3561969031359187,
                -1.5250681153426493,
                1.1808572722180044,
                0.006140951070945433,
                -0.13698941007400853,
                -0.42220793207202106,
                -0.016531497888100873,
                0.47777913386931326,
                0.009669260406608,
                -1.6916933619953234,
                -0.4612391930609103,
                0.8417474901759435,
                -0.24190484659992056,
                -0.6840379551499387,
                -1.9165999902657032,
                0.31535035923912047,
                0.40492223630241114,
                -0.21540008450999143,
                0.9562519045137733,
                0.28075077761104705,
                2.213808355073481,
                -0.04822564522107575,
                1.0912726657731104,
                3.368920447474639,
                -2.2539651440354036,
                -0.3771767601486389,
                1.1843177383489063,
                -1.0290706651489738,
                1.4480202027200353,
                0.6331923385168294,
                -1.373517357637752,
                -0.5752759557054007,
                0.6247797550596169,
                -0.3219699342435675,
                -0.6775351520679921,
                1.787353499904202,
                -1.0887912913806148,
                -1.748512840714025,
                0.843237738914588,
                1.1803128213839933,
                0.913129913077056,
                0.1851047755111892,
                0.11285747712200198,
                -1.9730055017835015,
                0.038523228113098745,
                0.4530426026347499,
                0.07791503650933558,
                -0.3241940958618163,
                1.4528298587991246,
                1.0014318030252782,
                0.7571154116686983,
                0.5743639835657467,
                1.0288490019427352,
                -1.4490428783730553,
                0.5994894930762275,
                -0.7417966776641425,
            ],
            samples_seed_zero
        );

        r.set_seed(0xdeadcafe);
        let samples_seed_deadcafe = (0..100).map(|_| r.next_gaussian()).collect::<Vec<_>>();

        assert_eq!(
            vec![
                -0.37142873501738216,
                -0.2857289586826198,
                0.4591723704429033,
                0.852886858766378,
                -0.6109493184730145,
                -0.007497042587924218,
                1.28557330679665,
                0.08799601778172363,
                3.0133815720119346,
                0.7740125186218345,
                -0.5461120325081265,
                -2.0778225296986057,
                -0.5636121240995228,
                -0.5574313521948719,
                1.3078635185269982,
                0.37192037197961847,
                0.36435434402607314,
                -1.2677051911427535,
                -0.6818467647521901,
                -0.06174267245333722,
                0.8430527052887855,
                -0.3373123819135429,
                0.6871601475241282,
                1.6371741222020015,
                1.4279952434692873,
                0.20919500963056178,
                -1.82660243238412,
                0.8133501531198295,
                -0.9012957125791711,
                1.1487400485939425,
                -0.0019779970497020498,
                -0.8611246071848342,
                2.009211493753926,
                -0.43515131659607587,
                0.39631677769752327,
                0.19274321856695995,
                2.856476503084085,
                0.3766445913362024,
                1.3309107244124088,
                0.6294707488789295,
                0.24658833747350845,
                0.42892931695085607,
                0.7865558639022724,
                -0.17989635106616758,
                -0.34886462516446737,
                -2.3053572227762045,
                0.025692188939431475,
                -0.11943405199553274,
                0.988248116934403,
                0.10109994811212135,
                -0.3770819360499536,
                0.5908065212843225,
                -0.6439575804706476,
                0.37734234369445946,
                -0.8427988907914478,
                -0.5227546008361261,
                -0.8137407910894721,
                -2.429720043175947,
                0.43209437029017606,
                -0.9616705341358877,
                0.2429255203677865,
                -1.4277111645610354,
                -0.7409970543739163,
                -1.2032742630053053,
                0.4955425771108176,
                -0.5324380507946169,
                -0.6608551250551176,
                0.10852482337614731,
                -0.4015433105497661,
                -1.3902292594954082,
                -0.2549339454505033,
                -0.809696229000899,
                0.12455722615902093,
                -0.419783307641969,
                1.0305946289547407,
                0.9499155992549504,
                -0.8363828106755771,
                -0.7397510037343193,
                0.9091717741302867,
                -0.3246737997451562,
                -0.24658787585105557,
                0.31627350631248374,
                1.5376057213131178,
                -0.19443727027899665,
                0.5269273007538314,
                0.27138165217123866,
                0.11425983502970814,
                -0.5393066808424779,
                0.2190539485808326,
                0.5125085364745283,
                0.0978306366440497,
                0.938182519386583,
                -0.0193932796023017,
                -0.9246991907031391,
                -0.10878731285484698,
                0.7993866962466235,
                -1.792856136116364,
                0.7324124324392213,
                -1.6463106153596478,
                1.5434303183778717,
            ],
            samples_seed_deadcafe
        );

        r.set_seed(u64::MIN);
        let samples_seed_min = (0..100).map(|_| r.next_gaussian()).collect::<Vec<_>>();

        assert_eq!(
            vec![
                0.8025330637390305,
                -0.9015460884175122,
                2.080920790428163,
                0.7637707684364894,
                0.9845745328825128,
                -1.6834122587673428,
                -0.027290262907887285,
                0.11524570286202315,
                -0.39016704137993785,
                -0.6433888131264491,
                0.052460907198835775,
                0.5213420769298895,
                -0.8239670026881707,
                0.26071819402835644,
                -0.4529877558422544,
                1.4031473817209366,
                0.27113061707020236,
                -0.007054015349837401,
                0.9049586994113287,
                0.8568542481006806,
                0.3723340814425109,
                0.3976728390023819,
                0.06294576961546386,
                0.9414599976474556,
                0.44110379103508873,
                -0.7318797311599887,
                -0.01176361185227962,
                -0.15736219614735453,
                -0.5822582291186266,
                -0.2059701784999411,
                -0.39990122591137445,
                0.8913156150655253,
                0.41076063425965825,
                -1.1712365002966285,
                -0.3905082189100106,
                0.49014040388330665,
                0.9597752538041666,
                0.7523861952143763,
                -0.657956415573505,
                0.6450323331598297,
                -0.3154523215417022,
                1.054894794114192,
                0.5957831787424875,
                1.0225509680217193,
                -2.3561969031359187,
                -1.5250681153426493,
                1.1808572722180044,
                0.006140951070945433,
                -0.13698941007400853,
                -0.42220793207202106,
                -0.016531497888100873,
                0.47777913386931326,
                0.009669260406608,
                -1.6916933619953234,
                -0.4612391930609103,
                0.8417474901759435,
                -0.24190484659992056,
                -0.6840379551499387,
                -1.9165999902657032,
                0.31535035923912047,
                0.40492223630241114,
                -0.21540008450999143,
                0.9562519045137733,
                0.28075077761104705,
                2.213808355073481,
                -0.04822564522107575,
                1.0912726657731104,
                3.368920447474639,
                -2.2539651440354036,
                -0.3771767601486389,
                1.1843177383489063,
                -1.0290706651489738,
                1.4480202027200353,
                0.6331923385168294,
                -1.373517357637752,
                -0.5752759557054007,
                0.6247797550596169,
                -0.3219699342435675,
                -0.6775351520679921,
                1.787353499904202,
                -1.0887912913806148,
                -1.748512840714025,
                0.843237738914588,
                1.1803128213839933,
                0.913129913077056,
                0.1851047755111892,
                0.11285747712200198,
                -1.9730055017835015,
                0.038523228113098745,
                0.4530426026347499,
                0.07791503650933558,
                -0.3241940958618163,
                1.4528298587991246,
                1.0014318030252782,
                0.7571154116686983,
                0.5743639835657467,
                1.0288490019427352,
                -1.4490428783730553,
                0.5994894930762275,
                -0.7417966776641425,
            ],
            samples_seed_min
        );

        r.set_seed(u64::MAX);
        let samples_seed_max = (0..100).map(|_| r.next_gaussian()).collect::<Vec<_>>();

        assert_eq!(
            vec![
                1.7853314409882288,
                -0.9204169061847902,
                0.4869392448030407,
                0.4568888042977182,
                1.6999200838250526,
                0.4522041234427653,
                0.1584891839707202,
                0.6871970039216588,
                0.6043016467728685,
                -1.3274576230778463,
                -0.4714219908792415,
                0.45305907110809684,
                -0.2719721293511846,
                -0.4077479322563506,
                0.12963477190797204,
                0.5989389150968187,
                1.568216146471534,
                -0.44389782683274537,
                -0.5446725078013468,
                0.505993989113119,
                -1.41969042799245,
                -1.6035278755540598,
                -0.3446898587179721,
                1.0818033491829633,
                -1.5108240669379067,
                -0.5106253270979619,
                2.2835814375266175,
                -0.8766511588721186,
                0.4887076911079266,
                -0.4177129839415986,
                0.08751079347244697,
                0.5380465524263339,
                -0.11830046070621286,
                0.5958612982999119,
                -1.1656180744613576,
                -0.9210384397272062,
                -0.9539045751854309,
                -1.5054110687412372,
                0.6444404082324894,
                -1.2046109360416113,
                -0.09771454358412592,
                0.25825202596720626,
                -0.8085347457744689,
                -0.12116540181634104,
                -1.6995400080844005,
                0.8710437976938143,
                -0.8039907845312686,
                0.3928335336828287,
                -1.8064149745343892,
                -0.2931037506877832,
                -1.152397417208955,
                -0.5952764025093765,
                -0.09457126721862644,
                1.8504206310793319,
                2.0764844071055077,
                0.17599160801065653,
                0.27297236554057547,
                0.7412383554143049,
                0.26885033192842345,
                0.948294471285929,
                1.7606398277346884,
                1.6848759892214153,
                1.4141175545811555,
                0.5662156234188576,
                0.2788663291906919,
                -0.7371505268729761,
                0.926100734281395,
                1.1433546032389539,
                -1.461333343587599,
                0.28247271834418153,
                -0.015743266789281785,
                0.8300506283282397,
                0.5639415560227957,
                -1.381909931977721,
                -0.24049492777972456,
                0.31401350722414345,
                -0.019428262826375907,
                -0.5062098421765737,
                1.962112918019726,
                1.7391655549290834,
                1.1553302209274914,
                0.2150123492937442,
                1.9879369783222887,
                0.17777081284690138,
                0.44552750339647296,
                1.021158340760682,
                -1.0466004442258205,
                -0.12028345483838856,
                -0.7939173816432622,
                1.657765036846459,
                1.1199671167361285,
                -0.9812745438393444,
                0.060038254402153116,
                -0.03648838366363723,
                1.0715548475032628,
                1.3588967519296595,
                0.3941491500090559,
                1.4764971967746034,
                1.4984919664195802,
                1.0628534713413902,
            ],
            samples_seed_max
        );
    }
}
