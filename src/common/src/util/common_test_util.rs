use rand::rngs::SmallRng;
use rand::{rng, Rng, SeedableRng};
use std::env;

pub fn get_rng() -> SmallRng {
    match env::var("CRUSTY_SEED") {
        Ok(seed_str) => match seed_str.parse::<u64>() {
            Ok(seed) => {
                log::debug!("Using seed from CRUSTY_SEED: {}", seed);
                SmallRng::seed_from_u64(seed)
            }
            Err(_) => {
                let seed = rng().random::<u64>();
                log::debug!("Failed to parse CRUSTY_SEED, using random seed: {}", seed);
                SmallRng::seed_from_u64(seed)
            }
        },
        Err(_) => {
            let seed = rng().random::<u64>();
            log::debug!("No CRUSTY_SEED provided, using random seed: {}", seed);
            SmallRng::seed_from_u64(seed)
        }
    }
}

pub fn init() {
    // To change the log level for tests change the filter_level
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        //.filter_level(log::LevelFilter::Info)
        .try_init();
}

pub fn get_random_byte_vec(rng: &mut SmallRng, n: usize) -> Vec<u8> {
    let random_bytes: Vec<u8> = (0..n).map(|_| rng.random::<u8>()).collect();
    random_bytes
}

// removed strip point here

pub fn gen_random_int<T>(rng: &mut SmallRng, min: T, max: T) -> T
where
    T: rand::distr::uniform::SampleUniform,
{
    // let mut rng = rng();
    rng.sample(rand::distr::uniform::Uniform::new_inclusive(min, max).unwrap())
}

#[allow(dead_code)]
pub fn get_random_vec_of_byte_vec(
    rng: &mut SmallRng,
    n: usize,
    min_size: usize,
    max_size: usize,
) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    assert!(max_size >= min_size);
    let size_diff = max_size - min_size;
    for _ in 0..n {
        let size = if size_diff == 0 {
            min_size
        } else {
            rng.random_range(min_size..size_diff + min_size)
        };
        res.push((0..size).map(|_| rng.random::<u8>()).collect());
    }
    res
}

#[allow(dead_code)]
/// get_ascending_vec_of_byte_vec_0x: this function will create Vec<Vec<u8>>
/// the value of u8 in Vec<u8> is ascending from 1 to 16 (0x10) for each Vec<u8>
pub fn get_ascending_vec_of_byte_vec_0x(
    rng: &mut SmallRng,
    n: usize,
    min_size: usize,
    max_size: usize,
) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    assert!(max_size >= min_size);
    let size_diff = max_size - min_size;
    let mut elements = 1;
    for _ in 0..n {
        let size = if size_diff == 0 {
            min_size
        } else {
            rng.random_range(min_size..size_diff + min_size)
        };
        res.push((0..size).map(|_| elements).collect());
        elements += 1;
        if elements >= 16 {
            elements = 1;
        }
    }
    res
}

#[allow(dead_code)]
/// get_ascending_vec_of_byte_vec_0x: this function will create Vec<Vec<u8>>
/// the value of u8 in Vec<u8> is ascending from 1 to 255 (0x100) for each Vec<u8>
pub fn get_ascending_vec_of_byte_vec_02x(
    rng: &mut SmallRng,
    n: usize,
    min_size: usize,
    max_size: usize,
) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    assert!(max_size >= min_size);
    let size_diff = max_size - min_size;
    let mut elements = 1;
    for _ in 0..n {
        let size = if size_diff == 0 {
            min_size
        } else {
            rng.random_range(min_size..size_diff + min_size)
        };
        res.push((0..size).map(|_| elements).collect());
        if elements == 255 {
            elements = 1;
        } else {
            elements += 1;
        }
    }
    res
}

#[allow(dead_code)]
pub fn compare_unordered_byte_vecs(a: &[Vec<u8>], mut b: Vec<Vec<u8>>) -> bool {
    // Quick check
    if a.len() != b.len() {
        trace!("Vecs are different lengths");
        return false;
    }
    // check if they are the same ordered
    let non_match_count = a
        .iter()
        .zip(b.iter())
        .filter(|&(j, k)| j[..] != k[..])
        .count();
    if non_match_count == 0 {
        return true;
    }

    // Now check if they are out of order
    for x in a {
        let pos = b.iter().position(|y| y[..] == x[..]);
        match pos {
            None => {
                //Was not found, not equal
                trace!("Was not able to find value for {:?}", x);
                return false;
            }
            Some(idx) => {
                b.swap_remove(idx);
            }
        }
    }
    if !b.is_empty() {
        trace!("Values in B that did not match a {:?}", b);
    }
    //since they are the same size, b should be empty
    b.is_empty()
}
// end strip point

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_random_vec_bytes() {
        let n = 10_000;
        let mut min = 50;
        let mut max = 75;
        let mut rng = get_rng();
        let mut data = get_random_vec_of_byte_vec(&mut rng, n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            assert!(
                x.len() >= min && x.len() < max,
                "x's len {} was not withing [{},{}]",
                x.len(),
                min,
                max
            );
        }

        min = 134;
        max = 134;
        data = get_random_vec_of_byte_vec(&mut rng, n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            assert!(x.len() == min && x.len() == max);
        }

        min = 0;
        max = 14;
        data = get_random_vec_of_byte_vec(&mut rng, n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            assert!(
                x.len() >= min && x.len() < max,
                "x's len {} was not withing [{},{}]",
                x.len(),
                min,
                max
            );
        }
    }

    #[test]
    fn test_ascd_random_vec_bytes() {
        let mut rng = get_rng();
        let n = 10000;
        let mut min = 50;
        let mut max = 75;
        let mut data = get_ascending_vec_of_byte_vec_02x(&mut rng, n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            if x.len() < min || x.len() >= max {
                println!("!!!{:?}", x);
            }
            assert!(
                x.len() >= min && x.len() < max,
                "x's len {} was not withing [{},{}]",
                x.len(),
                min,
                max
            );
        }

        min = 13;
        max = 14;
        data = get_ascending_vec_of_byte_vec_02x(&mut rng, n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            if x.len() != min || x.len() != max {
                println!("!!!{:?}", x);
                println!("!!!x.len(){:?}", x.len());
                println!("111{}", x.len() == min && x.len() == max);
            }
            assert!(x.len() == min && x.len() == max - 1);
        }

        min = 0;
        max = 14;
        data = get_ascending_vec_of_byte_vec_02x(&mut rng, n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            assert!(
                x.len() >= min && x.len() < max,
                "x's len {} was not withing [{},{}]",
                x.len(),
                min,
                max
            );
        }
    }
}
