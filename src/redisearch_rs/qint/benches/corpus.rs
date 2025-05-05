#![cfg(feature = "test-extensions")]
use std::{
    fmt::Debug,
    io::{Read, Write},
    path::PathBuf,
};

use proptest::{
    prelude::{BoxedStrategy, Strategy},
    test_runner::{TestRng, TestRunner},
};

use qint::test_utils::{PropEncoding, qint_encoding};

pub struct Corpus {}

impl Corpus {
    pub fn get_raw() -> Vec<PropEncoding> {
        // check if filepath exists
        let path = Self::filepath();
        if path.exists() {
            // read from file
            deserialize(&path)
        } else {
            // generate and save the data
            Self::generate_and_save()
        }
    }

    pub fn get_sliced() -> (Vec<[u32; 2]>, Vec<[u32; 3]>, Vec<[u32; 4]>) {
        let mut data = Self::get_raw();

        // we best work with sliced data
        data.sort_by(|a, b| {
            let a_rank = match a {
                PropEncoding::QInt2(_) => 1,
                PropEncoding::QInt3(_) => 2,
                PropEncoding::QInt4(_) => 3,
            };
            let b_rank = match b {
                PropEncoding::QInt2(_) => 1,
                PropEncoding::QInt3(_) => 2,
                PropEncoding::QInt4(_) => 3,
            };
            a_rank.cmp(&b_rank)
        });

        // get the start index of the 3 byte data
        let idx3 = data
            .iter()
            .enumerate()
            .find(|(_, x)| matches!(x, PropEncoding::QInt3(_)))
            .map_or(usize::MAX, |(idx, _)| idx);

        // get the start index of the 4 byte data
        let idx4 = data
            .iter()
            .enumerate()
            .find(|(_, x)| matches!(x, PropEncoding::QInt4(_)))
            .map_or(usize::MAX, |(idx, _)| idx);

        // sanity checks
        if idx3 == usize::MAX || idx4 == usize::MAX {
            unreachable!(
                "Could not find 3 or 4 byte data if that happens try another seed but shouldn't happen"
            );
        }
        println!("idx3: {}, idx4: {}", idx3, idx4);
        //println!("data: {:?}", data);

        // generate 3 slices for the different sizes
        let slice2 = data[0..idx3 as usize]
            .iter()
            .map(|x| {
                if let PropEncoding::QInt2((values, _)) = x {
                    values.to_owned()
                } else {
                    unreachable!("Expected QInt2")
                }
            })
            .collect::<Vec<_>>();
        let slice3 = data[(idx3 + 1)..idx4]
            .iter()
            .map(|x| {
                if let PropEncoding::QInt3((values, _)) = x {
                    values.to_owned()
                } else {
                    unreachable!("Expected QInt3")
                }
            })
            .collect::<Vec<_>>();
        let slice4 = data[idx4 + 1..]
            .iter()
            .map(|x| {
                if let PropEncoding::QInt4((values, _)) = x {
                    values.to_owned()
                } else {
                    unreachable!("Expected QInt4")
                }
            })
            .collect::<Vec<_>>();

        (slice2, slice3, slice4)
    }

    fn filepath() -> PathBuf {
        let mut path = PathBuf::new();
        path.push("data");
        path.push("proptest.json");
        path
    }

    fn generate_and_save() -> Vec<PropEncoding> {
        let data = generate_data();
        serialize(&Self::filepath(), &data);
        data
    }
}

// Helper function to generate values from a BoxedStrategy
fn generate_values_from_strategy<T: Debug>(strategy: BoxedStrategy<T>, count: usize) -> Vec<T> {
    let mut runner = TestRunner::default();
    // this is a fixed seed for reproducibility
    let seed: [u8; 16] = [
        0xF0, 0x14, 0x23, 0x66, 0x39, 0x92, 0xA3, 0x11, 0xDB, 0x11, 0x58, 0xC0, 0xA1, 0xB2, 0xC3,
        0xD4,
    ];
    *runner.rng() = TestRng::from_seed(proptest::test_runner::RngAlgorithm::XorShift, &seed);

    // with the setup rng, we can generate values according to the strategy
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let value = strategy.new_tree(&mut runner).unwrap().current();
        values.push(value);
    }
    values
}

fn generate_data() -> Vec<PropEncoding> {
    generate_values_from_strategy(qint_encoding(), 500)
}

fn serialize(filepath: &PathBuf, data: &[PropEncoding]) {
    let mut file = std::fs::File::create(filepath).unwrap();
    // serialize with serde json
    let json = serde_json::to_string(data).unwrap();
    // write to file
    file.write_all(json.as_bytes()).unwrap();
}

fn deserialize(filepath: &PathBuf) -> Vec<PropEncoding> {
    // read from file
    let mut file = std::fs::File::open(filepath).unwrap();
    let mut buf = String::new();
    file.read_to_string(&mut buf).unwrap();
    // parse with serde json
    let data: Vec<PropEncoding> = serde_json::from_str(&buf).unwrap();
    data
}
