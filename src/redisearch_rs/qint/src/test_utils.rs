use proptest::{
    prelude::{BoxedStrategy, any},
    prop_compose, prop_oneof,
    strategy::Strategy,
};
use rand::{RngCore as _, SeedableRng as _};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PropEncoding {
    QInt2(([u32; 2], [usize; 2])),
    QInt3(([u32; 3], [usize; 3])),
    QInt4(([u32; 4], [usize; 4])),
}

impl PropEncoding {
    pub fn expected_written(&self) -> usize {
        match self {
            PropEncoding::QInt2((_, expected_size)) => expected_size.iter().sum::<usize>() + 1,
            PropEncoding::QInt3((_, expected_size)) => expected_size.iter().sum::<usize>() + 1,
            PropEncoding::QInt4((_, expected_size)) => expected_size.iter().sum::<usize>() + 1,
        }
    }

    pub fn leading_byte(&self) -> u8 {
        let mut leading_byte = 0b00000000u8;
        match &self {
            PropEncoding::QInt2((_, expected_size)) => {
                leading_byte |= (expected_size[0] - 1) as u8;
                leading_byte |= ((expected_size[1] - 1) << 2) as u8;
            }
            PropEncoding::QInt3((_, expected_size)) => {
                leading_byte |= (expected_size[0] - 1) as u8;
                leading_byte |= ((expected_size[1] - 1) << 2) as u8;
                leading_byte |= ((expected_size[2] - 1) << 4) as u8;
            }
            PropEncoding::QInt4((_, expected_size)) => {
                leading_byte |= (expected_size[0] - 1) as u8;
                leading_byte |= ((expected_size[1] - 1) << 2) as u8;
                leading_byte |= ((expected_size[2] - 1) << 4) as u8;
                leading_byte |= ((expected_size[3] - 1) << 6) as u8;
            }
        }
        leading_byte
    }
}

prop_compose! {
    // Generate a random number of bytes (1, 2, 3 or 4) f
    fn qint_varlen()(num_bytes in 1..=4usize, seed in any::<u64>()) -> (u32, usize) {
        let mut bytes = [0u8; 4];
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut forward_size = num_bytes;
        for item in bytes.iter_mut().take(num_bytes) {
            *item = (rng.next_u32() & 0x000000FF) as u8;
        }
        for idx in (1..num_bytes).rev() {
            if bytes[idx] == 0 {
                forward_size -= 1;
            } else {
                break;
            }
        }
        (u32::from_ne_bytes(bytes), forward_size)
    }
}

prop_compose! {
    fn qint2()(a in qint_varlen(), b in qint_varlen()) -> PropEncoding {
        PropEncoding::QInt2(([a.0, b.0], [a.1, b.1]))
    }
}

prop_compose! {
    fn qint3()(a in qint_varlen(), b in qint_varlen(), c in qint_varlen()) -> PropEncoding {
        PropEncoding::QInt3(([a.0, b.0, c.0], [a.1, b.1, c.1]))
    }
}

prop_compose! {
    fn qint4()(a in qint_varlen(), b in qint_varlen(), c in qint_varlen(), d in qint_varlen()) -> PropEncoding {
        PropEncoding::QInt4(([a.0, b.0, c.0, d.0], [a.1, b.1, c.1, d.1]))
    }
}

pub fn qint_encoding() -> BoxedStrategy<PropEncoding> {
    // Generate a random number of integers (2, 3, or 4) in a slice encapsulated in a PropEncoding enum
    prop_oneof![qint2(), qint3(), qint4()].boxed()
}
