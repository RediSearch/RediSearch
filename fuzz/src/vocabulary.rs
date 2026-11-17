/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The words and numbers programs are built from, and how field values are
//! drawn from them. `Populate` expands through [`Vocabulary::fixed`], so
//! changing it or the draws changes what existing programs write.

use rand::RngCore;

use crate::program::{Bytes, FieldKind, Literal, Program};

const WORDS: [&str; 14] = [
    "hello",
    "world",
    "hell",
    "running",
    "runs",
    "red",
    "green",
    "blue",
    "the",
    "a",
    "x-y",
    "hello world",
    "é",
    "日本語",
];
const NUMBERS: [i64; 10] = [0, 1, -1, 7, 100, -100, 1 << 31, 1 << 53, i64::MAX, i64::MIN];
const SMALL_NUMBERS: usize = 10_000;

pub struct Vocabulary {
    words: Vec<Bytes>,
    numbers: Vec<i64>,
}

impl Vocabulary {
    pub fn fixed() -> Vocabulary {
        Vocabulary {
            words: WORDS.iter().map(|&w| w.into()).collect(),
            numbers: NUMBERS.to_vec(),
        }
    }

    /// The fixed vocabulary plus every literal in `programs`, so values that
    /// reached new code get reused.
    pub fn with_literals_of(programs: &[&Program]) -> Vocabulary {
        let mut vocabulary = Vocabulary::fixed();
        for op in programs.iter().flat_map(|p| &p.ops) {
            for literal in op.kind.clone().literals() {
                match literal {
                    Literal::Word(word) => vocabulary.words.push(word.clone()),
                    Literal::Number(n) => vocabulary.numbers.push(*n),
                }
            }
        }
        vocabulary
    }

    pub fn word(&self, rng: &mut impl RngCore) -> Bytes {
        self.words[below(rng, self.words.len())].clone()
    }

    pub fn number(&self, rng: &mut impl RngCore) -> i64 {
        self.numbers[below(rng, self.numbers.len())]
    }

    /// A value for a field of `kind`, as `HSET` takes it. Tags are separated
    /// by commas.
    pub fn value(&self, kind: &FieldKind, rng: &mut impl RngCore) -> Bytes {
        let words = |separator: &str, most: usize, rng: &mut _| -> Bytes {
            let count = 1 + below(rng, most);
            let words: Vec<String> = (0..count)
                .map(|_| String::from_utf8_lossy(self.word(rng).as_slice()).into_owned())
                .collect();
            words.join(separator).into()
        };
        match kind {
            FieldKind::Text => words(" ", 3, rng),
            FieldKind::Tag => words(",", 2, rng),
            FieldKind::Numeric if below(rng, 2) == 0 => self.number(rng).to_string().into(),
            FieldKind::Numeric => below(rng, SMALL_NUMBERS).to_string().into(),
            FieldKind::Geo => {
                let mut coordinate = |degrees: i64| {
                    let span = usize::try_from(2 * degrees + 1).expect("positive");
                    let whole = i64::try_from(below(rng, span)).expect("small") - degrees;
                    format!("{whole}.{:04}", below(rng, 10_000))
                };
                let longitude = coordinate(179);
                format!("{longitude},{}", coordinate(84)).into()
            }
            FieldKind::Vector { dim, .. } => vector(*dim, rng),
        }
    }
}

/// `dim` FLOAT32 components between -10 and 10, in steps of a tenth.
pub fn vector(dim: u16, rng: &mut impl RngCore) -> Bytes {
    Bytes(
        (0..dim)
            .flat_map(|_| {
                let tenths = f32::from(u8::try_from(below(rng, 201)).expect("at most 200"));
                ((tenths - 100.0) / 10.0).to_le_bytes()
            })
            .collect(),
    )
}

/// Uses only `next_u64`, so `Populate` expansions do not depend on how a
/// `rand` version maps random bits to ranges.
fn below(rng: &mut impl RngCore, n: usize) -> usize {
    let n = u64::try_from(n).expect("fits");
    usize::try_from(rng.next_u64() % n).expect("below a usize")
}
