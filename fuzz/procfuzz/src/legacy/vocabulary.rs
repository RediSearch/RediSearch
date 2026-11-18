/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rand::RngCore;

use crate::legacy::program::{Bytes, Literal, Program, TypedOp};

/// The words and numbers programs are built from.
#[derive(Clone, Debug)]
pub struct Vocabulary {
    words: Vec<Bytes>,
    numbers: Vec<i64>,
}

impl Vocabulary {
    pub fn new(words: &[&str], numbers: &[i64]) -> Vocabulary {
        Vocabulary {
            words: words.iter().map(|&w| w.into()).collect(),
            numbers: numbers.to_vec(),
        }
    }

    /// Adds every literal in `program`, so values that reached new code get
    /// reused.
    pub fn learn<T: TypedOp>(&mut self, program: &Program<T>) {
        for op in &program.ops {
            for literal in op.kind.clone().literals() {
                match literal {
                    Literal::Word(word) => self.words.push(word.clone()),
                    Literal::Number(n) => self.numbers.push(*n),
                }
            }
        }
    }

    pub fn word(&self, rng: &mut impl RngCore) -> Bytes {
        self.words[below(rng, self.words.len())].clone()
    }

    pub fn number(&self, rng: &mut impl RngCore) -> i64 {
        self.numbers[below(rng, self.numbers.len())]
    }
}

/// A draw below `n` that uses only `next_u64`, so a target that expands ops
/// from a seeded generator gets the same expansion with any `rand` version.
pub fn below(rng: &mut impl RngCore, n: usize) -> usize {
    let n = u64::try_from(n).expect("fits");
    usize::try_from(rng.next_u64() % n).expect("below a usize")
}
