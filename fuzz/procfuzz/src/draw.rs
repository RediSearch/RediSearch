/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

const GOLDEN_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

/// The random decisions that make an op. A draw replays recorded values first,
/// then draws fresh ones, and records every value it returns, so an op can be
/// made again from its record, or from an edited one.
///
/// 0 is the simplest choice: a model orders its alternatives simplest first,
/// so that lowering a recorded value simplifies the op.
#[derive(Clone, Debug)]
pub struct Draw {
    replay: Vec<u64>,
    at: usize,
    taken: Vec<u64>,
    seed: u64,
}

impl Draw {
    pub fn new(seed: u64) -> Draw {
        Draw::replaying(&[], seed)
    }

    pub fn replaying(values: &[u64], seed: u64) -> Draw {
        Draw {
            replay: values.to_vec(),
            at: 0,
            taken: Vec::new(),
            seed,
        }
    }

    /// A value below `n`, or 0 if `n` is 0.
    pub fn below(&mut self, n: u64) -> u64 {
        let raw = match self.replay.get(self.at) {
            Some(&value) => value,
            None => self.raw(),
        };
        self.at += 1;
        let value = raw % n.max(1);
        self.taken.push(value);
        value
    }

    /// A value from `low` to `high`, both included; `low` is the simplest.
    pub fn between(&mut self, low: u64, high: u64) -> u64 {
        low + self.below(high - low + 1)
    }

    /// True one time in `n`. False is the simpler choice, so an option that
    /// adds something is taken only when this is true.
    pub fn one_in(&mut self, n: u64) -> bool {
        self.below(n) + 1 == n.max(1)
    }

    /// An index into a slice of `len` items, or 0 if it is empty.
    pub fn index(&mut self, len: usize) -> usize {
        let value = self.below(u64::try_from(len).expect("fits"));
        usize::try_from(value).expect("below a usize")
    }

    /// Draws even when `items` is empty, so the draws after it keep their
    /// meaning.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        let at = self.index(items.len());
        items.get(at)
    }

    /// A fresh value, not recorded: a seed or a replacement for a record.
    pub(crate) fn raw(&mut self) -> u64 {
        // SplitMix64.
        self.seed = self.seed.wrapping_add(GOLDEN_GAMMA);
        let mut z = self.seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub fn into_taken(self) -> Vec<u64> {
        self.taken
    }
}
