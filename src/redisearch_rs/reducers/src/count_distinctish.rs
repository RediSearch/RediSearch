/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The HyperLogLog reducers: `COUNT_DISTINCTISH`, and the `HLL` / `HLL_SUM` pair
//! the coordinator splits it into, `HLL` on the shards and `HLL_SUM` merging
//! their registers.

use std::hash::{DefaultHasher, Hasher};

use hyperloglog::HyperLogLog8;
use rlookup::{RLookupKey, RLookupRow};
use value::{SharedValue, Value};

use crate::accumulator::Accumulator;

/// Mixed into every value's hash. Shards, and server versions, must agree on it
/// for `HLL_SUM` to merge their registers.
pub const STABLE_SEED: u64 = 0x5f61767a;

type Hll = HyperLogLog8<Value, StableHasher>;

/// [`value::hash::hash_stable`] with [`STABLE_SEED`], its two halves XOR-ed into
/// 32 bits: the hash every version's per-shard registers are built from.
pub struct StableHasher(DefaultHasher);

impl Default for StableHasher {
    fn default() -> Self {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(STABLE_SEED);
        Self(hasher)
    }
}

impl Hasher for StableHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }

    fn finish(&self) -> u64 {
        u64::from(hash32::Hasher::finish32(self))
    }
}

impl hash32::Hasher for StableHasher {
    fn finish32(&self) -> u32 {
        let hash = self.0.finish();
        (hash as u32) ^ ((hash >> 32) as u32)
    }
}

/// A rank is at most the number of hash bits left after the register index, plus one.
const MAX_REGISTER: u8 = 32 - Hll::bits() + 1;

/// The serialized header: `flags: u32` (unused, always 0) then `bits: u8`, packed
/// and in native byte order, followed by the `1 << bits` registers.
const FLAGS_LEN: usize = size_of::<u32>();
const HEADER_LEN: usize = FLAGS_LEN + size_of::<u8>();

/// What [`CountDistinctish`] yields per group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Output {
    /// The estimated number of distinct values (`COUNT_DISTINCTISH`).
    Count,
    /// The serialized registers, for `HLL_SUM` to merge (`HLL`).
    Registers,
}

/// `COUNT_DISTINCTISH` or `HLL`: a HyperLogLog of the group's values.
pub struct CountDistinctish<'a> {
    key: &'a RLookupKey<'a>,
    output: Output,
}

impl<'a> CountDistinctish<'a> {
    pub const fn new(key: &'a RLookupKey<'a>, output: Output) -> Self {
        Self { key, output }
    }
}

impl Accumulator for CountDistinctish<'_> {
    type State = Hll;

    fn init(&self) -> Hll {
        Hll::new()
    }

    fn add(&self, hll: &mut Hll, row: &RLookupRow<'_>) {
        if let Some(value) = row.get(self.key).filter(|value| !value.is_null_static()) {
            hll.add(value);
        }
    }

    fn finalize(&self, hll: &Hll) -> SharedValue {
        match self.output {
            Output::Count => SharedValue::new_num(hll.count() as f64),
            Output::Registers => {
                // One more byte for the terminator `SharedValue::new_string` appends.
                let mut buf = Vec::with_capacity(HEADER_LEN + Hll::size() + 1);
                buf.extend_from_slice(&0u32.to_ne_bytes());
                buf.push(Hll::bits());
                buf.extend_from_slice(hll.registers());
                SharedValue::new_string(buf)
            }
        }
    }
}

/// `HLL_SUM`: the estimated number of distinct values across the registers
/// serialized by `HLL`.
///
/// Values other than strings holding the registers of a [`HyperLogLog8`], each a
/// rank a 32-bit hash can produce, are skipped.
pub struct HllSum<'a> {
    key: &'a RLookupKey<'a>,
}

impl<'a> HllSum<'a> {
    pub const fn new(key: &'a RLookupKey<'a>) -> Self {
        Self { key }
    }
}

impl Accumulator for HllSum<'_> {
    /// Allocated on the first well-formed value, so groups without one cost only a
    /// pointer.
    type State = Option<Box<Hll>>;

    fn init(&self) -> Option<Box<Hll>> {
        None
    }

    fn add(&self, state: &mut Option<Box<Hll>>, row: &RLookupRow<'_>) {
        let Some(other) = row
            .get(self.key)
            .and_then(|value| value.as_str_bytes())
            .and_then(parse_registers)
        else {
            return;
        };
        match state {
            Some(hll) => hll.merge(&other),
            None => *state = Some(Box::new(other)),
        }
    }

    fn finalize(&self, state: &Option<Box<Hll>>) -> SharedValue {
        SharedValue::new_num(state.as_ref().map_or(0, |hll| hll.count()) as f64)
    }
}

fn parse_registers(buf: &[u8]) -> Option<Hll> {
    let (header, registers) = buf.split_at_checked(HEADER_LEN)?;
    if header[FLAGS_LEN] != Hll::bits() || registers.iter().any(|&r| r > MAX_REGISTER) {
        return None;
    }
    Hll::try_from(registers).ok()
}
