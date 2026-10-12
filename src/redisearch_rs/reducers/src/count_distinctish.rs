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

use hyperloglog::HyperLogLog8;
use rlookup::{RLookupKey, RLookupRow};
use value::SharedValue;

use crate::accumulator::Accumulator;

/// Mixed into every value's hash. Shards, and server versions, must agree on it
/// for `HLL_SUM` to merge their registers.
const STABLE_SEED: u64 = 0x5f61767a;

type Hll = HyperLogLog8<()>;

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
            let hash = value::hash::hash_stable(value, STABLE_SEED);
            hll.add_precomputed_hash((hash as u32) ^ ((hash >> 32) as u32));
        }
    }

    fn finalize(&self, hll: &Hll) -> SharedValue {
        match self.output {
            Output::Count => SharedValue::new_num(hll.count() as f64),
            Output::Registers => {
                let mut buf = Vec::with_capacity(HEADER_LEN + Hll::size());
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
/// Values that are not strings holding well-formed registers of the same width are
/// skipped.
pub struct HllSum<'a> {
    key: &'a RLookupKey<'a>,
}

impl<'a> HllSum<'a> {
    pub const fn new(key: &'a RLookupKey<'a>) -> Self {
        Self { key }
    }
}

impl Accumulator for HllSum<'_> {
    type State = Hll;

    fn init(&self) -> Hll {
        Hll::new()
    }

    fn add(&self, hll: &mut Hll, row: &RLookupRow<'_>) {
        if let Some(other) = row
            .get(self.key)
            .and_then(|value| value.as_str_bytes())
            .and_then(parse_registers)
        {
            hll.merge(&other);
        }
    }

    fn finalize(&self, hll: &Hll) -> SharedValue {
        SharedValue::new_num(hll.count() as f64)
    }
}

fn parse_registers(buf: &[u8]) -> Option<Hll> {
    let (header, registers) = buf.split_at_checked(HEADER_LEN)?;
    if header[FLAGS_LEN] != Hll::bits() || registers.iter().any(|&r| r > MAX_REGISTER) {
        return None;
    }
    Hll::try_from(registers).ok()
}
