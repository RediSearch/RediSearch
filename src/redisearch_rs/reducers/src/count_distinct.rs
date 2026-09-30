/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The exact `COUNT_DISTINCT` reducer.

use rlookup::{RLookupKey, RLookupRow};
use rustc_hash::FxHashSet;
use value::SharedValue;

use crate::accumulator::Accumulator;

/// `COUNT_DISTINCT` of a property: the number of distinct [hashes][value::hash::hash]
/// of its values, skipping rows where it is missing or the static null.
pub struct CountDistinct<'a> {
    key: &'a RLookupKey<'a>,
}

impl<'a> CountDistinct<'a> {
    pub const fn new(key: &'a RLookupKey<'a>) -> Self {
        Self { key }
    }
}

impl Accumulator for CountDistinct<'_> {
    /// The hashes seen so far. They are already randomized per process, so the set
    /// only needs a cheap hasher of its own.
    type State = FxHashSet<u64>;

    fn init(&self) -> FxHashSet<u64> {
        FxHashSet::default()
    }

    fn add(&self, seen: &mut FxHashSet<u64>, row: &RLookupRow<'_>) {
        if let Some(value) = row.get(self.key).filter(|value| !value.is_null_static()) {
            seen.insert(value::hash::hash(value, 0));
        }
    }

    fn finalize(&self, seen: &FxHashSet<u64>) -> SharedValue {
        SharedValue::new_num(seen.len() as f64)
    }
}
