/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Deduplication identity for the `COLLECT … SORTBY DISTINCT` path.

use std::hash::{Hash, Hasher};

use fnv::Fnv64;
use rlookup::{RLookupKey, RLookupRow};
use value::Value;
use value::hash::hash_value;

/// The DISTINCT dedup identity: a precomputed FNV-1a digest of the projected
/// fields.
#[derive(Clone, Copy)]
pub struct DistinctKey(u64);

impl DistinctKey {
    /// Wrap a precomputed dedup digest.
    pub const fn new(hash: u64) -> Self {
        Self(hash)
    }
}

impl PartialEq for DistinctKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for DistinctKey {}

impl Hash for DistinctKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0);
    }
}

/// Compute the dedup digest from the projected field values, walked in a stable
/// field order, by folding each present value through the shared
/// [`value::hash::hash_value`].
pub fn dedup_hash<'a>(values: impl IntoIterator<Item = Option<&'a Value>>) -> u64 {
    let mut fnv = Fnv64::default();
    for value in values.into_iter().flatten() {
        hash_value(value, &mut fnv);
    }
    fnv.finish()
}

/// Hash the projected fields of `row` named by `keys`, in `keys` order.
pub fn dedup_hash_row<'a>(
    row: &'a RLookupRow<'static>,
    keys: impl IntoIterator<Item = &'a RLookupKey<'a>>,
) -> u64 {
    dedup_hash(keys.into_iter().map(|k| row.get(k).map(|v| &**v)))
}
