/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Deduplication identity for the `COLLECT … SORTBY DISTINCT` path.
//!
//! [`DistinctKey`] is the digest-only identity that keys the single-ended
//! `PriorityQueue` used by [`Storage::DistinctHeap`][super::storage::Storage].
//! The projected row it deduplicates lives in the queue's *priority/value*
//! half (a [`HeapEntry`][super::heap::HeapEntry] compared by sort key only), so
//! a winning duplicate replaces both the ranking key and the emitted row.

use std::hash::{Hash, Hasher};

use rlookup::{RLookupKey, RLookupRow};
use value::Value;

use super::dedup_hash::DistinctHasher;

/// The DISTINCT dedup identity: a precomputed FNV-1a digest of the projected
/// fields.
///
/// Built once by the caller via [`dedup_hash`] / [`dedup_hash_row`] and used as
/// the queue *item*, so neither hashing nor equality in the priority queue ever
/// re-walks a row. The caller — not `DistinctKey` — chooses *which* fields the
/// digest covers, because the identity is the **projected fields only** while
/// the stored row may also carry sort-key columns in some configs.
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

/// Compute the dedup digest from the projected `(name, value)` fields, walked
/// in a stable field order. Each field folds its name (always) and its value
/// (unless absent or `Null`); see [`DistinctHasher`].
///
/// Used by every config: the caller selects which fields are projected and
/// supplies each one's name and current value (`None` when the field is absent
/// from this row). Hashing by name lets the identity cover exactly the
/// projected fields regardless of which configs append sort-key columns to the
/// stored row.
///
/// Exposed (like the sibling [`Storage`][super::storage::Storage] /
/// [`EntryKey`][super::heap::EntryKey] types) so the crate's integration tests
/// in `reducers/tests/` can exercise the digest directly.
pub fn dedup_hash<'a>(fields: impl IntoIterator<Item = (&'a [u8], Option<&'a Value>)>) -> u64 {
    let mut hasher = DistinctHasher::default();
    for (name, value) in fields {
        hasher.push_field(name, value);
    }
    hasher.finish()
}

/// Hash the projected fields of `row` named by `keys`, in `keys` order. Each
/// key contributes its name and its current value (or `None` if absent), via
/// [`dedup_hash`].
pub fn dedup_hash_row<'a>(
    row: &'a RLookupRow<'static>,
    keys: impl IntoIterator<Item = &'a RLookupKey<'a>>,
) -> u64 {
    dedup_hash(
        keys.into_iter()
            .map(|k| (k.name().to_bytes(), row.get(k).map(|v| &**v))),
    )
}
