/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::hash::{Hash, Hasher};

use rlookup::{RLookupKey, RLookupRow};
use value::Value;

use super::dedup_hash::DistinctHasher;

/// A projected row paired with its precomputed dedup digest.
///
/// The wrapped [`RLookupRow`] is the payload yielded at finalize; `hash` is the
/// FNV-1a digest (built once by the caller via [`dedup_hash`] /
/// [`dedup_hash_row`]) that drives both [`Hash`] and [`Eq`], so neither
/// hashing nor comparison in the priority queue ever re-walks the row.
///
/// The caller — not `DistinctRow` — chooses *which* slots the digest covers,
/// because the dedup identity is the **projected fields only** and the stored
/// row may also carry sort-key columns in some configs.
pub struct DistinctRow {
    row: RLookupRow<'static>,
    hash: u64,
}

impl DistinctRow {
    /// Pair a projected `row` with its precomputed dedup digest.
    pub const fn from_parts(row: RLookupRow<'static>, hash: u64) -> Self {
        Self { row, hash }
    }

    /// The projected row, consumed at finalize.
    pub fn into_row(self) -> RLookupRow<'static> {
        self.row
    }
}

impl PartialEq for DistinctRow {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for DistinctRow {}

impl Hash for DistinctRow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
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
