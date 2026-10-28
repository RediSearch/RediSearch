/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The IDs of the documents an index has deleted, for the in-process GC.
//!
//! A doc table that tracks deletions records every ID it deletes into a
//! [`DeletedDocs`] from the moment the table is created, under the spec write
//! lock. A GC cycle takes a [`snapshot`](DeletedDocs::snapshot) under the read
//! lock, scans against it with no lock, and [`prune`](DeletedDocs::prune)s the
//! IDs it collected under the write lock. IDs deleted in between stay for the
//! next cycle, as do IDs the cycle could not collect.

pub use roaring::RoaringTreemap;

/// The set of deleted document IDs. See the [crate docs](crate).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DeletedDocs {
    ids: RoaringTreemap,
}

impl DeletedDocs {
    /// An empty set, for a doc table that has deleted nothing yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// A set holding `ids`.
    pub fn from_ids(ids: impl IntoIterator<Item = u64>) -> Self {
        Self {
            ids: ids.into_iter().collect(),
        }
    }

    /// Record `id` as deleted.
    pub fn record(&mut self, id: u64) {
        self.ids.insert(id);
    }

    /// A copy of the set, for a cycle to scan against.
    pub fn snapshot(&self) -> RoaringTreemap {
        self.ids.clone()
    }

    /// Forget the IDs a cycle collected every entry of.
    pub fn prune(&mut self, collected: &RoaringTreemap) {
        self.ids -= collected;
    }

    /// How many IDs the set holds.
    pub fn len(&self) -> u64 {
        self.ids.len()
    }

    /// Whether the set holds no ID.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}
