/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The document IDs an index may still hold entries for after their documents
//! were deleted, kept by the doc table for the in-process GC.
//!
//! A deleted document's postings stay in the inverted indexes until a GC cycle
//! removes them. A fork cycle finds them by checking every entry against the
//! doc table; the in-process cycle instead reads this set, so it needs no table
//! lookup and no lock while it scans. The set is a compressed bitmap, so it
//! grows with the number of IDs between two cycles rather than with the ID
//! range, and runs of consecutive IDs cost almost nothing.
//!
//! The set is maintained under the spec write lock, where deletions happen, and
//! consumed by [`DeletedDocs::snapshot`] / [`DeletedDocs::finish_cycle`] around a
//! GC cycle.

pub use roaring::RoaringTreemap;

/// Whether [`DeletedDocs`] accounts for every dead entry the indexes may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coverage {
    /// Some dead entries may have no ID in the set: it was created after the
    /// index, which may have been loaded with garbage (an RDB keeps it) or
    /// deleted documents before the set existed. A cycle must derive liveness
    /// from the doc table instead.
    Partial,
    /// Every dead entry's ID is in the set.
    Complete,
}

/// IDs of deleted documents whose index entries may not have been collected yet.
#[derive(Debug, Clone, PartialEq)]
pub struct DeletedDocs {
    ids: RoaringTreemap,
    coverage: Coverage,
}

impl Default for DeletedDocs {
    fn default() -> Self {
        Self::new()
    }
}

impl DeletedDocs {
    /// An empty set with [`Coverage::Partial`]: the indexes' garbage is unknown.
    pub fn new() -> Self {
        Self {
            ids: RoaringTreemap::new(),
            coverage: Coverage::Partial,
        }
    }

    /// A set with [`Coverage::Complete`] holding `ids`: for a table whose every
    /// dead entry is known, such as one built from scratch by a test.
    pub fn complete_from(ids: impl IntoIterator<Item = u64>) -> Self {
        Self {
            ids: ids.into_iter().collect(),
            coverage: Coverage::Complete,
        }
    }

    /// Record a document as deleted. IDs are never reused, so a recorded ID
    /// stays dead.
    pub fn record(&mut self, id: u64) {
        self.ids.insert(id);
    }

    /// Whether the set accounts for every dead entry; see [`Coverage`].
    pub const fn coverage(&self) -> Coverage {
        self.coverage
    }

    /// The IDs a GC cycle may treat as the only dead ones, or `None` when it
    /// must consult the doc table because the set is [`Coverage::Partial`].
    pub fn snapshot(&self) -> Option<RoaringTreemap> {
        match self.coverage {
            Coverage::Complete => Some(self.ids.clone()),
            Coverage::Partial => None,
        }
    }

    /// End a cycle that looked for the entries of every ID in `dead` and fully
    /// collected those in `collected`: the latter leave the set, the rest of
    /// `dead` is kept (or added, when `dead` came from a doc table sweep rather
    /// than from this set), and the set is [`Coverage::Complete`], since every
    /// dead entry's ID is now either in it or was recorded since the cycle began.
    pub fn finish_cycle(&mut self, dead: &RoaringTreemap, collected: &RoaringTreemap) {
        self.ids -= collected;
        self.ids |= dead - collected;
        self.coverage = Coverage::Complete;
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
