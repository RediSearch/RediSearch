/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Document existence checks for the child-side scanners.

use index_spec::IndexSpecReadGuard;
use roaring::RoaringTreemap;
use rqe_core::DocId;

/// Answers which documents exist, for the scanners that run in the forked child.
///
/// The scanners ask about every record of every index they visit. Asking the
/// spec's document table costs a dependent load into a scattered document
/// metadata entry per record, which dominates the scan on large indexes. The
/// child's table never changes, so [`DocExistence::snapshot`] answers from a
/// set of live document ids taken once instead.
pub struct DocExistence<'a> {
    spec: &'a IndexSpecReadGuard<'a>,
    live: LiveDocs,
    /// The table's maximum document id when the set was taken. The set answers
    /// for no id above it, so a table that changes afterwards can only make
    /// [`DocExistence::contains`] fall back to the lookup.
    max_doc_id: DocId,
}

/// The live document ids of a [`DocExistence`].
enum LiveDocs {
    /// One bit per document id: the cheapest check, but sized by the maximum
    /// document id, which only grows because ids are never reused.
    Bitmap(Box<[u64]>),
    /// The live ids alone, sized by their count, for a table whose maximum
    /// document id makes [`LiveDocs::Bitmap`] too large. A check costs a
    /// binary search instead of one load.
    Ids(RoaringTreemap),
}

impl<'a> DocExistence<'a> {
    /// Snapshot which documents of `spec` exist.
    ///
    /// Answers for ids up to the table's current maximum document id are only
    /// right while the table does not change, as in the forked child. Above
    /// that they come from [`IndexSpecReadGuard::doc_exists`].
    pub fn snapshot(spec: &'a IndexSpecReadGuard<'a>) -> Self {
        let live = spec.live_doc_bitmap().map_or_else(
            || {
                let mut ids = RoaringTreemap::new();
                spec.for_each_live_doc(|id| {
                    ids.insert(id);
                });
                LiveDocs::Ids(ids)
            },
            LiveDocs::Bitmap,
        );
        Self {
            spec,
            live,
            max_doc_id: spec.doc_table().maxDocId,
        }
    }

    /// Like [`DocExistence::snapshot`], but answering from `words` for ids up
    /// to `max_doc_id`, so tests can make the bitmap and the lookup disagree.
    #[cfg(feature = "unittest")]
    pub const fn with_bitmap(
        spec: &'a IndexSpecReadGuard<'a>,
        words: Box<[u64]>,
        max_doc_id: DocId,
    ) -> Self {
        Self {
            spec,
            live: LiveDocs::Bitmap(words),
            max_doc_id,
        }
    }

    /// Like [`DocExistence::with_bitmap`], but answering from the set of `ids`.
    #[cfg(feature = "unittest")]
    pub const fn with_ids(
        spec: &'a IndexSpecReadGuard<'a>,
        ids: RoaringTreemap,
        max_doc_id: DocId,
    ) -> Self {
        Self {
            spec,
            live: LiveDocs::Ids(ids),
            max_doc_id,
        }
    }

    /// Whether the document `id` exists.
    #[inline]
    pub fn contains(&self, id: DocId) -> bool {
        if id <= self.max_doc_id {
            match &self.live {
                LiveDocs::Bitmap(words) => {
                    if let Some(word) = usize::try_from(id / 64).ok().and_then(|i| words.get(i)) {
                        return word & (1 << (id % 64)) != 0;
                    }
                }
                LiveDocs::Ids(ids) => return contains_id(ids, id),
            }
        }
        self.spec.doc_exists(id)
    }
}

/// Kept out of line so that [`DocExistence::contains`], with only the bitmap
/// check inline, stays small enough to inline into the scanners' loops.
#[inline(never)]
fn contains_id(ids: &RoaringTreemap, id: DocId) -> bool {
    ids.contains(id)
}
