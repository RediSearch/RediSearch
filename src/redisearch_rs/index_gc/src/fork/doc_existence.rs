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
use inverted_index::DocLiveness;
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
    Bitmap {
        words: Box<[u64]>,
        /// The ids the bitmap answers for but does not hold: the gone documents,
        /// since ids are assigned in sequence. Answers [`DocLiveness::any_gone`],
        /// which `words` could only answer by visiting every id of a range.
        gone: RoaringTreemap,
    },
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
        let max_doc_id = spec.doc_table().maxDocId;
        let live = spec.live_doc_bitmap().map_or_else(
            || {
                let mut ids = RoaringTreemap::new();
                spec.for_each_live_doc(|id| {
                    ids.insert(id);
                });
                LiveDocs::Ids(ids)
            },
            |words| LiveDocs::bitmap(words, max_doc_id),
        );
        Self {
            spec,
            live,
            max_doc_id,
        }
    }

    /// Like [`DocExistence::snapshot`], but answering from `words` for ids up
    /// to `max_doc_id`, so tests can make the bitmap and the lookup disagree.
    #[cfg(feature = "unittest")]
    pub fn with_bitmap(
        spec: &'a IndexSpecReadGuard<'a>,
        words: Box<[u64]>,
        max_doc_id: DocId,
    ) -> Self {
        Self {
            spec,
            live: LiveDocs::bitmap(words, max_doc_id),
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
                LiveDocs::Bitmap { words, .. } => {
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

impl DocLiveness for &DocExistence<'_> {
    fn exists(&self, id: DocId) -> bool {
        self.contains(id)
    }

    fn any_gone(&self, first: DocId, last: DocId) -> bool {
        match &self.live {
            LiveDocs::Bitmap { words, gone } => {
                // Past what the bitmap answers for, `contains` asks the table, so a
                // range reaching there may hold a gone id.
                if last > bitmap_last_id(words, self.max_doc_id) {
                    return true;
                }
                let mut gone = gone.iter();
                gone.advance_to(first);
                gone.next().is_some_and(|id| id <= last)
            }
            // Counted rather than kept as a gone set, which this table's maximum
            // document id makes as large as the bitmap it replaces.
            LiveDocs::Ids(ids) => {
                last > self.max_doc_id || ids.range_cardinality(first..=last) <= last - first
            }
        }
    }
}

impl LiveDocs {
    fn bitmap(words: Box<[u64]>, max_doc_id: DocId) -> Self {
        let last = bitmap_last_id(&words, max_doc_id);
        let unset = words.iter().enumerate().flat_map(|(i, &word)| {
            let base = i as DocId * 64;
            let mut unset = !word;
            std::iter::from_fn(move || {
                (unset != 0).then(|| {
                    let bit = unset.trailing_zeros();
                    unset &= unset - 1;
                    base + DocId::from(bit)
                })
            })
        });
        let gone = RoaringTreemap::from_sorted_iter(
            unset.skip_while(|&id| id == 0).take_while(|&id| id <= last),
        )
        .expect("bit positions are visited in increasing order");
        Self::Bitmap { words, gone }
    }
}

/// The last id `words` answers for: the bitmap's end or `max_doc_id`, whichever comes first.
fn bitmap_last_id(words: &[u64], max_doc_id: DocId) -> DocId {
    (words.len() as DocId * 64)
        .saturating_sub(1)
        .min(max_doc_id)
}

/// Kept out of line so that [`DocExistence::contains`], with only the bitmap
/// check inline, stays small enough to inline into the scanners' loops.
#[inline(never)]
fn contains_id(ids: &RoaringTreemap, id: DocId) -> bool {
    ids.contains(id)
}
