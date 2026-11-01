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
use rqe_core::DocId;

/// Answers which documents exist, for the scanners that run in the forked child.
///
/// The scanners ask about every record of every index they visit. Asking the
/// spec's document table costs a dependent load into a scattered document
/// metadata entry per record, which dominates the scan on large indexes. The
/// child's table never changes, so [`DocExistence::snapshot`] answers from a
/// bitmap of live document ids taken once instead.
pub struct DocExistence<'a> {
    spec: &'a IndexSpecReadGuard<'a>,
    live: Option<LiveBitmap>,
}

struct LiveBitmap {
    words: Box<[u64]>,
    /// The table's maximum document id when the bitmap was taken. The bitmap
    /// answers for no id above it, so a table that changes afterwards can only
    /// make [`DocExistence::contains`] fall back to the lookup.
    max_doc_id: DocId,
}

impl<'a> DocExistence<'a> {
    /// Snapshot which documents of `spec` exist.
    ///
    /// Answers for ids up to the table's current maximum document id are only
    /// right while the table does not change, as in the forked child. Above
    /// that, or when [`IndexSpecReadGuard::live_doc_bitmap`] declines to build
    /// a bitmap, they come from [`IndexSpecReadGuard::doc_exists`].
    pub fn snapshot(spec: &'a IndexSpecReadGuard<'a>) -> Self {
        let live = spec.live_doc_bitmap().map(|words| LiveBitmap {
            words,
            max_doc_id: spec.doc_table().maxDocId,
        });
        Self { spec, live }
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
            live: Some(LiveBitmap { words, max_doc_id }),
        }
    }

    /// Whether the document `id` exists.
    pub fn contains(&self, id: DocId) -> bool {
        if let Some(live) = &self.live
            && id <= live.max_doc_id
            && let Some(word) = usize::try_from(id / 64)
                .ok()
                .and_then(|i| live.words.get(i))
        {
            return word & (1 << (id % 64)) != 0;
        }
        self.spec.doc_exists(id)
    }
}
