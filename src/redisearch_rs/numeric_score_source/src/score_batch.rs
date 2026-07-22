/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`NumericScoreBatch`] — a [`ScoreBatch`] over a drained numeric iterator.

use rqe_core::DocId;
use rqe_iterators::RQEIteratorError;
use top_k::ScoreBatch;

/// A [`ScoreBatch`] backed by a doc-id-ordered `Vec<(DocId, f64)>`.
///
/// Doc IDs must be strictly increasing, so [`skip_to`](ScoreBatch::skip_to) can
/// `partition_point`; this is asserted in debug builds.
pub struct NumericScoreBatch {
    items: Vec<(DocId, f64)>,
    pos: usize,
}

impl NumericScoreBatch {
    /// Create a batch from `(doc_id, score)` pairs sorted strictly ascending by
    /// `doc_id`.
    pub(crate) fn new(items: Vec<(DocId, f64)>) -> Self {
        debug_assert!(
            items.windows(2).all(|w| w[0].0 < w[1].0),
            "NumericScoreBatch: doc IDs must be strictly increasing"
        );
        Self { items, pos: 0 }
    }

    /// Drop records for which `keep` returns `Ok(false)`, preserving the strictly
    /// increasing doc-id order, and return the filtered batch. Called before the
    /// batch is read, so `pos` stays at `0`.
    ///
    /// `keep` is fallible so it can poll the query deadline per record. The first
    /// [`Err`] is returned and short-circuits the scan.
    pub(crate) fn retain(
        mut self,
        mut keep: impl FnMut(DocId, f64) -> Result<bool, RQEIteratorError>,
    ) -> Result<Self, RQEIteratorError> {
        debug_assert_eq!(self.pos, 0, "retain must run before the batch is read");
        // Two-pointer compaction so the first `Err` exits immediately instead of
        // scanning the rest of the batch as `Vec::retain` would.
        let mut write = 0;
        for read in 0..self.items.len() {
            let item = self.items[read];
            if keep(item.0, item.1)? {
                self.items[write] = item;
                write += 1;
            }
        }
        self.items.truncate(write);
        Ok(self)
    }
}

impl ScoreBatch for NumericScoreBatch {
    fn next(&mut self) -> Option<(DocId, f64)> {
        let item = self.items.get(self.pos).copied();
        if item.is_some() {
            self.pos += 1;
        }
        item
    }

    fn skip_to(&mut self, target: DocId) -> Option<(DocId, f64)> {
        self.pos += self.items[self.pos..].partition_point(|(id, _)| *id < target);
        self.next()
    }
}
