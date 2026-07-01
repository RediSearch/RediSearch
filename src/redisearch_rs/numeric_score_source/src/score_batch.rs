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
