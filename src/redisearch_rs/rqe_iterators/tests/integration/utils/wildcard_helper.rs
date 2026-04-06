/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IndexFlags_Index_DocIdsOnly, t_docId};
use inverted_index::{InvertedIndex, RSIndexResult, doc_ids_only::DocIdsOnly};

/// Holds an [`InvertedIndex`] so a
/// [`Wildcard`](rqe_iterators::inverted_index::Wildcard) iterator can be
/// created from it.
pub(crate) struct WildcardHelper {
    ii: InvertedIndex<DocIdsOnly>,
}

impl WildcardHelper {
    /// Create a new helper populated with the given `doc_ids`.
    pub(crate) fn new(doc_ids: &[t_docId]) -> Self {
        let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
        for &doc_id in doc_ids {
            let record = RSIndexResult::build_virt().doc_id(doc_id).build();
            ii.add_record(&record).expect("failed to add record");
        }
        Self { ii }
    }

    /// Create a [`Wildcard`](rqe_iterators::inverted_index::Wildcard)
    /// iterator from the underlying inverted index.
    pub(crate) fn create_wildcard(
        &self,
    ) -> rqe_iterators::inverted_index::Wildcard<'_, DocIdsOnly> {
        rqe_iterators::inverted_index::Wildcard::new(self.ii.reader(), 1.0)
    }
}
