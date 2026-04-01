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
use rqe_iterators_test_utils::MockContext;

/// Holds an [`InvertedIndex`] and a [`MockContext`] so a
/// [`Wildcard`](rqe_iterators::inverted_index::Wildcard) iterator can be
/// created from them.
pub(crate) struct WildcardHelper {
    ii: InvertedIndex<DocIdsOnly>,
    mock_ctx: MockContext,
}

impl WildcardHelper {
    /// Create a new helper populated with the given `doc_ids`.
    #[expect(unused)] // not used yet
    pub(crate) fn new(doc_ids: &[t_docId]) -> Self {
        let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
        for &doc_id in doc_ids {
            let record = RSIndexResult::build_virt().doc_id(doc_id).build();
            ii.add_record(&record).expect("failed to add record");
        }
        let mock_ctx = MockContext::new(0, 0);
        Self { ii, mock_ctx }
    }

    /// Create a [`Wildcard`](rqe_iterators::inverted_index::Wildcard)
    /// iterator from the underlying inverted index.
    #[expect(unused)] // not used yet
    pub(crate) fn create_wildcard(
        &self,
    ) -> rqe_iterators::inverted_index::Wildcard<'_, DocIdsOnly> {
        let reader = self.ii.reader();
        // SAFETY: `mock_ctx` provides a valid `RedisSearchCtx` with a valid
        // `spec` that outlives the returned iterator.
        unsafe { rqe_iterators::inverted_index::Wildcard::new(reader, self.mock_ctx.sctx(), 1.0) }
    }
}
