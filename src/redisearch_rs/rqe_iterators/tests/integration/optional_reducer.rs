/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use inverted_index::RSResultKind;
use rqe_iterators::{
    RQEIterator,
    empty::Empty,
    optional_reducer::{NewOptionalIterator, new_optional_iterator},
    wildcard::Wildcard,
};
use rqe_iterators_test_utils::MockContext;

use crate::utils::Mock;

mod optional_reducer_tests {
    use super::*;

    /// Shortcircuit 1: when the child iterator is at EOF, the factory drops it
    /// and returns a wildcard that covers the full document range.
    #[test]
    fn shortcircuit_1_empty_child_returns_wildcard_fallback() {
        const MAX_DOC_ID: ffi::t_docId = 10;

        let ctx = MockContext::new(MAX_DOC_ID, 0);

        // SAFETY:
        // - `ctx` provides a valid `QueryEvalCtx` whose pointer-chain satisfies
        //   the preconditions of `new_optional_iterator`.
        // - `spec.diskSpec` is null and `spec.rule.index_all` is false, so
        //   `new_wildcard_iterator` takes the fallback path.
        // - `ctx` outlives the `new_optional_iterator` call.
        let result = unsafe { new_optional_iterator(Empty, 1.0, ctx.qctx(), MAX_DOC_ID) };

        let NewOptionalIterator::WildcardFallback(mut wc) = result else {
            panic!("expected WildcardFallback, got a different variant");
        };

        // The fallback wildcard covers all documents up to maxDocId.
        assert_eq!(wc.num_estimated(), MAX_DOC_ID as usize);
        // It starts before the first document and is not yet at EOF.
        assert!(!wc.at_eof());

        // Results must be virtual (RSResultData_Virtual in C++).
        let r = wc
            .read()
            .expect("no error")
            .expect("first doc must be present");
        assert_eq!(r.doc_id, 1);
        assert_eq!(r.kind(), RSResultKind::Virtual);
    }

    /// Shortcircuit 2: when the child iterator is a wildcard (and not at EOF),
    /// the factory returns it as-is after applying the requested weight to the
    /// current result.
    ///
    /// The `query` pointer is never dereferenced in this branch, so a dangling
    /// pointer is sufficient.
    #[test]
    fn shortcircuit_2_wildcard_child_returned_as_passthrough_with_weight_applied() {
        const INITIAL_WEIGHT: f64 = 1.0;
        const NEW_WEIGHT: f64 = 3.5;
        const MAX_DOC_ID: ffi::t_docId = 100;

        let mut child = Wildcard::new(100, INITIAL_WEIGHT);
        // Advance the child so that `current()` holds a real document result.
        let read_result = child.read().unwrap().expect("first read must succeed");
        assert_eq!(read_result.doc_id, 1);
        assert_eq!(read_result.weight, INITIAL_WEIGHT);

        // SAFETY: the wildcard passthrough branch never dereferences `query`.
        let result =
            unsafe { new_optional_iterator(child, NEW_WEIGHT, NonNull::dangling(), MAX_DOC_ID) };

        let NewOptionalIterator::WildcardPassthrough(mut child) = result else {
            panic!("expected WildcardPassthrough, got a different variant");
        };

        // The factory must apply the new weight to the current result.
        let current = child.current().expect("wildcard current must be Some");
        assert_eq!(
            current.weight, NEW_WEIGHT,
            "factory must apply weight to current result"
        );
        // The read position must be preserved.
        assert_eq!(current.doc_id, 1, "read position must not change");
        // Results from a wildcard passthrough are virtual (RSResultData_Virtual in C++).
        assert_eq!(current.kind(), RSResultKind::Virtual);
    }

    /// An `InvertedIndex`-backed wildcard child (type `InvIdxWildcard`) takes the same
    /// `WildcardPassthrough` shortcircuit as a plain wildcard child, and its results are virtual.
    #[test]
    fn shortcircuit_2_inverted_index_wildcard_child_returned_as_passthrough() {
        use ffi::IndexFlags_Index_DocIdsOnly;
        use inverted_index::{InvertedIndex, RSIndexResult, doc_ids_only::DocIdsOnly};
        use rqe_iterators::{IteratorType, inverted_index::Wildcard as InvIdxWildcard};

        const MAX_DOC_ID: ffi::t_docId = 1000;
        const INITIAL_WEIGHT: f64 = 1.0;
        const NEW_WEIGHT: f64 = 2.0;

        // Build an InvertedIndex with docs 1..999 (matches the C++ test).
        let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
        for doc_id in 1u64..1000 {
            let record = RSIndexResult::build_virt().doc_id(doc_id).build();
            ii.add_record(&record).expect("failed to add record");
        }

        // `MockContext` provides the `RedisSearchCtx` required by `InvIdxWildcard::new`.
        let mock_ctx = MockContext::new(MAX_DOC_ID, 0);
        let reader = ii.reader();
        // SAFETY: `mock_ctx` provides a valid `RedisSearchCtx` with a valid `spec`
        // that outlives the iterator.
        let mut child = unsafe { InvIdxWildcard::new(reader, mock_ctx.sctx(), INITIAL_WEIGHT) };
        assert_eq!(child.type_(), IteratorType::InvIdxWildcard);

        // Advance so `current()` is Some — the factory will apply `NEW_WEIGHT` to it.
        let r = child
            .read()
            .expect("no error")
            .expect("first doc must be present");
        assert_eq!(r.doc_id, 1);
        assert_eq!(r.kind(), RSResultKind::Virtual);

        // SAFETY: the `WildcardPassthrough` branch never dereferences `query`.
        let result =
            unsafe { new_optional_iterator(child, NEW_WEIGHT, NonNull::dangling(), MAX_DOC_ID) };

        let NewOptionalIterator::WildcardPassthrough(mut child) = result else {
            panic!("expected WildcardPassthrough, got a different variant");
        };

        // The factory must return the same iterator type (C++ asserts pointer identity).
        assert_eq!(child.type_(), IteratorType::InvIdxWildcard);

        // Weight must be updated; read position must be preserved.
        let current = child.current().expect("wildcard current must be Some");
        assert_eq!(
            current.weight, NEW_WEIGHT,
            "factory must apply weight to current result"
        );
        assert_eq!(current.doc_id, 1, "read position must not change");
        // Results from an InvIdxWildcard passthrough are virtual (RSResultData_Virtual in C++).
        assert_eq!(current.kind(), RSResultKind::Virtual);
    }

    /// Regular case — non-optimized index: child is a plain [`Mock`] iterator
    /// (not empty, not a wildcard) and `rule.index_all` is false, so the factory
    /// wraps it in a plain [`Optional`].
    #[test]
    fn regular_non_optimized_child_wrapped_in_optional() {
        const MAX_DOC_ID: ffi::t_docId = 100;
        const WEIGHT: f64 = 2.0;
        const DOCS: [ffi::t_docId; 3] = [10, 20, 30];

        let ctx = MockContext::new(MAX_DOC_ID, 0);
        let child = Mock::new(DOCS);

        // SAFETY: `ctx` provides a valid `QueryEvalCtx`; `rule.index_all` is
        // false by default and `diskSpec` is null, so the regular `Optional`
        // path is taken.
        let result = unsafe { new_optional_iterator(child, WEIGHT, ctx.qctx(), MAX_DOC_ID) };

        assert!(
            matches!(result, NewOptionalIterator::Optional(_)),
            "expected Optional, got a different variant"
        );
    }

    /// Regular case — optimized index: child is a plain [`Mock`] iterator and
    /// `rule.index_all` is true, so the factory wraps it in an
    /// [`OptionalOptimized`] backed by an empty wildcard (because
    /// `existingDocs` is null in the [`MockContext`]).
    #[test]
    fn regular_optimized_child_wrapped_in_optional_optimized() {
        const MAX_DOC_ID: ffi::t_docId = 100;
        const WEIGHT: f64 = 2.0;
        const DOCS: [ffi::t_docId; 3] = [10, 20, 30];

        let ctx = MockContext::new(MAX_DOC_ID, 0);
        // SAFETY: no iterator from `ctx` is alive at this point.
        unsafe { ctx.set_index_all(true) };

        let child = Mock::new(DOCS);

        // SAFETY: `ctx` provides a valid `QueryEvalCtx`; `rule.index_all` is
        // true and `diskSpec` is null, so `new_wildcard_iterator_optimized` is
        // called. `existingDocs` is null, so an `EmptyWildcard` is used as the
        // wildcard side of the `OptionalOptimized`.
        let result = unsafe { new_optional_iterator(child, WEIGHT, ctx.qctx(), MAX_DOC_ID) };

        assert!(
            matches!(result, NewOptionalIterator::OptionalOptimized(_)),
            "expected OptionalOptimized, got a different variant"
        );
    }
}
