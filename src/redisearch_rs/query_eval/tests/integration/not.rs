/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_NOT → Not / Wildcard / Empty (via the reducer shortcircuits)

use query_eval::{QueryEvalContext, QueryNodeRef, eval, eval::Config};
use query_types::QueryNodeType;
use rqe_iterators::{IteratorType, RQEIterator};

use query::mock::{MockQueryEvalCtx, MockQueryNode};

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_new_sz)")]
fn eval_not_empty_child_falls_back_to_wildcard() {
    // A child that produces no results (here a QN_NULL → Empty) makes the
    // reducer shortcircuit to a wildcard over the whole index: NOT(nothing)
    // matches everything.
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.set_max_doc_id(3);
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let null_child = MockQueryNode::new(QueryNodeType::Null);
    let mut not = MockQueryNode::new(QueryNodeType::Not);
    not.opts_mut().weight = 1.0;
    not.set_children(&[null_child.as_ptr()]);
    let node = unsafe { QueryNodeRef::new(not.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("should not be None")
        .into_boxed();

    assert_eq!(it.type_(), IteratorType::Wildcard);
    for expected in [1, 2, 3] {
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, expected);
    }
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_new_sz)")]
fn eval_not_wildcard_child_reduces_to_empty() {
    // A wildcard child means NOT matches nothing → empty iterator.
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.set_max_doc_id(5);
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let mut wc_child = MockQueryNode::new(QueryNodeType::Wildcard);
    wc_child.opts_mut().weight = 1.0;
    let mut not = MockQueryNode::new(QueryNodeType::Not);
    not.opts_mut().weight = 1.0;
    not.set_children(&[wc_child.as_ptr()]);
    let node = unsafe { QueryNodeRef::new(not.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("should not be None")
        .into_boxed();

    assert_eq!(it.type_(), IteratorType::Empty);
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

// ---------------------------------------------------------------------------
// QN_NOT → Not, wrapping a real child iterator.
//
// The non-shortcircuit path needs a child that is neither empty nor a
// wildcard. A QN_IDS child resolving to a real document provides one, which
// requires the full-FFI `TestContext`.
// ---------------------------------------------------------------------------

// Disabled under Miri: `TestContext` and SDS creation call into the C library,
// which Miri cannot execute.
#[cfg(not(miri))]
mod not {
    use ffi::IndexFlags_Index_StoreFreqs;
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    use super::*;
    use crate::util::new_sds;

    #[test]
    fn eval_not_wraps_real_child_in_not() {
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        // Three documents → maxDocId == 3; the NOT iterator yields every doc id
        // *except* those matched by the child.
        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        let id_c = context.add_document("doc_c");
        assert_eq!((id_a, id_b, id_c), (1, 2, 3));

        // Disable timeout checks so the (clock-based) default deadline of the
        // zero-initialised `QueryEvalCtx` does not expire the iterator.
        let mut sctx = context.sctx;
        // SAFETY: `context.sctx` is a valid, exclusively-owned `RedisSearchCtx`.
        unsafe { sctx.as_mut().time.skipTimeoutChecks = true };

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        // QN_IDS child resolving to the middle document only (pre-resolved docId).
        let keys: Vec<ffi::sds> = vec![new_sds("doc_b")];
        let mut dids = vec![id_b];
        let mut ids_child = MockQueryNode::new(QueryNodeType::Ids);
        ids_child.set_ids(keys.as_ptr(), dids.as_mut_ptr(), keys.len());

        let mut not = MockQueryNode::new(QueryNodeType::Not);
        not.opts_mut().weight = 1.0;
        not.set_children(&[ids_child.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(not.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("should not be None")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::Not);
        // Every id except the one matched by the child (doc 2).
        for expected in [1, 3] {
            let r = it.read().unwrap().expect("should have a result");
            assert_eq!(r.doc_id, expected);
        }
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());

        for key in keys {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }

    #[test]
    fn eval_not_none_child_falls_back_to_wildcard() {
        let _guard = GlobalGuard::default();
        // Term context: the field exists but has no entry in `missingFieldDict`,
        // so a QN_MISSING child makes `eval_node` return `None` — i.e. a NULL
        // child pointer. This is distinct from a structurally-empty child (an
        // `Empty` *iterator*, a non-null pointer handled by the reducer): here
        // the NULL-child branch of `eval_not` builds a NOT over an `Empty`,
        // which always reduces to a wildcard over the whole index — NOT(nothing)
        // matches everything.
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        // Two documents → docTable maxDocId == 2; the wildcard fallback walks
        // every id. `add_document` only touches the DocTable, never the
        // `missingFieldDict`, so the QN_MISSING child still evaluates to `None`.
        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        assert_eq!((id_a, id_b), (1, 2));

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        // QN_MISSING child for a field with no missing values → evaluates to None.
        let mut missing_child = MockQueryNode::new(QueryNodeType::Missing);
        missing_child.set_missing_field(context.field_spec());

        let mut not = MockQueryNode::new(QueryNodeType::Not);
        not.opts_mut().weight = 1.0;
        not.set_children(&[missing_child.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(not.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("a not node always yields an iterator")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::Wildcard);
        for expected in [1, 2] {
            let r = it.read().unwrap().expect("should have a result");
            assert_eq!(r.doc_id, expected);
        }
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    #[test]
    fn eval_not_optimized_index_wraps_child_in_not_optimized() {
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        assert_eq!((id_a, id_b), (1, 2));

        // `index_all` makes the reducer take the optimized (wildcard-backed)
        // path, so `eval_not` must forward a `NotOptimized` variant.
        // SAFETY: no iterator from this context is alive yet.
        context.spec_write().rule_mut().set_index_all(true);

        // Disable timeout checks so the (clock-based) default deadline of the
        // zero-initialised `QueryEvalCtx` does not expire the iterator.
        let mut sctx = context.sctx;
        // SAFETY: `context.sctx` is a valid, exclusively-owned `RedisSearchCtx`.
        unsafe { sctx.as_mut().time.skipTimeoutChecks = true };

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        // QN_IDS child resolving to a real document → neither empty nor a
        // wildcard, so the reducer skips its shortcircuits and reaches the
        // optimized constructor.
        let keys: Vec<ffi::sds> = vec![new_sds("doc_b")];
        let mut dids = vec![id_b];
        let mut ids_child = MockQueryNode::new(QueryNodeType::Ids);
        ids_child.set_ids(keys.as_ptr(), dids.as_mut_ptr(), keys.len());

        let mut not = MockQueryNode::new(QueryNodeType::Not);
        not.opts_mut().weight = 1.0;
        not.set_children(&[ids_child.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(not.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("should not be None")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::NotOptimized);
        // `existingDocs` is null in the term context, so the backing wildcard is
        // empty: there are no "existing" documents to negate against, so the
        // optimized NOT yields nothing — but it must drain cleanly.
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());

        for key in keys {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }
}
