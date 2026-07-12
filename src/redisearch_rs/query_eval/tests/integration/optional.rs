/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_OPTIONAL → Optional / Wildcard (via the reducer shortcircuits)

use query_eval::{QueryEvalContext, QueryNodeRef, eval, eval::Config};
use query_node_type::QueryNodeType;
use rqe_iterators::{IteratorType, RQEIterator};

use query::mock::{MockQueryEvalCtx, MockQueryNode};

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_new_sz)")]
fn eval_optional_empty_child_falls_back_to_wildcard() {
    // A child that produces no results (here a QN_NULL → Empty) makes the
    // reducer shortcircuit to a wildcard over the whole index.
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.set_max_doc_id(3);
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let null_child = MockQueryNode::new(QueryNodeType::Null);
    let mut opt = MockQueryNode::new(QueryNodeType::Optional);
    opt.opts_mut().weight = 1.0;
    opt.set_children(&[null_child.as_ptr()]);
    let node = unsafe { QueryNodeRef::new(opt.as_non_null()) };

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
fn eval_optional_wildcard_child_passes_through() {
    // A wildcard child is returned as-is, with the optional node's weight
    // applied to its results.
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.set_max_doc_id(5);
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let mut wc_child = MockQueryNode::new(QueryNodeType::Wildcard);
    wc_child.opts_mut().weight = 1.0;
    let mut opt = MockQueryNode::new(QueryNodeType::Optional);
    opt.opts_mut().weight = 2.0;
    opt.set_children(&[wc_child.as_ptr()]);
    let node = unsafe { QueryNodeRef::new(opt.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("should not be None")
        .into_boxed();

    assert_eq!(it.type_(), IteratorType::Wildcard);
    let r = it.read().unwrap().expect("should have a result");
    assert_eq!(r.doc_id, 1);
    assert_eq!(r.weight, 2.0);
}

// ---------------------------------------------------------------------------
// QN_OPTIONAL paths that need a real `IndexSpec`, so they rely on the
// full-FFI `TestContext`:
//
// - wrapping a real child in an `Optional` (the non-shortcircuit path needs a
//   child that is neither empty nor a wildcard — a QN_IDS child resolving to
//   real documents provides one);
// - a child that evaluates to `None` (here a QN_MISSING node for a field with
//   no missing values), which yields a NULL child pointer and exercises the
//   wildcard fallback distinct from the structurally-empty shortcircuit.
// ---------------------------------------------------------------------------

// Disabled under Miri: `TestContext` and SDS creation call into the C library,
// which Miri cannot execute.
#[cfg(not(miri))]
mod optional {
    use ffi::IndexFlags_Index_StoreFreqs;
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    use super::*;
    use crate::util::new_sds;

    #[test]
    fn eval_optional_wraps_real_child_in_optional() {
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        // Two documents → maxDocId == 2; the optional iterator yields every doc
        // id, marking those matched by the child as real hits.
        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        assert_eq!((id_a, id_b), (1, 2));

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        // QN_IDS child resolving to the two known documents.
        let keys: Vec<ffi::sds> = vec![new_sds("doc_a"), new_sds("doc_b")];
        let mut ids_child = MockQueryNode::new(QueryNodeType::Ids);
        ids_child.set_ids(keys.as_ptr(), std::ptr::null_mut(), keys.len());

        let mut opt = MockQueryNode::new(QueryNodeType::Optional);
        opt.opts_mut().weight = 1.0;
        opt.set_children(&[ids_child.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(opt.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("should not be None")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::Optional);
        // The optional iterator walks every document id in the index.
        for expected in [1, 2] {
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
    fn eval_optional_none_child_falls_back_to_wildcard() {
        let _guard = GlobalGuard::default();
        // Term context: the field exists but has no entry in `missingFieldDict`,
        // so a QN_MISSING child makes `eval_node` return `None` — i.e. a NULL
        // child pointer. This is distinct from a structurally-empty child (an
        // `Empty` *iterator*, a non-null pointer), which the reducer handles via
        // its own shortcircuit. Here the NULL-child branch of `eval_optional`
        // substitutes a wildcard over the whole index, with weight dropped to 0.
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

        let mut opt = MockQueryNode::new(QueryNodeType::Optional);
        opt.opts_mut().weight = 1.0;
        opt.set_children(&[missing_child.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(opt.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("an optional node always yields an iterator")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::Wildcard);
        // Every doc id is a virtual hit, with the fallback weight of 0.
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, 1);
        assert_eq!(r.weight, 0.0);
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, 2);
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    #[test]
    fn eval_optional_optimized_index_wraps_child_in_optional_optimized() {
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        assert_eq!((id_a, id_b), (1, 2));

        // `index_all` makes the reducer take the optimized (wildcard-backed)
        // path, so `eval_optional` must forward an `OptionalOptimized` variant.
        // SAFETY: no iterator from this context is alive yet.
        context.spec_write().rule_mut().set_index_all(true);

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        // QN_IDS child resolving to a real document → neither empty nor a
        // wildcard, so the reducer skips its shortcircuits and reaches the
        // optimized constructor.
        let keys: Vec<ffi::sds> = vec![new_sds("doc_a")];
        let mut ids_child = MockQueryNode::new(QueryNodeType::Ids);
        ids_child.set_ids(keys.as_ptr(), std::ptr::null_mut(), keys.len());

        let mut opt = MockQueryNode::new(QueryNodeType::Optional);
        opt.opts_mut().weight = 1.0;
        opt.set_children(&[ids_child.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(opt.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("an optional node always yields an iterator")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::OptionalOptimized);
        // `existingDocs` is null in the term context, so the backing wildcard is
        // empty and the iterator yields nothing — but it must drain cleanly.
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());

        for key in keys {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }
}
