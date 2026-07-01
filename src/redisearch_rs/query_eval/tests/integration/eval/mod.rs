/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use query_eval::{QueryEvalContext, QueryNodeRef, eval};
use query_node_type::QueryNodeType;
use rqe_iterators::{IteratorType, RQEIterator};

use query::mock::{MockQueryEvalCtx, MockQueryNode};

// ---------------------------------------------------------------------------
// QAST_Iterate
// ---------------------------------------------------------------------------

#[test]
fn qast_iterate_evaluates_root_node() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };
    let mock_node = MockQueryNode::new(QueryNodeType::Null);
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::qast_iterate(&mut ctx, &node).into_boxed();

    assert_eq!(it.type_(), IteratorType::Empty);
    assert!(it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
}

// ---------------------------------------------------------------------------
// QN_NULL → Empty
// ---------------------------------------------------------------------------

#[test]
fn eval_null_returns_empty_iterator() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };
    let mock_node = MockQueryNode::new(QueryNodeType::Null);
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node)
        .expect("should not be None")
        .into_boxed();

    assert_eq!(it.type_(), IteratorType::Empty);
    assert!(it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
}

// ---------------------------------------------------------------------------
// QN_WILDCARD → Wildcard
// ---------------------------------------------------------------------------

#[test]
fn eval_wildcard_returns_wildcard_iterator() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.set_max_doc_id(100);
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let mut mock_node = MockQueryNode::new(QueryNodeType::Wildcard);
    mock_node.opts_mut().weight = 1.0;
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node)
        .expect("should not be None")
        .into_boxed();

    assert_eq!(it.type_(), IteratorType::Wildcard);
    assert!(!it.at_eof());

    let result = it.read().unwrap().expect("should have a result");
    assert_eq!(result.doc_id, 1);
}

#[test]
fn eval_wildcard_empty_index() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let mock_node = MockQueryNode::new(QueryNodeType::Wildcard);
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node)
        .expect("should not be None")
        .into_boxed();

    assert!(it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn eval_wildcard_respects_weight() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.set_max_doc_id(10);
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let mut mock_node = MockQueryNode::new(QueryNodeType::Wildcard);
    mock_node.opts_mut().weight = 2.5;
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node)
        .expect("should not be None")
        .into_boxed();
    let result = it.read().unwrap().expect("should have a result");
    assert_eq!(result.weight, 2.5);
}

// ---------------------------------------------------------------------------
// QN_IDS → IdList
// ---------------------------------------------------------------------------

#[test]
fn eval_ids_with_pre_resolved_doc_ids() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.enable_disk_mode();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let keys: Vec<ffi::sds> = vec![std::ptr::null_mut(); 3];
    let mut doc_ids: Vec<u64> = vec![10, 5, 20];

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node)
        .expect("should not be None")
        .into_boxed();

    assert_eq!(it.type_(), IteratorType::IdListSorted);
    assert!(!it.at_eof());

    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 5);
    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 10);
    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 20);
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn eval_ids_deduplicates() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.enable_disk_mode();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let keys: Vec<ffi::sds> = vec![std::ptr::null_mut(); 4];
    let mut doc_ids: Vec<u64> = vec![3, 3, 7, 7];

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node)
        .expect("should not be None")
        .into_boxed();

    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 3);
    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 7);
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn eval_ids_filters_zero_doc_ids() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.enable_disk_mode();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let keys: Vec<ffi::sds> = vec![std::ptr::null_mut(); 3];
    let mut doc_ids: Vec<u64> = vec![0, 5, 0];

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node)
        .expect("should not be None")
        .into_boxed();

    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 5);
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn eval_ids_all_zero_produces_empty_list() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.enable_disk_mode();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let keys: Vec<ffi::sds> = vec![std::ptr::null_mut(); 2];
    let mut doc_ids: Vec<u64> = vec![0, 0];

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node)
        .expect("should not be None")
        .into_boxed();

    assert!(it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn eval_ids_empty_keys() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(std::ptr::null(), std::ptr::null_mut(), 0);
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node)
        .expect("should not be None")
        .into_boxed();

    assert!(it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
}

// ---------------------------------------------------------------------------
// QN_OPTIONAL → Optional / Wildcard (via the reducer shortcircuits)
// ---------------------------------------------------------------------------

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

    let mut it = eval::eval_node(&mut ctx, &node)
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

    let mut it = eval::eval_node(&mut ctx, &node)
        .expect("should not be None")
        .into_boxed();

    assert_eq!(it.type_(), IteratorType::Wildcard);
    let r = it.read().unwrap().expect("should have a result");
    assert_eq!(r.doc_id, 1);
    assert_eq!(r.weight, 2.0);
}

// ---------------------------------------------------------------------------
// QN_MISSING → Missing inverted-index iterator
//
// These require a real `IndexSpec` with a populated `missingFieldDict`, which
// the lightweight `MockQueryEvalCtx` cannot provide, so they rely on the
// full-FFI `TestContext`.
// ---------------------------------------------------------------------------

// Disabled under Miri: `TestContext` calls into the C library (e.g. to build
// the spec and its `missingFieldDict`), and Miri cannot execute foreign
// (non-Rust) functions.
#[cfg(not(miri))]
mod missing {
    use ffi::IndexFlags_Index_StoreFreqs;
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    use super::*;

    #[test]
    fn eval_missing_returns_iterator_over_missing_docs() {
        let _guard = GlobalGuard::default();
        // Real spec whose `missingFieldDict` records docs 1, 2 and 3 as
        // missing a value for the indexed field.
        let context = TestContext::missing([1u64, 2, 3].into_iter());

        // The `QueryEvalCtx` is backed by the real `sctx`, so `eval_missing`
        // sees the populated `missingFieldDict`.
        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        let mut mock_node = MockQueryNode::new(QueryNodeType::Missing);
        mock_node.set_missing_field(context.field_spec());
        let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node)
            .expect("field has missing values, so should not be None")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::InvIdxMissing);
        for expected in [1, 2, 3] {
            let r = it.read().unwrap().expect("should have a result");
            assert_eq!(r.doc_id, expected);
        }
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    #[test]
    fn eval_missing_no_values_returns_none() {
        let _guard = GlobalGuard::default();
        // Term context: the field exists but has no entry in `missingFieldDict`
        // (no document is missing a value for it).
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        let mut mock_node = MockQueryNode::new(QueryNodeType::Missing);
        mock_node.set_missing_field(context.field_spec());
        let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

        assert!(
            eval::eval_node(&mut ctx, &node).is_none(),
            "no missing values for the field, so eval should return None"
        );
    }

    #[test]
    fn qast_iterate_substitutes_empty_for_none() {
        let _guard = GlobalGuard::default();
        // Term context: the field has no entry in `missingFieldDict`, so a
        // QN_MISSING node makes `eval_node` return `None`. `qast_iterate` must
        // substitute an `Empty` iterator instead of propagating that `None`.
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        let mut mock_node = MockQueryNode::new(QueryNodeType::Missing);
        mock_node.set_missing_field(context.field_spec());
        let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

        let mut it = eval::qast_iterate(&mut ctx, &node).into_boxed();

        assert_eq!(it.type_(), IteratorType::Empty);
        assert!(it.at_eof());
        assert!(matches!(it.read(), Ok(None)));
    }
}

/// Create an SDS string from `s`. The caller owns the result and must free
/// it with [`ffi::sdsfree`].
#[cfg(not(miri))]
fn new_sds(s: &str) -> ffi::sds {
    // SAFETY: `s` points to `s.len()` valid bytes; `sdsnewlen` copies them
    // into a freshly allocated SDS string.
    unsafe { ffi::sdsnewlen(s.as_ptr().cast(), s.len()) }
}

// ---------------------------------------------------------------------------
// QN_IDS → IdList, resolving key names through the DocTable
//
// The lightweight `MockQueryEvalCtx` has an empty `DocTable`, so the key→docId
// resolution path (`DocTable_GetId`) is exercised here with the full-FFI
// `TestContext`, which can register real documents.
// ---------------------------------------------------------------------------

// Disabled under Miri: `TestContext` and SDS creation call into the C library,
// which Miri cannot execute.
#[cfg(not(miri))]
mod ids_doctable {
    use ffi::IndexFlags_Index_StoreFreqs;
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    use super::*;

    #[test]
    fn eval_ids_resolves_keys_through_doc_table() {
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        // Register two documents; ids are assigned incrementally from 1.
        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        assert_eq!((id_a, id_b), (1, 2));

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        // Query for both known keys plus an unknown one, which must resolve to
        // id 0 and be filtered out. `doc_ids` is NULL, so the DocTable lookup
        // path is taken.
        let keys: Vec<ffi::sds> = vec![new_sds("doc_b"), new_sds("ghost"), new_sds("doc_a")];

        let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
        mock_node.set_ids(keys.as_ptr(), std::ptr::null_mut(), keys.len());
        let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node)
            .expect("should not be None")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::IdListSorted);
        // Results are sorted; the unknown key is dropped.
        let r = it.read().unwrap().unwrap();
        assert_eq!(r.doc_id, id_a);
        let r = it.read().unwrap().unwrap();
        assert_eq!(r.doc_id, id_b);
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());

        for key in keys {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }

    #[test]
    fn eval_ids_unknown_keys_produce_empty_list() {
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        // None of these keys exist in the (empty) DocTable.
        let keys: Vec<ffi::sds> = vec![new_sds("nope"), new_sds("missing")];

        let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
        mock_node.set_ids(keys.as_ptr(), std::ptr::null_mut(), keys.len());
        let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node)
            .expect("should not be None")
            .into_boxed();

        assert!(it.at_eof());
        assert!(matches!(it.read(), Ok(None)));

        for key in keys {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }
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

        let mut it = eval::eval_node(&mut ctx, &node)
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

        let mut it = eval::eval_node(&mut ctx, &node)
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

        let mut it = eval::eval_node(&mut ctx, &node)
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
