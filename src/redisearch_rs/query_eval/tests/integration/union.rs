/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_UNION → Union

use query_eval::{QueryEvalContext, QueryNodeRef, eval, eval::Config};
use query_types::QueryNodeType;
use rqe_iterators::{IteratorType, RQEIterator};

use query::mock::{MockQueryEvalCtx, MockQueryNode};

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_new_sz)")]
fn eval_union_merges_children() {
    // A union of two wildcard children matches every document in the index.
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.set_max_doc_id(3);
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let mut c1 = MockQueryNode::new(QueryNodeType::Wildcard);
    c1.opts_mut().weight = 1.0;
    let mut c2 = MockQueryNode::new(QueryNodeType::Wildcard);
    c2.opts_mut().weight = 1.0;
    let mut union = MockQueryNode::new(QueryNodeType::Union);
    union.opts_mut().weight = 1.0;
    union.set_children(&[c1.as_ptr(), c2.as_ptr()]);
    let node = unsafe { QueryNodeRef::new(union.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("should not be None")
        .into_boxed();

    assert_eq!(it.type_(), IteratorType::Union);
    for expected in [1, 2, 3] {
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, expected);
    }
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_new_sz)")]
fn eval_union_zero_weight_takes_quick_exit() {
    // A zero-weight union only needs the matching id set, so `quick_exit` is
    // enabled. In quick-exit mode the reducer shortcircuits a wildcard child:
    // the union of two wildcards collapses to a single wildcard (unlike the
    // full `Union` built when `quick_exit` is false).
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.set_max_doc_id(3);
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let mut c1 = MockQueryNode::new(QueryNodeType::Wildcard);
    c1.opts_mut().weight = 1.0;
    let mut c2 = MockQueryNode::new(QueryNodeType::Wildcard);
    c2.opts_mut().weight = 1.0;
    let mut union = MockQueryNode::new(QueryNodeType::Union);
    // Zero weight drives `quick_exit = true`.
    union.opts_mut().weight = 0.0;
    union.set_children(&[c1.as_ptr(), c2.as_ptr()]);
    let node = unsafe { QueryNodeRef::new(union.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("should not be None")
        .into_boxed();

    // Quick-exit collapsed the union to a single wildcard child.
    assert_eq!(it.type_(), IteratorType::Wildcard);
    for expected in [1, 2, 3] {
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, expected);
    }
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_new_sz)")]
fn eval_union_in_not_subtree_takes_quick_exit() {
    // Inside a `NOT` subtree only the matching id set matters, so `quick_exit`
    // is enabled via the `in_not_sub_tree` disjunct even though the node's weight is
    // non-zero. As above, quick-exit mode collapses the union of two wildcards to
    // a single wildcard child.
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.set_max_doc_id(3);
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };
    // Drive `quick_exit` through `ctx.in_not_sub_tree()` rather than a zero weight.
    ctx.set_in_not_sub_tree(true);

    let mut c1 = MockQueryNode::new(QueryNodeType::Wildcard);
    c1.opts_mut().weight = 1.0;
    let mut c2 = MockQueryNode::new(QueryNodeType::Wildcard);
    c2.opts_mut().weight = 1.0;
    let mut union = MockQueryNode::new(QueryNodeType::Union);
    // Non-zero weight, so only the `in_not_sub_tree` disjunct can enable quick-exit.
    union.opts_mut().weight = 1.0;
    union.set_children(&[c1.as_ptr(), c2.as_ptr()]);
    let node = unsafe { QueryNodeRef::new(union.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("should not be None")
        .into_boxed();

    // Quick-exit collapsed the union to a single wildcard child.
    assert_eq!(it.type_(), IteratorType::Wildcard);
    for expected in [1, 2, 3] {
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, expected);
    }
    assert!(matches!(it.read(), Ok(None)));
}

// ---------------------------------------------------------------------------
// QN_UNION → Union over multiple real children with distinct id sets.
//
// Exercising the merge/dedup needs children resolving to overlapping-but-
// distinct documents, which requires the full-FFI `TestContext`.
// ---------------------------------------------------------------------------

// Disabled under Miri: `TestContext` and SDS creation call into the C library,
// which Miri cannot execute.
#[cfg(not(miri))]
mod union {
    use ffi::IndexFlags_Index_StoreFreqs;
    use rqe_iterators::IteratorsConfig;
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    use super::*;

    fn new_sds(s: &str) -> ffi::sds {
        // SAFETY: `s` points to `s.len()` valid bytes; `sdsnewlen` copies them
        // into a freshly allocated SDS string.
        unsafe { ffi::sdsnewlen(s.as_ptr().cast(), s.len()) }
    }

    #[test]
    fn eval_union_merges_distinct_children() {
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        let id_c = context.add_document("doc_c");
        assert_eq!((id_a, id_b, id_c), (1, 2, 3));

        // The union path reads `config.min_union_iter_heap`, which the zero-init
        // `QueryEvalCtx` leaves NULL; provide a real config.
        let qctx = context.qctx();
        let mut cfg = IteratorsConfig {
            max_prefix_expansions: 200,
            min_term_prefix: 2,
            min_stem_length: 4,
            min_union_iter_heap: 20,
        };
        // SAFETY: `qctx` is a valid, exclusively-owned `QueryEvalCtx`; `cfg`
        // outlives `ctx` below and has the same layout as the C config.
        unsafe { (*qctx.as_ptr()).config = (&mut cfg as *mut IteratorsConfig).cast() };
        let mut ctx = unsafe { QueryEvalContext::new(qctx) };

        // child 1 matches {doc_a, doc_b}; child 2 matches {doc_b, doc_c}; the
        // union is {doc_a, doc_b, doc_c}.
        let keys1: Vec<ffi::sds> = vec![new_sds("doc_a"), new_sds("doc_b")];
        let keys2: Vec<ffi::sds> = vec![new_sds("doc_b"), new_sds("doc_c")];
        // IDS nodes carry pre-resolved docIds (resolved on the main thread in
        // production), positionally matching the keys.
        let mut dids1 = vec![id_a, id_b];
        let mut dids2 = vec![id_b, id_c];
        let mut c1 = MockQueryNode::new(QueryNodeType::Ids);
        c1.set_ids(keys1.as_ptr(), dids1.as_mut_ptr(), keys1.len());
        let mut c2 = MockQueryNode::new(QueryNodeType::Ids);
        c2.set_ids(keys2.as_ptr(), dids2.as_mut_ptr(), keys2.len());

        let mut union = MockQueryNode::new(QueryNodeType::Union);
        union.opts_mut().weight = 1.0;
        union.set_children(&[c1.as_ptr(), c2.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(union.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("should not be None")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::Union);
        for expected in [1, 2, 3] {
            let r = it.read().unwrap().expect("should have a result");
            assert_eq!(r.doc_id, expected);
        }
        assert!(matches!(it.read(), Ok(None)));

        for key in keys1.into_iter().chain(keys2) {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }

    #[test]
    fn eval_union_none_child_dropped() {
        let _guard = GlobalGuard::default();
        // A union child that evaluates to `None` (a QN_MISSING node for a field
        // with no missing values) yields a NULL child pointer, which the
        // multi-child path substitutes with a freshly built `Empty` iterator.
        // The union reducer then strips that empty child, leaving the single
        // real child — so the union collapses to it.
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        assert_eq!((id_a, id_b), (1, 2));

        // The union path reads `config.min_union_iter_heap`, which the zero-init
        // `QueryEvalCtx` leaves NULL; provide a real config.
        let qctx = context.qctx();
        let mut cfg = IteratorsConfig {
            max_prefix_expansions: 200,
            min_term_prefix: 2,
            min_stem_length: 4,
            min_union_iter_heap: 20,
        };
        // SAFETY: `qctx` is a valid, exclusively-owned `QueryEvalCtx`; `cfg`
        // outlives `ctx` below and has the same layout as the C config.
        unsafe { (*qctx.as_ptr()).config = (&mut cfg as *mut IteratorsConfig).cast() };
        let mut ctx = unsafe { QueryEvalContext::new(qctx) };

        // child 1: QN_MISSING for a field with no missing values → None.
        let mut missing_child = MockQueryNode::new(QueryNodeType::Missing);
        missing_child.set_missing_field(context.field_spec());
        // child 2: QN_IDS resolving to a real document (pre-resolved docId).
        let keys: Vec<ffi::sds> = vec![new_sds("doc_a")];
        let mut dids = vec![id_a];
        let mut ids_child = MockQueryNode::new(QueryNodeType::Ids);
        ids_child.set_ids(keys.as_ptr(), dids.as_mut_ptr(), keys.len());

        let mut union = MockQueryNode::new(QueryNodeType::Union);
        union.opts_mut().weight = 1.0;
        union.set_children(&[missing_child.as_ptr(), ids_child.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(union.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("a multi-child union always yields an iterator")
            .into_boxed();

        // The empty child was dropped, leaving the single QN_IDS child, to which
        // the union collapses.
        assert_eq!(it.type_(), IteratorType::IdListSorted);
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, 1);
        assert!(matches!(it.read(), Ok(None)));

        for key in keys {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }
}
