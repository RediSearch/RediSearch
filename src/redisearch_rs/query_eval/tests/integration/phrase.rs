/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_PHRASE → Intersection

use query_eval::{QueryEvalContext, QueryNodeRef, eval, eval::Config};
use query_node_type::QueryNodeType;
use rqe_iterators::{IteratorType, RQEIterator};

use query::mock::{MockQueryEvalCtx, MockQueryNode};

// ---------------------------------------------------------------------------
// QN_PHRASE → Intersection (single child shortcut)
// ---------------------------------------------------------------------------

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_new_sz)")]
fn eval_phrase_single_child_returns_child() {
    // An intersection of one child is equivalent to the child itself.
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.set_max_doc_id(3);
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let mut wc_child = MockQueryNode::new(QueryNodeType::Wildcard);
    wc_child.opts_mut().weight = 1.0;
    let mut phrase = MockQueryNode::new(QueryNodeType::Phrase);
    phrase.opts_mut().weight = 1.0;
    phrase.set_children(&[wc_child.as_ptr()]);
    let node = unsafe { QueryNodeRef::new(phrase.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("should not be None")
        .into_boxed();

    // The single child is returned directly, not wrapped in an intersection.
    assert_eq!(it.type_(), IteratorType::Wildcard);
    for expected in [1, 2, 3] {
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, expected);
    }
    assert!(matches!(it.read(), Ok(None)));
}

// ---------------------------------------------------------------------------
// QN_PHRASE → Intersection over multiple real children.
//
// The multi-child path needs children resolving to real documents. Two QN_IDS
// children provide that; with the query-wide default slop (-1) the phrase is a
// plain set intersection, requiring no positional offsets.
// ---------------------------------------------------------------------------

// Disabled under Miri: `TestContext` and SDS creation call into the C library,
// which Miri cannot execute.
#[cfg(not(miri))]
mod phrase {
    use ffi::IndexFlags_Index_StoreFreqs;
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    use super::*;
    use crate::util::new_sds;

    #[test]
    fn eval_phrase_intersects_children() {
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        let id_c = context.add_document("doc_c");
        assert_eq!((id_a, id_b, id_c), (1, 2, 3));

        // The phrase path reads `opts.slop`/`opts.flags`, which the zero-init
        // `QueryEvalCtx` leaves NULL; provide a real options struct with the
        // query-wide defaults (slop -1, no flags).
        let qctx = context.qctx();
        // SAFETY: `RSSearchOptions` is a plain C struct whose fields (integers,
        // flags, and pointers) are all valid when zero-initialised.
        let mut search_opts: ffi::RSSearchOptions = unsafe { std::mem::zeroed() };
        search_opts.slop = -1;
        // SAFETY: `qctx` is a valid, exclusively-owned `QueryEvalCtx`; `search_opts`
        // outlives `ctx` below.
        unsafe { (*qctx.as_ptr()).opts = &mut search_opts };
        let mut ctx = unsafe { QueryEvalContext::new(qctx) };

        // child 1 matches {doc_a, doc_b}; child 2 matches {doc_b, doc_c}; the
        // intersection is {doc_b}.
        let keys1: Vec<ffi::sds> = vec![new_sds("doc_a"), new_sds("doc_b")];
        let keys2: Vec<ffi::sds> = vec![new_sds("doc_b"), new_sds("doc_c")];
        let mut c1 = MockQueryNode::new(QueryNodeType::Ids);
        c1.set_ids(keys1.as_ptr(), std::ptr::null_mut(), keys1.len());
        let mut c2 = MockQueryNode::new(QueryNodeType::Ids);
        c2.set_ids(keys2.as_ptr(), std::ptr::null_mut(), keys2.len());

        let mut phrase = MockQueryNode::new(QueryNodeType::Phrase);
        phrase.opts_mut().weight = 1.0;
        // -1 → no slop constraint, i.e. a plain set intersection.
        phrase.opts_mut().max_slop = -1;
        phrase.set_children(&[c1.as_ptr(), c2.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(phrase.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("should not be None")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::Intersect);
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, 2);
        assert!(matches!(it.read(), Ok(None)));

        for key in keys1.into_iter().chain(keys2) {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }

    #[test]
    fn eval_phrase_in_order_flag_builds_intersection() {
        // A non-exact phrase with the query-wide `INORDER` flag set must forward
        // `in_order = true` into the constructed `Intersection`. Two real QN_IDS
        // children (neither empty nor a wildcard) are not reducible, so the
        // intersection is built (type `Intersect`); the IDS results carry no
        // offsets for the in-order check to reject, so the shared id still passes.
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        let id_c = context.add_document("doc_c");
        assert_eq!((id_a, id_b, id_c), (1, 2, 3));

        // Query-wide defaults except for the `INORDER` flag, which forces
        // `ctx.search_in_order()` to report `true`.
        let qctx = context.qctx();
        // SAFETY: `RSSearchOptions` is a plain C struct whose fields (integers,
        // flags, and pointers) are all valid when zero-initialised.
        let mut search_opts: ffi::RSSearchOptions = unsafe { std::mem::zeroed() };
        search_opts.slop = -1;
        search_opts.flags |= ffi::RSSearchFlags_Search_InOrder as u32;
        // SAFETY: `qctx` is a valid, exclusively-owned `QueryEvalCtx`; `search_opts`
        // outlives `ctx` below.
        unsafe { (*qctx.as_ptr()).opts = &mut search_opts };
        let mut ctx = unsafe { QueryEvalContext::new(qctx) };

        // child 1 matches {doc_a, doc_b}; child 2 matches {doc_b, doc_c}; the
        // intersection is {doc_b}.
        let keys1: Vec<ffi::sds> = vec![new_sds("doc_a"), new_sds("doc_b")];
        let keys2: Vec<ffi::sds> = vec![new_sds("doc_b"), new_sds("doc_c")];
        let mut c1 = MockQueryNode::new(QueryNodeType::Ids);
        c1.set_ids(keys1.as_ptr(), std::ptr::null_mut(), keys1.len());
        let mut c2 = MockQueryNode::new(QueryNodeType::Ids);
        c2.set_ids(keys2.as_ptr(), std::ptr::null_mut(), keys2.len());

        let mut phrase = MockQueryNode::new(QueryNodeType::Phrase);
        phrase.opts_mut().weight = 1.0;
        // Non-exact, default slop: only the query-wide `INORDER` flag drives
        // `in_order`.
        phrase.opts_mut().max_slop = -1;
        phrase.set_children(&[c1.as_ptr(), c2.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(phrase.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("should not be None")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::Intersect);
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, 2);
        assert!(matches!(it.read(), Ok(None)));

        for key in keys1.into_iter().chain(keys2) {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }

    #[test]
    fn eval_phrase_node_in_order_builds_intersection() {
        // A non-exact phrase with the per-node `in_order` option set (query-wide
        // `INORDER` flag clear) must still forward `in_order = true` into the
        // constructed `Intersection`. This drives the `node.opts().in_order != 0`
        // disjunct rather than `ctx.search_in_order()`. Two real QN_IDS children
        // (neither empty nor a wildcard) are not reducible, so the intersection
        // is built (type `Intersect`); the IDS results carry no offsets for the
        // in-order check to reject, so the shared id still passes.
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        let id_c = context.add_document("doc_c");
        assert_eq!((id_a, id_b, id_c), (1, 2, 3));

        // Query-wide defaults with the `INORDER` flag clear, so only the per-node
        // `in_order` option can drive in-order matching.
        let qctx = context.qctx();
        // SAFETY: `RSSearchOptions` is a plain C struct whose fields (integers,
        // flags, and pointers) are all valid when zero-initialised.
        let mut search_opts: ffi::RSSearchOptions = unsafe { std::mem::zeroed() };
        search_opts.slop = -1;
        // SAFETY: `qctx` is a valid, exclusively-owned `QueryEvalCtx`; `search_opts`
        // outlives `ctx` below.
        unsafe { (*qctx.as_ptr()).opts = &mut search_opts };
        let mut ctx = unsafe { QueryEvalContext::new(qctx) };

        // child 1 matches {doc_a, doc_b}; child 2 matches {doc_b, doc_c}; the
        // intersection is {doc_b}.
        let keys1: Vec<ffi::sds> = vec![new_sds("doc_a"), new_sds("doc_b")];
        let keys2: Vec<ffi::sds> = vec![new_sds("doc_b"), new_sds("doc_c")];
        let mut c1 = MockQueryNode::new(QueryNodeType::Ids);
        c1.set_ids(keys1.as_ptr(), std::ptr::null_mut(), keys1.len());
        let mut c2 = MockQueryNode::new(QueryNodeType::Ids);
        c2.set_ids(keys2.as_ptr(), std::ptr::null_mut(), keys2.len());

        let mut phrase = MockQueryNode::new(QueryNodeType::Phrase);
        phrase.opts_mut().weight = 1.0;
        // Non-exact, default slop, but the per-node `in_order` option is set.
        phrase.opts_mut().max_slop = -1;
        phrase.opts_mut().in_order = 1;
        phrase.set_children(&[c1.as_ptr(), c2.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(phrase.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("should not be None")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::Intersect);
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, 2);
        assert!(matches!(it.read(), Ok(None)));

        for key in keys1.into_iter().chain(keys2) {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }

    #[test]
    fn eval_phrase_none_child_becomes_empty() {
        let _guard = GlobalGuard::default();
        // A phrase child that evaluates to `None` (a QN_MISSING node for a field
        // with no missing values) yields a NULL child pointer, which the
        // multi-child path substitutes with a freshly built `Empty` iterator.
        // An empty child makes the whole intersection empty.
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        assert_eq!((id_a, id_b), (1, 2));

        // The phrase path reads `opts.slop`/`opts.flags`, which the zero-init
        // `QueryEvalCtx` leaves NULL; provide a real options struct with the
        // query-wide defaults (slop -1, no flags).
        let qctx = context.qctx();
        // SAFETY: `RSSearchOptions` is a plain C struct whose fields (integers,
        // flags, and pointers) are all valid when zero-initialised.
        let mut search_opts: ffi::RSSearchOptions = unsafe { std::mem::zeroed() };
        search_opts.slop = -1;
        // SAFETY: `qctx` is a valid, exclusively-owned `QueryEvalCtx`; `search_opts`
        // outlives `ctx` below.
        unsafe { (*qctx.as_ptr()).opts = &mut search_opts };
        let mut ctx = unsafe { QueryEvalContext::new(qctx) };

        // child 1: QN_MISSING for a field with no missing values → None.
        let mut missing_child = MockQueryNode::new(QueryNodeType::Missing);
        missing_child.set_missing_field(context.field_spec());
        // child 2: QN_IDS resolving to a real document.
        let keys: Vec<ffi::sds> = vec![new_sds("doc_a")];
        let mut ids_child = MockQueryNode::new(QueryNodeType::Ids);
        ids_child.set_ids(keys.as_ptr(), std::ptr::null_mut(), keys.len());

        let mut phrase = MockQueryNode::new(QueryNodeType::Phrase);
        phrase.opts_mut().weight = 1.0;
        phrase.opts_mut().max_slop = -1;
        phrase.set_children(&[missing_child.as_ptr(), ids_child.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(phrase.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("a multi-child phrase always yields an iterator")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::Empty);
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());

        for key in keys {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }

    #[test]
    fn eval_phrase_exact_builds_intersection() {
        // An exact (quoted) phrase forces slop 0 / in-order, which `eval_phrase`
        // must forward into a constructed `Intersection`. Two real QN_IDS
        // children (neither empty nor a wildcard) are not reducible, so the
        // reducer reaches its `Proceed` arm and `Intersection::new_with_slop_order`
        // is invoked with `(Some(0), true)` — the only test that exercises that
        // path. Both children resolve to the same document, so the intersection
        // is built (type `Intersect`) and yields that shared id (the IDS results
        // carry no offsets for the slop check to reject).
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        assert_eq!((id_a, id_b), (1, 2));

        let qctx = context.qctx();
        // SAFETY: `RSSearchOptions` is a plain C struct whose fields (integers,
        // flags, and pointers) are all valid when zero-initialised.
        let mut search_opts: ffi::RSSearchOptions = unsafe { std::mem::zeroed() };
        search_opts.slop = -1;
        // SAFETY: `qctx` is a valid, exclusively-owned `QueryEvalCtx`; `search_opts`
        // outlives `ctx` below.
        unsafe { (*qctx.as_ptr()).opts = &mut search_opts };
        let mut ctx = unsafe { QueryEvalContext::new(qctx) };

        // Both children resolve to the shared document `doc_b`.
        let keys1: Vec<ffi::sds> = vec![new_sds("doc_b")];
        let keys2: Vec<ffi::sds> = vec![new_sds("doc_b")];
        let mut c1 = MockQueryNode::new(QueryNodeType::Ids);
        c1.set_ids(keys1.as_ptr(), std::ptr::null_mut(), keys1.len());
        let mut c2 = MockQueryNode::new(QueryNodeType::Ids);
        c2.set_ids(keys2.as_ptr(), std::ptr::null_mut(), keys2.len());

        let mut phrase = MockQueryNode::new(QueryNodeType::Phrase);
        phrase.opts_mut().weight = 1.0;
        // Exact phrase → `eval_phrase` resolves `(Some(0), true)`.
        phrase.set_phrase_exact(1);
        phrase.set_children(&[c1.as_ptr(), c2.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(phrase.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("should not be None")
            .into_boxed();

        // The exact params flowed into a real intersection (not a reduced leaf).
        assert_eq!(it.type_(), IteratorType::Intersect);
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, 2);
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());

        for key in keys1.into_iter().chain(keys2) {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }
}

// ---------------------------------------------------------------------------
// QN_PHRASE reducer/slop paths that need no real index.
//
// These exercise the intersection-reducer shortcircuits (`Empty`, `Single`) and
// the slop/in-order resolution, which depend only on the node options, the
// lightweight `MockQueryEvalCtx`, and the evaluator configuration passed in.
// ---------------------------------------------------------------------------

// Disabled under Miri: building a multi-child node calls the C `array_new_sz`
// foreign function (via `set_children`), which Miri cannot execute.
#[cfg(not(miri))]
mod phrase_reducer {
    use super::*;

    #[test]
    fn eval_phrase_exact_with_empty_children_reduces_to_empty() {
        // An exact (quoted) phrase forces slop 0, in order. With two QN_NULL
        // children — both `Empty` iterators — the intersection reducer
        // shortcircuits any empty child to an overall `Empty` result.
        let mut mock_ctx = MockQueryEvalCtx::new();
        let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

        let c1 = MockQueryNode::new(QueryNodeType::Null);
        let c2 = MockQueryNode::new(QueryNodeType::Null);
        let mut phrase = MockQueryNode::new(QueryNodeType::Phrase);
        phrase.opts_mut().weight = 1.0;
        phrase.set_phrase_exact(1);
        phrase.set_children(&[c1.as_ptr(), c2.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(phrase.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("a multi-child phrase always yields an iterator")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::Empty);
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    #[test]
    fn eval_phrase_slop_all_wildcard_children_reduce_to_single() {
        // A non-exact phrase with an explicit slop (>= 0) takes the
        // node-slop-override branch (`Some(slop as u32)`), distinct from the
        // default-slop (-1) path. Two wildcard children are all stripped by the
        // reducer, leaving a single wildcard returned directly.
        let mut mock_ctx = MockQueryEvalCtx::new();
        mock_ctx.set_max_doc_id(3);
        let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

        let mut c1 = MockQueryNode::new(QueryNodeType::Wildcard);
        c1.opts_mut().weight = 1.0;
        let mut c2 = MockQueryNode::new(QueryNodeType::Wildcard);
        c2.opts_mut().weight = 1.0;
        let mut phrase = MockQueryNode::new(QueryNodeType::Phrase);
        phrase.opts_mut().weight = 1.0;
        // A concrete, non-negative slop exercises the `s => s` arm and the
        // `Some(slop as u32)` branch of the max-slop computation.
        phrase.opts_mut().max_slop = 2;
        phrase.set_children(&[c1.as_ptr(), c2.as_ptr()]);
        let node = unsafe { QueryNodeRef::new(phrase.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("a multi-child phrase always yields an iterator")
            .into_boxed();

        // All children were wildcards, so the reducer returns the single
        // remaining wildcard directly.
        assert_eq!(it.type_(), IteratorType::Wildcard);
        for expected in [1, 2, 3] {
            let r = it.read().unwrap().expect("should have a result");
            assert_eq!(r.doc_id, expected);
        }
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }
}
