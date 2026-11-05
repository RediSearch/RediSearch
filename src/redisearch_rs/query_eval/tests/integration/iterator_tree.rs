/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IndexFlags_Index_DocIdsOnly, QueryIterator};
use index_result::RSIndexResult;
use inverted_index::{InvertedIndex, doc_ids_only::DocIdsOnly, opaque};
use query_eval::{Config, DocIdCoverage, QueryEvalContext, QueryIteratorTree, QueryNodeMut};
use query_types::QueryNodeType;
use rqe_iterators::{
    Empty, IteratorType, RQEIterator, c2rust::CRQEIterator, interop::RQEIteratorWrapper,
    inverted_index::Wildcard as InvIdxWildcard,
};
use rqe_iterators_test_utils::{ContractChecker, MockContext};
use std::ptr::NonNull;

use query::mock::{MockQueryEvalCtx, MockQueryNode};

#[test]
fn new_evaluates_root_node() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };
    let mock_node = MockQueryNode::new(QueryNodeType::Null);
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let mut it =
        ContractChecker::new(QueryIteratorTree::new(&mut ctx, node, Config::default()).into_root());

    assert_eq!(it.type_(), IteratorType::Empty);
    assert!(it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn into_profiled_wraps_root() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };
    let mock_node = MockQueryNode::new(QueryNodeType::Null);
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let tree = QueryIteratorTree::new(&mut ctx, node, Config::default()).into_profiled();
    let mut it = ContractChecker::new(tree.into_root());

    assert_eq!(it.type_(), IteratorType::Profile);
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn doc_id_coverage_survives_profiling() {
    for (node_type, coverage) in [
        (QueryNodeType::Null, DocIdCoverage::Selective),
        (QueryNodeType::Wildcard, DocIdCoverage::Dense),
    ] {
        let mut mock_ctx = MockQueryEvalCtx::new();
        mock_ctx.set_max_doc_id(10);
        // SAFETY: `mock_ctx` is a fully initialised `QueryEvalCtx` whose pointer
        // fields are all valid, and it outlives `ctx` and the tree built from it.
        let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };
        let mock_node = MockQueryNode::new(node_type);
        // SAFETY: `mock_node` is a valid `RSQueryNode`, not accessed elsewhere
        // while `node` is alive.
        let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

        let tree = QueryIteratorTree::new(&mut ctx, node, Config::default());
        assert_eq!(tree.doc_id_coverage(), coverage);

        let tree = tree.into_profiled();
        assert_eq!(tree.doc_id_coverage(), coverage);
        let it = tree.into_root();
        assert_eq!(it.type_(), IteratorType::Profile);
    }
}

#[test]
fn dense_doc_id_coverage_for_wildcard_on_an_index_all_index() {
    // On an `INDEXALL ENABLE` index, `*` walks the existing-docs inverted index.
    let ctx = MockContext::new(10, 10);
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in 1..=10 {
        let record = RSIndexResult::build_virt().doc_id(doc_id).build();
        ii.add_record(&record).unwrap();
    }
    let existing_docs = Box::into_raw(Box::new(opaque::InvertedIndex::DocIdsOnly(ii)));
    ctx.spec_write().rule_mut().set_index_all(true);
    ctx.spec_write().set_existing_docs_ptr(existing_docs.cast());

    // SAFETY: `MockContext` builds a valid `QueryEvalCtx` whose spec, rule and
    // doc table outlive `qctx` and the tree built from it.
    let mut qctx = unsafe { QueryEvalContext::new(ctx.qctx()) };
    let mock_node = MockQueryNode::new(QueryNodeType::Wildcard);
    // SAFETY: `mock_node` is a valid `RSQueryNode`, not accessed elsewhere
    // while `node` is alive.
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let tree = QueryIteratorTree::new(&mut qctx, node, Config::default());
    assert_eq!(tree.doc_id_coverage(), DocIdCoverage::Dense);
    let it = tree.into_root();
    assert_eq!(it.type_(), IteratorType::InvIdxWildcard);
    drop(it);

    ctx.spec_write().set_existing_docs_ptr(std::ptr::null_mut());
    // SAFETY: `existing_docs` came from `Box::into_raw` above, and the iterator
    // reading it was dropped.
    drop(unsafe { Box::from_raw(existing_docs) });
}

#[test]
#[should_panic(expected = "Attempted to double-profile an iterator")]
fn into_profiled_twice_panics() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };
    let mock_node = MockQueryNode::new(QueryNodeType::Null);
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let _ = QueryIteratorTree::new(&mut ctx, node, Config::default())
        .into_profiled()
        .into_profiled();
}

#[test]
fn replace_root_hands_back_previous_root() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };
    let mock_node = MockQueryNode::new(QueryNodeType::Null);
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let mut tree = QueryIteratorTree::new(&mut ctx, node, Config::default());
    let previous = tree.root_ptr();
    let new_root = NonNull::new(RQEIteratorWrapper::boxed_new(Empty)).unwrap();

    // SAFETY: `new_root` is a freshly boxed, valid iterator that nothing else owns.
    let returned = tree.replace_root(unsafe { CRQEIterator::new(new_root) });

    assert_eq!(returned, previous);
    assert_eq!(tree.root_ptr(), new_root);
    assert_eq!(tree.into_raw_root(), new_root);
    // SAFETY: `replace_root` handed back ownership of the previous root, which
    // nothing else owns.
    drop(unsafe { CRQEIterator::new(returned) });
    // SAFETY: `into_raw_root` handed back ownership of `new_root`, which
    // nothing else owns.
    drop(unsafe { CRQEIterator::new(new_root) });
}

/// Replace the root of a tree built from `node_type` with `new_root`,
/// returning the tree's [`QueryIteratorTree::doc_id_coverage`] before and
/// after.
fn doc_id_coverage_across_replace_root(
    node_type: QueryNodeType,
    new_root: NonNull<QueryIterator>,
) -> (DocIdCoverage, DocIdCoverage) {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.set_max_doc_id(10);
    // SAFETY: `mock_ctx` is a fully initialised `QueryEvalCtx` whose pointer
    // fields are all valid, and it outlives `ctx` and the tree built from it.
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };
    let mock_node = MockQueryNode::new(node_type);
    // SAFETY: `mock_node` is a valid `RSQueryNode`, not accessed elsewhere
    // while `node` is alive.
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let mut tree = QueryIteratorTree::new(&mut ctx, node, Config::default());
    let before = tree.doc_id_coverage();
    // SAFETY: the caller hands over `new_root`, a valid iterator that nothing
    // else owns.
    let previous = tree.replace_root(unsafe { CRQEIterator::new(new_root) });
    let after = tree.doc_id_coverage();

    // SAFETY: `replace_root` handed back ownership of the previous root, which
    // nothing else owns.
    drop(unsafe { CRQEIterator::new(previous) });
    (before, after)
}

#[test]
fn replace_root_with_a_non_wildcard_makes_coverage_selective() {
    let new_root = NonNull::new(RQEIteratorWrapper::boxed_new(Empty)).unwrap();
    assert_eq!(
        doc_id_coverage_across_replace_root(QueryNodeType::Wildcard, new_root),
        (DocIdCoverage::Dense, DocIdCoverage::Selective)
    );
}

#[test]
fn replace_root_with_an_inverted_index_wildcard_makes_coverage_dense() {
    // The iterator `*` evaluates to on an `INDEXALL ENABLE` index.
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in 1..=10 {
        let record = RSIndexResult::build_virt().doc_id(doc_id).build();
        ii.add_record(&record).unwrap();
    }
    let wildcard = InvIdxWildcard::new(ii.reader(), 1.0);
    assert_eq!(wildcard.type_(), IteratorType::InvIdxWildcard);
    let new_root = NonNull::new(RQEIteratorWrapper::boxed_new(wildcard)).unwrap();

    assert_eq!(
        doc_id_coverage_across_replace_root(QueryNodeType::Null, new_root),
        (DocIdCoverage::Selective, DocIdCoverage::Dense)
    );
}
