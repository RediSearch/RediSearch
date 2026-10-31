/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use query_eval::{Config, QueryEvalContext, QueryIteratorTree, QueryNodeMut};
use query_types::QueryNodeType;
use rqe_iterators::{
    Empty, IteratorType, RQEIterator, c2rust::CRQEIterator, interop::RQEIteratorWrapper,
};
use rqe_iterators_test_utils::ContractChecker;
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
fn root_type_sees_through_profile() {
    for (node_type, root_type) in [
        (QueryNodeType::Null, IteratorType::Empty),
        (QueryNodeType::Wildcard, IteratorType::Wildcard),
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
        assert_eq!(tree.root_type(), root_type);

        let tree = tree.into_profiled();
        assert_eq!(tree.root_type(), root_type);
        let it = tree.into_root();
        assert_eq!(it.type_(), IteratorType::Profile);
    }
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

#[test]
fn replace_root_updates_root_type() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.set_max_doc_id(10);
    // SAFETY: `mock_ctx` is a fully initialised `QueryEvalCtx` whose pointer
    // fields are all valid, and it outlives `ctx` and the tree built from it.
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };
    let mock_node = MockQueryNode::new(QueryNodeType::Wildcard);
    // SAFETY: `mock_node` is a valid `RSQueryNode`, not accessed elsewhere
    // while `node` is alive.
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let mut tree = QueryIteratorTree::new(&mut ctx, node, Config::default());
    assert_eq!(tree.root_type(), IteratorType::Wildcard);

    let new_root = NonNull::new(RQEIteratorWrapper::boxed_new(Empty)).unwrap();
    // SAFETY: `new_root` is a freshly boxed, valid iterator that nothing else owns.
    let previous = tree.replace_root(unsafe { CRQEIterator::new(new_root) });
    assert_eq!(tree.root_type(), IteratorType::Empty);

    // SAFETY: `replace_root` handed back ownership of the previous root, which
    // nothing else owns.
    drop(unsafe { CRQEIterator::new(previous) });
}
