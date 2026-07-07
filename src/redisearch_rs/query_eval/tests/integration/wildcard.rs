/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_WILDCARD → Wildcard

use query_eval::{QueryEvalContext, QueryNodeRef, eval};
use query_node_type::QueryNodeType;
use rqe_iterators::{IteratorType, RQEIterator};

use query::mock::{MockQueryEvalCtx, MockQueryNode};

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
