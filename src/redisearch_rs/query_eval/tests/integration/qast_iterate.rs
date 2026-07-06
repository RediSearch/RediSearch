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
