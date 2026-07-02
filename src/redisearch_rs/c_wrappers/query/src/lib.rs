/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe Rust wrappers around the C query types used during query evaluation.

mod query_eval_ctx;
mod query_node_ref;

pub use query_eval_ctx::QueryEvalContext;
pub use query_node_ref::{QueryNode, QueryNodeMut, QueryNodeRef, WildcardMode};

/// Test-only mocks for the query wrapper types, shared with the `query_eval`
/// crate's evaluation-dispatcher tests.
#[cfg(feature = "unittest")]
pub mod mock;

#[cfg(test)]
mod _test_link {
    extern crate redisearch_rs;
    redis_mock::mock_or_stub_missing_redis_c_symbols!();
}
