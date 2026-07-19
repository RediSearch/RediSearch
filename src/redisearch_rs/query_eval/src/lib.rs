/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Query evaluation: traverses a parsed query AST and builds an executable
//! iterator tree.

pub mod eval;

// The query wrapper types live in the `query` crate (`c_wrappers/query`), and
// the scorer/expander name modules in `query_types`; both are re-exported here
// so `query_eval` (and its FFI crate) can refer to them through a single module.
pub use query::{QueryEvalContext, QueryNode, QueryNodeRef};
pub use query_types::{expanders, scorers};

#[cfg(test)]
mod _test_link {
    extern crate redisearch_rs;
    redis_mock::mock_or_stub_missing_redis_c_symbols!();
}
