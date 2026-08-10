/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Test-only mocks for the query wrapper types.
//!
//! These mocks are gated behind the `unittest` feature so they can be shared
//! across crates: this crate's own integration tests use them, and so does the
//! `query_eval` crate's evaluation-dispatcher tests.

mod query_eval_ctx;
mod query_node_ref;

pub use query_eval_ctx::MockQueryEvalCtx;
pub use query_node_ref::MockQueryNode;
