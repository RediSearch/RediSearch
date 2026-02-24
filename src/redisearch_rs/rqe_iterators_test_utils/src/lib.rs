/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Test utilities for rqe_iterators.
//!
//! This module provides utilities for testing iterators, including contexts
//! for setting up test environments.

#[allow(clippy::undocumented_unsafe_blocks)]
#[allow(clippy::multiple_unsafe_ops_per_block)]
pub mod mock_context;
#[allow(clippy::undocumented_unsafe_blocks)]
#[allow(clippy::multiple_unsafe_ops_per_block)]
pub mod test_context;

pub use mock_context::MockContext;
pub use test_context::{GlobalGuard, TestContext};
