/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(
    clippy::undocumented_unsafe_blocks,
    clippy::missing_safety_doc,
    clippy::multiple_unsafe_ops_per_block
)]

pub mod benchers;
pub mod ffi;

// Some of the missing C symbols are actually Rust-provided.
pub use redisearch_rs;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();
