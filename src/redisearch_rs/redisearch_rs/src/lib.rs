/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `redisearch_rs` is the entrypoint for all the modules, on the C side, who need to consume functionality
//! that's implemented in Rust.
//!
//! It exposes an FFI module for each workspace crate that must be consumed (directly) by the C code.
pub mod trie;

/// Registers the Redis module allocator as the global allocator for the application.
#[cfg(not(feature = "mock_allocator"))]
#[global_allocator]
static REDIS_MODULE_ALLOCATOR: redis_module::alloc::RedisAlloc = redis_module::alloc::RedisAlloc;
