/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for `tag_index_ffi`, driving the crate the way C does:
//! through raw pointers and the exported `Rust_*` entry points only.
//!
//! What is worth testing here is the boundary itself — the mode-erased handle,
//! its lifecycle, and the out-parameter protocols — not the indexing behaviour,
//! which `tag_index`'s own integration tests already cover.

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

mod handle;
mod iteration;
mod provenance;
mod suffix_expansion;
