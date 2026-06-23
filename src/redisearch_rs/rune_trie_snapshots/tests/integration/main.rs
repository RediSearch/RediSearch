/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests that snapshot the C rune-trie's behavior.

mod support;

mod boundary_cases;
mod contains_iteration;
mod delete_and_decrement;
mod dfa_iteration;
mod incr_mode;
mod insert_iterate;
mod range_iteration;
mod splits;
mod unicode;
mod wildcard_iteration;

// Force-link the lib crate so its build script's `static:-bundle=redisearch_all`
// directive (the only place we declare the dependency on `libredisearch_all.a`)
// is honored by the linker when building this test binary. Without this, the
// integration target never references the lib, so rustc skips its rlib entirely
// and the link directive is silently dropped.
extern crate rune_trie_snapshots;
// Pull the combined C static library into the test binary.
extern crate redisearch_rs;
// Provide Rust-backed implementations for the Redis allocator symbols the C
// trie calls into (rm_malloc / rm_free etc.).
redis_mock::mock_or_stub_missing_redis_c_symbols!();
