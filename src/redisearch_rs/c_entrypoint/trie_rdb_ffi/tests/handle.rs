/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Lifecycle of the [`LexTrieRs`] handle.
//!
//! The save and load entry points need a `RedisModuleIO`, which no Rust-side
//! mock provides, so they are exercised from the C++ interop suite against
//! `redismock`'s RDB IO instead. What is testable here is the allocation
//! boundary — and it is worth testing under `miri`, which reports the leak or
//! the double free these assertions cannot see on their own.

use redis_mock::mock_or_stub_missing_redis_c_symbols;
use trie_rdb_ffi::{LexTrieRs_Free, LexTrieRs_New};

mock_or_stub_missing_redis_c_symbols!();

#[test]
fn new_allocates_a_handle_that_free_releases() {
    let sut = LexTrieRs_New();

    assert!(!sut.is_null());

    // SAFETY: `sut` came from `LexTrieRs_New` and has not been freed.
    unsafe { LexTrieRs_Free(sut) };
}
