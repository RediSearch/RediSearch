/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types and functions for benchmarking trie operations.
use redis_mock::bind_redis_alloc_symbols_to_mock_impl;
use std::{ffi::c_void, ptr::NonNull};

bind_redis_alloc_symbols_to_mock_impl!();

pub use bencher::OperationBencher;

pub mod bencher;
pub mod corpus;

// Convenient aliases for the trie types that are being benchmarked.
pub type RustTrieMap = trie_rs::TrieMap<NonNull<c_void>>;
