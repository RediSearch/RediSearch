/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types and functions for benchmarking trie operations.
use std::{ffi::c_void, ptr::NonNull};

// Force the compiler to link the symbols defined in `redis_mock`,
// since they are required by `libtrie.a`.
extern crate redis_mock;

pub use bencher::OperationBencher;

pub mod bencher;
pub mod c_map;
pub mod corpus;
pub mod ffi;

// Convenient aliases for the trie types that are being benchmarked.
pub use c_map::CTrieMap;
pub type RustTrieMap = trie_rs::TrieMap<NonNull<c_void>>;
