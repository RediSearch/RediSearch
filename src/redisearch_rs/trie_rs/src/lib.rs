/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A trie map implementation with minimal memory footprint.
//!
//! Check [`TrieMap`]'s documentation for more details.

pub mod iter;
mod node;
mod trie;
mod utils;

pub use trie::TrieMap;
