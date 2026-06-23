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

pub mod rdb;
pub mod str_trie_map;
mod trie_count;
mod trie_map;
mod trie_map_opaque;

pub use trie_count::TrieCount;
pub use trie_map::TrieMap;
pub use trie_map::iter;
pub use trie_map_opaque::TrieMapOpaque;
