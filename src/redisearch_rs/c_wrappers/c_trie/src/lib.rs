/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Rust wrapper for the C Trie API.
//!
//! This crate provides a safe Rust interface to the C Trie implementation, one
//! module per trie kind: [`TermsTrie`] for the primary term index and
//! [`SuffixTrie`] for the suffix index that answers queries which are not
//! front-anchored. [`LoweredPattern`], the wildcard pattern both kinds walk
//! with, sits alongside them.

mod pattern;
mod suffix;
mod terms;

pub use pattern::LoweredPattern;
pub use suffix::{SuffixMode, SuffixTrie, SuffixWalk};
pub use terms::{TermsTrie, TermsTrieAllIterator, TermsTrieDecrResult};
