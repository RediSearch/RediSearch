/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types and functions for benchmarking trie operations.
//!
//! The harness compares the Rust [`TermDictionary`] (the swap target for
//! `sp->terms`, owns case-folding via ICU) against the C `Trie` configured
//! in `Trie_Sort_Lex` mode. See [`crate::bencher`] and [`crate::c_trie`].
//!
//! [`TermDictionary`]: trie_rs::str::term_dict::TermDictionary
use redis_mock::mock_or_stub_missing_redis_c_symbols;

mock_or_stub_missing_redis_c_symbols!();

pub use bencher::{FoldMode, OperationBencher};

pub mod bencher;
pub mod c_trie;
pub mod corpus;

/// The Rust type the bench harness compares against the C `Trie`. This is
/// the production swap target for `sp->terms`: a case-folded wrapper over
/// `StrTrieMap<TermEntry>`.
pub type RustTrieMap = trie_rs::str::term_dict::TermDictionary;
