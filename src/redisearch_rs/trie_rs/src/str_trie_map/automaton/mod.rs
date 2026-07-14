/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! UTF-8-aware streaming automatons for
//! [`StrTrieMap`](super::StrTrieMap) queries.
//!
//! The byte-level automaton framework —
//! [`Automaton`](crate::iter::Automaton), its driver, and the wildcard
//! NFA — lives with the byte-keyed trie and knows nothing about key
//! encoding. The automatons here are its Unicode-aware consumers: they
//! reassemble codepoints from the byte stream (via [`utf8`]'s
//! `CodepointDecoder`, handling trie edges that split a codepoint) and
//! match under per-codepoint case folding, which only makes sense for
//! the UTF-8 keys a [`StrTrieMap`](super::StrTrieMap) stores. Keys that
//! are not valid UTF-8 never match.
//!
//! - [`case_fold`]: [`CaseFoldExact`] — case-insensitive exact match.
//! - [`wildcard`]: [`CodepointWildcard`] — wildcard matching where `?`
//!   consumes one codepoint, plus its trie automaton
//!   ([`CodepointWildcardNfa`]).

pub mod case_fold;
mod utf8;
pub mod wildcard;

pub use case_fold::CaseFoldExact;
pub use wildcard::{CodepointWildcard, CodepointWildcardNfa};
