/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! String utility functions for query evaluation.
//!
//! These are pure-Rust replacements for C helpers that were previously
//! implemented using `libnu` for Unicode operations. Rust's standard library
//! provides equivalent Unicode support via [`str::to_lowercase`] and
//! [`char::to_lowercase`].

/// Convert a UTF-8 string to a lowercase array of runes ([`u16`]) for trie
/// lookups.
///
/// Each Unicode codepoint is lowercased and then truncated to [`u16`].
pub fn str_to_lower_runes(s: &str) -> Vec<u16> {
    s.chars()
        .flat_map(char::to_lowercase)
        .map(|c| c as u16)
        .collect()
}
