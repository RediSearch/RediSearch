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
//! implemented using `libnu` for Unicode operations.

/// Maximum number of runes (lowercased codepoints) allowed in a single
/// conversion, matching the C `MAX_RUNESTR_LEN` constant.
pub const MAX_RUNE_STR_LEN: usize = 1024;

/// Error returned when the lowercased rune sequence exceeds
/// [`MAX_RUNE_STR_LEN`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("lowercased rune length {len} exceeds maximum {MAX_RUNE_STR_LEN}")]
pub struct RuneStrTooLong {
    pub len: usize,
}

/// Convert a UTF-8 string to a lowercase array of runes ([`u16`]) for trie
/// lookups.
///
/// Each Unicode codepoint is lowercased and then truncated to [`u16`].
///
/// Returns [`Err(`[`RuneStrTooLong`]`)`] if the resulting rune count exceeds
/// [`MAX_RUNE_STR_LEN`].
pub fn str_to_lower_runes(s: &str) -> Result<Vec<u16>, RuneStrTooLong> {
    let runes: Vec<u16> = s
        .chars()
        .flat_map(char::to_lowercase)
        .map(|c| c as u16)
        .collect();
    if runes.len() > MAX_RUNE_STR_LEN {
        return Err(RuneStrTooLong { len: runes.len() });
    }
    Ok(runes)
}
