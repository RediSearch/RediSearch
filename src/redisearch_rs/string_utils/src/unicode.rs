/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Per-character Unicode case folding.
//!
//! These are pure-Rust replacements for C helpers that were previously
//! implemented using `libnu` for Unicode operations.

use std::borrow::Cow;

/// Convert a UTF-8 string to lowercase per-character, without
/// context-dependent casing rules.
///
/// Unlike [`str::to_lowercase`], this lowercases each [`char`] independently
/// (via [`char::to_lowercase`]), which matches the behaviour of the C
/// `unicode_tolower` function backed by libnu.
pub fn tolower(s: &str) -> String {
    s.chars().flat_map(char::to_lowercase).collect()
}

/// Convert a UTF-8 string to lowercase per-character, borrowing it unchanged
/// when it is already lowercase.
///
/// Lowercases each [`char`] independently like [`tolower`], but
/// allocates only when a character actually changes.
pub fn tolower_cow(s: &str) -> Cow<'_, str> {
    // `s` is already lowercase when folding every char leaves it unchanged.
    if s.chars().all(|c| c.to_lowercase().eq([c])) {
        Cow::Borrowed(s)
    } else {
        Cow::Owned(tolower(s))
    }
}

/// Convert a UTF-8 string to lowercase per-character, without
/// context-dependent casing rules.
///
/// Returns `None` — without allocating the full lowercase copy — once the
/// result would exceed `max` codepoints.
pub fn tolower_capped(s: &str, max: usize) -> Option<String> {
    let mut out = String::new();
    for (count, c) in s.chars().flat_map(char::to_lowercase).enumerate() {
        if count == max {
            return None;
        }
        out.push(c);
    }
    Some(out)
}
