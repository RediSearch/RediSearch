/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared string utility functions.
//!
//! These are pure-Rust replacements for C helpers that were previously
//! implemented using `libnu` for Unicode operations.

use std::borrow::Cow;

pub mod runes;

/// Convert a UTF-8 string to lowercase per-character, without
/// context-dependent casing rules.
///
/// Unlike [`str::to_lowercase`], this lowercases each [`char`] independently
/// (via [`char::to_lowercase`]), which matches the behaviour of the C
/// `unicode_tolower` function backed by libnu.
pub fn unicode_tolower(s: &str) -> String {
    s.chars().flat_map(char::to_lowercase).collect()
}

/// Convert a UTF-8 string to lowercase per-character, borrowing it unchanged
/// when it is already lowercase.
///
/// Lowercases each [`char`] independently like [`unicode_tolower`], but
/// allocates only when a character actually changes.
pub fn unicode_tolower_cow(s: &str) -> Cow<'_, str> {
    // `s` is already lowercase when folding every char leaves it unchanged.
    if s.chars().all(|c| c.to_lowercase().eq([c])) {
        Cow::Borrowed(s)
    } else {
        Cow::Owned(unicode_tolower(s))
    }
}

/// Convert a UTF-8 string to lowercase per-character, without
/// context-dependent casing rules.
///
/// Returns `None` — without allocating the full lowercase copy — once the
/// result would exceed `max` codepoints.
pub fn unicode_tolower_capped(s: &str, max: usize) -> Option<String> {
    let mut out = String::new();
    for (count, c) in s.chars().flat_map(char::to_lowercase).enumerate() {
        if count == max {
            return None;
        }
        out.push(c);
    }
    Some(out)
}

/// Equivalent to C `isspace()` in the POSIX/C locale.
///
/// Rust's [`u8::is_ascii_whitespace`] excludes vertical tab (`\x0B`), so we
/// match the C definition explicitly.
const fn is_c_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | b'\x0B' | b'\x0C')
}

/// Remove tag escape sequences and optionally convert to lowercase.
///
/// 1. Strips backslash escapes that precede ASCII punctuation or whitespace.
/// 2. If `case_sensitive` is `false`, converts the result to lowercase using
///    [`unicode_tolower`] (per-character, no context-dependent rules).
pub fn tag_strtolower(s: &str, case_sensitive: bool) -> Cow<'_, str> {
    let bytes = s.as_bytes();
    // Only `\` before ASCII punctuation or C-locale whitespace counts as
    // an escape; `\` before a letter (e.g. `\n` as two literal bytes) is
    // kept verbatim—matching the original C `tag_strtolower` behaviour.
    let has_escape = bytes
        .windows(2)
        .any(|w| w[0] == b'\\' && (w[1].is_ascii_punctuation() || is_c_space(w[1])));

    if case_sensitive && !has_escape {
        return Cow::Borrowed(s);
    }

    if !has_escape {
        return Cow::Owned(unicode_tolower(s));
    }

    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            let next = bytes[i + 1];
            if next.is_ascii_punctuation() || is_c_space(next) {
                result.push(next);
                i += 2;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }

    // SAFETY: removing `\` (0x5C, a single-byte ASCII character) before
    // another ASCII byte (punctuation or whitespace) cannot break a
    // multi-byte UTF-8 sequence, so the output is valid UTF-8 whenever
    // the input is.
    let unescaped =
        String::from_utf8(result).expect("invalid UTF-8 after removing escape characters");

    if case_sensitive {
        Cow::Owned(unescaped)
    } else {
        Cow::Owned(unicode_tolower(&unescaped))
    }
}
