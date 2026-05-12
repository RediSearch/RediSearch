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

use std::borrow::Cow;

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
///    [`str::to_lowercase`] (whole-string, preserving context-dependent rules
///    like Greek final sigma).
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
        return Cow::Owned(s.to_lowercase());
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
        Cow::Owned(unescaped.to_lowercase())
    }
}
