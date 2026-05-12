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

/// Maximum number of runes allowed in a single trie key.
pub const MAX_RUNESTR_LEN: usize = 1024;

/// Remove backslash escape sequences from a wildcard pattern.
///
/// Returns [`Cow::Borrowed`] when no escapes are present (zero-copy fast
/// path), or [`Cow::Owned`] with escapes removed.
pub fn wildcard_remove_escape(s: &str) -> Cow<'_, str> {
    let bytes = s.as_bytes();
    let Some(first_escape) = bytes.iter().position(|&b| b == b'\\') else {
        return Cow::Borrowed(s);
    };

    let mut result = Vec::with_capacity(bytes.len() - 1);
    result.extend_from_slice(&bytes[..first_escape]);

    let mut read = first_escape;
    while read < bytes.len() {
        if bytes[read] == b'\\' {
            read += 1;
            if read >= bytes.len() {
                break;
            }
        }
        result.push(bytes[read]);
        read += 1;
    }

    // SAFETY: removing `\` (0x5C, a single-byte ASCII character) before
    // another byte cannot break a multi-byte UTF-8 sequence, so the
    // output is valid UTF-8 whenever the input is.
    Cow::Owned(unsafe { String::from_utf8_unchecked(result) })
}

/// Convert a UTF-8 string to a lowercase array of runes (`u16`) for trie
/// lookups.
///
/// Each Unicode codepoint is lowercased and then truncated to `u16`.
///
/// Returns [`None`] if the resulting rune count exceeds
/// [`MAX_RUNESTR_LEN`].
pub fn str_to_lower_runes(s: &str) -> Option<Vec<u16>> {
    let cap = s.len().min(MAX_RUNESTR_LEN);
    let mut runes = Vec::with_capacity(cap);
    for c in s.chars() {
        for lc in c.to_lowercase() {
            runes.push(lc as u16);
            if runes.len() > MAX_RUNESTR_LEN {
                return None;
            }
        }
    }
    Some(runes)
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
    let unescaped = unsafe { String::from_utf8_unchecked(result) };

    if case_sensitive {
        Cow::Owned(unescaped)
    } else {
        Cow::Owned(unescaped.to_lowercase())
    }
}
