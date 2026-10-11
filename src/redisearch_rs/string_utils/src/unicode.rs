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

use crate::libnu::{nu_seq_len, nu_utf8_decode, nu_utf8_encode};

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

/// Lowercase a byte string per-character, whether or not it is valid UTF-8.
///
/// Each character is lowercased as by [`tolower`], but the input is decoded and
/// re-encoded with [`nu_utf8_decode`] and [`nu_utf8_encode`], so it need not be
/// valid UTF-8:
///
/// - A malformed sequence decodes to whatever codepoint its payload bits spell
///   and is written back in canonical form, so its bytes can change even when
///   its case does not.
/// - A sequence whose lead byte announces more bytes than remain is kept
///   verbatim, along with everything after it.
/// - The string ends at the first sequence decoding to codepoint 0, a NUL byte
///   or an overlong encoding of one. A NUL right after a multibyte lead byte is
///   read as that sequence's continuation instead.
/// - A string that starts with a NUL byte is returned unchanged in full.
///
/// A [`Cow::Borrowed`] result is always a prefix of `bytes`.
pub fn tolower_bytes(bytes: &[u8]) -> Cow<'_, [u8]> {
    // ASCII up to the end or the first NUL: lowercasing cannot change any
    // length, so the result is at most a lowercased prefix.
    let ascii_len = bytes
        .iter()
        .position(|&b| b == 0 || !b.is_ascii())
        .unwrap_or(bytes.len());
    if ascii_len == bytes.len() || bytes[ascii_len] == 0 {
        if ascii_len == 0 {
            return Cow::Borrowed(bytes);
        }
        let head = &bytes[..ascii_len];
        return if head.iter().any(u8::is_ascii_uppercase) {
            Cow::Owned(head.to_ascii_lowercase())
        } else {
            Cow::Borrowed(head)
        };
    }

    let mut out = Vec::with_capacity(bytes.len());
    let mut pos = 0;
    while pos < bytes.len() {
        let seq_len = nu_seq_len(bytes[pos]);
        let Some(seq) = bytes.get(pos..pos + seq_len) else {
            out.extend_from_slice(&bytes[pos..]);
            break;
        };
        let codepoint = nu_utf8_decode(seq);
        if codepoint == 0 {
            break;
        }
        match char::from_u32(codepoint) {
            Some(c) => {
                for lower in c.to_lowercase() {
                    nu_utf8_encode(u32::from(lower), &mut out);
                }
            }
            None => nu_utf8_encode(codepoint, &mut out),
        }
        pos += seq_len;
    }

    if out.is_empty() {
        // Only an overlong NUL at the very start stops the walk before it
        // writes anything, and like a leading NUL byte it leaves the string
        // unchanged.
        Cow::Borrowed(bytes)
    } else if bytes.starts_with(&out) {
        Cow::Borrowed(&bytes[..out.len()])
    } else {
        Cow::Owned(out)
    }
}
