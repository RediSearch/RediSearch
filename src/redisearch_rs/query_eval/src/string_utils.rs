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
