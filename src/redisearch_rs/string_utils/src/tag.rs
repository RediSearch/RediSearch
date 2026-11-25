/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Normalisation of tag field values.

use std::borrow::Cow;

/// Equivalent to C `isspace()` in the POSIX/C locale.
///
/// Rust's [`u8::is_ascii_whitespace`] excludes vertical tab (`\x0B`), so we
/// match the C definition explicitly.
const fn is_c_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | b'\x0B' | b'\x0C')
}

/// Remove the escapes from a query's tag value, which indexed values never
/// carry.
///
/// A backslash is dropped when it precedes ASCII punctuation or whitespace, so
/// `\,` becomes `,` while `\n`, as two literal bytes, is kept. Removal is
/// single-level: `\\,` becomes `\,`.
///
/// A query token is raw bytes, which need not be valid UTF-8 and may hold
/// interior NULs. Escapes are only removed up to the first NUL, and everything
/// from it on is kept verbatim.
///
/// A [`Cow::Borrowed`] result is `bytes` itself.
pub fn unescape(bytes: &[u8]) -> Cow<'_, [u8]> {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let (text, rest) = bytes.split_at(end);
    let is_escape = |w: &[u8]| w[0] == b'\\' && (w[1].is_ascii_punctuation() || is_c_space(w[1]));
    if !text.windows(2).any(is_escape) {
        return Cow::Borrowed(bytes);
    }

    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < text.len() {
        if text.get(i..i + 2).is_some_and(is_escape) {
            i += 1;
        }
        result.push(text[i]);
        i += 1;
    }
    result.extend_from_slice(rest);
    Cow::Owned(result)
}
