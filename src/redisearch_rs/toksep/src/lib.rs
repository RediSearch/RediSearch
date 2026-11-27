/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#![warn(
    clippy::unwrap_used,
    clippy::dbg_macro,
    clippy::todo,
    clippy::unimplemented,
    clippy::print_stdout,
    clippy::panic,
    clippy::unnecessary_safety_comment,
    clippy::ptr_as_ptr,
    clippy::borrow_as_ptr,
    clippy::ref_as_ptr,
    clippy::indexing_slicing
)]

//! Allocation-free token boundaries over arbitrary bytes.

/// Whether a byte separates tokens: space, tab, or ASCII punctuation except
/// backslash and underscore.
#[inline]
#[expect(
    clippy::indexing_slicing,
    reason = "Every byte indexes a 256-entry table"
)]
pub const fn is_separator(byte: u8) -> bool {
    const MAP: [bool; 256] = {
        let mut map = [false; 256];
        let mut index = 0;
        while index < map.len() {
            let byte = index as u8;
            map[index] = byte == b' '
                || byte == b'\t'
                || (byte.is_ascii_punctuation() && byte != b'\\' && byte != b'_');
            index += 1;
        }
        map
    };
    MAP[byte as usize]
}

/// Length of the prefix before the first NUL or unescaped [`is_separator`] byte.
///
/// A backslash escapes the following byte unless it is itself escaped. NUL always
/// terminates the token. No bytes after the terminator are read, and escape bytes
/// remain part of the returned span. If no terminator occurs, the entire input is a token.
pub fn token_len(bytes: impl IntoIterator<Item = u8>) -> usize {
    let mut bytes = bytes.into_iter();
    let mut len = 0;
    while let Some(byte) = bytes.next() {
        if byte == 0 || is_separator(byte) {
            return len;
        }
        len += 1;
        if byte == b'\\' {
            match bytes.next() {
                Some(0) | None => return len,
                Some(_) => len += 1,
            }
        }
    }
    len
}
