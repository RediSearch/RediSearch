/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Byte-oriented token boundaries shared by tokenization and highlighting.

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

/// Whether `byte` is space, tab, or ASCII punctuation other than underscore and backslash.
#[expect(
    clippy::indexing_slicing,
    reason = "Every byte indexes the complete byte alphabet"
)]
pub const fn is_separator(byte: u8) -> bool {
    // Matching ranges directly makes the long-token benchmark slower than C.
    const TABLE: [bool; 256] = {
        let mut table = [false; 256];
        let mut index = 0;
        while index < table.len() {
            table[index] = matches!(index as u8, b'\t' | b' ' ..= b'/' | b':' ..= b'@' | b'[' | b']' | b'^' | b'`' | b'{' ..= b'~');
            index += 1;
        }
        table
    };
    TABLE[byte as usize]
}

/// Counts bytes before the first NUL or unescaped [separator](is_separator),
/// or until `bytes` is exhausted. Backslash escapes exactly the following byte;
/// escape bytes remain part of the token.
///
/// Bytes are consumed lazily, including the terminator when present, so a caller
/// with an unbounded source need not scan beyond the current token.
pub fn token_len(bytes: impl IntoIterator<Item = u8>) -> usize {
    let mut escaped = false;
    let mut len = 0;
    for byte in bytes {
        if byte == 0 {
            break;
        }
        if is_separator(byte) {
            if !escaped {
                break;
            }
            escaped = false;
        } else {
            escaped = !escaped && byte == b'\\';
        }
        len += 1;
    }
    len
}
