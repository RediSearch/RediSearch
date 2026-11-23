/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for byte-oriented token separation.

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

use std::ffi::{c_char, c_int};
use std::ptr;

/// Returns the current token's borrowed start and writes its raw length to `tok_len`.
/// Advances `s` past one unescaped separator, or sets it to NULL when the next
/// byte is NUL. Escapes and input bytes are left unchanged.
///
/// # Safety
///
/// 1. `s` must be aligned and [valid] for reading and writing one initialized pointer.
/// 2. The pointer in `s` must address a [valid], initialized, NUL-terminated byte
///    string that is not mutated for the duration of the call.
/// 3. `tok_len` must be aligned and [valid] for writing one [`usize`]. It need not
///    be initialized. Both output slots must be disjoint from each other and
///    from the input string, and exclusively accessed during the call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn toksep(s: *mut *mut c_char, tok_len: *mut usize) -> *mut c_char {
    debug_assert_non_null(s);
    debug_assert_non_null(tok_len);
    // SAFETY: 1, 3. The caller supplies an initialized, exclusively accessed pointer slot.
    let orig = unsafe { s.read() };
    debug_assert_non_null(orig);
    let len = toksep::token_len((0..).map(|offset| {
        // SAFETY: 2. token_len stops at or before the first NUL, so each read is in the string.
        unsafe { orig.wrapping_add(offset).read() as u8 }
    }));
    let end = orig.wrapping_add(len);
    // SAFETY: 2. token_len ends at a readable NUL or separator in the input string.
    let next = if unsafe { end.read() } == 0 {
        ptr::null_mut()
    } else {
        let next = end.wrapping_add(1);
        // SAFETY: 2. A non-NUL separator is followed by another byte in the terminated string.
        if unsafe { next.read() } == 0 {
            ptr::null_mut()
        } else {
            next
        }
    };
    // SAFETY: 1, 3. The caller grants exclusive write access to the disjoint output slots.
    unsafe { s.write(next) };
    // SAFETY: 3. A raw write initializes the output without reading its previous contents.
    unsafe { tok_len.write(len) };
    orig
}

/// Returns zero or one according to whether the low byte of `c` is a
/// [separator](toksep::is_separator).
#[unsafe(no_mangle)]
pub extern "C" fn istoksep(c: c_int) -> c_int {
    c_int::from(toksep::is_separator(c as u8))
}

#[track_caller]
fn debug_assert_non_null<T>(pointer: *const T) {
    debug_assert!(!pointer.is_null());
}
