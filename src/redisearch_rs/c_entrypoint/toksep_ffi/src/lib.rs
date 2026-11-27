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

//! Borrowed token spans for C callers.

use std::{
    ffi::{c_char, c_int},
    ptr,
};

/// Returns 1 if the low byte of `byte` separates tokens according to
/// [`toksep::is_separator`], or 0 otherwise.
#[unsafe(no_mangle)]
pub extern "C" fn istoksep(byte: c_int) -> c_int {
    toksep::is_separator(byte as u8).into()
}

/// Returns the original cursor and writes the next token's byte length to `len`.
///
/// Bytes are never modified. The cursor advances past one separator, or becomes
/// null at the terminating NUL or after a trailing separator.
///
/// # Safety
///
/// 1. `cursor` must be aligned and [valid] for reading and writing an initialized pointer.
/// 2. The pointer stored in `cursor` must be non-null and [valid] for reading initialized
///    bytes through a terminating NUL in the same allocation, unchanged during the call.
/// 3. `len` must be aligned and [valid] for writing. Both output locations must be
///    exclusively accessible and disjoint from each other and the input bytes.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn toksep(cursor: *mut *mut c_char, len: *mut usize) -> *mut c_char {
    debug_assert!(!cursor.is_null(), "toksep cursor slot is null");
    debug_assert!(!len.is_null(), "toksep length output is null");
    // SAFETY: 1. The caller provides an initialized, readable pointer slot.
    let start = unsafe { cursor.read() };
    debug_assert!(!start.is_null(), "toksep input is null");
    let mut position = start.cast::<u8>();
    let token_len = ::toksep::token_len(std::iter::from_fn(|| {
        // SAFETY: 2. The scanner stops at or before the input's terminating NUL.
        let byte = unsafe { position.read() };
        position = position.wrapping_add(1);
        Some(byte)
    }));
    // SAFETY: 2. token_len stops at a byte within the NUL-terminated allocation.
    let end = unsafe { start.add(token_len) };
    // SAFETY: 2. The token ends at an initialized separator or NUL.
    let terminator = unsafe { end.read() };
    let mut next = ptr::null_mut();
    if terminator != 0 {
        // SAFETY: 2. A non-NUL terminator is followed by at least the final NUL.
        let following = unsafe { end.add(1) };
        // SAFETY: 2. following is still within the initialized input.
        if unsafe { following.read() } != 0 {
            next = following;
        }
    }
    // SAFETY: 1, 3. The output slots are writable and do not overlap the input.
    unsafe { cursor.write(next) };
    // SAFETY: 3. Writing accepts an uninitialized length output.
    unsafe { len.write(token_len) };
    start
}
