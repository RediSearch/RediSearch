/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI layer to access, from C, the string utilities implemented in Rust.

use std::{ffi::c_char, slice};

/// Returns whether the `len` bytes at `s` are well-formed UTF-8, as
/// [`string_utils::utf8::is_valid`] defines it.
///
/// The bytes are taken as given: a `len` that spans an interior NUL is checked in full, and a
/// value shorter than `len` is not detected.
///
/// # Safety
///
/// 1. When `len` is non-zero, `s` must point to a valid region of memory of length `len`. An
///    empty value needs no valid pointer — `len == 0` returns without reading `s`.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn StringUtils_IsValidUtf8(s: *const c_char, len: usize) -> bool {
    if len == 0 {
        return true;
    }

    // SAFETY: see safety point 1 above.
    let bytes = unsafe { slice::from_raw_parts(s.cast::<u8>(), len) };

    string_utils::utf8::is_valid(bytes)
}
