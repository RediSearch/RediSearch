/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrappers for C string-obfuscation helpers.

use ffi::{Obfuscate_Number, Obfuscate_Text};
use std::ffi::CStr;

/// Returns a static string representation of the obfuscated number.
pub fn obfuscate_number(number: f64) -> &'static str {
    // SAFETY: `Obfuscate_Number` is a C function that returns a pointer to a
    // static NUL-terminated string.
    let obfuscated = unsafe { Obfuscate_Number(number) };
    // SAFETY: The returned pointer is a valid, NUL-terminated, static C string.
    unsafe { CStr::from_ptr(obfuscated) }.to_str().unwrap()
}

/// Returns a static string representation of the obfuscated text.
pub fn obfuscate_text(text: &[u8]) -> &'static str {
    // SAFETY: `Obfuscate_Text` expects a `*const c_char` pointer. `text` is a
    // valid byte slice, and the function returns a pointer to a static
    // NUL-terminated string.
    let obfuscated = unsafe { Obfuscate_Text(text.as_ptr().cast()) };
    // SAFETY: The returned pointer is a valid, NUL-terminated, static C string.
    unsafe { CStr::from_ptr(obfuscated) }.to_str().unwrap()
}
