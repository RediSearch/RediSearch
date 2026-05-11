/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Raw FFI binding to libnu's `nu_tofold`.
//!
//! See `deps/libnu/casemap.h` for the upstream declaration.

use std::ffi::c_char;

unsafe extern "C" {
    /// Return the case-folded mapping for `codepoint` as a null-terminated UTF-8
    /// string. Returns NULL if no folding mapping exists (the input codepoint
    /// is its own fold).
    pub fn nu_tofold(codepoint: u32) -> *const c_char;
}
