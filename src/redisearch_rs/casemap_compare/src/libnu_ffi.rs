/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Raw FFI bindings to libnu.
//!
//! - `nu_tofold` performs a static-table lookup (no UTF-8 encoding step).
//! - `nu_utf8_write` is libnu's runtime UTF-8 encoder; for 4-byte codepoints
//!   it routes through `b4_utf8` in `deps/libnu/utf8_internal.h`, which is
//!   the suspected source of malformed-bytes bugs.
//!
//! See `deps/libnu/casemap.h` and `deps/libnu/utf8.h` for the upstream
//! declarations.

use std::ffi::c_char;

unsafe extern "C" {
    /// Return the case-folded mapping for `codepoint` as a null-terminated UTF-8
    /// string. Returns NULL if no folding mapping exists (the input codepoint
    /// is its own fold).
    pub fn nu_tofold(codepoint: u32) -> *const c_char;

    /// Encode `unicode` as UTF-8 into the buffer at `utf8`. Returns a pointer
    /// to one past the last byte written. The caller must guarantee the
    /// buffer has at least 4 bytes available (the maximum UTF-8 codepoint
    /// length).
    pub fn nu_utf8_write(unicode: u32, utf8: *mut c_char) -> *mut c_char;
}
