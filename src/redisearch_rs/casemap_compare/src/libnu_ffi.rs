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
//! - `nu_tofold` performs a static-table lookup for case folding (no UTF-8
//!   encoding step).
//! - `nu_tolower` performs a static-table lookup for lowercase mapping. This
//!   is the production text-normalisation path: `unicode_tolower` in
//!   `src/util/strconv.h` drives this for every indexed/queried token via
//!   `nu_strtransformnlen` + `nu_tolower` + `nu_casemap_read`.
//! - `nu_utf8_write` is libnu's runtime UTF-8 encoder; for 4-byte codepoints
//!   it routes through `b4_utf8` in `deps/libnu/utf8_internal.h`, which is
//!   the suspected source of malformed-bytes bugs.
//! - `nu_utf8_read_shim` is the in-crate wrapper around libnu's `nu_utf8_read`
//!   decoder (the upstream symbol is `static inline`, so we expose it via
//!   `csrc/nu_utf8_read_shim.c`). The decoder shares the mask-and-shift
//!   idiom with `b4_utf8` (in `utf8_2b/3b/4b` at `utf8_internal.h`), so the
//!   round-trip sweep treats it as a similar regression-prone surface.
//!
//! See `deps/libnu/casemap.h` and `deps/libnu/utf8.h` for the upstream
//! declarations.

use std::ffi::c_char;

unsafe extern "C" {
    /// Return the case-folded mapping for `codepoint` as a null-terminated UTF-8
    /// string. Returns NULL if no folding mapping exists (the input codepoint
    /// is its own fold).
    pub fn nu_tofold(codepoint: u32) -> *const c_char;

    /// Return the lowercase mapping for `codepoint` as a null-terminated UTF-8
    /// string. Returns NULL if no lowercase mapping exists (the input
    /// codepoint is its own lowercase form). Unconditional — no context
    /// sensitivity (cf. `_nu_tolower`, which handles Greek final sigma).
    pub fn nu_tolower(codepoint: u32) -> *const c_char;

    /// Encode `unicode` as UTF-8 into the buffer at `utf8`. Returns a pointer
    /// to one past the last byte written. The caller must guarantee the
    /// buffer has at least 4 bytes available (the maximum UTF-8 codepoint
    /// length).
    pub fn nu_utf8_write(unicode: u32, utf8: *mut c_char) -> *mut c_char;

    /// Decode one UTF-8 codepoint starting at `utf8`. Writes the decoded
    /// codepoint into `*unicode` (if non-NULL) and returns a pointer to the
    /// byte after the consumed codepoint.
    ///
    /// This is the in-crate wrapper around libnu's `static inline`
    /// `nu_utf8_read`, defined in `csrc/nu_utf8_read_shim.c`. The wrapper
    /// just delegates — all decode logic stays inside libnu's `utf8_2b` /
    /// `utf8_3b` / `utf8_4b` helpers in `deps/libnu/utf8_internal.h`.
    pub fn nu_utf8_read_shim(utf8: *const c_char, unicode: *mut u32) -> *const c_char;
}
