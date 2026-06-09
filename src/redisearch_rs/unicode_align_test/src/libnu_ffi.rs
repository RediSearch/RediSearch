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
//! - The `*_shim` length-prediction functions wrap libnu's `nu_strlen`,
//!   `nu_strnlen`, `nu_bytelen`, `nu_bytenlen`, `nu_strtransformnlen`,
//!   `nu_writestr`, and `nu_writenstr` with the production iterator /
//!   transform pair baked in. The wrappers exist because Rust cannot pass
//!   `static inline` `nu_utf8_read` / `nu_casemap_read` as function-pointer
//!   arguments. See `csrc/length_prediction_shim.c`.
//!
//! See `deps/libnu/casemap.h`, `deps/libnu/utf8.h`, `deps/libnu/strings.h`,
//! and `deps/libnu/extra.h` for the upstream declarations.

use std::ffi::c_char;

unsafe extern "C" {
    /// Return the case-folded mapping for `codepoint` as a null-terminated UTF-8
    /// string. Returns NULL if no folding mapping exists (the input codepoint
    /// is its own fold).
    ///
    /// # Safety
    ///
    /// Pure static-table lookup; any `u32` is a valid argument. The returned
    /// pointer, if non-NULL, borrows from libnu's static data — it must be
    /// treated as a `'static` immutable byte slice that is null-terminated
    /// UTF-8 and must not be freed or mutated.
    pub fn nu_tofold(codepoint: u32) -> *const c_char;

    /// Return the lowercase mapping for `codepoint` as a null-terminated UTF-8
    /// string. Returns NULL if no lowercase mapping exists (the input
    /// codepoint is its own lowercase form). Unconditional — no context
    /// sensitivity (cf. `_nu_tolower`, which handles Greek final sigma).
    ///
    /// # Safety
    ///
    /// Pure static-table lookup; any `u32` is a valid argument. The returned
    /// pointer, if non-NULL, borrows from libnu's static data — it must be
    /// treated as a `'static` immutable byte slice that is null-terminated
    /// UTF-8 and must not be freed or mutated.
    pub fn nu_tolower(codepoint: u32) -> *const c_char;

    /// Encode `unicode` as UTF-8 into the buffer at `utf8`. Returns a pointer
    /// to one past the last byte written. The caller must guarantee the
    /// buffer has at least 4 bytes available (the maximum UTF-8 codepoint
    /// length).
    ///
    /// # Safety
    ///
    /// `utf8` must be non-null, properly aligned for `c_char`, and point to
    /// at least 4 writable bytes. Any `u32` is accepted as `unicode` —
    /// `utf8_codepoint_length` in `deps/libnu/utf8_internal.h` clamps the
    /// natural length to 4, so out-of-range inputs cannot overrun. The
    /// returned pointer is `utf8` advanced by 1–4 bytes.
    pub fn nu_utf8_write(unicode: u32, utf8: *mut c_char) -> *mut c_char;

    /// Decode one UTF-8 codepoint starting at `utf8`. Writes the decoded
    /// codepoint into `*unicode` (if non-NULL) and returns a pointer to the
    /// byte after the consumed codepoint.
    ///
    /// This is the in-crate wrapper around libnu's `static inline`
    /// `nu_utf8_read`, defined in `csrc/nu_utf8_read_shim.c`. The wrapper
    /// just delegates — all decode logic stays inside libnu's `utf8_2b` /
    /// `utf8_3b` / `utf8_4b` helpers in `deps/libnu/utf8_internal.h`.
    ///
    /// # Safety
    ///
    /// `utf8` must point to a *complete* UTF-8 sequence — at least as many
    /// readable bytes as the lead byte's natural length (1 for `0x00..=0x7F`,
    /// 2 for `0xC0..=0xDF`, 3 for `0xE0..=0xEF`, 4 for `0xF0..=0xF7`). libnu
    /// performs no bounds check; a shorter buffer causes an out-of-bounds
    /// read. `unicode` must be non-null and properly aligned for a `u32`
    /// write (libnu does write through it; the "if non-NULL" note above is
    /// historical and not exercised by this crate).
    pub fn nu_utf8_read_shim(utf8: *const c_char, unicode: *mut u32) -> *const c_char;

    /// Predicted codepoint count after lowercasing the first `max_len` bytes
    /// of `encoded` via `nu_tolower` + `nu_casemap_read`. Mirrors the call
    /// shape in `unicode_tolower()` (`src/util/strconv.h`) and
    /// `strToLowerRunes()` (`src/trie/rune_util.c`).
    ///
    /// # Safety
    ///
    /// `encoded` must point to at least `max_len` readable bytes containing
    /// a sequence of well-formed UTF-8 codepoints (libnu also stops early at
    /// the first NUL byte). `max_len` may be `0` only if `encoded` points to
    /// a real allocation — libnu's `_nu_strtransformnlen_unconditional`
    /// forms `encoded + max_len` for the loop bound, which is UB under C
    /// when `encoded` is a dangling sentinel such as Rust's empty-slice
    /// `as_ptr()`. Callers passing potentially empty input must short-circuit
    /// before calling.
    pub fn nu_strtransformnlen_lower_shim(encoded: *const c_char, max_len: usize) -> isize;

    /// Predicted codepoint count for a NUL-terminated UTF-8 string read via
    /// `nu_utf8_read`.
    ///
    /// # Safety
    ///
    /// `encoded` must be non-null and point to a NUL-terminated sequence of
    /// well-formed UTF-8 codepoints. The terminator presence is what bounds
    /// the read — there is no length argument.
    pub fn nu_strlen_shim(encoded: *const c_char) -> isize;

    /// Predicted codepoint count for the first `max_len` bytes of `encoded`
    /// read via `nu_utf8_read` (stops early at NUL).
    ///
    /// # Safety
    ///
    /// `encoded` must point to at least `max_len` readable bytes containing
    /// well-formed UTF-8. `max_len` may be `0` only if `encoded` points to a
    /// real allocation — same dangling-pointer caveat as
    /// [`nu_strtransformnlen_lower_shim`].
    pub fn nu_strnlen_shim(encoded: *const c_char, max_len: usize) -> isize;

    /// Predicted byte count to encode the NUL-terminated `unicode` array via
    /// `nu_utf8_write`.
    ///
    /// # Safety
    ///
    /// `unicode` must be non-null, properly aligned for `u32`, and point to a
    /// sequence of codepoints terminated by a `0` codepoint. The terminator
    /// presence is what bounds the read — there is no length argument.
    pub fn nu_bytelen_shim(unicode: *const u32) -> isize;

    /// Predicted byte count to encode the first `max_len` codepoints of
    /// `unicode` via `nu_utf8_write` (stops early at NUL).
    ///
    /// # Safety
    ///
    /// `unicode` must point to at least `max_len` readable, properly aligned
    /// `u32` codepoints. `max_len` may be `0` only if `unicode` points to a
    /// real allocation — same dangling-pointer caveat as
    /// [`nu_strtransformnlen_lower_shim`].
    pub fn nu_bytenlen_shim(unicode: *const u32, max_len: usize) -> isize;

    /// Encode the first `max_len` codepoints of `unicode` into `encoded` via
    /// `nu_utf8_write`. Does *not* null-terminate. Returns 0.
    ///
    /// # Safety
    ///
    /// `unicode` must point to at least `max_len` readable, properly aligned
    /// `u32` codepoints. `encoded` must be non-null, properly aligned for
    /// `c_char`, and writable for at least the sum of per-codepoint UTF-8
    /// lengths of `unicode[..max_len]` (worst case `4 * max_len` bytes).
    /// Callers that don't know the exact size up front should size `encoded`
    /// from an independent reference such as
    /// [`crate::checks::actual_encoded_byte_count`] rather than from the
    /// predictor under test.
    pub fn nu_writenstr_shim(unicode: *const u32, max_len: usize, encoded: *mut c_char) -> i32;
}
