/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Libnu probes — direct invocations of `nu_tofold`, `nu_tolower`,
//! `nu_utf8_read`, `nu_utf8_write`, and the length-prediction APIs, with no
//! ICU involvement.
//!
//! Tests under `tests/check_*.rs` consume these helpers to assert
//! self-consistency invariants of libnu itself (encoder validity, decoder
//! round-trip, length-prediction agreement, casemap-table terminator
//! presence, fold idempotence). Failures here imply a regression in
//! `deps/libnu/` and are gating.

use std::ffi::CStr;

use crate::libnu_ffi;

/// Fold `s` using libnu's per-codepoint `nu_tofold`.
///
/// For each input codepoint:
///
/// - If `nu_tofold(cp)` returns non-NULL, the result is a null-terminated
///   UTF-8 string of zero or more codepoints which replaces the input.
/// - Otherwise (no mapping), the codepoint is copied verbatim.
///
/// This exercises libnu's fold tables (the same tables `runeFold` in
/// `src/trie/rune_util.c` uses) but skips the buggy `nu_utf8_write`
/// re-encode — we use Rust's native UTF-8 handling instead so that
/// divergence numbers reflect *fold-table* differences against ICU and
/// are not contaminated by the `b4_utf8` encoder bug. Use
/// [`fold_libnu_raw`] or [`encode_codepoint_with_libnu`] to observe the
/// encoder bug directly.
pub fn fold_libnu(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        let cp = ch as u32;
        // SAFETY: `nu_tofold` is a pure function; any u32 input is accepted
        // and the returned pointer is either NULL or points to a static,
        // null-terminated UTF-8 buffer owned by libnu's data tables.
        let encoded = unsafe { libnu_ffi::nu_tofold(cp) };
        if encoded.is_null() {
            out.push(ch);
            continue;
        }
        // SAFETY: `encoded` is non-NULL and points to a null-terminated
        // UTF-8 byte sequence inside libnu's static data.
        let cstr = unsafe { CStr::from_ptr(encoded) };
        match cstr.to_str() {
            Ok(mapped) => out.push_str(mapped),
            Err(_) => out.push('\u{FFFD}'),
        }
    }
    out
}

/// Like [`fold_libnu`] but returns the raw bytes copied out of libnu's
/// `nu_tofold` table, without any UTF-8 validation.
///
/// [`fold_libnu`] silently substitutes U+FFFD when libnu hands back bytes that
/// don't decode as UTF-8, which is the wrong behaviour for the question
/// "does libnu ever emit invalid UTF-8?". This function preserves whatever
/// libnu actually wrote, so callers can validate it explicitly with
/// `std::str::from_utf8`.
///
/// Passthrough codepoints (those where `nu_tofold` returns NULL) are
/// re-encoded with Rust's standard UTF-8 encoder, which is by construction
/// valid — any invalid bytes in the result therefore came from libnu's
/// folding tables.
pub fn fold_libnu_raw(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    for ch in s.chars() {
        let cp = ch as u32;
        // SAFETY: see `fold_libnu` — `nu_tofold` is a pure function and the
        // returned pointer is either NULL or points to a static,
        // null-terminated buffer owned by libnu.
        let encoded = unsafe { libnu_ffi::nu_tofold(cp) };
        if encoded.is_null() {
            let mut buf = [0u8; 4];
            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
            continue;
        }
        // SAFETY: `encoded` is non-NULL and points to a null-terminated byte
        // sequence inside libnu's static data.
        let cstr = unsafe { CStr::from_ptr(encoded) };
        out.extend_from_slice(cstr.to_bytes());
    }
    out
}

/// Lowercase `s` using libnu's per-codepoint `nu_tolower`.
///
/// This mirrors the production text-normalisation path in
/// `unicode_tolower()` at `src/util/strconv.h:121` and `strToLowerRunes()`
/// at `src/trie/rune_util.c:65`: for each input codepoint, `nu_tolower`
/// returns either NULL (passthrough) or a null-terminated UTF-8 mapping
/// that may be longer than one codepoint (e.g. multi-codepoint expansions
/// in some scripts). We iterate the mapping by decoding it natively as
/// UTF-8, which is equivalent to the `nu_casemap_read` loop in production
/// (`nu_casemap_read` aliases `nu_utf8_read`).
///
/// Unlike production, this function does *not* re-encode the result via
/// `nu_utf8_write`. That isolates lowercase-table divergence from the
/// `b4_utf8` encoder concerns covered by [`encode_codepoint_with_libnu`].
pub fn lower_libnu(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        let cp = ch as u32;
        // SAFETY: `nu_tolower` is a pure function; any u32 input is accepted
        // and the returned pointer is either NULL or points to a static,
        // null-terminated UTF-8 buffer owned by libnu's data tables.
        let encoded = unsafe { libnu_ffi::nu_tolower(cp) };
        if encoded.is_null() {
            out.push(ch);
            continue;
        }
        // SAFETY: `encoded` is non-NULL and points to a null-terminated
        // UTF-8 byte sequence inside libnu's static data.
        let cstr = unsafe { CStr::from_ptr(encoded) };
        match cstr.to_str() {
            Ok(mapped) => out.push_str(mapped),
            Err(_) => out.push('\u{FFFD}'),
        }
    }
    out
}

/// Encode `cp` to UTF-8 using libnu's *runtime* encoder (`nu_utf8_write`),
/// returning whatever bytes libnu wrote without any validation.
///
/// `nu_utf8_write` dispatches to `b{1..4}_utf8` based on the codepoint's
/// natural UTF-8 length. The 4-byte path (`b4_utf8` in
/// `deps/libnu/utf8_internal.h`) is what RediSearch invokes for any
/// supplementary-plane codepoint via two live call sites:
///
/// - `unicode_tolower()` in `src/util/strconv.h` — lowercases a UTF-8
///   string in place, re-encoding each lowered codepoint through
///   `nu_utf8_write` (used during query/document text normalisation).
/// - `runesToStr()` in `src/trie/rune_util.c` — converts a trie rune
///   array back to UTF-8 via `nu_writestr`/`nu_utf8_write` (used when
///   returning matched terms from the trie).
///
/// Both paths produce corrupted bytes for every supplementary-plane
/// codepoint with bit 12 set. This is the function to sweep when asking
/// "does libnu *ever* emit invalid UTF-8 from its runtime encoder?".
pub fn encode_codepoint_with_libnu(cp: u32) -> Vec<u8> {
    let mut buf = [0u8; 4];
    let begin = buf.as_mut_ptr() as *mut std::ffi::c_char;
    // SAFETY: libnu's `utf8_codepoint_length` (deps/libnu/utf8_internal.h)
    // returns at most 4 for any u32 — its final branch returns 4
    // unconditionally for codepoints >= 0x10000. So `nu_utf8_write` writes
    // at most 4 bytes regardless of the input value (out-of-range u32
    // inputs encode to non-canonical bytes but cannot overrun the buffer).
    // `nu_utf8_write` returns a pointer to one past the last byte written.
    let end = unsafe { libnu_ffi::nu_utf8_write(cp, begin) };
    // SAFETY: `end` is derived from `begin` (returned by `nu_utf8_write`
    // pointing into the same 4-byte allocation), so both pointers share an
    // origin and the difference fits in `isize`.
    let written = unsafe { end.offset_from(begin) } as usize;
    buf[..written].to_vec()
}

/// Decode the first UTF-8 codepoint at the start of `bytes` using libnu's
/// `nu_utf8_read` (via the in-crate `nu_utf8_read_shim` wrapper around the
/// `static inline` upstream symbol).
///
/// Returns `(codepoint, advance)` where `codepoint` is the decoded scalar
/// (as `u32`, since the caller may want to inspect malformed values that
/// don't fit `char`) and `advance` is the number of bytes consumed.
///
/// `bytes` must be non-empty. The function inspects the lead byte to decide
/// how many bytes to read (1–4) and only touches that many — `nu_utf8_read`
/// does no bounds checking, so the caller must ensure the slice has enough
/// content for the lead byte's class. For the round-trip sweep this is
/// trivially true because we always feed it a canonical encoding.
pub fn decode_codepoint_with_libnu(bytes: &[u8]) -> (u32, usize) {
    debug_assert!(
        !bytes.is_empty(),
        "decode_codepoint_with_libnu requires non-empty input"
    );
    let mut decoded: u32 = 0;
    let begin = bytes.as_ptr() as *const std::ffi::c_char;
    // SAFETY: `nu_utf8_read` reads 1–4 bytes from `begin` depending on the
    // lead byte's range and writes one `u32` to `&mut decoded`. The caller
    // contract requires `bytes` to hold a full codepoint, so the read stays
    // in bounds.
    let end = unsafe { libnu_ffi::nu_utf8_read_shim(begin, &mut decoded) };
    // SAFETY: `end` is derived from `begin` (both point into `bytes`), so
    // they share an origin and the difference fits in `isize`.
    let consumed = unsafe { end.offset_from(begin) } as usize;
    (decoded, consumed)
}

/// Predicted codepoint count after lowercasing `bytes` via libnu's
/// `nu_strtransformnlen` with `nu_tolower` + `nu_casemap_read`. Mirrors
/// the `nu_strtransformnlen` call at `src/util/strconv.h:132` and
/// `src/trie/rune_util.c:68`.
pub fn predict_lower_len(bytes: &[u8]) -> isize {
    // libnu's `_nu_strtransformnlen_unconditional` forms `encoded + max_len`
    // before iterating; passing Rust's dangling empty-slice sentinel is UB
    // under the C standard even when `max_len == 0`. Empty in → 0 out.
    if bytes.is_empty() {
        return 0;
    }
    let begin = bytes.as_ptr() as *const std::ffi::c_char;
    // SAFETY: libnu reads up to `bytes.len()` bytes starting at `begin` and
    // stops at the first NUL byte. The slice provides exactly that much
    // valid memory.
    unsafe { libnu_ffi::nu_strtransformnlen_lower_shim(begin, bytes.len()) }
}

/// Predicted codepoint count for the NUL-terminated UTF-8 buffer `s` via
/// libnu's `nu_strlen`.
///
/// `s` must end with a NUL byte. The function reads codepoints until the
/// NUL terminator.
pub fn predict_strlen(s: &[u8]) -> isize {
    debug_assert!(
        s.contains(&0),
        "predict_strlen requires NUL-terminated input"
    );
    let begin = s.as_ptr() as *const std::ffi::c_char;
    // SAFETY: libnu reads codepoints starting at `begin` until it hits a
    // NUL byte; the caller-provided NUL terminator within `s` guarantees a
    // bounded read.
    unsafe { libnu_ffi::nu_strlen_shim(begin) }
}

/// Predicted codepoint count for the first `bytes.len()` bytes of `bytes`
/// via libnu's `nu_strnlen` (stops early at NUL).
pub fn predict_strnlen(bytes: &[u8]) -> isize {
    // See [`predict_lower_len`]: libnu computes `encoded + max_len` on the
    // C side; bypass the FFI entirely for the empty case to avoid forming a
    // pointer from Rust's dangling empty-slice sentinel.
    if bytes.is_empty() {
        return 0;
    }
    let begin = bytes.as_ptr() as *const std::ffi::c_char;
    // SAFETY: libnu reads at most `bytes.len()` bytes starting at `begin`.
    unsafe { libnu_ffi::nu_strnlen_shim(begin, bytes.len()) }
}

/// Predicted UTF-8 byte count to encode `unicode` via libnu's `nu_bytelen`
/// with `nu_utf8_write`. `unicode` must contain a 0 codepoint terminator
/// somewhere — libnu reads until it finds one and ignores everything past it.
pub fn predict_bytelen(unicode: &[u32]) -> isize {
    debug_assert!(
        unicode.contains(&0),
        "predict_bytelen requires a 0 codepoint terminator"
    );
    // SAFETY: libnu reads codepoints starting at `unicode.as_ptr()` until
    // it hits a 0 sentinel; the caller-provided 0 within `unicode`
    // guarantees a bounded read.
    unsafe { libnu_ffi::nu_bytelen_shim(unicode.as_ptr()) }
}

/// Predicted UTF-8 byte count to encode the first `unicode.len()` codepoints
/// of `unicode` via libnu's `nu_bytenlen` (stops early at NUL).
pub fn predict_bytenlen(unicode: &[u32]) -> isize {
    // See [`predict_lower_len`]: same dangling-pointer hazard, just over
    // `*const u32` instead of `*const c_char`.
    if unicode.is_empty() {
        return 0;
    }
    // SAFETY: libnu reads at most `unicode.len()` codepoints from
    // `unicode.as_ptr()`.
    unsafe { libnu_ffi::nu_bytenlen_shim(unicode.as_ptr(), unicode.len()) }
}

/// Sum the per-codepoint UTF-8 byte counts that libnu's `nu_utf8_write`
/// would emit for `unicode`, stopping at the first 0 codepoint (the same
/// terminator semantics `nu_bytelen` / `nu_bytenlen` use).
///
/// This is the "actual" byte count to compare against [`predict_bytelen`] /
/// [`predict_bytenlen`]: both predictors compute the same value by invoking
/// the write iterator once per codepoint internally, so the sum here is the
/// reference value the predictors must agree with.
pub fn actual_encoded_byte_count(unicode: &[u32]) -> usize {
    let mut total = 0usize;
    for &cp in unicode {
        if cp == 0 {
            break;
        }
        total += encode_codepoint_with_libnu(cp).len();
    }
    total
}

/// Encode the first `unicode.len()` codepoints of `unicode` into a freshly
/// allocated UTF-8 buffer via libnu's `nu_writenstr`. Stops at the first 0
/// codepoint. The buffer is sized from [`actual_encoded_byte_count`] rather
/// than from the predictor under test, so a buggy predictor cannot trigger
/// an out-of-bounds write.
pub fn write_with_libnu(unicode: &[u32]) -> Vec<u8> {
    let expected = actual_encoded_byte_count(unicode);
    let mut buf = vec![0u8; expected];
    if expected == 0 {
        return buf;
    }
    let dst = buf.as_mut_ptr() as *mut std::ffi::c_char;
    // SAFETY: `buf` has `expected` bytes, sized from
    // `actual_encoded_byte_count` (independent of `nu_writenstr`). The
    // writer emits exactly that many bytes because both routines invoke
    // `nu_utf8_write` per codepoint and stop at the first 0.
    let rc = unsafe { libnu_ffi::nu_writenstr_shim(unicode.as_ptr(), unicode.len(), dst) };
    assert_eq!(rc, 0, "nu_writenstr returned non-zero status: {rc}");
    buf
}

/// Which libnu case-mapping table to look up — `nu_tofold` or `nu_tolower`.
///
/// Both tables are read via the same `nu_casemap_read` loop in production
/// (`src/trie/rune_util.c:97` and `src/util/strconv.h:160`), so the
/// terminator-walk logic is identical; only the head-lookup function differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseMapTable {
    /// `nu_tofold` — full case-folding table (`deps/libnu/gen/_tofold.c`).
    Fold,
    /// `nu_tolower` — lowercase table (`deps/libnu/gen/_tolower.c`). This is
    /// the production text-normalisation path.
    Lower,
}

/// Outcome of walking a `nu_tofold` / `nu_tolower` mapping via the
/// `nu_casemap_read` loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasemapWalk {
    /// The head-lookup function (`nu_tofold` / `nu_tolower`) returned NULL —
    /// the codepoint has no mapping and is its own fold/lowercase.
    NoMapping,
    /// The loop read the codepoint-0 terminator within the allowed
    /// iteration budget. Inner [`Vec`] is the decoded expansion sequence
    /// (zero or more codepoints, terminator excluded).
    Terminated(Vec<u32>),
    /// The loop hit the iteration cap before finding a terminator. The
    /// inner [`Vec`] holds the first `cap` codepoints that were read. This
    /// is the signal of a malformed entry in libnu's generated data
    /// (`deps/libnu/gen/_tofold.c` or `_tolower.c`); in production the
    /// equivalent C loop has no cap and would read out-of-bounds past the
    /// static table.
    CapReached(Vec<u32>),
}

/// Look up `cp` in the chosen libnu casemap table and walk the resulting
/// multi-codepoint expansion via `nu_casemap_read` until a 0 codepoint is
/// read or `cap` iterations have elapsed.
///
/// This mirrors the production C loop verbatim:
///
/// ```text
/// const char *map = nu_tolower(cp);              // or nu_tofold(cp)
/// if (map != NULL) {
///     uint32_t mu;
///     while (1) {
///         map = nu_casemap_read(map, &mu);
///         if (mu == 0) break;
///         // ...use mu...
///     }
/// }
/// ```
///
/// Call sites: `__fold` and `strToLowerRunes` in `src/trie/rune_util.c`,
/// `unicode_tolower` in `src/util/strconv.h`.
///
/// `nu_casemap_read` is a `#define` alias for `nu_utf8_read`, so each
/// iteration decodes one UTF-8 codepoint from libnu's static mapping bytes.
/// A well-formed table entry is a null-terminated UTF-8 string, so the loop
/// reaches the codepoint-0 terminator within (mapping_length + 1) reads.
/// The `cap` is defensive: if a table is malformed (missing terminator),
/// the production loop would read OOB into adjacent static memory; this
/// helper instead returns [`CasemapWalk::CapReached`].
pub fn walk_casemap_expansion(cp: u32, table: CaseMapTable, cap: usize) -> CasemapWalk {
    let head = match table {
        // SAFETY: `nu_tofold` is a pure table lookup; any `u32` input is
        // accepted and the returned pointer is either NULL or points into
        // libnu's static null-terminated mapping data.
        CaseMapTable::Fold => unsafe { libnu_ffi::nu_tofold(cp) },
        // SAFETY: `nu_tolower` is a pure table lookup; any `u32` input is
        // accepted and the returned pointer is either NULL or points into
        // libnu's static null-terminated mapping data.
        CaseMapTable::Lower => unsafe { libnu_ffi::nu_tolower(cp) },
    };
    if head.is_null() {
        return CasemapWalk::NoMapping;
    }

    let mut codepoints = Vec::new();
    let mut ptr = head;
    for _ in 0..cap {
        let mut decoded: u32 = 0;
        // SAFETY: `ptr` starts inside libnu's static mapping data (returned
        // by `nu_tofold` / `nu_tolower`) and advances by one codepoint per
        // iteration via `nu_utf8_read`. As long as the table entry contains
        // a 0 byte within `cap` codepoints (the well-formed invariant), the
        // read stays inside that entry. If the invariant is violated the
        // cap bounds the read to `cap` codepoints' worth of bytes past the
        // start — same as the production C loop would do under the same
        // assumption, except production has no cap at all.
        ptr = unsafe { libnu_ffi::nu_utf8_read_shim(ptr, &mut decoded) };
        if decoded == 0 {
            return CasemapWalk::Terminated(codepoints);
        }
        codepoints.push(decoded);
    }
    CasemapWalk::CapReached(codepoints)
}
