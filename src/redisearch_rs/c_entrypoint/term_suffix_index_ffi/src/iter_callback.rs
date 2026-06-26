/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Callback-style iteration over a [`TermSuffixIndex`].
//!
//! The caller passes a [`TermSuffixIterateCallback`] that is invoked once
//! per matched term, fully drained within the call.

use std::ffi::{c_char, c_int, c_void};
use std::{ptr, slice, str};

use super::*;

/// Invoke `cb` once per member term containing the UTF-8 needle
/// `(needle, len)` as a substring; a term may be reported more than once.
/// Iteration stops early when the callback returns a non-zero value. An
/// empty needle reports no matches.
///
/// # Safety
///
/// 1. `tsi` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`].
/// 2. `needle` must point to a [valid] byte sequence of length `len`.
/// 3. `cb` cannot be NULL and must not modify or free `tsi`, nor
///    retain the term pointer beyond the call.
///
/// # Panics
///
/// Panics if `needle` is not valid UTF-8.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_IterateContains(
    tsi: *const TermSuffixIndex,
    needle: *const c_char,
    len: usize,
    cb: TermSuffixIterateCallback,
    ctx: *mut c_void,
) {
    debug_assert!(!tsi.is_null(), "tsi cannot be NULL");
    debug_assert!(!needle.is_null(), "needle cannot be NULL");
    let Some(cb) = cb else {
        debug_assert!(false, "cb cannot be NULL");
        return;
    };

    // Safety: ensured by caller (1.)
    let index = unsafe { &*tsi };
    // Safety: ensured by caller (2.)
    let bytes = unsafe { slice::from_raw_parts(needle.cast::<u8>(), len) };

    let needle = str::from_utf8(bytes).expect("needle must be valid UTF-8");
    for term in index.iter_contains(needle) {
        // Safety: ensured by caller (3.)
        let outcome = unsafe {
            cb(
                term.as_ptr().cast::<c_char>(),
                term.len(),
                ctx,
                ptr::null_mut(),
            )
        };
        if outcome != 0 {
            break;
        }
    }
}

/// Invoke `cb` once per member term ending with the UTF-8 needle
/// `(needle, len)`; each matching term is reported exactly once. Iteration
/// stops early when the callback returns a non-zero value. An empty
/// needle reports no matches.
///
/// # Safety
///
/// 1. `tsi` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`].
/// 2. `needle` must point to a [valid] byte sequence of length `len`.
/// 3. `cb` cannot be NULL and must not modify or free `tsi`, nor
///    retain the term pointer beyond the call.
///
/// # Panics
///
/// Panics if `needle` is not valid UTF-8.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_IterateSuffix(
    tsi: *const TermSuffixIndex,
    needle: *const c_char,
    len: usize,
    cb: TermSuffixIterateCallback,
    ctx: *mut c_void,
) {
    debug_assert!(!tsi.is_null(), "tsi cannot be NULL");
    debug_assert!(!needle.is_null(), "needle cannot be NULL");
    let Some(cb) = cb else {
        debug_assert!(false, "cb cannot be NULL");
        return;
    };

    // Safety: ensured by caller (1.)
    let index = unsafe { &*tsi };
    // Safety: ensured by caller (2.)
    let bytes = unsafe { slice::from_raw_parts(needle.cast::<u8>(), len) };

    let needle = str::from_utf8(bytes).expect("needle must be valid UTF-8");
    for term in index.iter_suffix(needle) {
        // Safety: ensured by caller (3.)
        let outcome = unsafe {
            cb(
                term.as_ptr().cast::<c_char>(),
                term.len(),
                ctx,
                ptr::null_mut(),
            )
        };
        if outcome != 0 {
            break;
        }
    }
}

/// Invoke `cb` once per member term matching the wildcard pattern
/// `(pattern, len)` (`*` matches any run of characters, `?` exactly one
/// byte); a term may be reported more than once. Iteration stops early
/// when the callback returns a non-zero value.
///
/// Returns 0 when the pattern has no literal token that can anchor the
/// search; the caller must then fall back to a full scan. Returns 1
/// otherwise, even when no term matched.
///
/// # Safety
///
/// 1. `tsi` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`].
/// 2. `pattern` must point to a [valid] byte sequence of length `len`.
/// 3. `cb` cannot be NULL and must not modify or free `tsi`, nor
///    retain the term pointer beyond the call.
///
/// # Panics
///
/// Panics if `pattern` is not valid UTF-8.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_IterateWildcard(
    tsi: *const TermSuffixIndex,
    pattern: *const c_char,
    len: usize,
    cb: TermSuffixIterateCallback,
    ctx: *mut c_void,
) -> c_int {
    debug_assert!(!tsi.is_null(), "tsi cannot be NULL");
    debug_assert!(!pattern.is_null(), "pattern cannot be NULL");
    let Some(cb) = cb else {
        debug_assert!(false, "cb cannot be NULL");
        // No way to report matches; have the caller fall back to a full scan.
        return 0;
    };

    // Safety: ensured by caller (1.)
    let index = unsafe { &*tsi };
    // Safety: ensured by caller (2.)
    let bytes = unsafe { slice::from_raw_parts(pattern.cast::<u8>(), len) };

    let pattern = str::from_utf8(bytes).expect("pattern must be valid UTF-8");
    let Some(matches) = index.iter_wildcard(pattern) else {
        return 0;
    };
    for term in matches {
        // Safety: ensured by caller (3.)
        let outcome = unsafe {
            cb(
                term.as_ptr().cast::<c_char>(),
                term.len(),
                ctx,
                ptr::null_mut(),
            )
        };
        if outcome != 0 {
            break;
        }
    }
    1
}

/// Callback invoked once per term.
///
/// `term` points to `len` UTF-8 bytes, NOT NUL-terminated, valid only
/// for the duration of the call. `ctx` is the caller context passed to
/// the iterate function; `payload` is always NULL. Return 0 to continue
/// the iteration; any other value stops it.
pub type TermSuffixIterateCallback = Option<
    unsafe extern "C" fn(
        term: *const c_char,
        len: usize,
        ctx: *mut c_void,
        payload: *mut c_void,
    ) -> c_int,
>;
