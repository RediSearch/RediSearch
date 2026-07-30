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
///    [`TermSuffixIndex_New`], with no mutating call
///    ([`TermSuffixIndex_Add`], [`TermSuffixIndex_Remove`],
///    [`TermSuffixIndex_Free`]) running concurrently. Concurrent
///    read-only calls are allowed.
/// 2. `needle` must point to a [valid] byte sequence of length `len`.
/// 3. `cb` must not modify or free `tsi`, nor retain the term
///    pointer beyond the call.
///
/// # Panics
///
/// Panics if `cb` is NULL, or if `needle` is not valid UTF-8.
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
    let cb = cb.expect("callback must not be NULL");

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
///    [`TermSuffixIndex_New`], with no mutating call
///    ([`TermSuffixIndex_Add`], [`TermSuffixIndex_Remove`],
///    [`TermSuffixIndex_Free`]) running concurrently. Concurrent
///    read-only calls are allowed.
/// 2. `needle` must point to a [valid] byte sequence of length `len`.
/// 3. `cb` must not modify or free `tsi`, nor retain the term
///    pointer beyond the call.
///
/// # Panics
///
/// Panics if `cb` is NULL, or if `needle` is not valid UTF-8.
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
    let cb = cb.expect("callback must not be NULL");

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
/// When `should_stop` is non-NULL it is polled periodically while the
/// candidate set is scanned; once it returns `true` the scan is abandoned
/// and only the terms gathered so far are reported. This bounds the
/// expensive scan by a caller-owned deadline, which the per-term callback
/// alone cannot do because matches are gathered before any callback fires.
/// Pass NULL to scan without a deadline.
///
/// Returns 0 when the pattern has no literal token that can anchor the
/// search; the caller must then fall back to a full scan. Returns 1
/// otherwise, even when no term matched or the scan stopped early.
///
/// # Safety
///
/// 1. `tsi` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`], with no mutating call
///    ([`TermSuffixIndex_Add`], [`TermSuffixIndex_Remove`],
///    [`TermSuffixIndex_Free`]) running concurrently. Concurrent
///    read-only calls are allowed.
/// 2. `pattern` must point to a [valid] byte sequence of length `len`.
/// 3. `cb` must not modify or free `tsi`, nor retain the term
///    pointer beyond the call.
/// 4. If `should_stop` is non-NULL it must be safe to call with `stop_ctx`
///    for the duration of this call, and must not modify or free `tsi`.
///
/// # Panics
///
/// Panics if `cb` is NULL, or if `pattern` is not valid UTF-8.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_IterateWildcard(
    tsi: *const TermSuffixIndex,
    pattern: *const c_char,
    len: usize,
    cb: TermSuffixIterateCallback,
    ctx: *mut c_void,
    should_stop: TermSuffixShouldStop,
    stop_ctx: *mut c_void,
) -> c_int {
    debug_assert!(!tsi.is_null(), "tsi cannot be NULL");
    debug_assert!(!pattern.is_null(), "pattern cannot be NULL");
    let cb = cb.expect("callback must not be NULL");

    // Safety: ensured by caller (1.)
    let index = unsafe { &*tsi };
    // Safety: ensured by caller (2.)
    let bytes = unsafe { slice::from_raw_parts(pattern.cast::<u8>(), len) };

    let pattern = str::from_utf8(bytes).expect("pattern must be valid UTF-8");
    // Safety: ensured by caller (4.)
    let should_stop = || should_stop.is_some_and(|f| unsafe { f(stop_ctx) });
    let Some(matches) = index.iter_wildcard(pattern, should_stop) else {
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
/// the iterate function. Return 0 to continue the iteration; any other
/// value stops it.
///
/// `payload` is always NULL: a term suffix index stores no per-term
/// payload. The slot exists only to keep the callback signature uniform
/// with the other trie iterate callbacks.
pub type TermSuffixIterateCallback = Option<
    unsafe extern "C" fn(
        term: *const c_char,
        len: usize,
        ctx: *mut c_void,
        payload: *mut c_void,
    ) -> c_int,
>;

/// Stop predicate polled while a wildcard scan walks its candidates.
///
/// `ctx` is the `stop_ctx` passed to the iterate function. Return `true`
/// to abandon the scan (e.g. once a deadline has passed); the caller owns
/// the decision and any clock it consults. A NULL predicate never stops.
pub type TermSuffixShouldStop = Option<unsafe extern "C" fn(ctx: *mut c_void) -> bool>;
