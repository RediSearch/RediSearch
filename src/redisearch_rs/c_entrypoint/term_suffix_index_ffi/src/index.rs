/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for creating, mutating and freeing a [`TermSuffixIndex`].

use std::ffi::c_char;

use super::TermSuffixIndex;

/// Create a new, empty [`TermSuffixIndex`]. Free it with
/// [`TermSuffixIndex_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn TermSuffixIndex_New() -> *mut TermSuffixIndex {
    Box::into_raw(Box::new(TermSuffixIndex::new()))
}

/// Free a [`TermSuffixIndex`] and all terms it owns.
///
/// # Safety
///
/// 1. `t` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`].
/// 2. No iterator obtained from `t` may be alive.
/// 3. `t` must not be used after this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_Free(t: *mut TermSuffixIndex) {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // Safety: ensured by caller (1., 2., 3.)
    drop(unsafe { Box::from_raw(t) });
}

/// Estimated heap memory currently held by the index, in bytes.
///
/// # Safety
///
/// 1. `t` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_MemUsage(t: *const TermSuffixIndex) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // Safety: ensured by caller (1.)
    let index = unsafe { &*t };
    index.mem_usage()
}

/// Add `term` (`len` UTF-8 bytes) to the index. Adding an existing,
/// empty or non-UTF-8 term is a no-op.
///
/// # Safety
///
/// 1. `t` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`].
/// 2. `term` must point to a [valid] byte sequence of length `len`.
/// 3. No iterator obtained from `t` may be alive.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_Add(
    t: *mut TermSuffixIndex,
    term: *const c_char,
    len: usize,
) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // Safety: ensured by caller (1., 3.)
    let index = unsafe { &mut *t };
    // Safety: ensured by caller (2.)
    let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), len) };

    let Ok(term) = std::str::from_utf8(bytes) else {
        debug_assert!(false, "term must be valid UTF-8");
        return;
    };
    index.add(term);
}

/// Remove `term` (`len` UTF-8 bytes) from the index. Removing an absent,
/// empty or non-UTF-8 term is a no-op.
///
/// # Safety
///
/// 1. `t` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`].
/// 2. `term` must point to a [valid] byte sequence of length `len`.
/// 3. No iterator obtained from `t` may be alive.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_Remove(
    t: *mut TermSuffixIndex,
    term: *const c_char,
    len: usize,
) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // Safety: ensured by caller (1., 3.)
    let index = unsafe { &mut *t };
    // Safety: ensured by caller (2.)
    let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), len) };

    let Ok(term) = std::str::from_utf8(bytes) else {
        debug_assert!(false, "term must be valid UTF-8");
        return;
    };
    index.remove(term);
}
