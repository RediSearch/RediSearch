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
use std::{slice, str};

use super::TermSuffixIndex;

/// Create a new, empty [`TermSuffixIndex`]. Must be freed with
/// [`TermSuffixIndex_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn TermSuffixIndex_New() -> *mut TermSuffixIndex {
    Box::into_raw(Box::new(TermSuffixIndex::new()))
}

/// Free a [`TermSuffixIndex`] and all terms it owns.
///
/// # Safety
///
/// 1. `tsi` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`].
/// 2. No iterator obtained from `tsi` may be alive.
/// 3. `tsi` must not be used after this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_Free(tsi: *mut TermSuffixIndex) {
    debug_assert!(!tsi.is_null(), "tsi cannot be NULL");

    // Safety: ensured by caller (1., 2., 3.)
    drop(unsafe { Box::from_raw(tsi) });
}

/// Estimated heap memory currently held by the index, in bytes.
///
/// # Safety
///
/// 1. `tsi` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_MemUsage(tsi: *const TermSuffixIndex) -> usize {
    debug_assert!(!tsi.is_null(), "tsi cannot be NULL");

    // Safety: ensured by caller (1.)
    let index = unsafe { &*tsi };
    index.mem_usage()
}

/// Add `term` (`len` UTF-8 bytes) to the index. Adding an existing or
/// empty term is a no-op.
///
/// # Safety
///
/// 1. `tsi` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`].
/// 2. No iterator obtained from `tsi` may be alive.
/// 3. `term` must point to a [valid] byte sequence of length `len`.
///
/// # Panics
///
/// Panics if `term` is not valid UTF-8.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_Add(
    tsi: *mut TermSuffixIndex,
    term: *const c_char,
    len: usize,
) {
    debug_assert!(!tsi.is_null(), "tsi cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // Safety: ensured by caller (1., 2.)
    let index = unsafe { &mut *tsi };
    // Safety: ensured by caller (3.)
    let bytes = unsafe { slice::from_raw_parts(term.cast::<u8>(), len) };

    let term = str::from_utf8(bytes).expect("term must be valid UTF-8");
    index.add(term);
}

/// Remove `term` (`len` UTF-8 bytes) from the index. Removing an absent
/// or empty term is a no-op.
///
/// # Safety
///
/// 1. `tsi` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`].
/// 2. No iterator obtained from `tsi` may be alive.
/// 3. `term` must point to a [valid] byte sequence of length `len`.
///
/// # Panics
///
/// Panics if `term` is not valid UTF-8.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_Remove(
    tsi: *mut TermSuffixIndex,
    term: *const c_char,
    len: usize,
) {
    debug_assert!(!tsi.is_null(), "tsi cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // Safety: ensured by caller (1., 2.)
    let index = unsafe { &mut *tsi };
    // Safety: ensured by caller (3.)
    let bytes = unsafe { slice::from_raw_parts(term.cast::<u8>(), len) };

    let term = str::from_utf8(bytes).expect("term must be valid UTF-8");
    index.remove(term);
}
