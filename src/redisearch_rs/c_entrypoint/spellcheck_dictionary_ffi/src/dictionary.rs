/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for creating, mutating, querying and freeing a
//! [`SpellCheckDictionary`].

use std::ffi::c_char;
use std::{slice, str};

use super::SpellCheckDictionary;

/// Create a new, empty [`SpellCheckDictionary`]. Must be freed with
/// [`SpellCheckDictionary_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn SpellCheckDictionary_New() -> *mut SpellCheckDictionary {
    Box::into_raw(Box::new(SpellCheckDictionary::new()))
}

/// Free a [`SpellCheckDictionary`] and all terms it owns.
///
/// # Safety
///
/// 1. `dict` must be a [valid], non-null pointer obtained from
///    [`SpellCheckDictionary_New`].
/// 2. No iterator obtained from `dict` may be alive.
/// 3. `dict` must not be used after this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_Free(dict: *mut SpellCheckDictionary) {
    debug_assert!(!dict.is_null(), "dict cannot be NULL");

    // Safety: ensured by caller (1., 2., 3.)
    drop(unsafe { Box::from_raw(dict) });
}

/// Add `term` (`len` UTF-8 bytes) to the dictionary, stored verbatim
/// (case-preserving). Empty or over-long terms are rejected.
///
/// Returns `true` only if the term was newly added.
///
/// # Safety
///
/// 1. `dict` must be a [valid], non-null pointer obtained from
///    [`SpellCheckDictionary_New`].
/// 2. No other call on `dict` (mutating or read-only) may run concurrently
///    with this call, and no iterator obtained from `dict` may be alive.
/// 3. `term` must point to a [valid] byte sequence of length `len`.
///
/// # Panics
///
/// Panics if `term` is not valid UTF-8.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_Add(
    dict: *mut SpellCheckDictionary,
    term: *const c_char,
    len: usize,
) -> bool {
    debug_assert!(!dict.is_null(), "dict cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // Safety: ensured by caller (1., 2.)
    let dictionary = unsafe { &mut *dict };
    // Safety: ensured by caller (3.)
    let bytes = unsafe { slice::from_raw_parts(term.cast::<u8>(), len) };

    let term = str::from_utf8(bytes).expect("term must be valid UTF-8");
    dictionary.add(term)
}

/// Remove `term` (`len` UTF-8 bytes) from the dictionary. Matched verbatim,
/// so removal is case-sensitive.
///
/// Returns `true` if the term was present.
///
/// # Safety
///
/// 1. `dict` must be a [valid], non-null pointer obtained from
///    [`SpellCheckDictionary_New`].
/// 2. No other call on `dict` (mutating or read-only) may run concurrently
///    with this call, and no iterator obtained from `dict` may be alive.
/// 3. `term` must point to a [valid] byte sequence of length `len`.
///
/// # Panics
///
/// Panics if `term` is not valid UTF-8.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_Remove(
    dict: *mut SpellCheckDictionary,
    term: *const c_char,
    len: usize,
) -> bool {
    debug_assert!(!dict.is_null(), "dict cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // Safety: ensured by caller (1., 2.)
    let dictionary = unsafe { &mut *dict };
    // Safety: ensured by caller (3.)
    let bytes = unsafe { slice::from_raw_parts(term.cast::<u8>(), len) };

    let term = str::from_utf8(bytes).expect("term must be valid UTF-8");
    dictionary.remove(term)
}

/// The number of terms stored in the dictionary.
///
/// # Safety
///
/// 1. `dict` must be a [valid], non-null pointer obtained from
///    [`SpellCheckDictionary_New`].
/// 2. No mutating call on `dict` may run concurrently with this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_Len(dict: *const SpellCheckDictionary) -> usize {
    debug_assert!(!dict.is_null(), "dict cannot be NULL");

    // Safety: ensured by caller (1., 2.)
    let dictionary = unsafe { &*dict };
    dictionary.len()
}

/// Check whether a stored term equals `term` (`len` UTF-8 bytes), ignoring
/// case. An over-long `term` never matches.
///
/// Returns `true` if such a term exists.
///
/// # Safety
///
/// 1. `dict` must be a [valid], non-null pointer obtained from
///    [`SpellCheckDictionary_New`].
/// 2. No mutating call on `dict` may run concurrently with this call.
/// 3. `term` must point to a [valid] byte sequence of length `len`.
///
/// # Panics
///
/// Panics if `term` is not valid UTF-8.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_Contains(
    dict: *const SpellCheckDictionary,
    term: *const c_char,
    len: usize,
) -> bool {
    debug_assert!(!dict.is_null(), "dict cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // Safety: ensured by caller (1., 2.)
    let dictionary = unsafe { &*dict };
    // Safety: ensured by caller (3.)
    let bytes = unsafe { slice::from_raw_parts(term.cast::<u8>(), len) };

    let term = str::from_utf8(bytes).expect("term must be valid UTF-8");
    dictionary.contains(term)
}
