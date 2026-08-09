/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Cursor-style iteration over a [`SpellCheckDictionary`].
//!
//! The caller obtains an opaque iterator, advances it with
//! [`SpellCheckDictionaryIterator_Next`], and frees it with
//! [`SpellCheckDictionaryIterator_Free`].

use std::ffi::{c_char, c_int};
use std::{slice, str};

use super::*;

/// Iterate over every term stored in the dictionary, each in its original
/// case, in lexicographical order.
///
/// The caller owns the returned iterator: advance it with
/// [`SpellCheckDictionaryIterator_Next`] and free it exactly once with
/// [`SpellCheckDictionaryIterator_Free`] when done (including when it is
/// exhausted or abandoned early).
///
/// # Safety
///
/// 1. `dict` must be a [valid], non-null pointer obtained from
///    [`SpellCheckDictionary_New`].
/// 2. No mutating call on `dict` may run while the iterator lives, and
///    `dict` must not be freed until then.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_IterateAll<'sd>(
    dict: *const SpellCheckDictionary,
) -> *mut SpellCheckDictionaryIterator<'sd> {
    debug_assert!(!dict.is_null(), "dict cannot be NULL");

    // Safety: ensured by caller (1., 2.)
    let dictionary = unsafe { &*dict };

    let iter = Box::new(dictionary.dump());
    Box::into_raw(Box::new(SpellCheckDictionaryIterator {
        iter,
        current: None,
    }))
}

/// Iterate over the stored terms within Levenshtein edit distance
/// `max_dist` (in codepoints) of `term` (`len` UTF-8 bytes), each in its
/// stored case. Matching ignores case; an over-long `term` matches nothing.
///
/// The matches are computed eagerly by this call; subsequent mutations of
/// the dictionary do not affect what the iterator yields.
///
/// The caller owns the returned iterator: advance it with
/// [`SpellCheckDictionaryIterator_Next`] and free it exactly once with
/// [`SpellCheckDictionaryIterator_Free`] when done (including when it is
/// exhausted or abandoned early).
///
/// # Safety
///
/// 1. `dict` must be a [valid], non-null pointer obtained from
///    [`SpellCheckDictionary_New`].
/// 2. No mutating call on `dict` may run concurrently with this call or
///    while the iterator lives, and `dict` must not be freed until then.
/// 3. `term` must point to a [valid] byte sequence of length `len`.
///
/// # Panics
///
/// Panics if `term` is not valid UTF-8.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_IterateFuzzy<'sd>(
    dict: *const SpellCheckDictionary,
    term: *const c_char,
    len: usize,
    max_dist: u32,
) -> *mut SpellCheckDictionaryIterator<'sd> {
    debug_assert!(!dict.is_null(), "dict cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // Safety: ensured by caller (1., 2.)
    let dictionary = unsafe { &*dict };
    // Safety: ensured by caller (3.)
    let bytes = unsafe { slice::from_raw_parts(term.cast::<u8>(), len) };

    let term = str::from_utf8(bytes).expect("term must be valid UTF-8");
    let matches: Vec<String> = dictionary.fuzzy_matches(term, max_dist).collect();
    Box::into_raw(Box::new(SpellCheckDictionaryIterator {
        iter: Box::new(matches.into_iter()),
        current: None,
    }))
}

/// Advance the iterator. Returns 1 and points `*str`/`*len` at the next
/// term — borrowed from the iterator, not copied into caller-provided
/// storage — if there is one, or returns 0 once exhausted.
///
/// Returning 0 does not free the iterator; it must still be released
/// with [`SpellCheckDictionaryIterator_Free`].
///
/// The string `*str` points at is NOT NUL-terminated, owned by the
/// iterator, and only valid until the next call to
/// [`SpellCheckDictionaryIterator_Next`] or
/// [`SpellCheckDictionaryIterator_Free`].
///
/// # Safety
///
/// 1. `it` must be a [valid], non-null pointer to a live
///    [`SpellCheckDictionaryIterator`].
/// 2. `str` and `len` must be [valid], non-null pointers to writable
///    locations.
/// 3. The [`SpellCheckDictionary`] the iterator was obtained from must
///    still be alive, with no mutating call on it running concurrently.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionaryIterator_Next(
    it: *mut SpellCheckDictionaryIterator,
    str: *mut *const c_char,
    len: *mut usize,
) -> c_int {
    debug_assert!(!it.is_null(), "it cannot be NULL");
    debug_assert!(!str.is_null(), "str cannot be NULL");
    debug_assert!(!len.is_null(), "len cannot be NULL");

    // Safety: ensured by caller (1., 3.)
    let iterator = unsafe { &mut *it };

    let Some(term) = iterator.iter.next() else {
        return 0;
    };
    let term = iterator.current.insert(term);

    // Safety: ensured by caller (2.)
    unsafe {
        *str = term.as_ptr().cast::<c_char>();
    }
    // Safety: ensured by caller (2.)
    unsafe {
        *len = term.len();
    }
    1
}

/// Free an iterator obtained from [`SpellCheckDictionary_IterateAll`] or
/// [`SpellCheckDictionary_IterateFuzzy`]. Invalidates any string pointer
/// previously returned by [`SpellCheckDictionaryIterator_Next`].
///
/// # Safety
///
/// 1. `it` must be a [valid], non-null pointer to a live
///    [`SpellCheckDictionaryIterator`].
/// 2. `it` must not be used after this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionaryIterator_Free(it: *mut SpellCheckDictionaryIterator) {
    debug_assert!(!it.is_null(), "it cannot be NULL");

    // Safety: ensured by caller (1., 2.)
    drop(unsafe { Box::from_raw(it) });
}

/// Yields the terms of a [`SpellCheckDictionary`].
///
/// Obtained from [`SpellCheckDictionary_IterateAll`] or
/// [`SpellCheckDictionary_IterateFuzzy`], advanced with
/// [`SpellCheckDictionaryIterator_Next`], freed with
/// [`SpellCheckDictionaryIterator_Free`].
pub struct SpellCheckDictionaryIterator<'sd> {
    iter: Box<dyn Iterator<Item = String> + 'sd>,
    /// Keeps the most recently yielded string alive so the pointer
    /// stays valid until the next advance (or free).
    current: Option<String>,
}
