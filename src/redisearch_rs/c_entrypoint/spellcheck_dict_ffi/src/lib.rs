/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for [`spellcheck_dict::SpellCheckDictionary`], the Rust
//! replacement for the per-name `Trie *` dictionaries behind `FT.DICTADD`,
//! `FT.DICTDEL` and `FT.DICTDUMP` (`dictionary.c`) and consumed by the spell
//! checker (`spell_check.c`).
//!
//! All string parameters are byte pointers with an explicit length and must
//! be valid UTF-8: dictionary terms come from user-supplied
//! `RedisModuleString`s. Invalid UTF-8 is rejected — mutations become no-ops
//! that report "nothing changed", lookups report "not found" — and trips a
//! debug assertion.
//!
//! Case folding lives inside [`spellcheck_dict::SpellCheckDictionary`]:
//! membership ([`SpellCheckDictionary_Contains`]) and fuzzy matching
//! ([`SpellCheckDictionary_IterateFuzzy`]) are case-insensitive, while
//! insertion and removal are case-sensitive and preserve the stored case
//! (mirroring the C `Trie` + `LoweringFilterFunc` behaviour).

#![allow(non_camel_case_types, non_snake_case)]

use std::ffi::{c_char, c_int};

use spellcheck_dict::SpellCheckDictionary as SpellCheckDictionaryImpl;

/// A set of dictionary terms supporting exact membership, fuzzy
/// (edit-distance) lookup and full enumeration.
///
/// Opaque to C; obtained from [`NewSpellCheckDictionary`] and freed with
/// [`SpellCheckDictionary_Free`].
pub struct SpellCheckDictionary(SpellCheckDictionaryImpl);

/// Yields the terms matched by an iteration over a
/// [`SpellCheckDictionary`].
///
/// Opaque to C; obtained from [`SpellCheckDictionary_IterateAll`] or
/// [`SpellCheckDictionary_IterateFuzzy`], advanced with
/// [`SpellCheckDictionaryIterator_Next`], and freed with
/// [`SpellCheckDictionaryIterator_Free`].
pub struct SpellCheckDictionaryIterator<'d> {
    iter: Box<dyn Iterator<Item = String> + 'd>,
    /// Keeps the most recently yielded string alive so the pointer handed to
    /// C stays valid until the next advance (or free).
    current: Option<String>,
}

/// Create a new, empty [`SpellCheckDictionary`].
///
/// Free it with [`SpellCheckDictionary_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewSpellCheckDictionary() -> *mut SpellCheckDictionary {
    Box::into_raw(Box::new(SpellCheckDictionary(
        SpellCheckDictionaryImpl::new(),
    )))
}

/// Free a [`SpellCheckDictionary`] and all terms it owns.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`SpellCheckDictionary`] obtained from
///   [`NewSpellCheckDictionary`] and cannot be NULL.
/// - No iterator obtained from `t` may be alive.
/// - `t` must not be used after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_Free(t: *mut SpellCheckDictionary) {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer obtained
    // from `NewSpellCheckDictionary`, with no outstanding iterators.
    drop(unsafe { Box::from_raw(t) });
}

/// Add `term` (a UTF-8 string of `len` bytes) to the dictionary. Returns 1 if
/// the term was newly inserted, 0 if it was already a member. Adding a
/// non-UTF-8 term is a no-op that returns 0.
///
/// Insertion is case-sensitive: `"Foo"` and `"foo"` are distinct members.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`SpellCheckDictionary`] obtained from
///   [`NewSpellCheckDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No iterator obtained from `t` may be alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_Add(
    t: *mut SpellCheckDictionary,
    term: *const c_char,
    len: usize,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // SpellCheckDictionary, with no outstanding iterators borrowing it.
    let SpellCheckDictionary(dict) = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), len) };

    let Ok(term) = std::str::from_utf8(bytes) else {
        debug_assert!(false, "term must be valid UTF-8");
        return 0;
    };
    c_int::from(dict.add(term))
}

/// Remove `term` (a UTF-8 string of `len` bytes) from the dictionary. Returns
/// 1 if the term was a member and got removed, 0 otherwise. Removing a
/// non-UTF-8 term is a no-op that returns 0.
///
/// Removal is case-sensitive: it only removes the exact stored spelling.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`SpellCheckDictionary`] obtained from
///   [`NewSpellCheckDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No iterator obtained from `t` may be alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_Remove(
    t: *mut SpellCheckDictionary,
    term: *const c_char,
    len: usize,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // SpellCheckDictionary, with no outstanding iterators borrowing it.
    let SpellCheckDictionary(dict) = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), len) };

    let Ok(term) = std::str::from_utf8(bytes) else {
        debug_assert!(false, "term must be valid UTF-8");
        return 0;
    };
    c_int::from(dict.remove(term))
}

/// Number of terms currently stored in the dictionary.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`SpellCheckDictionary`] obtained from
///   [`NewSpellCheckDictionary`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_Len(t: *const SpellCheckDictionary) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // SpellCheckDictionary.
    let SpellCheckDictionary(dict) = unsafe { &*t };
    dict.len()
}

/// Report whether `term` (a UTF-8 string of `len` bytes) is a member.
/// Matching is case-insensitive. Returns 1 if a case-insensitive match
/// exists, 0 otherwise. A non-UTF-8 term reports 0.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`SpellCheckDictionary`] obtained from
///   [`NewSpellCheckDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_Contains(
    t: *const SpellCheckDictionary,
    term: *const c_char,
    len: usize,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // SpellCheckDictionary.
    let SpellCheckDictionary(dict) = unsafe { &*t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), len) };

    let Ok(term) = std::str::from_utf8(bytes) else {
        debug_assert!(false, "term must be valid UTF-8");
        return 0;
    };
    c_int::from(dict.contains(term))
}

/// Iterate over every term stored in the dictionary, in lexicographical
/// order. Backs `FT.DICTDUMP`.
///
/// Invoke [`SpellCheckDictionaryIterator_Next`] to get the results from the
/// iteration.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`SpellCheckDictionary`] obtained from
///   [`NewSpellCheckDictionary`] and cannot be NULL.
/// - `t` must not be modified or freed while the iterator lives.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_IterateAll<'d>(
    t: *const SpellCheckDictionary,
) -> *mut SpellCheckDictionaryIterator<'d> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // SpellCheckDictionary that outlives the iterator.
    let SpellCheckDictionary(dict) = unsafe { &*t };

    let iter = Box::new(dict.dump());
    Box::into_raw(Box::new(SpellCheckDictionaryIterator {
        iter,
        current: None,
    }))
}

/// Iterate over every term whose case-folded form is within Levenshtein edit
/// distance `max_dist` (in codepoints) of the UTF-8 needle `(term, len)`.
/// Matching is case-insensitive; yielded terms keep their stored case. Backs
/// the spell checker's suggestion search.
///
/// A non-UTF-8 needle yields no matches.
///
/// Invoke [`SpellCheckDictionaryIterator_Next`] to get the results from the
/// iteration.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`SpellCheckDictionary`] obtained from
///   [`NewSpellCheckDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - `t` must not be modified or freed while the iterator lives. The needle
///   bytes need only stay valid for the duration of this call — they are
///   case-folded into an owned copy before the function returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionary_IterateFuzzy<'d>(
    t: *const SpellCheckDictionary,
    term: *const c_char,
    len: usize,
    max_dist: u32,
) -> *mut SpellCheckDictionaryIterator<'d> {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // SpellCheckDictionary that outlives the iterator.
    let SpellCheckDictionary(dict) = unsafe { &*t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), len) };

    let iter: Box<dyn Iterator<Item = String>> = match std::str::from_utf8(bytes) {
        Ok(needle) => Box::new(dict.fuzzy_matches(needle, max_dist)),
        Err(_) => {
            debug_assert!(false, "needle must be valid UTF-8");
            Box::new(std::iter::empty())
        }
    };
    Box::into_raw(Box::new(SpellCheckDictionaryIterator {
        iter,
        current: None,
    }))
}

/// Advance the iterator. Returns 1 and stores the next term into
/// `(*str, *len)` if there is one, or returns 0 once exhausted.
///
/// The string written to `*str` is NOT NUL-terminated, owned by the iterator,
/// and only valid until the next call to [`SpellCheckDictionaryIterator_Next`]
/// or [`SpellCheckDictionaryIterator_Free`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`SpellCheckDictionaryIterator`] obtained from
///   [`SpellCheckDictionary_IterateAll`] or
///   [`SpellCheckDictionary_IterateFuzzy`] and cannot be NULL.
/// - `str` and `len` must be valid, non-NULL pointers to writable locations.
/// - The [`SpellCheckDictionary`] the iterator was obtained from must still be
///   alive and unmodified since the iterator was created.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionaryIterator_Next(
    it: *mut SpellCheckDictionaryIterator,
    str: *mut *const c_char,
    len: *mut usize,
) -> c_int {
    debug_assert!(!it.is_null(), "it cannot be NULL");
    debug_assert!(!str.is_null(), "str cannot be NULL");
    debug_assert!(!len.is_null(), "len cannot be NULL");

    // SAFETY: caller is to ensure `it` is a valid, non-null pointer to a live
    // SpellCheckDictionaryIterator.
    let iterator = unsafe { &mut *it };

    let Some(term) = iterator.iter.next() else {
        return 0;
    };
    let term = iterator.current.insert(term);

    // SAFETY: caller is to ensure `str` points to a writable location.
    unsafe {
        *str = term.as_ptr().cast::<c_char>();
    }
    // SAFETY: caller is to ensure `len` points to a writable location.
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
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`SpellCheckDictionaryIterator`] obtained from
///   [`SpellCheckDictionary_IterateAll`] or
///   [`SpellCheckDictionary_IterateFuzzy`] and cannot be NULL.
/// - `it` must not be used after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SpellCheckDictionaryIterator_Free(it: *mut SpellCheckDictionaryIterator) {
    debug_assert!(!it.is_null(), "it cannot be NULL");

    // SAFETY: caller is to ensure `it` is a valid, non-null pointer obtained
    // from `SpellCheckDictionary_IterateAll` or
    // `SpellCheckDictionary_IterateFuzzy`.
    drop(unsafe { Box::from_raw(it) });
}
