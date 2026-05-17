/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Lex-order iterator FFI for [`crate::TrieLex`].
//!
//! Mirrors the C contract of `Trie_Iterate(t, "", 0, 0, 1)` →
//! `TrieIterator_Next(it, &rstr, &slen, NULL, &score, NULL, &dist)`:
//! callers get a borrowed pointer to the current key's rune buffer, valid
//! until the next call to [`TrieLexIterator_Next`] or until the iterator is
//! freed with [`TrieLexIterator_Free`].

use crate::TrieLex;
use crate::TrieLexEntry;
use crate::encoding::{be_runes_to_runes, trie_lex_rune, trie_lex_t_len};
use lending_iterator::LendingIterator;
use std::ffi::c_int;
use trie_rs::iter::LendingIter;
use trie_rs::iter::filter::VisitAll;

/// Iterator over all entries of a [`TrieLex`] in lex order.
///
/// The lifetime ties the iterator to the trie it was created from. cheadergen
/// renders it as a bare `typedef struct TrieLexIterator TrieLexIterator;`
/// because the non-`repr(C)` fields are private to the crate.
pub struct TrieLexIterator<'tm> {
    iter: LendingIter<'tm, TrieLexEntry, VisitAll>,
    /// Scratch buffer that holds the current key as decoded runes, refreshed
    /// on every call to [`TrieLexIterator_Next`]. The returned pointer is
    /// borrowed from this buffer and invalidated by the next call.
    rune_buf: Vec<trie_lex_rune>,
}

/// Start a lex-order iteration over every entry in `t`.
///
/// # Safety
///
/// * `t` must point to a valid [`TrieLex`] allocated by
///   [`crate::TrieLex_New`].
/// * `t` must outlive the returned iterator and must not be freed or mutated
///   while the iterator is alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieLex_IterateAll<'tm>(t: *const TrieLex) -> *mut TrieLexIterator<'tm> {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: caller invariant: `t` is a valid, live `TrieLex` and not aliased
    // mutably for the iterator's lifetime.
    let TrieLex(trie) = unsafe { &*t };
    let it = Box::new(TrieLexIterator {
        iter: trie.lending_iter(),
        rune_buf: Vec::new(),
    });
    Box::into_raw(it)
}

/// Advance to the next entry and write the key/length/score into the out
/// pointers. Returns 1 if an entry was produced, 0 when exhausted.
///
/// The `*rstr_out` pointer is borrowed from the iterator and invalidated by
/// the next call. `slen_out` and `score_out` may be NULL to skip those
/// fields; `score_out=NULL` matches the `numDocs=NULL, matchCtx=NULL`
/// call shape used by `src/dictionary.c`.
///
/// # Safety
///
/// * `it` must be a non-null pointer returned by [`TrieLex_IterateAll`] and
///   not yet freed.
/// * `rstr_out` and `slen_out` must be valid for writes if non-null.
/// * `score_out`, when non-null, must be valid for a `f32` write.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieLexIterator_Next(
    it: *mut TrieLexIterator<'_>,
    rstr_out: *mut *mut trie_lex_rune,
    slen_out: *mut trie_lex_t_len,
    score_out: *mut f32,
) -> c_int {
    debug_assert!(!it.is_null(), "it cannot be NULL");
    // SAFETY: caller invariant: `it` is a live iterator created by
    // `TrieLex_IterateAll` and not aliased.
    let iter = unsafe { &mut *it };

    let Some((key_bytes, entry)) = iter.iter.next() else {
        return 0;
    };

    iter.rune_buf = be_runes_to_runes(key_bytes);
    let len = iter.rune_buf.len() as trie_lex_t_len;
    let ptr = iter.rune_buf.as_mut_ptr();

    if !rstr_out.is_null() {
        // SAFETY: caller invariant: `rstr_out` is a valid out-pointer.
        unsafe { *rstr_out = ptr };
    }
    if !slen_out.is_null() {
        // SAFETY: caller invariant: `slen_out` is a valid out-pointer.
        unsafe { *slen_out = len };
    }
    if !score_out.is_null() {
        // SAFETY: caller invariant: `score_out` is a valid out-pointer.
        unsafe { *score_out = entry.score };
    }
    1
}

/// Release an iterator returned by [`TrieLex_IterateAll`].
///
/// # Safety
///
/// * `it` must be either NULL or a pointer returned by
///   [`TrieLex_IterateAll`] that has not already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieLexIterator_Free(it: *mut TrieLexIterator<'_>) {
    if it.is_null() {
        return;
    }
    // SAFETY: caller invariant: `it` came from `Box::into_raw` in
    // `TrieLex_IterateAll` and is not aliased.
    drop(unsafe { Box::from_raw(it) });
}
