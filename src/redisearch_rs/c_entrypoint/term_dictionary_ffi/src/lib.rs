/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for [`term_dictionary::TermDictionary`], the Rust
//! replacement for the C terms trie behind `sp->terms` (the
//! `NewTrie`/`Trie_InsertStringBuffer`/`Trie_GetNode`/`Trie_Iterate`
//! family).
//!
//! All string parameters are byte pointers with an explicit length and
//! must be valid UTF-8: terms are tokenizer output and patterns come from
//! the query parser, so every caller already holds UTF-8. The dictionary
//! case-folds every key and pattern internally (see the
//! [`term_dictionary`] crate docs), so callers pass the raw term as-is.
//!
//! The dictionary follows a readers-writer contract: read-only calls
//! ([`TermDictionary_Get`], [`TermDictionary_Len`],
//! [`TermDictionary_MemUsage`] and the iterate functions) may run
//! concurrently with each other, while [`TermDictionary_AddTerm`],
//! [`TermDictionary_ReplaceTerm`], [`TermDictionary_Remove`],
//! [`TermDictionary_DecrementNumDocs`] and
//! [`TermDictionary_Free`] require exclusive access — no other call on
//! the same dictionary, and no live iterator obtained from it. An
//! iterator itself is single-threaded: it may not be advanced or freed
//! from two threads at once, though separate iterators over the same
//! dictionary may.
//!
//! Mutating entry points hold terms to the C terms trie's eligibility
//! rules (see [`storable_term`]). Two divergences from that trie are
//! deliberate: fuzzy pattern length is unbounded here, where
//! `Trie_IterateFuzzy` rejects a pattern above `TRIE_MAX_PREFIX`; and a
//! term with an embedded NUL is stored whole, where `strToRunes` stops
//! at the NUL.
//!
//! Keys are UTF-8 throughout, where the C trie decodes them into
//! `uint16_t` runes: a codepoint above U+FFFF survives here but is
//! truncated to a different character there. Exact search never went
//! through the trie, so it matches such a term either way; prefix,
//! suffix, contains, fuzzy and wildcard expansion find it here and find
//! nothing in the C trie. The flow tests in
//! `tests/pytests/test_terms_trie_encoding.py` pin the C outcomes, so
//! swapping a call site over to this dictionary changes them.

#![allow(non_camel_case_types, non_snake_case)]

use std::ffi::{c_char, c_int};
use std::slice;

use term_dictionary::{
    DecrResult as DecrResultImpl, InsertOutcome as InsertOutcomeImpl, LendingStrIter, TermEntry,
};

/// Opaque to C; obtained from [`NewTermDictionary`] and freed with
/// [`TermDictionary_Free`]. Re-exported rather than wrapped so Rust
/// callers holding the spec's opaque pointer can recover the dictionary
/// by casting, depending only on the pure crate.
pub use term_dictionary::TermDictionary;

/// Yields the matching terms (and their payloads) of an iteration over
/// a [`TermDictionary`].
///
/// Opaque to C; obtained from one of the `TermDictionary_Iterate*`
/// functions, advanced with [`TermDictionaryIterator_Next`], and freed
/// with [`TermDictionaryIterator_Free`].
pub struct TermDictionaryIterator<'td> {
    iter: Box<dyn LendingStrIter<'td, Data = TermEntry> + 'td>,
}

/// Outcome of [`TermDictionary_AddTerm`] and
/// [`TermDictionary_ReplaceTerm`].
///
/// The discriminants are those of the C terms trie's `TRIE_OK_NEW` and
/// `TRIE_OK_UPDATED`, so a call site that swaps
/// `Trie_InsertStringBuffer` for one of these keeps the meaning of its
/// existing comparison. New callers should still compare by name.
#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
#[cheadergen::config(prefix_with_name)]
pub enum TermDictionaryInsertOutcome {
    /// An existing entry was modified in place.
    Updated = 0,
    /// No prior entry existed; a new terminal was created.
    New = 1,
    /// The term is ineligible (see [`storable_term`]); nothing was
    /// stored. The C path reports these as `TRIE_OK_UPDATED`; the
    /// separate value lets a caller tracking distinct-term statistics
    /// tell a rejected term from a repeated one, while still comparing
    /// unequal to [`New`](Self::New) as that caller expects.
    Unsupported = 2,
}

impl From<InsertOutcomeImpl> for TermDictionaryInsertOutcome {
    fn from(outcome: InsertOutcomeImpl) -> Self {
        match outcome {
            InsertOutcomeImpl::New => Self::New,
            InsertOutcomeImpl::Updated => Self::Updated,
        }
    }
}

/// Outcome of [`TermDictionary_DecrementNumDocs`].
#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
#[cheadergen::config(prefix_with_name)]
pub enum TermDictionaryDecrResult {
    /// No terminal entry exists for the given term.
    NotFound = 0,
    /// `num_docs` was decremented and is still `> 0`.
    Updated = 1,
    /// `num_docs` reached `0`; the entry was removed.
    Deleted = 2,
    /// The term is ineligible (see [`storable_term`]), so no entry could
    /// exist for it. Unlike [`NotFound`](Self::NotFound) it says nothing
    /// about the add and delete counts having diverged.
    Unsupported = 3,
}

impl From<DecrResultImpl> for TermDictionaryDecrResult {
    fn from(result: DecrResultImpl) -> Self {
        match result {
            DecrResultImpl::NotFound => Self::NotFound,
            DecrResultImpl::Updated => Self::Updated,
            DecrResultImpl::Deleted => Self::Deleted,
        }
    }
}

/// Borrow `(ptr, len)` as a UTF-8 string. An empty length yields the
/// empty string regardless of `ptr`.
///
/// # Safety
///
/// When `len > 0`, `ptr` must point to `len` bytes that stay valid and
/// unmodified for the chosen lifetime `'a`.
///
/// # Panics
///
/// Panics if the bytes are not valid UTF-8; `what` names the argument in
/// the message.
unsafe fn term_arg<'a>(ptr: *const c_char, len: usize, what: &'static str) -> &'a str {
    let bytes = if len == 0 {
        &[]
    } else {
        debug_assert!(!ptr.is_null(), "ptr cannot be NULL when len > 0");
        // SAFETY: caller guarantees `ptr` points to `len` valid bytes.
        unsafe { slice::from_raw_parts(ptr.cast::<u8>(), len) }
    };

    std::str::from_utf8(bytes).unwrap_or_else(|_| panic!("{what} must be valid UTF-8"))
}

/// Byte bound the C terms trie's `Trie_InsertStringBuffer` applies,
/// `TRIE_INITIAL_STRING_LEN * sizeof(rune)`. Coarser than
/// [`MAX_TERM_RUNES`] rather than implied by it: a term of multi-byte
/// codepoints can fall under the rune bound and still exceed this one.
pub const MAX_TERM_BYTES: usize = 512;

/// Codepoint bound the C terms trie applies, `TRIE_INITIAL_STRING_LEN`.
/// Structural there — its iterator carries fixed `rune` and `stackNode`
/// arrays of that size, so a longer key cannot be walked.
pub const MAX_TERM_RUNES: usize = 256;

/// Borrow `(ptr, len)` as a term, or `None` for one the C terms trie
/// would have rejected: empty, over [`MAX_TERM_BYTES`], or at
/// [`MAX_TERM_RUNES`] codepoints. Reproduced so the same terms stay out
/// of the dictionary as out of that trie, which has no such limits of
/// its own.
///
/// Codepoints are counted on the raw term, as `strToRunes` does, not on
/// the case-folded form stored — folding can change the count.
///
/// Only the mutating entry points need this: a lookup or removal of an
/// ineligible term reports it absent anyway, no insert having accepted
/// it.
///
/// # Safety
///
/// Same as [`term_arg`].
///
/// # Panics
///
/// Panics if the bytes are not valid UTF-8.
unsafe fn storable_term<'a>(ptr: *const c_char, len: usize, what: &'static str) -> Option<&'a str> {
    // SAFETY: forwarded from this function's own contract.
    let term = unsafe { term_arg(ptr, len, what) };

    let storable =
        !term.is_empty() && term.len() <= MAX_TERM_BYTES && term.chars().count() < MAX_TERM_RUNES;

    storable.then_some(term)
}

/// Box a term iterator into the C-facing [`TermDictionaryIterator`].
///
/// Each term is lent out of the iterator's own traversal buffer, so
/// walking a dictionary allocates nothing per term.
fn wrap_iter<'td>(
    iter: impl LendingStrIter<'td, Data = TermEntry> + 'td,
) -> *mut TermDictionaryIterator<'td> {
    Box::into_raw(Box::new(TermDictionaryIterator {
        iter: Box::new(iter),
    }))
}

/// Create a new, empty [`TermDictionary`].
///
/// Free it with [`TermDictionary_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewTermDictionary() -> *mut TermDictionary {
    Box::into_raw(Box::new(TermDictionary::new()))
}

/// Free a [`TermDictionary`] and all terms it owns.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - No other access to `t` may occur concurrently with this call —
///   neither another mutator nor a read-only call such as
///   [`TermDictionary_Len`], and no iterator obtained from `t` may be
///   alive.
/// - `t` must not be used after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_Free(t: *mut TermDictionary) {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer
    // obtained from `NewTermDictionary`, with no outstanding iterators.
    drop(unsafe { Box::from_raw(t) });
}

/// The number of unique terms stored in the dictionary.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_Len(t: *const TermDictionary) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary.
    let dict = unsafe { &*t };
    dict.len()
}

/// Estimated heap memory currently held by the dictionary, in bytes.
/// See [`TermDictionary::mem_usage`]. Mirrors the C `TrieType_MemUsage`
/// entry point.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_MemUsage(t: *const TermDictionary) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary.
    let dict = unsafe { &*t };
    dict.mem_usage()
}

/// ADD_INCR insert: accumulate both `score` and `num_docs` onto the
/// existing entry for `(term, len)`, or create a fresh terminal if
/// absent. The term is case-folded internally.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No other access to `t` may occur concurrently with this call —
///   neither another mutator nor a read-only call such as
///   [`TermDictionary_Len`], and no iterator obtained from `t` may be
///   alive.
///
/// # Panics
///
/// Panics if `term` is not valid UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_AddTerm(
    t: *mut TermDictionary,
    term: *const c_char,
    len: usize,
    score: f32,
    num_docs: usize,
) -> TermDictionaryInsertOutcome {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary, with no outstanding iterators.
    let dict = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { storable_term(term, len, "term") }) else {
        return TermDictionaryInsertOutcome::Unsupported;
    };
    dict.add_term(term, score, num_docs).into()
}

/// ADD_REPLACE insert: overwrite `score`, but still accumulate
/// `num_docs` onto the existing count for `(term, len)`. Creates a fresh
/// terminal if absent. The term is case-folded internally.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No other access to `t` may occur concurrently with this call —
///   neither another mutator nor a read-only call such as
///   [`TermDictionary_Len`], and no iterator obtained from `t` may be
///   alive.
///
/// # Panics
///
/// Panics if `term` is not valid UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_ReplaceTerm(
    t: *mut TermDictionary,
    term: *const c_char,
    len: usize,
    score: f32,
    num_docs: usize,
) -> TermDictionaryInsertOutcome {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary, with no outstanding iterators.
    let dict = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { storable_term(term, len, "term") }) else {
        return TermDictionaryInsertOutcome::Unsupported;
    };
    dict.replace_term(term, score, num_docs).into()
}

/// Remove the entry for `(term, len)`. Returns 1 if a term was removed,
/// 0 if it was absent. The term is case-folded internally.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No other access to `t` may occur concurrently with this call —
///   neither another mutator nor a read-only call such as
///   [`TermDictionary_Len`], and no iterator obtained from `t` may be
///   alive.
///
/// # Panics
///
/// Panics if `term` is not valid UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_Remove(
    t: *mut TermDictionary,
    term: *const c_char,
    len: usize,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary, with no outstanding iterators.
    let dict = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let term = unsafe { term_arg(term, len, "term") };
    if dict.remove(term).is_some() { 1 } else { 0 }
}

/// Look up the entry for `(term, len)`. Returns 1 and writes the entry's
/// `score`/`num_docs` into the (optional, may be NULL) out-pointers if
/// the term is present; returns 0 otherwise, leaving the out-pointers
/// untouched. The term is case-folded internally.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - `out_score` and `out_num_docs` must each be NULL or point to a
///   writable location.
/// - The out-pointers must not overlap each other or the
///   [`TermDictionary`] `t` points to: this call writes through them
///   while holding a shared reference to the entry it read.
///
/// # Panics
///
/// Panics if `term` is not valid UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_Get(
    t: *const TermDictionary,
    term: *const c_char,
    len: usize,
    out_score: *mut f32,
    out_num_docs: *mut usize,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary.
    let dict = unsafe { &*t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let term = unsafe { term_arg(term, len, "term") };
    let Some(entry) = dict.get(term) else {
        return 0;
    };

    if !out_score.is_null() {
        // SAFETY: caller is to ensure `out_score` is writable when non-null.
        unsafe { *out_score = entry.score };
    }
    if !out_num_docs.is_null() {
        // SAFETY: caller is to ensure `out_num_docs` is writable when non-null.
        unsafe { *out_num_docs = entry.num_docs };
    }
    1
}

/// Decrement the `num_docs` count for `(term, len)` by `delta`
/// (saturating — when the count reaches zero the entry is removed). The
/// term is case-folded internally.
///
/// Reports [`TermDictionaryDecrResult::NotFound`] when no entry exists
/// for the term.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No other access to `t` may occur concurrently with this call —
///   neither another mutator nor a read-only call such as
///   [`TermDictionary_Len`], and no iterator obtained from `t` may be
///   alive.
///
/// # Panics
///
/// Panics if `term` is not valid UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_DecrementNumDocs(
    t: *mut TermDictionary,
    term: *const c_char,
    len: usize,
    delta: usize,
) -> TermDictionaryDecrResult {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary, with no outstanding iterators.
    let dict = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { storable_term(term, len, "term") }) else {
        return TermDictionaryDecrResult::Unsupported;
    };
    dict.decrement_num_docs(term, delta).into()
}

/// Iterate over every term in the dictionary in lexicographical order.
///
/// Invoke [`TermDictionaryIterator_Next`] to get the results.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `t` must not be modified or freed while the iterator lives.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_Iterate<'td>(
    t: *const TermDictionary,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let dict = unsafe { &*t };
    wrap_iter(dict.iter())
}

/// Iterate over every term sharing the case-folded prefix `(str, len)`,
/// in lexicographical order.
///
/// Invoke [`TermDictionaryIterator_Next`] to get the results.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `str` must point to a valid byte sequence of length `len`.
/// - `t` must not be modified or freed while the iterator lives.
///
/// # Panics
///
/// Panics if the prefix is not valid UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IteratePrefix<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let dict = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes.
    let prefix = unsafe { term_arg(str, len, "prefix") };
    wrap_iter(dict.prefixed_iter(prefix))
}

/// Iterate over every term ending with the case-folded suffix
/// `(str, len)`, in lexicographical order. An empty suffix yields no
/// terms.
///
/// Invoke [`TermDictionaryIterator_Next`] to get the results.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `str` must point to a valid byte sequence of length `len`.
/// - `t` must not be modified or freed while the iterator lives.
///
/// # Panics
///
/// Panics if the suffix is not valid UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IterateSuffix<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let dict = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes.
    let suffix = unsafe { term_arg(str, len, "suffix") };
    wrap_iter(dict.suffixed_iter(suffix))
}

/// Iterate over every term containing the case-folded substring
/// `(str, len)`, in lexicographical order. An empty substring yields no
/// terms.
///
/// Invoke [`TermDictionaryIterator_Next`] to get the results.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `str` must point to a valid byte sequence of length `len`.
/// - `t` must not be modified or freed while the iterator lives.
/// - The substring bytes `(str, len)` must stay valid and unmodified
///   while the iterator lives — the iterator matches candidates against
///   them on every advance.
///
/// # Panics
///
/// Panics if the substring is not valid UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IterateContains<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let dict = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes that
    // outlive the iterator.
    let target = unsafe { term_arg::<'td>(str, len, "substring") };
    wrap_iter(dict.contains_iter(target))
}

/// Iterate over every term matching the case-folded wildcard pattern
/// `(str, len)` (`*` matches any run of characters, `?` exactly one), in
/// lexicographical order.
///
/// The pattern is the escaped form: the walk resolves `\` itself, so a
/// caller that unescapes beforehand collapses each sequence twice and
/// searches for the wrong term.
///
/// Invoke [`TermDictionaryIterator_Next`] to get the results.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `str` must point to a valid byte sequence of length `len`.
/// - `t` must not be modified or freed while the iterator lives.
///
/// # Panics
///
/// Panics if the pattern is not valid UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IterateWildcard<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let dict = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes that
    // outlive the iterator.
    let pattern = unsafe { term_arg::<'td>(str, len, "pattern") };
    wrap_iter(dict.wildcard_iter(pattern))
}

/// Iterate over every term whose case-folded form is within Levenshtein
/// edit distance `max_dist` (in codepoints) of the case-folded pattern
/// `(str, len)`, in lexicographical order.
///
/// Invoke [`TermDictionaryIterator_Next`] to get the results.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `str` must point to a valid byte sequence of length `len`.
/// - `t` must not be modified or freed while the iterator lives.
///
/// # Panics
///
/// Panics if the pattern is not valid UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IterateFuzzy<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
    max_dist: u32,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let dict = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes.
    let pattern = unsafe { term_arg::<'td>(str, len, "pattern") };
    wrap_iter(dict.fuzzy_iter(pattern, max_dist))
}

/// Advance the iterator. Returns 1 and stores the next term and its
/// payload into the out-pointers if there is one, or returns 0 once
/// exhausted.
///
/// The term written to `*term` is NOT NUL-terminated, owned by the
/// iterator, and only valid until the next call to
/// [`TermDictionaryIterator_Next`] or [`TermDictionaryIterator_Free`].
/// `score` and `num_docs` are optional (may be NULL).
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`TermDictionaryIterator`] obtained from
///   one of the `TermDictionary_Iterate*` functions and cannot be NULL.
/// - `term` and `len` must be valid, non-NULL pointers to writable
///   locations; `score` and `num_docs` must each be NULL or point to a
///   writable location.
/// - The out-pointers must not overlap each other, the
///   [`TermDictionaryIterator`] `it` points to, or the buffer that iterator
///   lends the term from: this call writes through them while holding
///   exclusive access to the iterator.
/// - The [`TermDictionary`] the iterator was obtained from must still be
///   alive and unmodified since the iterator was created.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionaryIterator_Next(
    it: *mut TermDictionaryIterator,
    term: *mut *const c_char,
    len: *mut usize,
    score: *mut f32,
    num_docs: *mut usize,
) -> c_int {
    debug_assert!(!it.is_null(), "it cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");
    debug_assert!(!len.is_null(), "len cannot be NULL");

    // SAFETY: caller is to ensure `it` is a valid, non-null pointer to a
    // live TermDictionaryIterator.
    let iterator = unsafe { &mut *it };

    let Some((next_term, entry)) = iterator.iter.next_borrowed() else {
        return 0;
    };

    // SAFETY: caller is to ensure `term` points to a writable location.
    unsafe { *term = next_term.as_ptr().cast::<c_char>() };
    // SAFETY: caller is to ensure `len` points to a writable location.
    unsafe { *len = next_term.len() };
    if !score.is_null() {
        // SAFETY: caller is to ensure `score` is writable when non-null.
        unsafe { *score = entry.score };
    }
    if !num_docs.is_null() {
        // SAFETY: caller is to ensure `num_docs` is writable when non-null.
        unsafe { *num_docs = entry.num_docs };
    }
    1
}

/// Free an iterator obtained from one of the `TermDictionary_Iterate*`
/// functions. Invalidates any term pointer previously returned by
/// [`TermDictionaryIterator_Next`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`TermDictionaryIterator`] obtained from
///   one of the `TermDictionary_Iterate*` functions and cannot be NULL.
/// - `it` must not be used after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionaryIterator_Free(it: *mut TermDictionaryIterator) {
    debug_assert!(!it.is_null(), "it cannot be NULL");

    // SAFETY: caller is to ensure `it` is a valid, non-null pointer
    // obtained from a `TermDictionary_Iterate*` function.
    drop(unsafe { Box::from_raw(it) });
}
