/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for [`term_dict::TermDictionary`], the Rust
//! replacement for the C terms trie behind `sp->terms` (the
//! `NewTrie`/`Trie_InsertStringBuffer`/`Trie_GetNode`/`Trie_Iterate`
//! family in `trie.c`).
//!
//! All string parameters are byte pointers with an explicit length and
//! must be valid UTF-8: terms are tokenizer output and patterns are
//! produced by the query parser, both from UTF-8 input. The dictionary
//! case-folds every key and pattern internally (see the crate docs), so
//! callers pass the raw term as-is. Invalid UTF-8 is rejected —
//! mutations become no-ops, lookups report "not found", and iterator
//! constructors yield nothing — and trips a debug assertion.

use std::ffi::{c_char, c_int};
use std::slice;

use term_dict::{
    DecrResult as DecrResultImpl, InsertOutcome as InsertOutcomeImpl,
    TermDictionary as TermDictionaryImpl, TermEntry,
};

/// Term dictionary mapping each indexed term to its score and the
/// number of documents containing it. Backs the FT.SEARCH terms trie
/// (`sp->terms`) used for GC, exact lookups, and prefix/fuzzy/wildcard
/// query expansion.
///
/// Opaque to C; obtained from [`NewTermDictionary`] and freed with
/// [`TermDictionary_Free`].
pub struct TermDictionary(TermDictionaryImpl);

/// Yields the matching terms (and their payloads) of an iteration over
/// a [`TermDictionary`].
///
/// Opaque to C; obtained from one of the `TermDictionary_Iterate*`
/// functions, advanced with [`TermDictionaryIterator_Next`], and freed
/// with [`TermDictionaryIterator_Free`].
pub struct TermDictionaryIterator<'td> {
    iter: Box<dyn Iterator<Item = (String, f32, usize, u32)> + 'td>,
    /// Keeps the most recently yielded term alive so the pointer handed
    /// to C stays valid until the next advance (or free).
    current: Option<String>,
}

/// Outcome of [`TermDictionary_AddTerm`], [`TermDictionary_ReplaceTerm`]
/// and [`TermDictionary_Insert`].
#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
#[cheadergen::config(prefix_with_name)]
pub enum TermDictionaryInsertOutcome {
    /// No prior entry existed; a new terminal was created.
    New = 0,
    /// An existing entry was modified in place.
    Updated = 1,
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
/// empty string regardless of `ptr`. Returns `None` (and trips a debug
/// assertion) when the bytes are not valid UTF-8.
///
/// # Safety
///
/// When `len > 0`, `ptr` must point to `len` bytes that stay valid and
/// unmodified for the chosen lifetime `'a`.
unsafe fn term_arg<'a>(ptr: *const c_char, len: usize) -> Option<&'a str> {
    let bytes = if len == 0 {
        &[]
    } else {
        debug_assert!(!ptr.is_null(), "ptr cannot be NULL when len > 0");
        // SAFETY: caller guarantees `ptr` points to `len` valid bytes.
        unsafe { slice::from_raw_parts(ptr.cast::<u8>(), len) }
    };

    match std::str::from_utf8(bytes) {
        Ok(s) => Some(s),
        Err(_) => {
            debug_assert!(false, "term must be valid UTF-8");
            None
        }
    }
}

/// Box a term iterator into the C-facing [`TermDictionaryIterator`],
/// mapping each `(term, entry)` pair to owned `(term, score, num_docs,
/// dist)` so no borrow escapes across the FFI boundary.
fn wrap_iter<'td>(
    iter: impl Iterator<Item = (String, &'td TermEntry)> + 'td,
) -> *mut TermDictionaryIterator<'td> {
    let iter = iter.map(|(term, entry)| (term, entry.score, entry.num_docs, 0));
    Box::into_raw(Box::new(TermDictionaryIterator {
        iter: Box::new(iter),
        current: None,
    }))
}

/// Create a new, empty [`TermDictionary`].
///
/// Free it with [`TermDictionary_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewTermDictionary() -> *mut TermDictionary {
    Box::into_raw(Box::new(TermDictionary(TermDictionaryImpl::new())))
}

/// Free a [`TermDictionary`] and all terms it owns.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - No iterator obtained from `t` may be alive.
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
    let TermDictionary(dict) = unsafe { &*t };
    dict.len()
}

/// Estimated heap memory currently held by the dictionary, in bytes.
/// Counts the trie node and key storage, matching the role of the C
/// `TrieType_MemUsage`.
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
    let TermDictionary(dict) = unsafe { &*t };
    dict.mem_usage()
}

/// ADD_INCR insert: accumulate both `score` and `num_docs` onto the
/// existing entry for `(term, len)`, or create a fresh terminal if
/// absent. The term is case-folded internally.
///
/// A non-UTF-8 term is a no-op and reports
/// [`TermDictionaryInsertOutcome::New`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No iterator obtained from `t` may be alive.
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
    let TermDictionary(dict) = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { term_arg(term, len) }) else {
        return TermDictionaryInsertOutcome::New;
    };
    dict.add_term(term, score, num_docs).into()
}

/// ADD_REPLACE insert: overwrite `score`, but still accumulate
/// `num_docs` onto the existing count for `(term, len)`. Creates a fresh
/// terminal if absent. The term is case-folded internally.
///
/// A non-UTF-8 term is a no-op and reports
/// [`TermDictionaryInsertOutcome::New`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No iterator obtained from `t` may be alive.
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
    let TermDictionary(dict) = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { term_arg(term, len) }) else {
        return TermDictionaryInsertOutcome::New;
    };
    dict.replace_term(term, score, num_docs).into()
}

/// Primitive overwrite: install `(score, num_docs)` for `(term, len)`,
/// replacing any prior entry without accumulating. Intended for bulk
/// seeding; production indexing should use [`TermDictionary_AddTerm`] /
/// [`TermDictionary_ReplaceTerm`]. The term is case-folded internally.
///
/// Reports [`TermDictionaryInsertOutcome::Updated`] when a prior entry
/// was overwritten, [`TermDictionaryInsertOutcome::New`] otherwise. A
/// non-UTF-8 term is a no-op and reports
/// [`TermDictionaryInsertOutcome::New`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No iterator obtained from `t` may be alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_Insert(
    t: *mut TermDictionary,
    term: *const c_char,
    len: usize,
    score: f32,
    num_docs: usize,
) -> TermDictionaryInsertOutcome {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary, with no outstanding iterators.
    let TermDictionary(dict) = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { term_arg(term, len) }) else {
        return TermDictionaryInsertOutcome::New;
    };
    match dict.insert(term, TermEntry { score, num_docs }) {
        Some(_) => TermDictionaryInsertOutcome::Updated,
        None => TermDictionaryInsertOutcome::New,
    }
}

/// Remove the entry for `(term, len)`. Returns 1 if a term was removed,
/// 0 if it was absent or not valid UTF-8. The term is case-folded
/// internally.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No iterator obtained from `t` may be alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_Remove(
    t: *mut TermDictionary,
    term: *const c_char,
    len: usize,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary, with no outstanding iterators.
    let TermDictionary(dict) = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { term_arg(term, len) }) else {
        return 0;
    };
    if dict.remove(term).is_some() { 1 } else { 0 }
}

/// Look up the entry for `(term, len)`. Returns 1 and writes the entry's
/// `score`/`num_docs` into the (optional, may be NULL) out-pointers if
/// the term is present; returns 0 otherwise (absent or not valid UTF-8),
/// leaving the out-pointers untouched. The term is case-folded
/// internally.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - `out_score` and `out_num_docs` must each be NULL or point to a
///   writable location.
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
    let TermDictionary(dict) = unsafe { &*t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { term_arg(term, len) }) else {
        return 0;
    };
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
/// for the term or it is not valid UTF-8.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No iterator obtained from `t` may be alive.
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
    let TermDictionary(dict) = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { term_arg(term, len) }) else {
        return TermDictionaryDecrResult::NotFound;
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
    let TermDictionary(dict) = unsafe { &*t };
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IteratePrefix<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let TermDictionary(dict) = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes.
    let Some(prefix) = (unsafe { term_arg(str, len) }) else {
        return wrap_iter(std::iter::empty());
    };
    wrap_iter(dict.prefixed_iter(prefix))
}

/// Iterate over every term ending with the case-folded suffix
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IterateSuffix<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let TermDictionary(dict) = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes.
    let Some(suffix) = (unsafe { term_arg(str, len) }) else {
        return wrap_iter(std::iter::empty());
    };
    wrap_iter(dict.suffixed_iter(suffix))
}

/// Iterate over every term containing the case-folded substring
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
/// - The substring bytes `(str, len)` must stay valid and unmodified
///   while the iterator lives — the iterator matches candidates against
///   them on every advance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IterateContains<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let TermDictionary(dict) = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes that
    // outlive the iterator.
    let Some(target) = (unsafe { term_arg::<'td>(str, len) }) else {
        return wrap_iter(std::iter::empty());
    };
    wrap_iter(dict.contains_iter(target))
}

/// Iterate over every term matching the case-folded wildcard pattern
/// `(str, len)` (`*` matches any run of characters, `?` exactly one), in
/// lexicographical order.
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
/// - The pattern bytes `(str, len)` must stay valid and unmodified while
///   the iterator lives — the iterator matches candidates against them
///   on every advance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IterateWildcard<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let TermDictionary(dict) = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes that
    // outlive the iterator.
    let Some(pattern) = (unsafe { term_arg::<'td>(str, len) }) else {
        return wrap_iter(std::iter::empty());
    };
    wrap_iter(dict.wildcard_iter(pattern))
}

/// Iterate over every term within Levenshtein edit distance `max_dist`
/// (in codepoints) of the case-folded prefix `(str, len)`. With
/// `prefix_mode` set, a term matches when any prefix of it is within
/// `max_dist`; otherwise the whole term must be. The reported `dist`
/// out-parameter of [`TermDictionaryIterator_Next`] carries the matched
/// distance for each term.
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IterateDfa<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
    max_dist: u32,
    prefix_mode: bool,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let TermDictionary(dict) = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes.
    let Some(prefix) = (unsafe { term_arg(str, len) }) else {
        return Box::into_raw(Box::new(TermDictionaryIterator {
            iter: Box::new(std::iter::empty()),
            current: None,
        }));
    };

    let iter = dict
        .iterate_dfa(prefix, max_dist, prefix_mode)
        .map(|(term, entry, dist)| (term, entry.score, entry.num_docs, dist));
    Box::into_raw(Box::new(TermDictionaryIterator {
        iter: Box::new(iter),
        current: None,
    }))
}

/// Iterate over every term in the lexicographic range bounded by `min`
/// and `max`, in lexicographical order. Both bounds are case-folded
/// internally.
///
/// A NULL `min` pointer leaves the lower side unbounded; a NULL `max`
/// pointer leaves the upper side unbounded (a non-NULL pointer with
/// `len == 0` is the empty-string bound, which is distinct from NULL).
/// `include_min` / `include_max` choose closed vs. open bounds. This
/// mirrors the C `Trie_IterateRange` used for `FT.SEARCH` LEXRANGE
/// queries.
///
/// Invoke [`TermDictionaryIterator_Next`] to get the results.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermDictionary`] obtained from
///   [`NewTermDictionary`] and cannot be NULL.
/// - `min` must be NULL or point to a valid byte sequence of length
///   `min_len`; likewise `max` / `max_len`.
/// - `t` must not be modified or freed while the iterator lives.
/// - Any non-NULL bound bytes must stay valid and unmodified while the
///   iterator lives — the iterator compares candidates against them on
///   every advance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IterateRange<'td>(
    t: *const TermDictionary,
    min: *const c_char,
    min_len: usize,
    include_min: bool,
    max: *const c_char,
    max_len: usize,
    include_max: bool,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let TermDictionary(dict) = unsafe { &*t };

    // A NULL pointer means "no bound on this side". A non-NULL pointer
    // holding invalid UTF-8 yields an empty iterator, matching the other
    // iterator constructors.
    let min_bound = if min.is_null() {
        None
    } else {
        // SAFETY: caller is to ensure `min` points to `min_len` valid
        // bytes that outlive the iterator.
        match unsafe { term_arg::<'td>(min, min_len) } {
            Some(bound) => Some(bound),
            None => return wrap_iter(std::iter::empty()),
        }
    };
    let max_bound = if max.is_null() {
        None
    } else {
        // SAFETY: caller is to ensure `max` points to `max_len` valid
        // bytes that outlive the iterator.
        match unsafe { term_arg::<'td>(max, max_len) } {
            Some(bound) => Some(bound),
            None => return wrap_iter(std::iter::empty()),
        }
    };

    wrap_iter(dict.range_iter(min_bound, include_min, max_bound, include_max))
}

/// Advance the iterator. Returns 1 and stores the next term and its
/// payload into the out-pointers if there is one, or returns 0 once
/// exhausted.
///
/// The term written to `*term` is NOT NUL-terminated, owned by the
/// iterator, and only valid until the next call to
/// [`TermDictionaryIterator_Next`] or [`TermDictionaryIterator_Free`].
/// `score`, `num_docs` and `dist` are optional (may be NULL); `dist` is
/// the matched edit distance for iterators created by
/// [`TermDictionary_IterateDfa`] and `0` for all other iterators.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`TermDictionaryIterator`] obtained from
///   one of the `TermDictionary_Iterate*` functions and cannot be NULL.
/// - `term` and `len` must be valid, non-NULL pointers to writable
///   locations; `score`, `num_docs` and `dist` must each be NULL or
///   point to a writable location.
/// - The [`TermDictionary`] the iterator was obtained from must still be
///   alive and unmodified since the iterator was created.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionaryIterator_Next(
    it: *mut TermDictionaryIterator,
    term: *mut *const c_char,
    len: *mut usize,
    score: *mut f32,
    num_docs: *mut usize,
    dist: *mut u32,
) -> c_int {
    debug_assert!(!it.is_null(), "it cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");
    debug_assert!(!len.is_null(), "len cannot be NULL");

    // SAFETY: caller is to ensure `it` is a valid, non-null pointer to a
    // live TermDictionaryIterator.
    let iterator = unsafe { &mut *it };

    let Some((next_term, next_score, next_num_docs, next_dist)) = iterator.iter.next() else {
        return 0;
    };
    let stored = iterator.current.insert(next_term);

    // SAFETY: caller is to ensure `term` points to a writable location.
    unsafe { *term = stored.as_ptr().cast::<c_char>() };
    // SAFETY: caller is to ensure `len` points to a writable location.
    unsafe { *len = stored.len() };
    if !score.is_null() {
        // SAFETY: caller is to ensure `score` is writable when non-null.
        unsafe { *score = next_score };
    }
    if !num_docs.is_null() {
        // SAFETY: caller is to ensure `num_docs` is writable when non-null.
        unsafe { *num_docs = next_num_docs };
    }
    if !dist.is_null() {
        // SAFETY: caller is to ensure `dist` is writable when non-null.
        unsafe { *dist = next_dist };
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
