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
//! All string parameters are byte pointers with an explicit length.
//! Terms are tokenizer output and patterns come from the query parser,
//! so they are normally valid UTF-8; anything else is rejected rather
//! than treated as a caller error — see [`term_arg`]. The dictionary
//! case-folds every key and pattern internally (see the
//! [`term_dictionary`] crate docs), so callers pass the raw term as-is.

#![allow(non_camel_case_types, non_snake_case)]

use std::ffi::{c_char, c_int, c_void};
use std::slice;

use term_dictionary::{
    DecrResult as DecrResultImpl, InsertOutcome as InsertOutcomeImpl, TermEntry,
};

/// Opaque to C; obtained from [`NewTermDictionary`] and freed with
/// [`TermDictionary_Free`]. Re-exported rather than wrapped so Rust
/// callers holding the spec's opaque pointer can recover the dictionary
/// by casting, depending only on the pure crate.
pub use term_dictionary::TermDictionary;

/// Stop predicate polled while a pattern walk traverses the dictionary.
///
/// `ctx` is the `stop_ctx` passed to the iterate function. Return `true`
/// to abandon the walk (e.g. once a deadline has passed); the caller owns
/// the decision and any clock it consults. A NULL predicate never stops.
/// The [`term_dictionary`] crate docs state how often it is polled.
pub type TermDictionaryShouldStop = Option<unsafe extern "C" fn(ctx: *mut c_void) -> bool>;

/// Yields the matching terms (and their payloads) of an iteration over
/// a [`TermDictionary`].
///
/// Opaque to C; obtained from one of the `TermDictionary_Iterate*`
/// functions, advanced with [`TermDictionaryIterator_Next`], and freed
/// with [`TermDictionaryIterator_Free`].
pub struct TermDictionaryIterator<'td> {
    iter: Box<dyn Iterator<Item = (String, f32, usize)> + 'td>,
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
    /// The term is not valid UTF-8, so the dictionary cannot hold it and
    /// nothing was stored. Distinct from [`New`](Self::New) so that a
    /// caller tracking distinct-term statistics does not count a term the
    /// dictionary never accepted.
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
    /// The term is not valid UTF-8, so the dictionary never held it. A
    /// no-op rather than a miss: unlike [`NotFound`](Self::NotFound) it
    /// says nothing about the add and delete counts having diverged.
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
/// empty string regardless of `ptr`. Returns `None` when the bytes are
/// not valid UTF-8.
///
/// `None` is an expected input class, not a caller error: a TEXT field
/// holds arbitrary bytes, and the tokenizer hands the indexer whatever
/// it finds there. Every entry point turns it into a no-op, reporting it
/// through [`TermDictionaryInsertOutcome::Unsupported`] or
/// [`TermDictionaryDecrResult::Unsupported`] where its outcome enum has
/// one.
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

    std::str::from_utf8(bytes).ok()
}

/// Box a term iterator into the C-facing [`TermDictionaryIterator`],
/// mapping each `(term, entry)` pair to owned `(term, score, num_docs)`
/// so no borrow escapes across the FFI boundary.
fn wrap_iter<'td>(
    iter: impl Iterator<Item = (String, &'td TermEntry)> + 'td,
) -> *mut TermDictionaryIterator<'td> {
    let iter = iter.map(|(term, entry)| (term, entry.score, entry.num_docs));
    Box::into_raw(Box::new(TermDictionaryIterator {
        iter: Box::new(iter),
        current: None,
    }))
}

/// Adapt a C stop predicate into the Rust closure the pattern walks take.
/// A NULL predicate never stops.
///
/// # Safety
///
/// A non-NULL `should_stop` must be safe to call with `stop_ctx` for as
/// long as the returned closure is held, i.e. for the iterator's lifetime.
fn stop_predicate(
    should_stop: TermDictionaryShouldStop,
    stop_ctx: *mut c_void,
) -> impl FnMut() -> bool {
    // SAFETY: the caller of the iterate function guarantees `should_stop`
    // is safe to call with `stop_ctx` while the iterator lives.
    move || should_stop.is_some_and(|f| unsafe { f(stop_ctx) })
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
/// A non-UTF-8 term is a no-op and reports
/// [`TermDictionaryInsertOutcome::Unsupported`].
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
    let dict = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { term_arg(term, len) }) else {
        return TermDictionaryInsertOutcome::Unsupported;
    };
    dict.add_term(term, score, num_docs).into()
}

/// ADD_REPLACE insert: overwrite `score`, but still accumulate
/// `num_docs` onto the existing count for `(term, len)`. Creates a fresh
/// terminal if absent. The term is case-folded internally.
///
/// A non-UTF-8 term is a no-op and reports
/// [`TermDictionaryInsertOutcome::Unsupported`].
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
    let dict = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { term_arg(term, len) }) else {
        return TermDictionaryInsertOutcome::Unsupported;
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
/// [`TermDictionaryInsertOutcome::Unsupported`].
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
    let dict = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { term_arg(term, len) }) else {
        return TermDictionaryInsertOutcome::Unsupported;
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
    let dict = unsafe { &mut *t };
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
    let dict = unsafe { &*t };
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
/// for the term, and [`TermDictionaryDecrResult::Unsupported`] when it
/// is not valid UTF-8 and so was never stored.
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
    let dict = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let Some(term) = (unsafe { term_arg(term, len) }) else {
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
/// - If `should_stop` is non-NULL it must be safe to call with `stop_ctx`
///   for as long as the iterator lives.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IteratePrefix<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
    should_stop: TermDictionaryShouldStop,
    stop_ctx: *mut c_void,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let dict = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes.
    let Some(prefix) = (unsafe { term_arg(str, len) }) else {
        return wrap_iter(std::iter::empty());
    };
    wrap_iter(dict.prefixed_iter(prefix, stop_predicate(should_stop, stop_ctx)))
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
/// - If `should_stop` is non-NULL it must be safe to call with `stop_ctx`
///   for as long as the iterator lives.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IterateSuffix<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
    should_stop: TermDictionaryShouldStop,
    stop_ctx: *mut c_void,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let dict = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes.
    let Some(suffix) = (unsafe { term_arg(str, len) }) else {
        return wrap_iter(std::iter::empty());
    };
    wrap_iter(dict.suffixed_iter(suffix, stop_predicate(should_stop, stop_ctx)))
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
/// - If `should_stop` is non-NULL it must be safe to call with `stop_ctx`
///   for as long as the iterator lives.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IterateContains<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
    should_stop: TermDictionaryShouldStop,
    stop_ctx: *mut c_void,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let dict = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes that
    // outlive the iterator.
    let Some(target) = (unsafe { term_arg::<'td>(str, len) }) else {
        return wrap_iter(std::iter::empty());
    };
    wrap_iter(dict.contains_iter(target, stop_predicate(should_stop, stop_ctx)))
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
/// - If `should_stop` is non-NULL it must be safe to call with `stop_ctx`
///   for as long as the iterator lives.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDictionary_IterateWildcard<'td>(
    t: *const TermDictionary,
    str: *const c_char,
    len: usize,
    should_stop: TermDictionaryShouldStop,
    stop_ctx: *mut c_void,
) -> *mut TermDictionaryIterator<'td> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermDictionary that outlives the iterator.
    let dict = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes that
    // outlive the iterator.
    let Some(pattern) = (unsafe { term_arg::<'td>(str, len) }) else {
        return wrap_iter(std::iter::empty());
    };
    wrap_iter(dict.wildcard_iter(pattern, stop_predicate(should_stop, stop_ctx)))
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
    let Some(pattern) = (unsafe { term_arg::<'td>(str, len) }) else {
        return wrap_iter(std::iter::empty());
    };
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

    let Some((next_term, next_score, next_num_docs)) = iterator.iter.next() else {
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
