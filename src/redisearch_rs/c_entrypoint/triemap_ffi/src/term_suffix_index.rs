/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for [`trie_rs::term_suffix_index::TermSuffixIndex`],
//! the Rust replacement for the C suffix tries behind `WITHSUFFIXTRIE`
//! (`addSuffixTrie`/`deleteSuffixTrie` and friends in `suffix.c`).
//!
//! All string parameters are byte pointers with an explicit length and
//! must be valid UTF-8: members are tokenizer output (already
//! case-folded UTF-8) and needles are produced by the query parser
//! from UTF-8 input. Invalid UTF-8 is rejected — mutations become
//! no-ops and lookups yield no matches — and trips a debug assertion.

use std::ffi::{c_char, c_int};
use std::rc::Rc;

use trie_rs::term_suffix_index::TermSuffixIndex as TermSuffixIndexImpl;

/// A set of indexed terms supporting substring (`*foo*`), ends-with
/// (`*foo`) and exact lookups, used to accelerate contains/suffix/
/// wildcard queries on `WITHSUFFIXTRIE` fields.
///
/// Opaque to C; obtained from [`NewTermSuffixIndex`] and freed with
/// [`TermSuffixIndex_Free`].
pub struct TermSuffixIndex(TermSuffixIndexImpl);

/// Yields the terms (or keys, for [`TermSuffixIndex_IterateAll`])
/// matched by an iteration over a [`TermSuffixIndex`].
///
/// Opaque to C; obtained from one of the `TermSuffixIndex_Iterate*`
/// functions, advanced with [`TermSuffixIndexIterator_Next`], and
/// freed with [`TermSuffixIndexIterator_Free`].
pub struct TermSuffixIndexIterator<'si> {
    iter: Box<dyn Iterator<Item = Rc<str>> + 'si>,
    /// Keeps the most recently yielded string alive so the pointer
    /// handed to C stays valid until the next advance (or free).
    current: Option<Rc<str>>,
}

/// Create a new, empty [`TermSuffixIndex`].
///
/// Free it with [`TermSuffixIndex_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewTermSuffixIndex() -> *mut TermSuffixIndex {
    Box::into_raw(Box::new(TermSuffixIndex(TermSuffixIndexImpl::new())))
}

/// Free a [`TermSuffixIndex`] and all terms it owns.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermSuffixIndex`] obtained from
///   [`NewTermSuffixIndex`] and cannot be NULL.
/// - No iterator obtained from `t` may be alive.
/// - `t` must not be used after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_Free(t: *mut TermSuffixIndex) {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer
    // obtained from `NewTermSuffixIndex`, with no outstanding iterators.
    drop(unsafe { Box::from_raw(t) });
}

/// Add `term` (a UTF-8 string of `len` bytes) to the index. Adding a
/// term that is already a member is a no-op, as is adding an empty or
/// non-UTF-8 term.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermSuffixIndex`] obtained from
///   [`NewTermSuffixIndex`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No iterator obtained from `t` may be alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_Add(
    t: *mut TermSuffixIndex,
    term: *const c_char,
    len: usize,
) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermSuffixIndex, with no outstanding iterators borrowing it.
    let TermSuffixIndex(index) = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), len) };

    let Ok(term) = std::str::from_utf8(bytes) else {
        debug_assert!(false, "term must be valid UTF-8");
        return;
    };
    index.add(term);
}

/// Remove `term` (a UTF-8 string of `len` bytes) from the index.
/// Removing a term that is not a member is a no-op, as is removing an
/// empty or non-UTF-8 term.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermSuffixIndex`] obtained from
///   [`NewTermSuffixIndex`] and cannot be NULL.
/// - `term` must point to a valid byte sequence of length `len`.
/// - No iterator obtained from `t` may be alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_Remove(
    t: *mut TermSuffixIndex,
    term: *const c_char,
    len: usize,
) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermSuffixIndex, with no outstanding iterators borrowing it.
    let TermSuffixIndex(index) = unsafe { &mut *t };
    // SAFETY: caller is to ensure `term` points to `len` valid bytes.
    let bytes = unsafe { std::slice::from_raw_parts(term.cast::<u8>(), len) };

    let Ok(term) = std::str::from_utf8(bytes) else {
        debug_assert!(false, "term must be valid UTF-8");
        return;
    };
    index.remove(term);
}

/// Estimated heap memory currently held by the index, in bytes.
/// Counts the trie structure only; term buffers are excluded, matching
/// the node-count estimate of the C `TrieType_MemUsage`.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermSuffixIndex`] obtained from
///   [`NewTermSuffixIndex`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_MemUsage(t: *const TermSuffixIndex) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermSuffixIndex.
    let TermSuffixIndex(index) = unsafe { &*t };
    index.mem_usage()
}

/// Iterate over every member term containing the UTF-8 needle
/// `(str, len)` as a substring. A term may be yielded more than once.
///
/// An empty or non-UTF-8 needle yields no matches. A needle shorter
/// than `MIN_SUFFIX` codepoints silently yields a subset of the
/// matching terms; callers must enforce the minimum query length
/// upstream (the query engine's `minTermPrefix` gate).
///
/// Invoke [`TermSuffixIndexIterator_Next`] to get the results from the
/// iteration.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermSuffixIndex`] obtained from
///   [`NewTermSuffixIndex`] and cannot be NULL.
/// - `str` must point to a valid byte sequence of length `len`.
/// - `t` must not be modified or freed while the iterator lives.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_IterateContains<'si>(
    t: *const TermSuffixIndex,
    str: *const c_char,
    len: usize,
) -> *mut TermSuffixIndexIterator<'si> {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    debug_assert!(!str.is_null(), "str cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermSuffixIndex that outlives the iterator.
    let TermSuffixIndex(index) = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes.
    let bytes = unsafe { std::slice::from_raw_parts(str.cast::<u8>(), len) };

    let iter: Box<dyn Iterator<Item = Rc<str>>> = match std::str::from_utf8(bytes) {
        Ok(needle) => Box::new(index.iter_contains(needle)),
        Err(_) => {
            debug_assert!(false, "needle must be valid UTF-8");
            Box::new(std::iter::empty())
        }
    };
    Box::into_raw(Box::new(TermSuffixIndexIterator {
        iter,
        current: None,
    }))
}

/// Iterate over every member term ending with the UTF-8 needle
/// `(str, len)`. Each matching term is yielded exactly once.
///
/// An empty or non-UTF-8 needle yields no matches. A needle shorter
/// than `MIN_SUFFIX` codepoints silently yields a subset of the
/// matching terms; callers must enforce the minimum query length
/// upstream (the query engine's `minTermPrefix` gate).
///
/// Invoke [`TermSuffixIndexIterator_Next`] to get the results from the
/// iteration.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermSuffixIndex`] obtained from
///   [`NewTermSuffixIndex`] and cannot be NULL.
/// - `str` must point to a valid byte sequence of length `len`.
/// - `t` must not be modified or freed while the iterator lives.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_IterateSuffix<'si>(
    t: *const TermSuffixIndex,
    str: *const c_char,
    len: usize,
) -> *mut TermSuffixIndexIterator<'si> {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    debug_assert!(!str.is_null(), "str cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermSuffixIndex that outlives the iterator.
    let TermSuffixIndex(index) = unsafe { &*t };
    // SAFETY: caller is to ensure `str` points to `len` valid bytes.
    let bytes = unsafe { std::slice::from_raw_parts(str.cast::<u8>(), len) };

    let iter: Box<dyn Iterator<Item = Rc<str>>> = match std::str::from_utf8(bytes) {
        Ok(needle) => Box::new(index.iter_suffix(needle)),
        Err(_) => {
            debug_assert!(false, "needle must be valid UTF-8");
            Box::new(std::iter::empty())
        }
    };
    Box::into_raw(Box::new(TermSuffixIndexIterator {
        iter,
        current: None,
    }))
}

/// Iterate over every key stored in the index — each member term plus
/// every indexed proper suffix — in lexicographical order.
/// Introspection aid for `FT.DEBUG DUMP_SUFFIX_TRIE`.
///
/// Invoke [`TermSuffixIndexIterator_Next`] to get the results from the
/// iteration.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`TermSuffixIndex`] obtained from
///   [`NewTermSuffixIndex`] and cannot be NULL.
/// - `t` must not be modified or freed while the iterator lives.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_IterateAll<'si>(
    t: *const TermSuffixIndex,
) -> *mut TermSuffixIndexIterator<'si> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: caller is to ensure `t` is a valid, non-null pointer to a
    // TermSuffixIndex that outlives the iterator.
    let TermSuffixIndex(index) = unsafe { &*t };

    let iter = Box::new(index.keys().map(Rc::from));
    Box::into_raw(Box::new(TermSuffixIndexIterator {
        iter,
        current: None,
    }))
}

/// Advance the iterator. Returns 1 and stores the next string into
/// `(*str, *len)` if there is one, or returns 0 once exhausted.
///
/// The string written to `*str` is NOT NUL-terminated, owned by the
/// iterator, and only valid until the next call to
/// [`TermSuffixIndexIterator_Next`] or [`TermSuffixIndexIterator_Free`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`TermSuffixIndexIterator`] obtained
///   from one of the `TermSuffixIndex_Iterate*` functions and cannot
///   be NULL.
/// - `str` and `len` must be valid, non-NULL pointers to writable
///   locations.
/// - The [`TermSuffixIndex`] the iterator was obtained from must still
///   be alive and unmodified since the iterator was created.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndexIterator_Next(
    it: *mut TermSuffixIndexIterator,
    str: *mut *const c_char,
    len: *mut usize,
) -> c_int {
    debug_assert!(!it.is_null(), "it cannot be NULL");
    debug_assert!(!str.is_null(), "str cannot be NULL");
    debug_assert!(!len.is_null(), "len cannot be NULL");

    // SAFETY: caller is to ensure `it` is a valid, non-null pointer to a
    // live TermSuffixIndexIterator.
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

/// Free an iterator obtained from one of the `TermSuffixIndex_Iterate*`
/// functions. Invalidates any string pointer previously returned by
/// [`TermSuffixIndexIterator_Next`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`TermSuffixIndexIterator`] obtained
///   from one of the `TermSuffixIndex_Iterate*` functions and cannot
///   be NULL.
/// - `it` must not be used after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndexIterator_Free(it: *mut TermSuffixIndexIterator) {
    debug_assert!(!it.is_null(), "it cannot be NULL");

    // SAFETY: caller is to ensure `it` is a valid, non-null pointer
    // obtained from a `TermSuffixIndex_Iterate*` function.
    drop(unsafe { Box::from_raw(it) });
}
