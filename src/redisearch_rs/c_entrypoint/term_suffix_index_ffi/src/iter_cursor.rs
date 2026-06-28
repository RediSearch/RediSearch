/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Cursor-style iteration over a [`TermSuffixIndex`].
//!
//! The caller obtains an opaque iterator, advances it with
//! [`TermSuffixIndexIterator_Next`], and frees it with
//! [`TermSuffixIndexIterator_Free`].

use std::ffi::{c_char, c_int};

use super::*;

/// Iterate over every key stored in the index — each member term plus
/// every indexed proper suffix — in lexicographical order.
///
/// The caller owns the returned iterator: advance it with
/// [`TermSuffixIndexIterator_Next`] and free it exactly once with
/// [`TermSuffixIndexIterator_Free`] when done (including when it is
/// exhausted or abandoned early).
///
/// # Safety
///
/// 1. `tsi` must be a [valid], non-null pointer obtained from
///    [`TermSuffixIndex_New`].
/// 2. `tsi` must not be modified or freed while the iterator lives.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndex_IterateAll<'si>(
    tsi: *const TermSuffixIndex,
) -> *mut TermSuffixIndexIterator<'si> {
    debug_assert!(!tsi.is_null(), "tsi cannot be NULL");

    // Safety: ensured by caller (1., 2.)
    let index = unsafe { &*tsi };

    let iter = Box::new(index.keys());
    Box::into_raw(Box::new(TermSuffixIndexIterator {
        iter,
        current: None,
    }))
}

/// Advance the iterator. Returns 1 and stores the next string into
/// `(*str, *len)` if there is one, or returns 0 once exhausted.
///
/// Returning 0 does not free the iterator; it must still be released
/// with [`TermSuffixIndexIterator_Free`].
///
/// The string written to `*str` is NOT NUL-terminated, owned by the
/// iterator, and only valid until the next call to
/// [`TermSuffixIndexIterator_Next`] or [`TermSuffixIndexIterator_Free`].
///
/// # Safety
///
/// 1. `it` must be a [valid], non-null pointer to a live
///    [`TermSuffixIndexIterator`].
/// 2. `str` and `len` must be [valid], non-null pointers to writable
///    locations.
/// 3. The [`TermSuffixIndex`] the iterator was obtained from must still
///    be alive and unmodified.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndexIterator_Next(
    it: *mut TermSuffixIndexIterator,
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

/// Free an iterator obtained from [`TermSuffixIndex_IterateAll`].
/// Invalidates any string pointer previously returned by
/// [`TermSuffixIndexIterator_Next`].
///
/// # Safety
///
/// 1. `it` must be a [valid], non-null pointer to a live
///    [`TermSuffixIndexIterator`].
/// 2. `it` must not be used after this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermSuffixIndexIterator_Free(it: *mut TermSuffixIndexIterator) {
    debug_assert!(!it.is_null(), "it cannot be NULL");

    // Safety: ensured by caller (1., 2.)
    drop(unsafe { Box::from_raw(it) });
}

/// Yields the keys of a [`TermSuffixIndex`].
///
/// Obtained from [`TermSuffixIndex_IterateAll`], advanced
/// with [`TermSuffixIndexIterator_Next`], freed with
/// [`TermSuffixIndexIterator_Free`].
pub struct TermSuffixIndexIterator<'si> {
    iter: Box<dyn Iterator<Item = String> + 'si>,
    /// Keeps the most recently yielded string alive so the pointer
    /// stays valid until the next advance (or free).
    current: Option<String>,
}
