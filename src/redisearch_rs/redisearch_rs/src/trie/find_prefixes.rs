/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::{TrieMap, tm_len_t};
use libc::c_char;
use low_memory_thin_vec::LowMemoryThinVec;
use std::ffi::c_void;

/// Find nodes that have a given prefix. Results are placed in an array.
/// The `results` buffer is initialized by this function using the Redis allocator
/// and should be freed by calling [`TrieMapResultBuf_Free`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
/// - `str` can be NULL only if `len == 0`. It is not necessarily NULL-terminated.
/// - `len` can be 0. If so, `str` is regarded as an empty string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_FindPrefixes(
    t: *mut TrieMap,
    str: *const c_char,
    len: tm_len_t,
) -> TrieMapResultBuf {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: The safety requirements of this function
    // state the caller is to ensure that the pointer `t` is
    // a valid TrieMap obtained from `NewTrieMap` and cannot be NULL.
    // If that invariant is upheld, then the following line is sound.
    let TrieMap(trie) = unsafe { &mut *t };

    let prefix: &[u8] = if len > 0 {
        debug_assert!(!str.is_null(), "str cannot be NULL if len > 0");
        // SAFETY: The safety requirements of this function
        // state the caller is to ensure that the pointer `str` is
        // a valid pointer to a string of length `len` and cannot be NULL.
        // If that invariant is upheld, then the following line is sound.
        unsafe { std::slice::from_raw_parts(str.cast(), len as usize) }
    } else {
        &[]
    };

    let iter = trie.prefixes_iter(prefix).copied();
    TrieMapResultBuf(LowMemoryThinVec::from_iter(iter))
}

/// Opaque type TrieMapResultBuf. Holds the results of [`TrieMap_FindPrefixes`].
#[repr(transparent)]
pub struct TrieMapResultBuf(pub LowMemoryThinVec<*mut c_void>);

/// Free the [`TrieMapResultBuf`] and its contents.
#[unsafe(no_mangle)]
pub extern "C" fn TrieMapResultBuf_Free(buf: TrieMapResultBuf) {
    drop(buf);
}

/// Get the data from the TrieMapResultBuf as an array of values.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `buf` must point to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMapResultBuf_Data(buf: *mut TrieMapResultBuf) -> *mut *mut c_void {
    debug_assert!(!buf.is_null(), "buf cannot be NULL");

    // SAFETY:
    // As per the safety invariants of this function:
    // - `buf` is not NULL
    // - `buf` points to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`]
    let TrieMapResultBuf(data) = unsafe { &mut *buf };
    data.as_mut_ptr()
}

/// Retrieve an element from the buffer, via a 0-initialized index.
///
/// It returns `NULL` if the index is out of bounds.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `buf` must point to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMapResultBuf_GetByIndex(
    buf: *mut TrieMapResultBuf,
    index: usize,
) -> *mut c_void {
    debug_assert!(!buf.is_null(), "buf cannot be NULL");

    // SAFETY:
    // As per the safety invariants of this function:
    // - `buf` is not NULL
    // - `buf` points to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`]
    let TrieMapResultBuf(data) = unsafe { &mut *buf };
    match data.get(index) {
        Some(element) => *element,
        None => std::ptr::null_mut(),
    }
}

/// Get the length of the TrieMapResultBuf.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `buf` must point to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMapResultBuf_Len(buf: *mut TrieMapResultBuf) -> usize {
    debug_assert!(!buf.is_null(), "buf cannot be NULL");

    // SAFETY:
    // As per the safety invariants of this function:
    // - `buf` is not NULL
    // - `buf` points to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`]
    let TrieMapResultBuf(data) = unsafe { &*buf };
    data.len()
}
