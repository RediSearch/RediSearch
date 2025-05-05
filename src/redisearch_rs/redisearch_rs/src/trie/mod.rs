/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(non_camel_case_types, non_snake_case)]

use iter_types::TrieMapIteratorImpl;
use lending_iterator::LendingIterator;
use libc::timespec;
use low_memory_thin_vec::LowMemoryThinVec;
use redis_module::raw::RedisModule_Free;
use std::{
    ffi::{c_char, c_int, c_void},
    slice,
};
use trie_rs::iter::{RangeBoundary, RangeFilter, RangeLendingIter};
use wildcard::WildcardPattern;

/// cbindgen:ignore
mod iter_types;

/// The length of a key string in the trie.
pub type tm_len_t = u16;

/// This special pointer is returned when [`TrieMap_Find`] cannot find anything.
#[unsafe(no_mangle)]
#[used]
pub static mut TRIEMAP_NOTFOUND: *mut ::std::os::raw::c_void = c"NOT FOUND".as_ptr() as *mut _;

/// Used by [`TrieMapIterator`] to determine type of query.
#[repr(C)]
#[allow(dead_code)]
#[derive(Debug)]
pub enum tm_iter_mode {
    TM_PREFIX_MODE = 0,
    TM_CONTAINS_MODE = 1,
    TM_SUFFIX_MODE = 2,
    TM_WILDCARD_MODE = 3,
}

/// Default iteration mode for [`TrieMap_Iterate`].
#[unsafe(no_mangle)]
static TM_ITER_MODE_DEFAULT: tm_iter_mode = tm_iter_mode::TM_PREFIX_MODE;

/// Opaque type TrieMap. Can be instantiated with [`NewTrieMap`].
pub struct TrieMap(trie_rs::TrieMap<*mut c_void>);

/// Callback type for passing to [`TrieMap_IterateRange`].
pub type TrieMapRangeCallback =
    Option<unsafe extern "C" fn(*const c_char, libc::size_t, *mut c_void, *mut c_void)>;

/// Opaque type TrieMapIterator. Obtained from calling [`TrieMap_Iterate`].
pub struct TrieMapIterator<'tm> {
    iter: TrieMapIteratorImpl<'tm>,
    timeout: Option<IteratorTimeoutState>,
}

struct IteratorTimeoutState {
    deadline: timespec,
    counter: u8,
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

/// Create a new [`TrieMap`]. Returns an opaque pointer to the newly created trie.
///
/// To free the trie, use [`TrieMap_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewTrieMap() -> *mut TrieMap {
    let map = Box::new(TrieMap(trie_rs::TrieMap::new()));
    Box::into_raw(map)
}

/// Callback type for passing to [`TrieMap_Delete`].
pub type freeCB = Option<unsafe extern "C" fn(*mut c_void)>;

/// Callback type for passing to [`TrieMap_Add`].
pub type TrieMapReplaceFunc =
    Option<unsafe extern "C" fn(oldval: *mut c_void, newval: *mut c_void) -> *mut c_void>;

/// Add a new string to a trie. Returns 1 if the key is new to the trie or 0 if
/// it already existed.
///
/// If `cb` is given, instead of replacing and freeing the value using `rm_free`,
/// we call the callback with the old and new value, and the function should return the value to set in the
/// node, and take care of freeing any unwanted pointers. The returned value
/// can be NULL and doesn't have to be either the old or new value.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///  - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
///  - `str` can be NULL only if `len == 0`. It is not necessarily NULL-terminated.
///  - `len` can be 0. If so, `str` is regarded as an empty string.
///  - `value` holds a pointer to the value of the record, which can be NULL
///  - `cb` must not free the value it returns
///  - The Redis allocator must be initialized before calling this function,
///    and `RedisModule_Free` must not get mutated while running this function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_Add(
    t: *mut TrieMap,
    str: *const c_char,
    len: tm_len_t,
    value: *mut c_void,
    cb: TrieMapReplaceFunc,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: The safety requirements of this function
    // require the caller to ensure that the pointer `t` is
    // a valid TrieMap obtained from `NewTrieMap` and cannot be NULL.
    // If that invariant is upheld, then the following line is sound.
    let TrieMap(trie) = unsafe { &mut *t };

    let key: &[u8] = if len > 0 {
        debug_assert!(!str.is_null(), "str cannot be NULL if len > 0");
        // SAFETY: The safety requirements of this function
        // require the caller to ensure that the pointer `str` is
        // a valid pointer to a C string, with a length of `len` bytes.
        // If that invariant is upheld, then the following line is sound.
        unsafe { slice::from_raw_parts(str.cast(), len as usize) }
    } else {
        &[]
    };

    let mut was_vacant = true;
    trie.insert_with(key, |old| {
        if let Some(old_value) = old {
            was_vacant = false;
            if let Some(cb) = cb {
                // SAFETY: The safety requirements of this function
                // require `cb` has the correct signature and does
                // not free the value it returns.
                unsafe { cb(old_value, value) }
            } else {
                // SAFETY:
                // The safety requirements of this function
                // require the caller to ensure that the Redis allocator is initialized,
                // and that `RedisModule_Free` does not get mutated while running this function.
                let rm_free = unsafe { RedisModule_Free.expect("Redis allocator not available") };
                // SAFETY:
                // The safety requirements of this function
                // require the caller to ensure that the Redis allocator is properly initialized.
                unsafe { rm_free(old_value) };
                value
            }
        } else {
            value
        }
    });

    if was_vacant { 1 } else { 0 }
}

#[unsafe(no_mangle)]
/// The number of unique keys stored in the provided triemap.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
pub unsafe extern "C" fn TrieMap_NUniqueKeys(t: *mut TrieMap) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: The safety requirements of this function
    // state the caller is to ensure that the pointer `t` is
    // a valid TrieMap obtained from `NewTrieMap` and cannot be NULL.
    // If that invariant is upheld, then the following line is sound.
    let TrieMap(trie) = unsafe { &mut *t };
    trie.n_unique_keys()
}

#[unsafe(no_mangle)]
/// The number of nodes stored in the provided triemap.
///
/// It's greater or equal to the number returned by [`TrieMap_NUniqueKeys`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
pub unsafe extern "C" fn TrieMap_NNodes(t: *mut TrieMap) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: The safety requirements of this function
    // state the caller is to ensure that the pointer `t` is
    // a valid TrieMap obtained from `NewTrieMap` and cannot be NULL.
    // If that invariant is upheld, then the following line is sound.
    let TrieMap(trie) = unsafe { &mut *t };
    trie.n_nodes()
}

/// Find the entry with a given string and length, and return its value, even if
/// that was NULL.
///
/// Returns the tree root if the key is empty.
///
/// NOTE: If the key does not exist in the trie, we return the special
/// constant value TRIEMAP_NOTFOUND, so checking if the key exists is done by
/// comparing to it, because NULL can be a valid result.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
/// - `str` can be NULL only if `len == 0`. It is not necessarily NULL-terminated.
/// - `len` can be 0. If so, `str` is regarded as an empty string.
/// - The value behind the returned pointer must not be destroyed by the caller.
///   Use [`TrieMap_Delete`] to remove it instead.
/// - In case [`TRIE_NOTFOUND`] is returned, the key does not exist in the trie,
///   and the pointer must not be dereferenced.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_Find(
    t: *mut TrieMap,
    str: *const c_char,
    len: tm_len_t,
) -> *mut c_void {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: The safety requirements of this function
    // state the caller is to ensure that the pointer `t` is
    // a valid TrieMap obtained from `NewTrieMap` and cannot be NULL.
    // If that invariant is upheld, then the following line is sound.
    let TrieMap(trie) = unsafe { &mut *t };

    let key: &[u8] = if len > 0 {
        debug_assert!(!str.is_null(), "str cannot be NULL if len > 0");
        // SAFETY: The safety requirements of this function
        // state the caller is to ensure that the pointer `str` is
        // a valid pointer to a C string, with a length of `len` bytes.
        // If that invariant is upheld, then the following line is sound.
        unsafe { slice::from_raw_parts(str.cast(), len as usize) }
    } else {
        // `str` is allowed to be NULL if len is 0,
        // but `slice::from_raw_parts` requires a non-null pointer.
        // Therefore, we use an empty slice instead.
        &[]
    };

    // Static muts are footguns, but there's no real way around them given
    // the intention to mimic the API of the original C implementation.
    #[allow(static_mut_refs)]
    // SAFETY: TRIEMAP_NOTFOUND is a pointer to a static mut `c_void`.
    // It is only referred to by this function and is not available outside this module,
    // except through the `extern void * TRIEMAP_NOTFOUND`.
    // The caller is responsible for ensuring that the returned pointer is not dereferenced
    // in case it is equal to TRIEMAP_NOTFOUND.
    let value = *trie.find(key).unwrap_or(unsafe { &TRIEMAP_NOTFOUND });

    value
}

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

/// Mark a node as deleted. It also optimizes the trie by merging nodes if
/// needed. If freeCB is given, it will be used to free the value (not the node)
/// of the deleted node. If it doesn't, we simply call free().
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
/// - `str` can be NULL only if `len == 0`. It is not necessarily NULL-terminated.
/// - `len` can be 0. If so, `str` is regarded as an empty string.
/// - if `func` is not NULL, it must be a valid function pointer of the type [`freeCB`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_Delete(
    t: *mut TrieMap,
    str: *const c_char,
    len: tm_len_t,
    func: freeCB,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: The safety requirements of this function
    // state the caller is to ensure that the pointer `t` is
    // a valid TrieMap obtained from `NewTrieMap` and cannot be NULL.
    // If that invariant is upheld, then the following line is sound.
    let TrieMap(trie) = unsafe { &mut *t };

    let key: &[u8] = if len > 0 {
        debug_assert!(!str.is_null(), "str cannot be NULL if len > 0");
        // SAFETY: The safety requirements of this function
        // state the caller is to ensure that the pointer `str` is
        // a valid pointer to a C string, with a length of `len` bytes.
        // If that invariant is upheld, then the following line is sound.
        unsafe { slice::from_raw_parts(str.cast(), len as usize) }
    } else {
        // `str` is allowed to be NULL if len is 0,
        // but `slice::from_raw_parts` requires a non-null pointer.
        // Therefore, we use an empty slice instead.
        &[]
    };

    trie.remove(key)
        .map(|old_val| {
            if let Some(f) = func {
                // SAFETY: The safety requirements of this function
                // require the caller to ensure that the pointer `func` is
                // either NULL or a valid pointer to a function of type `freeCB.
                // If that invariant is upheld, then the following line is sound.
                unsafe { f(old_val) }
            } else {
                // SAFETY:
                // The safety requirements of this function
                // require the caller to ensure that the Redis allocator is initialized,
                // and that `RedisModule_Free` does not get mutated while running this function.
                let rm_free = unsafe { RedisModule_Free.expect("Redis allocator not available") };
                // SAFETY:
                // The safety requirements of this function
                // require the caller to ensure that the Redis allocator is properly initialized.
                unsafe { rm_free(old_val) };
            }
            1
        })
        .unwrap_or(0)
}

/// Free the trie's root and all its children recursively. If freeCB is given, we
/// call it to free individual payload values (not the nodes). If not, free() is used instead.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `func` must either be NULL or a valid pointer to a function of type [`freeCB`].
/// - The Redis allocator must be initialized before calling this function,
///   and `RedisModule_Free` must not get mutated while running this function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_Free(t: *mut TrieMap, func: freeCB) {
    if t.is_null() {
        return;
    }

    // Reconstruct the original Box<TrieMap> which will take care of freeing the memory
    // upon dropping.
    // SAFETY: The safety requirements of this function
    // state the caller is to ensure that the pointer `t` is
    // a valid TrieMap obtained from `NewTrieMap` and cannot be NULL.
    // If that invariant is upheld, then the following line is sound.
    let trie = unsafe { Box::from_raw(t) };
    let values = trie.0.into_values();

    let free = func.unwrap_or_else(|| {
        // SAFETY:
        // The safety requirements of this function
        // require the caller to ensure that the Redis allocator is initialized,
        // and that `RedisModule_Free` does not get mutated while running this function.
        #[cfg(not(miri))]
        unsafe {
            RedisModule_Free.expect("Redis allocator not available")
        }
        #[cfg(miri)]
        // When testing under Miri, we use the custom allocator shim provided by
        // redis_module_test
        redis_mock::allocator::free_shim
    });

    // Iterate over all values and free them by calling `func` given the data.
    for value in values {
        // SAFETY:
        // `free` either refers to `RedisModule_Free` or a custom function provided by the caller.
        // In the former case, the safety requirements of this function
        // require the caller to ensure that the Redis allocator is initialized,
        // and that `RedisModule_Free` does not get mutated while running this function.
        // In the latter case, the caller is responsible for ensuring that the provided function
        // is safe to call with the given data.
        unsafe { free(value) }
    }
}

/// Determines the amount of memory used by the trie in bytes.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_MemUsage(t: *mut TrieMap) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: The safety requirements of this function
    // state the caller is to ensure that the pointer `t` is
    // a valid TrieMap obtained from `NewTrieMap` and cannot be NULL.
    // If that invariant is upheld, then the following line is sound.
    let TrieMap(trie) = unsafe { &*t };
    trie.mem_usage()
}

/// Iterate the trie for all the suffixes of a given prefix. This returns an
/// iterator object even if the prefix was not found, and subsequent calls to
/// TrieMapIterator_Next are needed to get the results from the iteration. If the
/// prefix is not found, the first call to next will return 0.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
/// - `t` must not be freed while the iterator lives.
/// - `prefix` must point to a valid pointer to a byte sequence of length `prefix_len`,
///   which will be set to the current key. It may only be NULL in case `prefix_len == 0`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_Iterate<'tm>(
    t: *mut TrieMap,
    prefix: *const c_char,
    prefix_len: tm_len_t,
    iter_mode: tm_iter_mode,
) -> *mut TrieMapIterator<'tm> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    let pattern: &[u8] = if prefix_len > 0 {
        debug_assert!(!prefix.is_null(), "prefix cannot be NULL if prefix_len > 0");
        // SAFETY: Caller is to ensure that the pointer `prefix` is
        // a valid pointer to a byte sequence of length `prefix_len`.
        unsafe { std::slice::from_raw_parts(prefix.cast(), prefix_len as usize) }
    } else {
        &[]
    };

    // SAFETY: Caller is to ensure that the pointer `t` is
    // a valid, non-null pointer to a TrieMap.
    let TrieMap(trie) = unsafe { &*t };

    let iter = match iter_mode {
        tm_iter_mode::TM_PREFIX_MODE => {
            TrieMapIteratorImpl::Plain(trie.prefixed_lending_iter(pattern))
        }
        tm_iter_mode::TM_CONTAINS_MODE => {
            let finder = memchr::memmem::Finder::new(pattern);
            TrieMapIteratorImpl::Filtered(
                trie.lending_iter()
                    .filter(Box::new(move |(key, _)| finder.find(key).is_some())),
            )
        }
        tm_iter_mode::TM_SUFFIX_MODE => TrieMapIteratorImpl::Filtered(
            trie.lending_iter()
                .filter(Box::new(|(k, _)| k.ends_with(pattern))),
        ),
        tm_iter_mode::TM_WILDCARD_MODE => TrieMapIteratorImpl::Wildcard(
            trie.wildcard_iter(WildcardPattern::parse(pattern)).into(),
        ),
    };

    let iter = TrieMapIterator {
        iter,
        timeout: None,
    };
    let iter = Box::new(iter);

    Box::into_raw(iter)
}

/// Set timeout limit used for affix queries. This timeout is checked in
/// [`TrieMapIterator_Next`], [`TrieMapIterator_NextContains`], and [`TrieMapIterator_NextWildcard`],
/// which will return `0` if the timeout is reached.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid TrieMapIterator obtained from [`TrieMap_Iterate`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMapIterator_SetTimeout(it: *mut TrieMapIterator, timeout: timespec) {
    debug_assert!(!it.is_null(), "it cannot be NULL");

    // SAFETY: caller is to ensure `it` points to a valid
    // TrieMapIterator obtained from `TrieMap_Iterate`
    let TrieMapIterator {
        timeout: it_timeout,
        ..
    } = unsafe { &mut *it };

    *it_timeout = Some(IteratorTimeoutState {
        deadline: timeout,
        counter: 0,
    });
}

/// Free a trie iterator
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid TrieMapIterator obtained from [`TrieMap_Iterate`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMapIterator_Free(it: *mut TrieMapIterator) {
    debug_assert!(!it.is_null(), "it cannot be NULL");

    // SAFETY: caller is to ensure `it` points to a valid
    // TrieMapIterator obtained from `TrieMap_Iterate`
    unsafe {
        let _ = Box::from_raw(it);
    };
}

/// Iterate to the next matching entry in the trie. Returns 1 if we can continue,
/// or 0 if we're done and should exit
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid TrieMapIterator obtained from [`TrieMap_Iterate`] and cannot be NULL.
/// - `ptr` must point to a valid pointer to a byte sequence, which will be set to the current key. This
///   pointer is invalidated upon calling [`TrieMapIterator_Next`], [`TrieMapIterator_NextContains`],
///   or [`TrieMapIterator_NextWildcard`] again.
/// - `len` must point to a valid `tm_len_t` which will be set to the length of the current key.
/// - `value` must point to a valid pointer, which will be set to the value of the current key.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMapIterator_Next(
    it: *mut TrieMapIterator,
    ptr: *mut *mut c_char,
    len: *mut tm_len_t,
    value: *mut *mut c_void,
) -> c_int {
    debug_assert!(!it.is_null(), "it cannot be NULL");
    debug_assert!(!ptr.is_null(), "ptr cannot be NULL");
    debug_assert!(!len.is_null(), "len cannot be NULL");
    debug_assert!(!value.is_null(), "value cannot be NULL");

    // SAFETY: caller is to ensure that the iterator is valid and not null
    let TrieMapIterator { iter, timeout } = unsafe { &mut *it };

    if let Some(IteratorTimeoutState { deadline, counter }) = timeout {
        *counter += 1;
        // For optimized builds, we only check the deadline
        // once every 100 iterations. In development,
        // we're checking each iterationn.
        if *counter == 100 || cfg!(debug_assertions) {
            let now = timespec_monotonic_now();

            if now.tv_sec > deadline.tv_sec && now.tv_nsec > deadline.tv_nsec {
                return 0;
            }

            *counter = 0;
        }
    }

    let Some((k, v)) = LendingIterator::next(iter) else {
        return 0;
    };

    // SAFETY: caller is to ensure that `ptr` is
    // a mutable, well-aligned pointer to a `c_char` array
    unsafe {
        ptr.write(k.as_ptr().cast::<c_char>().cast_mut());
    }
    // SAFETY: caller is to ensure that `len` is
    // a mutable, well-aligned pointer to a `tm_len_t`
    unsafe {
        len.write(k.len() as tm_len_t);
    }
    // SAFETY: caller is to ensure that `ptr` is
    // a mutable, well-aligned pointer to a `*mut c_void`
    unsafe {
        value.write(*v);
    }

    1
}

/// Iterate the trie within the specified key range.
///
/// If `minLen` is 0, `min` is regarded as an empty string. It `minlen` is -1, the itaration starts from the beginning of the trie.
/// If `maxLen` is 0, `max` is regarded as an empty string. If `maxlen` is -1, the iteration goes to the end of the trie.
/// `includeMin` and `includeMax` determine whether the min and max values are included in the iteration.
///
/// The passed [`TrieMapRangeCallback`] function is called for each key found,
/// passing the key and its length, the value, and the `ctx` pointer passed to this
/// function.
///
/// Panics in case the passed callback is NULL.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `trie` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
/// - `min` can be NULL only if `minlen == 0` or `minlen == -1`. It is not necessarily NULL-terminated.
/// - `minlen` can be 0. If so, `min` is regarded as an empty string.
/// - `max` can be NULL only if `maxlen == 0` or `maxlen == -1`. It is not necessarily NULL-terminated.
/// - `maxlen` can be 0. If so, `max` is regarded as an empty string.
/// - `callback` must be a valid pointer to a function of type [`TrieMapRangeCallback`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_IterateRange(
    trie: *mut TrieMap,
    min: *const c_char, // May be NULL iff minlen == 0
    minlen: c_int,      // if 0, execute special case
    includeMin: bool,
    max: *const c_char, // May be NULL iff minlen == 0
    maxlen: c_int,
    includeMax: bool,
    callback: TrieMapRangeCallback,
    ctx: *mut c_void,
) {
    let Some(callback) = callback else {
        #[cfg(debug_assertions)]
        {
            panic!("TrieMap_IterateRange with a NULL callback");
        }
        #[cfg(not(debug_assertions))]
        {
            return; // It makes no sense to iterate without a callback
        }
    };

    debug_assert!(!trie.is_null(), "trie cannot be NULL");

    let min: Option<&[u8]> = match minlen {
        ..0 => None,
        0 => Some([].as_slice()),
        1.. => {
            debug_assert!(!min.is_null(), "min cannot be NULL if minlen > 0");
            // SAFETY: caller is to ensure that min is not null in case minlen > 0,
            // and that min points to a contiguous slice of bytes of len minlen
            Some(unsafe { std::slice::from_raw_parts(min.cast(), minlen as usize) })
        }
    };

    let max: Option<&[u8]> = match maxlen {
        ..0 => None,
        0 => Some([].as_slice()),
        1.. => {
            debug_assert!(!max.is_null(), "max cannot be NULL if maxlen > 0");
            // SAFETY: caller is to ensure that max is not null in case maxlen > 0,
            // and that max points to a contiguous slice of bytes of len maxlen
            Some(unsafe { std::slice::from_raw_parts(max.cast(), maxlen as usize) })
        }
    };

    // SAFETY: caller is to ensure that `trie` is valid and not null
    let TrieMap(trie) = unsafe { &mut *trie };

    let filter = RangeFilter {
        min: min.map(|m| RangeBoundary {
            value: m,
            is_included: includeMin,
        }),
        max: max.map(|m| RangeBoundary {
            value: m,
            is_included: includeMax,
        }),
    };
    let iter: RangeLendingIter<_> = trie.range_iter(filter).into();
    iter.fuse().for_each(|(key, value)| {
        let key_len = key.len();
        // `u8` and `c_char` can be safely transmuted back and forth.
        let key_ptr = key.as_ptr().cast();
        // Safety: caller is to ensure `callback` be
        // a valid pointer to a function of type [`TrieMapRangeCallback`]
        unsafe {
            (callback)(key_ptr, key_len, *value, ctx);
        }
    });
}

/// Get current time from monotonic clock.
/// Calls `clock_gettime` with `clk_id == CLOCK_MONOTONIC_RAW`.
pub fn timespec_monotonic_now() -> timespec {
    let mut ts = std::mem::MaybeUninit::uninit();
    // SAFETY:
    // We have exclusive access to a pointer of the correct type
    let ret = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, ts.as_mut_ptr()) };
    if ret == 0 {
        // SAFETY:
        // `ts` was initialized by before call to `clock_gettime`
        unsafe { ts.assume_init() }
    } else {
        panic!("Couldn't get the current time from the system monotonic clock")
    }
}
