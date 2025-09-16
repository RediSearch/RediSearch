/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(non_camel_case_types, non_snake_case)]

use redis_module::raw::RedisModule_Free;
use std::{
    ffi::{c_char, c_int, c_void},
    slice,
};

mod find_prefixes;
mod iter;
/// cbindgen:ignore
mod iter_types;
mod range;

pub use find_prefixes::*;
pub use iter::*;
pub use range::*;

/// The length of a key string in the trie.
pub type tm_len_t = u16;

/// This special pointer is returned when [`TrieMap_Find`] cannot find anything.
#[unsafe(no_mangle)]
#[used]
pub static mut TRIEMAP_NOTFOUND: *mut ::std::os::raw::c_void = c"NOT FOUND".as_ptr() as *mut _;

/// Opaque type TrieMap. Can be instantiated with [`NewTrieMap`].
pub struct TrieMap(trie_rs::TrieMap<*mut c_void>);

/// Create a new [`TrieMap`]. Returns an opaque pointer to the newly created trie.
///
/// To free the trie, use [`TrieMap_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewTrieMap() -> *mut TrieMap {
    let map = Box::new(TrieMap(trie_rs::TrieMap::new()));
    Box::into_raw(map)
}

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

/// Find the entry with a given string and length, and return its value, even if
/// that was NULL.
///
/// Returns the tree root if the key is empty.
///
/// NOTE: If the key does not exist in the trie, we return the special
/// constant value [`TRIEMAP_NOTFOUND`], so checking if the key exists is done by
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
/// - In case [`TRIEMAP_NOTFOUND`] is returned, the key does not exist in the trie,
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

/// Callback type for passing to [`TrieMap_Delete`].
pub type freeCB = Option<unsafe extern "C" fn(*mut c_void)>;

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

#[allow(unused_attributes)]
#[unsafe(no_mangle)]
#[inline(always)]
pub extern "C" fn inline_me(x: i32) -> i32 {
    x + 42
}
