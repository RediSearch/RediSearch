#![allow(non_camel_case_types, non_snake_case)]

use std::{
    ffi::{c_char, c_int, c_void},
    slice,
};

use iter_types::TrieMapIteratorImpl;
use lending_iterator::LendingIterator;

use redis_module::raw::RedisModule_Free;

use wildcard::WildcardPattern;

use crate::iter::{WildcardFilter, WildcardFixedLenFilter};

/// Holds the length of a key string in the trie.
///
/// C equivalent:
/// ```c
/// typedef uint16_t tm_len_t;
/// ```
pub type tm_len_t = u16;

/// This special pointer is returned when [`TrieMap_Find`] cannot find anything.
///
/// C equivalent:
/// ```c
/// void *TRIEMAP_NOTFOUND = "NOT FOUND";
/// ```
#[unsafe(no_mangle)]
#[used]
pub static mut TRIEMAP_NOTFOUND: *mut ::std::os::raw::c_void = c"NOT FOUND".as_ptr() as *mut _;

/// Used by TrieMapIterator to determine type of query.
///
/// C equivalent:
/// ```c
/// typedef enum {
///     TM_PREFIX_MODE = 0,
///     TM_CONTAINS_MODE = 1,
///     TM_SUFFIX_MODE = 2,
///     TM_WILDCARD_MODE = 3,
///     TM_WILDCARD_FIXED_LEN_MODE = 4,
///   } tm_iter_mode;
#[repr(C)]
#[allow(dead_code)]
#[derive(Debug)]
enum tm_iter_mode {
    TM_PREFIX_MODE = 0,
    TM_CONTAINS_MODE = 1,
    TM_SUFFIX_MODE = 2,
    TM_WILDCARD_MODE = 3,
    TM_WILDCARD_FIXED_LEN_MODE = 4,
}

/// Default iteration mode for [`TrieMap_Iterate`].
#[unsafe(no_mangle)]
static TM_ITER_MODE_DEFAULT: tm_iter_mode = tm_iter_mode::TM_PREFIX_MODE;

/// Opaque type TrieMap. Can be instantiated with [`NewTrieMap`].
#[repr(transparent)]
struct TrieMap(crate::trie::TrieMap<*mut c_void>);

/// Callback type for passing to [`TrieMap_IterateRange`].
///
/// C equivalent:
/// ```c
/// typedef void(TrieMapRangeCallback)(const char *, size_t, void *, void *);
/// ```
type TrieMapRangeCallback =
    Option<unsafe extern "C" fn(*const c_char, libc::size_t, *mut c_void, *mut c_void)>;

/// Type of the functions [`TrieMapIterator_Next`], [`TrieMapIterator_NextContains`],
/// and [`TrieMapIterator_NextWildcard`].
///
///  C equivalent:
/// ```c
/// typedef int (*TrieMapIterator_NextFunc)(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value);
/// ```
#[allow(dead_code)]
type TrieMapIterator_NextFunc = Option<
    unsafe extern "C" fn(
        it: *mut TrieMapIterator,
        ptr: *mut *mut c_char,
        len: *mut tm_len_t,
        value: *mut *mut c_void,
    ) -> c_int,
>;

/// Opaque type TrieMapIterator. Obtained from calling [`TrieMap_Iterate`].
struct TrieMapIterator<'tm> {
    iter: TrieMapIteratorImpl<'tm>,
    timeout: Option<IteratorTimeoutState>,
}

struct IteratorTimeoutState {
    deadline: libc::timespec,
    counter: u8,
}

/// Opaque type TrieMapResultBuf. Holds the results of [`TrieMap_FindPrefixes`].
#[repr(C)]
struct TrieMapResultBuf(low_memory_thin_vec::LowMemoryThinVec<*mut c_void>);

/// Free the [`TrieMapResultBuf`] and its contents.
///
/// C equivalent:
/// ```c
/// void TrieMapResultBuf_Free(TrieMapResultBuf *buf);
/// ```
#[unsafe(no_mangle)]
extern "C" fn TrieMapResultBuf_Free(buf: TrieMapResultBuf) {
    drop(buf);
}

/// Get the data from the TrieMapResultBuf as an array of values.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `buf` must point to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`] and cannot be NULL.
///
/// C equivalent:
/// ```c
/// void **TrieMapResultBuf_Data(TrieMapResultBuf *buf);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMapResultBuf_Data(buf: *mut TrieMapResultBuf) -> *mut *mut c_void {
    debug_assert!(!buf.is_null(), "buf cannot be NULL");

    // SAFETY:
    // As per the safety invariants of this function:
    // - `buf` is not NULL
    // - `buf` points to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`]
    let TrieMapResultBuf(data) = unsafe { &mut *buf };
    data.as_mut_ptr()
}

/// Get the length of the TrieMapResultBuf.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `buf` must point to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`] and cannot be NULL.
///
/// C equivalent:
/// ```c
/// size_t TrieMapResultBuf_Len(TrieMapResultBuf *buf);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMapResultBuf_Len(buf: *mut TrieMapResultBuf) -> usize {
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
///
/// C equivalent:
/// ```c
/// TrieMap *NewTrieMap();
/// ```
#[unsafe(no_mangle)]
extern "C" fn NewTrieMap() -> *mut TrieMap {
    let map = Box::new(TrieMap(crate::trie::TrieMap::new()));
    Box::into_raw(map)
}

/// Callback type for passing to [`TrieMap_Delete`].
///
/// C equivalent:
/// ```c
/// typedef void (*freeCB)(void *);
/// ```
type freeCB = Option<unsafe extern "C" fn(*mut c_void)>;

/// Callback type for passing to [`TrieMap_Add`].
///
/// C equivalent:
/// ```c
/// typedef void *(*TrieMapReplaceFunc)(void *oldval, void *newval);
/// ```
type TrieMapReplaceFunc =
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
///
/// C equivalent:
/// ```c
/// int TrieMap_Add(TrieMap *t, const char *str, tm_len_t len, void *value, TrieMapReplaceFunc cb);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_Add(
    t: *mut TrieMap,
    str: *const c_char,
    len: tm_len_t,
    value: *mut c_void,
    cb: TrieMapReplaceFunc,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    if len > 0 {
        debug_assert!(!str.is_null(), "str cannot be NULL if len > 0");
    }

    // SAFETY: The safety requirements of this function
    // require the caller to ensure that the pointer `t` is
    // a valid TrieMap obtained from `NewTrieMap` and cannot be NULL.
    // If that invariant is upheld, then the following line is sound.
    let TrieMap(trie) = unsafe { &mut *t };

    let key = if len > 0 {
        // SAFETY: The safety requirements of this function
        // require the caller to ensure that the pointer `str` is
        // a valid pointer to a C string, with a length of `len` bytes.
        // If that invariant is upheld, then the following line is sound.
        unsafe { slice::from_raw_parts(str, len as usize) }
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
///
/// C equivalent:
/// ```c
/// void *TrieMap_Find(TrieMap *t, const char *str, tm_len_t len);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_Find(
    t: *mut TrieMap,
    str: *const c_char,
    len: tm_len_t,
) -> *mut c_void {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    if len > 0 {
        debug_assert!(!str.is_null(), "str cannot be NULL if len > 0");
    }

    // SAFETY: The safety requirements of this function
    // state the caller is to ensure that the pointer `t` is
    // a valid TrieMap obtained from `NewTrieMap` and cannot be NULL.
    // If that invariant is upheld, then the following line is sound.
    let TrieMap(trie) = unsafe { &mut *t };

    let key = if len > 0 {
        // SAFETY: The safety requirements of this function
        // state the caller is to ensure that the pointer `str` is
        // a valid pointer to a C string, with a length of `len` bytes.
        // If that invariant is upheld, then the following line is sound.
        unsafe { slice::from_raw_parts(str, len as usize) }
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
/// - `results` must be a mutable, aligned pointer to a valid memory location.
///
/// C equivalent:
/// ```c
/// int TrieMap_FindPrefixes(TrieMap *t, const char *str, tm_len_t len, TrieMapResultBuf *results);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_FindPrefixes(
    t: *mut TrieMap,
    str: *const c_char,
    len: tm_len_t,
    results: *mut TrieMapResultBuf,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    if len > 0 {
        debug_assert!(!str.is_null(), "str cannot be NULL if len > 0");
    }

    // SAFETY: The safety requirements of this function
    // state the caller is to ensure that the pointer `t` is
    // a valid TrieMap obtained from `NewTrieMap` and cannot be NULL.
    // If that invariant is upheld, then the following line is sound.
    let TrieMap(trie) = unsafe { &mut *t };

    let prefix = if len > 0 {
        // SAFETY: The safety requirements of this function
        // state the caller is to ensure that the pointer `str` is
        // a valid pointer to a string of length `len` and cannot be NULL.
        // If that invariant is upheld, then the following line is sound.
        unsafe { std::slice::from_raw_parts(str, len as usize) }
    } else {
        &[]
    };

    let iter = trie.values_prefix(prefix).copied();

    let res = low_memory_thin_vec::LowMemoryThinVec::from_iter(iter);
    let len = res.len();
    let res_buf = TrieMapResultBuf(res);
    // SAFETY: The safety requirements of this function
    // state the caller is to ensure that the pointer `results` is
    // a valid, mutable, aligned pointer to a valid memory location.
    // If that invariant is upheld, then the following line is sound.
    unsafe {
        std::ptr::write(results, res_buf);
    };

    len.try_into()
        .expect("Number of results did not fit into a `c_int`")
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
///
/// C equivalent:
/// ```c
/// int TrieMap_Delete(TrieMap *t, const char *str, tm_len_t len, freeCB func);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_Delete(
    t: *mut TrieMap,
    str: *const c_char,
    len: tm_len_t,
    func: freeCB,
) -> c_int {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    if len > 0 {
        debug_assert!(!str.is_null(), "str cannot be NULL if len > 0");
    }

    // SAFETY: The safety requirements of this function
    // state the caller is to ensure that the pointer `t` is
    // a valid TrieMap obtained from `NewTrieMap` and cannot be NULL.
    // If that invariant is upheld, then the following line is sound.
    let TrieMap(trie) = unsafe { &mut *t };

    let key = if len > 0 {
        // SAFETY: The safety requirements of this function
        // state the caller is to ensure that the pointer `str` is
        // a valid pointer to a C string, with a length of `len` bytes.
        // If that invariant is upheld, then the following line is sound.
        unsafe { slice::from_raw_parts(str, len as usize) }
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
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
/// - `func` must either be NULL or a valid pointer to a function of type [`freeCB`].
/// - The Redis allocator must be initialized before calling this function,
///    and `RedisModule_Free` must not get mutated while running this function.
///
/// C equivalent:
/// ```c
/// void TrieMap_Free(TrieMap *t, freeCB func);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_Free(t: *mut TrieMap, func: freeCB) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
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
        #[cfg(not(all(miri, test)))]
        unsafe {
            RedisModule_Free.expect("Redis allocator not available")
        }
        #[cfg(all(test, miri))]
        // When testing under Miri, we use the custom allocator shim provided by
        // redis_module_test
        redis_module_test::ffi_polyfill::free_shim
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
///
///  C equivalent:
/// ```c
/// size_t TrieMap_MemUsage(TrieMap *t);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_MemUsage(t: *mut TrieMap) -> usize {
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
///
///
///
/// C equivalent:
/// ```c
/// TrieMapIterator *TrieMap_Iterate(TrieMap *t, const char *prefix, tm_len_t prefixLen);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_Iterate<'tm>(
    t: *mut TrieMap,
    prefix: *const c_char,
    prefix_len: tm_len_t,
    iter_mode: tm_iter_mode,
) -> *mut TrieMapIterator<'tm> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    let pattern = if prefix_len > 0 {
        debug_assert!(!prefix.is_null(), "prefix cannot be NULL if prefix_len > 0");
        // SAFETY: Caller is to ensure that the pointer `prefix` is
        // a valid pointer to a byte sequence of length `prefix_len`.
        unsafe { std::slice::from_raw_parts(prefix, prefix_len as usize) }
    } else {
        &[]
    };

    // SAFETY: Caller is to ensure that the pointer `t` is
    // a valid, non-null pointer to a TrieMap.
    let TrieMap(trie) = unsafe { &*t };

    let iter = match iter_mode {
        tm_iter_mode::TM_PREFIX_MODE => {
            TrieMapIteratorImpl::Plain(trie.lending_iter_prefix(pattern))
        }
        tm_iter_mode::TM_CONTAINS_MODE => {
            // SAFETY: `c_char` and `u8` have the same layout and alignment and are valid for all bit patterns.
            let pattern: &[u8] = unsafe { std::mem::transmute::<&[c_char], &[u8]>(pattern) };
            let finder = memchr::memmem::Finder::new(pattern);
            TrieMapIteratorImpl::Filtered(trie.lending_iter().filter(Box::new(move |(key, _)| {
                // SAFETY: `c_char` and `u8` have the same layout and alignment and are valid for all bit patterns.
                let key = unsafe { std::mem::transmute::<&[c_char], &[u8]>(*key) };
                finder.find(key).is_some()
            })))
        }
        tm_iter_mode::TM_SUFFIX_MODE => TrieMapIteratorImpl::Filtered(
            trie.lending_iter()
                .filter(Box::new(|(k, _)| k.ends_with(pattern))),
        ),
        tm_iter_mode::TM_WILDCARD_MODE => {
            let pattern = WildcardPattern::parse(pattern);
            let iter = if let Some(wildcard::Token::Literal(prefix)) = pattern.tokens().first() {
                trie.lending_iter_prefix(prefix)
            } else {
                trie.lending_iter()
            }
            .with_traversal_filter(WildcardFilter(pattern.clone()));

            TrieMapIteratorImpl::Wildcard(iter.filter(Box::new(move |(key, _)| {
                pattern.matches(key) == wildcard::MatchOutcome::Match
            })))
        }
        tm_iter_mode::TM_WILDCARD_FIXED_LEN_MODE => {
            let pattern = WildcardPattern::parse(pattern);

            let iter = if let Some(wildcard::Token::Literal(prefix)) = pattern.tokens().first() {
                trie.lending_iter_prefix(prefix)
            } else {
                trie.lending_iter()
            }
            .with_traversal_filter(WildcardFixedLenFilter(pattern.clone()));

            TrieMapIteratorImpl::WildcardFixedLen(iter.filter(Box::new(move |(key, _)| {
                pattern.matches_fixed_len(key) == wildcard::MatchOutcome::Match
            })))
        }
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
///
/// C equivalent:
/// ```c
/// void TrieMapIterator_SetTimeout(TrieMapIterator *it, struct timespec timeout);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMapIterator_SetTimeout(it: *mut TrieMapIterator, timeout: libc::timespec) {
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
///
///  C equivalent:
/// ```c
/// void TrieMapIterator_Free(TrieMapIterator *it);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMapIterator_Free(it: *mut TrieMapIterator) {
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
///     or [`TrieMapIterator_NextWildcard`] again.
/// - `len` must point to a valid `tm_len_t` which will be set to the length of the current key.
/// - `value` must point to a valid pointer, which will be set to the value of the current key.
///
/// C equivalent:
/// ```c
/// int TrieMapIterator_Next(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMapIterator_Next(
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
        ptr.write(k.as_ptr().cast_mut());
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

/// Iterate to the next matching entry in the trie. Returns 1 if we can continue,
/// or 0 if we're done and should exit.
/// Used by Contains and Suffix queries.
///
/// # Safety
/// See [`TrieMapIterator_Next`]
///
///  C equivalent:
/// ```c
/// int TrieMapIterator_NextContains(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMapIterator_NextContains(
    it: *mut TrieMapIterator,
    ptr: *mut *mut c_char,
    len: *mut tm_len_t,
    value: *mut *mut c_void,
) -> c_int {
    // Safety: see `TrieMapIterator_Next`
    unsafe { TrieMapIterator_Next(it, ptr, len, value) }
}

/// Iterate to the next matching entry in the trie. Returns 1 if we can continue,
/// or 0 if we're done and should exit.
/// Used by Wildcard queries.
///
/// # Safety
/// See [`TrieMapIterator_Next`]
///
/// C equivalent:
/// ```c
/// int TrieMapIterator_NextWildcard(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMapIterator_NextWildcard(
    it: *mut TrieMapIterator,
    ptr: *mut *mut c_char,
    len: *mut tm_len_t,
    value: *mut *mut c_void,
) -> c_int {
    // Safety: see `TrieMapIterator_Next`
    unsafe { TrieMapIterator_Next(it, ptr, len, value) }
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
///
/// C equivalent:
/// ```c
/// void TrieMap_IterateRange(TrieMap *trie, const char *min, int minlen, bool includeMin,
///   const char *max, int maxlen, bool includeMax,
///   TrieMapRangeCallback callback, void *ctx);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_IterateRange(
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
    /// Simple generic function that builds an iterator
    /// over the trie entries, applies the
    /// passed predicate filter, and
    /// consumes the results by calling the
    /// passed callback.
    /// Needs to be a generic because the iterator type
    /// is determined by the (unnamable) predicate
    /// closure, and because naming `LendingIterator`
    /// trait objects is hard, if not impossible.
    ///
    /// This way, we avoid having to name the types,
    /// but can still DRY and enjoy optmizations.
    ///
    /// # Safety `callback` must be a valid pointer to a function of type [`TrieMapRangeCallback`]
    unsafe fn consume_iter<P: Fn(&(&[i8], &*mut c_void)) -> bool>(
        trie: &crate::trie::TrieMap<*mut c_void>,
        pred: P,
        callback: unsafe extern "C" fn(*const c_char, libc::size_t, *mut c_void, *mut c_void),
        ctx: *mut c_void,
    ) {
        trie.lending_iter()
            .filter(pred)
            .fuse()
            // Safety: caller is to ensure `callback` be
            // a valid pointer to a function of type [`TrieMapRangeCallback`]
            .for_each(|(key, value)| unsafe {
                (callback)(key.as_ptr(), key.len(), *value, ctx);
            });
    }

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
    if minlen > 0 {
        debug_assert!(!min.is_null(), "min cannot be NULL if minlen > 0");
    }
    if maxlen > 0 {
        debug_assert!(!max.is_null(), "max cannot be NULL if maxlen > 0");
    }

    let min = match minlen {
        ..0 => None,
        0 => Some([].as_slice()),
        // SAFETY: caller is to ensure that min is not null in case minlen > 0,
        // and that min points to a contiguous slice of bytes of len minlen
        1.. => Some(unsafe { std::slice::from_raw_parts(min, minlen as usize) }),
    };

    let max = match maxlen {
        ..0 => None,
        0 => Some([].as_slice()),
        // SAFETY: caller is to ensure that max is not null in case maxlen > 0,
        // and that max points to a contiguous slice of bytes of len maxlen
        1.. => Some(unsafe { std::slice::from_raw_parts(max, maxlen as usize) }),
    };

    // SAFETY: caller is to ensure that `trie` is valid and not null
    let TrieMap(trie) = unsafe { &mut *trie };

    // Safety: caller is to ensure `callback`
    // if a valid pointer of type [`TrieMapRangeCallback`],
    // All below uses of unsafe are sound under this
    // assumption
    #[allow(clippy::undocumented_unsafe_blocks)]
    match (min, includeMin, max, includeMax) {
        (None, _, None, _) => trie.lending_iter().fuse().for_each(|(key, value)| unsafe {
            (callback)(key.as_ptr(), key.len(), *value, ctx);
        }),
        (None, _, Some(max), true) => unsafe {
            consume_iter(trie, |(key, _)| *key <= max, callback, ctx)
        },
        (None, _, Some(max), false) => unsafe {
            consume_iter(trie, |(key, _)| *key < max, callback, ctx)
        },
        (Some(min), true, None, _) => unsafe {
            consume_iter(trie, |(key, _)| *key >= min, callback, ctx)
        },
        (Some(min), false, None, _) => unsafe {
            consume_iter(trie, |(key, _)| *key > min, callback, ctx)
        },
        (Some(min), true, Some(max), true) => unsafe {
            consume_iter(trie, |(key, _)| *key >= min && *key <= max, callback, ctx)
        },
        (Some(min), true, Some(max), false) => unsafe {
            consume_iter(trie, |(key, _)| *key >= min && *key < max, callback, ctx)
        },
        (Some(min), false, Some(max), true) => unsafe {
            consume_iter(trie, |(key, _)| *key > min && *key <= max, callback, ctx)
        },
        (Some(min), false, Some(max), false) => unsafe {
            consume_iter(trie, |(key, _)| *key > min && *key < max, callback, ctx)
        },
    }
}

/// Returns a random value for a key that has a given prefix.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
/// - `prefix` can be NULL only if `pflen == 0`. It is not necessarily NULL-terminated.
///
///  C equivalent:
/// ```c
/// void *TrieMap_RandomValueByPrefix(TrieMap *t, const char *prefix, tm_len_t pflen);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_RandomValueByPrefix(
    t: *mut TrieMap,
    prefix: *const c_char,
    pflen: tm_len_t,
) -> *mut c_void {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    if pflen > 0 {
        debug_assert!(!prefix.is_null(), "prefix cannot be NULL if pflen > 0");
    }
    let mut buf = std::mem::MaybeUninit::uninit();

    // Safety: We adhere to all the safety requirements of `TrieMap_FindPrefixes`
    let data_len = unsafe { TrieMap_FindPrefixes(t, prefix, pflen, buf.as_mut_ptr()) };
    // Safety: `buf` has been initialized by `TrieMap_FindPrefixes`
    let mut buf = unsafe { buf.assume_init() };

    // Safety: We adhere to all the safety requirements of `TrieMapResultBuf_Data`
    let data = unsafe { TrieMapResultBuf_Data(&mut buf as *mut _) };

    // Safety: `TrieMapResultBuf_Data` returns a pointer to the data,
    // and its length is provided by `data_len`
    let data = unsafe { std::slice::from_raw_parts(data, data_len as usize) };

    let i = rand::random_range(..data.len());

    data[i]
}

/// Get current time from monotonic clock.
/// Calls `clock_gettime` with `clk_id == CLOCK_MONOTONIC_RAW`.
fn timespec_monotonic_now() -> libc::timespec {
    let mut ts = std::mem::MaybeUninit::uninit();
    // SAFETY:
    // We have exclusive access to a pointer of the correct type
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, ts.as_mut_ptr()) };
    // SAFETY:
    // `ts` was initialized by before call to `clock_gettime`
    unsafe { ts.assume_init() }
}

mod iter_types {
    use crate::iter::{VisitAll, WildcardFilter, WildcardFixedLenFilter};
    use lending_iterator::{lending_iterator::adapters::Filter, prelude::*};
    use std::ffi::{c_char, c_void};

    pub type BoxedPredicate = Box<dyn Fn(&(&[i8], &*mut c_void)) -> bool>;

    pub enum TrieMapIteratorImpl<'tm> {
        Plain(crate::iter::LendingIter<'tm, *mut c_void, VisitAll>),
        Filtered(Filter<crate::iter::LendingIter<'tm, *mut c_void, VisitAll>, BoxedPredicate>),
        Wildcard(
            Filter<crate::iter::LendingIter<'tm, *mut c_void, WildcardFilter<'tm>>, BoxedPredicate>,
        ),
        WildcardFixedLen(
            Filter<
                crate::iter::LendingIter<'tm, *mut c_void, WildcardFixedLenFilter<'tm>>,
                BoxedPredicate,
            >,
        ),
    }

    #[gat]
    #[allow(clippy::needless_lifetimes)]
    impl<'tm> LendingIterator for TrieMapIteratorImpl<'tm> {
        type Item<'next>
        where
            Self: 'next,
        = (&'next [c_char], &'tm *mut c_void);

        fn next(&mut self) -> Option<Self::Item<'_>> {
            match self {
                TrieMapIteratorImpl::Plain(iter) => LendingIterator::next(iter),
                TrieMapIteratorImpl::Filtered(iter) => LendingIterator::next(iter),
                TrieMapIteratorImpl::Wildcard(iter) => LendingIterator::next(iter),
                TrieMapIteratorImpl::WildcardFixedLen(iter) => LendingIterator::next(iter),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use redis_module_test::{self as _, str2c_char}; // Redis allocator shim
    use std::mem::MaybeUninit;

    use super::*;

    macro_rules! assert_entries {
        ($pattern:literal, $mode:expr, $iter_fn:expr, $expected:expr $(,)?) => {
            with_trie_iter($pattern, $mode, Some($iter_fn), |entries| {
                assert_eq!(
                    entries,
                    $expected.map(|(k, v)| (k.to_owned(), v)),
                    "Pattern {:?} should have yielded entries {:?} in mode {:?}",
                    String::from_utf8_lossy($pattern),
                    $expected,
                    $mode,
                );
            });
        };
    }

    pub fn bytes2c_char<const N: usize>(input: &[u8; N]) -> [c_char; N] {
        input.map(|b| b as c_char)
    }

    /// Create a [`TrieMap`], fill it with entries,
    /// call the callback passing the [`TrieMap`] pointer,
    /// and free the map.
    ///
    /// Map structure at the point the callback in invoked:
    ///
    /// ```text
    /// "" (-)
    ///  ↳––––"bi" (-)
    ///        ↳––––"ke" (&0)
    ///              ↳––––"r" (&1)
    ///        ↳––––"s" (&2)
    ///  ↳––––"c" (-)
    ///        ↳––––"ider" (&3)
    ///        ↳––––"ool" (&4)
    ///              ↳––––"er" (&5)
    /// ```
    fn with_trie_map<F>(f: F)
    where
        F: FnOnce(*mut TrieMap),
    {
        let t = NewTrieMap();
        let entries: Vec<_> = [
            ("bike", 0u8),
            ("biker", 1),
            ("bis", 2),
            ("cool", 3),
            ("cooler", 4),
            ("cider", 5),
        ]
        .into_iter()
        .map(|(k, v)| {
            let k = str2c_char(k);
            let v = Box::into_raw(Box::new(v)) as *mut c_void;
            let k_len = k.len() as tm_len_t;
            (Box::into_raw(k) as *mut c_char, k_len, v)
        })
        .collect();
        for (str, len, value) in entries.iter().copied() {
            // Safety: We adhere to all the safety requirements of `TrieMap_Add`
            unsafe { TrieMap_Add(t, str, len, value, None) };
        }

        f(t);

        // Safety: We adhere to all the safety requirements of `TrieMap_Free`
        unsafe { TrieMap_Free(t, None) };

        for (str, len, _) in entries {
            // Safety: we're reconstructing the `Box<[c_char]>`s created earlier
            let k = unsafe { std::slice::from_raw_parts_mut(str, len as usize) };
            // Safety: we're reconstructing the `Box<[c_char]>`s created earlier
            unsafe { std::mem::transmute::<&mut [i8], std::boxed::Box<[i8]>>(k) };
        }
    }

    /// Creates a map using [`with_trie_map`],
    /// sets up a [`TrieMapIterator`] with the passed
    /// config, collects the iteration results in a
    /// [`Vec<(String, u8)>`] of which each item
    /// corresponds to one entry the iterator yielded.
    /// Then, calls the callback, passing the entries
    /// and takes care of freeing the iterator.
    fn with_trie_iter<F, const N: usize>(
        pattern: &[u8; N],
        iter_mode: tm_iter_mode,
        iter_fn: TrieMapIterator_NextFunc,
        f: F,
    ) where
        F: FnOnce(Vec<(String, u8)>),
    {
        let iter_fn = iter_fn.unwrap();
        with_trie_map(|t| {
            let pattern = bytes2c_char(pattern);
            // Safety: We adhere to all the safety requirements of `TrieMap_Iterate`
            let it = unsafe {
                TrieMap_Iterate(t, pattern.as_ptr(), pattern.len() as tm_len_t, iter_mode)
            };

            let mut char: *mut c_char = std::ptr::null_mut();
            let mut len: tm_len_t = 0;
            let mut value: *mut c_void = std::ptr::null_mut();

            let mut entries = Vec::new();
            // Safety: We adhere to all the safety requirements of `TrieMap_Next`,
            // `TrieMap_NextContains`, and `TrieMap_NextWildcard`, which
            // are the instances of `TrieMap_NextFunc` that have been defined..
            while let 1 = unsafe {
                iter_fn(
                    it,
                    &mut char as *mut *mut c_char,
                    &mut len as *mut tm_len_t,
                    &mut value as *mut *mut c_void,
                )
            } {
                // Safety: We're reconstructing the keys and the values created in `with_trie_map`
                let key = unsafe { std::slice::from_raw_parts(char, len as usize) };
                let key =
                    String::from_utf8(key.iter().copied().map(|c| c as u8).collect()).unwrap();

                // Safety: We're reconstructing the keys and the values created in `with_trie_map`
                let value = unsafe { *(value as *mut u8) };

                entries.push((key, value));
            }

            f(entries);

            // Safety: We adhere to all the safety requirements of `TrieMapIterator_Free`
            unsafe { TrieMapIterator_Free(it) };
        });
    }

    #[test]
    fn test_trie_find_prefixes() {
        with_trie_map(|t| {
            let prefix = str2c_char("b");
            let mut buf: MaybeUninit<TrieMapResultBuf> = MaybeUninit::uninit();

            // Safety: We adhere to all the safety requirements of `TrieMap_FindPrefixes`
            let data_len = unsafe {
                TrieMap_FindPrefixes(
                    t,
                    prefix.as_ptr(),
                    prefix.len() as tm_len_t,
                    buf.as_mut_ptr(),
                )
            };
            // Safety: `buf` has been initialized by `TrieMap_FindPrefixes`
            let mut buf = unsafe { buf.assume_init() };

            // Safety: We adhere to all the safety requirements of `TrieMapResultBuf_Data`
            let data = unsafe { TrieMapResultBuf_Data(&mut buf as *mut _) };
            // Safety: `TrieMapResultBuf_Data` returns a pointer to the data,
            // and its length is provided by `data_len`
            let data = unsafe { std::slice::from_raw_parts(data, data_len as usize) };
            let mut results = Vec::with_capacity(data_len as usize);
            for &v in data {
                // Safety: `v` was created in `with_trie_map`
                // and is a pointer to a `u8` value in disguise.
                let value = unsafe { *(v as *mut u8) };
                results.push(value);
            }

            assert_eq!(results, [0, 1, 2]);

            TrieMapResultBuf_Free(buf);
        });
    }

    #[test]
    fn test_trie_iter_prefix() {
        assert_entries!(
            b"bi",
            tm_iter_mode::TM_PREFIX_MODE,
            TrieMapIterator_NextContains,
            [("bike", 0), ("biker", 1), ("bis", 2)],
        );

        assert_entries!(
            b"ci",
            tm_iter_mode::TM_PREFIX_MODE,
            TrieMapIterator_NextContains,
            [("cider", 5)],
        );

        assert_entries!(
            b"",
            tm_iter_mode::TM_PREFIX_MODE,
            TrieMapIterator_NextContains,
            [
                ("bike", 0),
                ("biker", 1),
                ("bis", 2),
                ("cider", 5),
                ("cool", 3),
                ("cooler", 4),
            ],
        );
    }

    #[test]
    fn test_trie_iter_contains() {
        assert_entries!(
            b"ik",
            tm_iter_mode::TM_CONTAINS_MODE,
            TrieMapIterator_NextContains,
            [("bike", 0), ("biker", 1)],
        );
    }

    #[test]
    fn test_trie_iter_suffix() {
        assert_entries!(
            b"er",
            tm_iter_mode::TM_SUFFIX_MODE,
            TrieMapIterator_NextContains,
            [("biker", 1), ("cider", 5), ("cooler", 4)],
        );
    }

    #[test]
    fn test_trie_iter_wildcard() {
        assert_entries!(
            b"*",
            tm_iter_mode::TM_WILDCARD_MODE,
            TrieMapIterator_NextWildcard,
            [
                ("bike", 0),
                ("biker", 1),
                ("bis", 2),
                ("cider", 5),
                ("cool", 3),
                ("cooler", 4),
            ],
        );

        assert_entries!(
            b"c*",
            tm_iter_mode::TM_WILDCARD_MODE,
            TrieMapIterator_NextWildcard,
            [("cider", 5), ("cool", 3), ("cooler", 4)],
        );

        assert_entries!(
            b"*r",
            tm_iter_mode::TM_WILDCARD_MODE,
            TrieMapIterator_NextWildcard,
            [("biker", 1), ("cider", 5), ("cooler", 4)],
        );

        assert_entries!(
            b"*i*",
            tm_iter_mode::TM_WILDCARD_MODE,
            TrieMapIterator_NextWildcard,
            [("bike", 0), ("biker", 1), ("bis", 2), ("cider", 5)],
        );

        assert_entries!(
            b"*i*",
            tm_iter_mode::TM_WILDCARD_MODE,
            TrieMapIterator_NextWildcard,
            [("bike", 0), ("biker", 1), ("bis", 2), ("cider", 5)],
        );

        assert_entries!(
            b"?i?er",
            tm_iter_mode::TM_WILDCARD_MODE,
            TrieMapIterator_NextWildcard,
            [("biker", 1), ("cider", 5)],
        );

        assert_entries!(
            b"????",
            tm_iter_mode::TM_WILDCARD_MODE,
            TrieMapIterator_NextWildcard,
            [("bike", 0), ("cool", 3)],
        );

        assert_entries!(
            b"ci???",
            tm_iter_mode::TM_WILDCARD_MODE,
            TrieMapIterator_NextWildcard,
            [("cider", 5)],
        );

        assert_entries!(
            b"cider",
            tm_iter_mode::TM_WILDCARD_MODE,
            TrieMapIterator_NextWildcard,
            [("cider", 5)],
        );

        assert_entries!(
            b"******?",
            tm_iter_mode::TM_WILDCARD_MODE,
            TrieMapIterator_NextWildcard,
            [
                ("bike", 0),
                ("biker", 1),
                ("bis", 2),
                ("cider", 5),
                ("cool", 3),
                ("cooler", 4),
            ],
        );

        assert_entries!(
            b"*????",
            tm_iter_mode::TM_WILDCARD_MODE,
            TrieMapIterator_NextWildcard,
            [
                ("bike", 0),
                ("biker", 1),
                ("cider", 5),
                ("cool", 3),
                ("cooler", 4),
            ],
        );
    }

    #[test]
    fn test_trie_iter_wildcard_fixed_len() {
        assert_entries!(
            b"?i?er",
            tm_iter_mode::TM_WILDCARD_FIXED_LEN_MODE,
            TrieMapIterator_NextWildcard,
            [("biker", 1), ("cider", 5)],
        );

        assert_entries!(
            b"????",
            tm_iter_mode::TM_WILDCARD_FIXED_LEN_MODE,
            TrieMapIterator_NextWildcard,
            [("bike", 0), ("cool", 3)],
        );

        assert_entries!(
            b"ci???",
            tm_iter_mode::TM_WILDCARD_FIXED_LEN_MODE,
            TrieMapIterator_NextWildcard,
            [("cider", 5)],
        );

        assert_entries!(
            b"cider",
            tm_iter_mode::TM_WILDCARD_FIXED_LEN_MODE,
            TrieMapIterator_NextWildcard,
            [("cider", 5)],
        );
    }

    #[test]
    fn test_trie_iter_timeout() {
        with_trie_map(|t| {
            let pattern = bytes2c_char(b"");
            // Safety: We adhere to all the safety requirements of `TrieMap_Iterate`
            let it = unsafe {
                TrieMap_Iterate(
                    t,
                    pattern.as_ptr(),
                    pattern.len() as tm_len_t,
                    tm_iter_mode::TM_PREFIX_MODE,
                )
            };

            let mut char: *mut c_char = std::ptr::null_mut();
            let mut len: tm_len_t = 0;
            let mut value: *mut c_void = std::ptr::null_mut();

            let mut deadline = timespec_monotonic_now();
            deadline.tv_nsec += 1000 * 200; // Timeout in 200 ms

            // Safety: We adhere to all the safety requirements of `TrieMapIterator_SetTimeout`
            unsafe { TrieMapIterator_SetTimeout(it, deadline) };

            for _ in 0..2 {
                assert_eq!(
                    1,
                    // Safety: We adhere to all the safety requirements of `TrieMapIterator_Next`
                    unsafe {
                        TrieMapIterator_Next(
                            it,
                            &mut char as *mut *mut c_char,
                            &mut len as *mut tm_len_t,
                            &mut value as *mut *mut c_void,
                        )
                    },
                    "Before the deadline passes, next should yield a result"
                );
            }

            // Wait until the deadline has passed.
            // We're using a monotonic timer, so this should not be flaky
            while {
                let now = timespec_monotonic_now();
                now.tv_sec <= deadline.tv_sec || now.tv_nsec <= deadline.tv_nsec
            } {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }

            for _ in 0..2 {
                assert_eq!(
                    0,
                    // Safety: We adhere to all the safety requirements of `TrieMapIterator_Next`
                    unsafe {
                        TrieMapIterator_Next(
                            it,
                            &mut char as *mut *mut c_char,
                            &mut len as *mut tm_len_t,
                            &mut value as *mut *mut c_void,
                        )
                    },
                    "After the deadline passes, next should not yield a result"
                );
            }

            // Safety: We adhere to all the safety requirements of `TrieMapIterator_Free`
            unsafe { TrieMapIterator_Free(it) };
        });
    }

    #[test]
    fn test_trie_iter_range() {
        type ResultsVec = Vec<(String, u8)>;

        macro_rules! assert_range {
            (None, None, $expected:expr $(,)?) => {
                assert_range!(None::<&str>, true, None::<&str>, true, $expected)
            };
            ($min:expr, $include_min:expr, None, $expected:expr $(,)?) => {
                assert_range!($min, $include_min, None::<&str>, true, $expected)
            };
            (None, $max:expr, $include_max:expr, $expected:expr $(,)?) => {
                assert_range!(None::<&str>, true, $max, $include_max, $expected)
            };
            ($min: expr, $include_min:expr, $max:expr, $include_max:expr, $expected:expr $(,)?) => {
                let results = do_iterate($min, $include_min, $max, $include_max);
                let range_start = if $include_min { "[" } else { "(" };
                let range_end = if $include_max { "]" } else { ")" };
                let min = $min.map(|m| format!("{m:?}")).unwrap_or("←".to_owned());
                let max = $max.map(|m| format!("{m:?}")).unwrap_or("→".to_owned());

                assert_eq!(
                    results,
                    $expected.map(|(k, v)| (k.to_owned(), v)),
                    "Invalid results for range {range_start} {min} ; {max} {range_end}"
                );
            };
        }

        unsafe extern "C" fn callback(
            key: *const c_char,
            key_len: libc::size_t,
            value: *mut c_void,
            ctx: *mut c_void,
        ) {
            // Safety: the passed context was indeed a `&mut ResultsVec`
            let results = unsafe { &mut *(ctx as *mut ResultsVec) };

            // Safety: We're reconstructing the keys and the values created in `with_trie_map`
            let key = unsafe { std::slice::from_raw_parts(key, key_len) };
            let key = String::from_utf8(key.iter().copied().map(|c| c as u8).collect()).unwrap();

            // Safety: We're reconstructing the keys and the values created in `with_trie_map`
            let value = unsafe { *(value as *mut u8) };

            results.push((key, value));
        }
        fn do_iterate(
            min: Option<&str>,
            include_min: bool,
            max: Option<&str>,
            include_max: bool,
        ) -> ResultsVec {
            let mut results: ResultsVec = Vec::new();
            with_trie_map(|t| {
                let min_c_char = min.map(str2c_char);
                let max_c_char = max.map(str2c_char);

                let (min_ptr, min_len) = min_c_char
                    .as_ref()
                    .map(|min| (min.as_ptr(), min.len() as c_int))
                    .unwrap_or((std::ptr::null(), -1));
                let (max_ptr, max_len) = max_c_char
                    .as_ref()
                    .map(|max| (max.as_ptr(), max.len() as c_int))
                    .unwrap_or((std::ptr::null(), -1));

                // Safety: We adhere to all the safety requirements of `TrieMap_IterateRange`
                unsafe {
                    TrieMap_IterateRange(
                        t,
                        min_ptr,
                        min_len,
                        include_min,
                        max_ptr,
                        max_len,
                        include_max,
                        Some(callback),
                        (&mut results) as *mut ResultsVec as *mut _,
                    )
                };
            });
            results
        }

        assert_range!(
            Some("biker"),
            true,
            Some("cool"),
            true,
            [("biker", 1), ("bis", 2), ("cider", 5), ("cool", 3)],
        );

        assert_range!(
            Some("biker"),
            false,
            Some("cool"),
            true,
            [("bis", 2), ("cider", 5), ("cool", 3)],
        );

        assert_range!(
            Some("biker"),
            true,
            Some("cool"),
            false,
            [("biker", 1), ("bis", 2), ("cider", 5)],
        );

        assert_range!(
            Some("biker"),
            false,
            Some("cool"),
            false,
            [("bis", 2), ("cider", 5)],
        );

        assert_range!(
            None,
            Some("cool"),
            true,
            [
                ("bike", 0),
                ("biker", 1),
                ("bis", 2),
                ("cider", 5),
                ("cool", 3)
            ],
        );

        assert_range!(
            Some("bike"),
            true,
            None,
            [
                ("bike", 0),
                ("biker", 1),
                ("bis", 2),
                ("cider", 5),
                ("cool", 3),
                ("cooler", 4),
            ],
        );

        assert_range!(
            None,
            None,
            [
                ("bike", 0),
                ("biker", 1),
                ("bis", 2),
                ("cider", 5),
                ("cool", 3),
                ("cooler", 4),
            ],
        );
    }

    #[test]
    fn test_trie_random_value_by_prefix() {
        with_trie_map(|t| {
            let prefix = str2c_char("bi");
            for _ in 0..1000 {
                // Safety: We adhere to all the safety requirements of `TrieMap_RandomValueByPrefix`
                let res = unsafe {
                    TrieMap_RandomValueByPrefix(t, prefix.as_ptr(), prefix.len() as tm_len_t)
                };
                // Safety: with_trie_map inserts values of type u8
                let res = unsafe { *(res as *mut u8) };

                assert!((0..=2).contains(&res), "Result should be in range 0..=2")
            }
        });
    }
}
