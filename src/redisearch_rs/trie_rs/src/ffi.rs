#![allow(non_camel_case_types, non_snake_case)]

use std::ffi::{c_char, c_int, c_void};

/// Holds the length of a key string in the trie.
///
/// C equivalent:
/// ```c
/// typedef uint16_t tm_len_t;
/// ```
pub type tm_len_t = u16;

/// This special pointer is returned when TrieMap_Find cannot find anything.
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
pub enum TrieMap {}

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
#[expect(dead_code)]
type TrieMapIterator_NextFunc = Option<
    unsafe extern "C" fn(
        it: *mut TrieMapIterator,
        ptr: *mut *mut c_char,
        len: *mut tm_len_t,
        value: *mut *mut c_void,
    ) -> c_int,
>;

/// Opaque type TrieMapIterator. Obtained from calling [`TrieMap_Iterate`].
enum TrieMapIterator {}

/// Opaque type TrieMapResultBuf. Holds the results of [`TrieMap_FindPrefixes`].
enum TrieMapResultBuf {}

/// Free the [`TrieMapResultBuf`] and its contents.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `buf` must point to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`] and cannot be NULL.
///
/// C equivalent:
/// ```c
/// void TrieMapResultBuf_Free(TrieMapResultBuf *buf);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMapResultBuf_Free(buf: *mut TrieMapResultBuf) {
    debug_assert!(!buf.is_null(), "buf cannot be NULL");
    todo!()
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
    todo!()
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
unsafe extern "C" fn NewTrieMap() -> *mut TrieMap {
    todo!()
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
/// If value is given, it is saved as a payload inside the trie node.
/// If the key already exists, we replace the old value with the new value, using
/// free() to free the old value.
///
/// If cb is given, instead of replacing and freeing, we call the callback with
/// the old and new value, and the function should return the value to set in the
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

    let _unused = (value, cb);
    todo!()
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

    todo!()
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

    let _unused = results;
    todo!()
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

    let _unused = func;
    todo!()
}

/// Free the trie's root and all its children recursively. If freeCB is given, we
/// call it to free individual payload values (not the nodes). If not, free() is used instead.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
///
/// C equivalent:
/// ```c
/// void TrieMap_Free(TrieMap *t, freeCB func);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_Free(t: *mut TrieMap, func: freeCB) {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    let _unused = func;
    todo!()
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

    let _unused = t;
    todo!()
}

/// Iterate the trie for all the suffixes of a given prefix. This returns an
/// iterator object even if the prefix was not found, and subsequent calls to
/// TrieMapIterator_Next are needed to get the results from the iteration. If the
/// prefix is not found, the first call to next will return 0.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
///
/// C equivalent:
/// ```c
/// TrieMapIterator *TrieMap_Iterate(TrieMap *t, const char *prefix, tm_len_t prefixLen);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_Iterate(
    t: *mut TrieMap,
    prefix: *const c_char,
    prefix_len: tm_len_t,
    iter_mode: tm_iter_mode,
) -> *mut TrieMapIterator {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    if prefix_len > 0 {
        debug_assert!(!prefix.is_null(), "prefix cannot be NULL if prefix_len > 0");
    }

    let _unused = (t, prefix, prefix_len, iter_mode);
    todo!()
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

    let _unused = (it, timeout);
    todo!()
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

    let _unused = it;
    todo!()
}

/// Iterate to the next matching entry in the trie. Returns 1 if we can continue,
/// or 0 if we're done and should exit
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid TrieMapIterator obtained from [`TrieMap_Iterate`] and cannot be NULL.
/// - `ptr` must point to a valid pointer to a C string, which will be set to the current key.
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

    todo!()
}

/// Iterate to the next matching entry in the trie. Returns 1 if we can continue,
/// or 0 if we're done and should exit.
/// Used by Contains and Suffix queries.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid TrieMapIterator obtained from [`TrieMap_Iterate`] and cannot be NULL.
/// - `ptr` must point to a valid pointer to a C string, which will be set to the current key.
/// - `len` must point to a valid `tm_len_t` which will be set to the length of the current key.
/// - `value` must point to a valid pointer, which will be set to the value of the current key.
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
    debug_assert!(!it.is_null(), "it cannot be NULL");
    debug_assert!(!ptr.is_null(), "ptr cannot be NULL");
    debug_assert!(!len.is_null(), "len cannot be NULL");
    debug_assert!(!value.is_null(), "value cannot be NULL");

    todo!()
}

/// Iterate to the next matching entry in the trie. Returns 1 if we can continue,
/// or 0 if we're done and should exit.
/// Used by Wildcard queries.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid TrieMapIterator obtained from [`TrieMap_Iterate`] and cannot be NULL.
/// - `ptr` must point to a valid pointer to a C string, which will be set to the current key.
/// - `len` must point to a valid `tm_len_t` which will be set to the length of the current key.
/// - `value` must point to a valid pointer, which will be set to the value of the current key.
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
    debug_assert!(!it.is_null(), "it cannot be NULL");
    debug_assert!(!ptr.is_null(), "ptr cannot be NULL");
    debug_assert!(!len.is_null(), "len cannot be NULL");
    debug_assert!(!value.is_null(), "value cannot be NULL");

    todo!()
}

/// Iterate the trie for all the suffixes of a given prefix. This returns an
/// iterator object even if the prefix was not found, and subsequent calls to
/// TrieMapIterator_Next are needed to get the results from the iteration. If the
/// prefix is not found, the first call to next will return 0.
///
/// If `minLen` is 0, `min` is regarded as an empty string. It `minlen` is -1, the itaration starts from the beginning of the trie.
/// If `maxLen` is 0, `max` is regarded as an empty string. If `maxlen` is -1, the iteration goes to the end of the trie.
/// `includeMin` and `includeMax` determine whether the min and max values are included in the iteration.
///
/// The passed [`TrieMapRangeCallback`] function is called for each key found,
/// passing the key and its length, the value, and the `ctx` pointer passed to this
/// function.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `trie` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
/// - `min` can be NULL only if `minlen == 0` or `minlen == -1`. It is not necessarily NULL-terminated.
/// - `minlen` can be 0. If so, `min` is regarded as an empty string.
/// - `max` can be NULL only if `maxlen == 0` or `maxlen == -1`. It is not necessarily NULL-terminated.
/// - `maxlen` can be 0. If so, `max` is regarded as an empty string.
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
    debug_assert!(!trie.is_null(), "trie cannot be NULL");
    if minlen > 0 {
        debug_assert!(!min.is_null(), "min cannot be NULL if minlen > 0");
    }
    if maxlen > 0 {
        debug_assert!(!max.is_null(), "max cannot be NULL if maxlen > 0");
    }

    let _unused = (
        trie, min, minlen, includeMin, max, maxlen, includeMax, callback, ctx,
    );
    todo!()
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
    let _unused = (t, prefix, pflen);
    todo!()
}
