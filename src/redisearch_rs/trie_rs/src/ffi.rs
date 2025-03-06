#![allow(non_camel_case_types)]

use std::ffi::{c_char, c_int, c_void};

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
enum tm_iter_mode {
    TM_PREFIX_MODE = 0,
    TM_CONTAINS_MODE = 1,
    TM_SUFFIX_MODE = 2,
    TM_WILDCARD_MODE = 3,
    TM_WILDCARD_FIXED_LEN_MODE = 4,
}

/// Default mode for TrieMapIterator
#[unsafe(no_mangle)]
static TM_ITER_MODE_DEFAULT: tm_iter_mode = tm_iter_mode::TM_PREFIX_MODE;

/// Opaque type TrieMap
pub enum TrieMap {}

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

/// Opaque type TrieMapIterator
enum TrieMapIterator {}

/// Opaque type TrieMapResultBuf. Holds the results of [`TrieMap_FindPrefixes`].
enum TrieMapResultBuf {}

/// Free the TrieMapResultBuf and its data.
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
/// C equivalent:
/// ```c
/// TrieMap *NewTrieMap();
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn NewTrieMap() -> *mut TrieMap {
    todo!()
}

/// C equivalent:
/// ```c
/// typedef void (*freeCB)(void *);
/// ```
type freeCB = Option<unsafe extern "C" fn(*mut c_void)>;

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
    todo!()
}

/// Find the entry with a given string and length, and return its value, even if
/// that was NULL.
///
/// NOTE: If the key does not exist in the trie, we return the special
/// constant value TRIEMAP_NOTFOUND, so checking if the key exists is done by
/// comparing to it, becase NULL can be a valid result.
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
    todo!()
}

/// Find nodes that have a given prefix. Results are placed in an array.
/// The `results` buffer is initialized by this function using the Redis allocator
/// and should be freed by calling [`TrieMapResultBuf_Free`].
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
    todo!()
}

/// Mark a node as deleted. It also optimizes the trie by merging nodes if
/// needed. If freeCB is given, it will be used to free the value of the deleted
/// node. If it doesn't, we simply call free()
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
    todo!()
}

/// Free the trie's root and all its children recursively. If freeCB is given, we
/// call it to free individual payload values. If not, free() is used instead.
///
/// C equivalent:
/// ```c
/// void TrieMap_Free(TrieMap *t, freeCB func);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_Free(t: *mut TrieMap, func: freeCB) {
    todo!()
}

/// C equivalent:
/// ```c
/// size_t TrieMap_MemUsage(TrieMap *t);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_MemUsage(t: *mut TrieMap) -> usize {
    todo!()
}

/// Iterate the trie for all the suffixes of a given prefix. This returns an
/// iterator object even if the prefix was not found, and subsequent calls to
/// TrieMapIterator_Next are needed to get the results from the iteration. If the
/// prefix is not found, the first call to next will return 0
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
    todo!()
}

/// Set timeout limit used for affix queries
///
/// C equivalent:
/// ```c
/// void TrieMapIterator_SetTimeout(TrieMapIterator *it, struct timespec timeout);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMapIterator_SetTimeout(it: *mut TrieMapIterator, timeout: libc::timespec) {
    todo!()
}

/// Free a trie iterator
///
///  C equivalent:
/// ```c
/// void TrieMapIterator_Free(TrieMapIterator *it);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMapIterator_Free(it: *mut TrieMapIterator) {
    todo!()
}

/// Iterate to the next matching entry in the trie. Returns 1 if we can continue,
/// or 0 if we're done and should exit
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
    todo!()
}

/// Iterate to the next matching entry in the trie. Returns 1 if we can continue,
/// or 0 if we're done and should exit.
/// Used by Contains and Suffix queries.
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
    todo!()
}

/// Iterate to the next matching entry in the trie. Returns 1 if we can continue,
/// or 0 if we're done and should exit.
/// Used by Wildcard queries.
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
    todo!()
}

/// C equivalent:
/// ```c
/// void TrieMap_IterateRange(TrieMap *trie, const char *min, int minlen, bool includeMin,
///   const char *max, int maxlen, bool includeMax,
///   TrieMapRangeCallback callback, void *ctx);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_IterateRange(
    trie: *mut TrieMap,
    min: *const c_char,
    minlen: c_int,
    includeMin: bool,
    max: *const c_char,
    maxlen: c_int,
    includeMax: bool,
    callback: TrieMapRangeCallback,
    ctx: *mut c_void,
) {
    todo!()
}

/// C equivalent:
/// ```c
/// void *TrieMap_RandomValueByPrefix(TrieMap *t, const char *prefix, tm_len_t pflen);
/// ```
#[unsafe(no_mangle)]
unsafe extern "C" fn TrieMap_RandomValueByPrefix(
    t: *mut TrieMap,
    prefix: *const c_char,
    pflen: tm_len_t,
) -> *mut c_void {
    todo!()
}
