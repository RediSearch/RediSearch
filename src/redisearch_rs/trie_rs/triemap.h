#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Used by TrieMapIterator to determine type of query.
 *
 * C equivalent:
 * ```c
 * typedef enum {
 *     TM_PREFIX_MODE = 0,
 *     TM_CONTAINS_MODE = 1,
 *     TM_SUFFIX_MODE = 2,
 *     TM_WILDCARD_MODE = 3,
 *     TM_WILDCARD_FIXED_LEN_MODE = 4,
 *   } tm_iter_mode;
 */
typedef enum tm_iter_mode {
  TM_PREFIX_MODE = 0,
  TM_CONTAINS_MODE = 1,
  TM_SUFFIX_MODE = 2,
  TM_WILDCARD_MODE = 3,
  TM_WILDCARD_FIXED_LEN_MODE = 4,
} tm_iter_mode;

/**
 * Opaque type TrieMapIterator. Obtained from calling [`TrieMap_Iterate`].
 */
typedef struct TrieMapIterator TrieMapIterator;

/**
 * Opaque type TrieMapResultBuf. Holds the results of [`TrieMap_FindPrefixes`].
 */
typedef struct TrieMapResultBuf TrieMapResultBuf;

/**
 * A trie data structure that maps keys of type `&[c_char]` to values.
 * The node labels and children are stored in a [`LowMemoryThinVec`],
 * so as to minimize memory usage.
 */
typedef struct TrieMap_____c_void TrieMap_____c_void;

/**
 * Opaque type TrieMap. Can be instantiated with [`NewTrieMap`].
 */
typedef struct TrieMap_____c_void MyTrieMap;

/**
 * Holds the length of a key string in the trie.
 *
 * C equivalent:
 * ```c
 * typedef uint16_t tm_len_t;
 * ```
 */
typedef uint16_t tm_len_t;

/**
 * Callback type for passing to [`TrieMap_Add`].
 *
 * C equivalent:
 * ```c
 * typedef void *(*TrieMapReplaceFunc)(void *oldval, void *newval);
 * ```
 */
typedef void *(*TrieMapReplaceFunc)(void *oldval, void *newval);

/**
 * Callback type for passing to [`TrieMap_Delete`].
 *
 * C equivalent:
 * ```c
 * typedef void (*freeCB)(void *);
 * ```
 */
typedef void (*freeCB)(void*);

/**
 * Callback type for passing to [`TrieMap_IterateRange`].
 *
 * C equivalent:
 * ```c
 * typedef void(TrieMapRangeCallback)(const char *, size_t, void *, void *);
 * ```
 */
typedef void (*TrieMapRangeCallback)(const char*, size_t, void*, void*);

/**
 * This special pointer is returned when [`TrieMap_Find`] cannot find anything.
 *
 * C equivalent:
 * ```c
 * void *TRIEMAP_NOTFOUND = "NOT FOUND";
 * ```
 */
extern void *TRIEMAP_NOTFOUND;

/**
 * Default iteration mode for [`TrieMap_Iterate`].
 */
extern const enum tm_iter_mode TM_ITER_MODE_DEFAULT;

/**
 * Free the [`TrieMapResultBuf`] and its contents.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `buf` must point to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`] and cannot be NULL.
 *
 * C equivalent:
 * ```c
 * void TrieMapResultBuf_Free(TrieMapResultBuf *buf);
 * ```
 */
void TrieMapResultBuf_Free(struct TrieMapResultBuf *buf);

/**
 * Get the data from the TrieMapResultBuf as an array of values.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `buf` must point to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`] and cannot be NULL.
 *
 * C equivalent:
 * ```c
 * void **TrieMapResultBuf_Data(TrieMapResultBuf *buf);
 * ```
 */
void **TrieMapResultBuf_Data(struct TrieMapResultBuf *buf);

/**
 * Get the length of the TrieMapResultBuf.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `buf` must point to a valid TrieMapResultBuf initialized by [`TrieMap_FindPrefixes`] and cannot be NULL.
 *
 * C equivalent:
 * ```c
 * size_t TrieMapResultBuf_Len(TrieMapResultBuf *buf);
 * ```
 */
size_t TrieMapResultBuf_Len(struct TrieMapResultBuf *buf);

/**
 * Create a new [`TrieMap`]. Returns an opaque pointer to the newly created trie.
 *
 * To free the trie, use [`TrieMap_Free`].
 *
 * C equivalent:
 * ```c
 * TrieMap *NewTrieMap();
 * ```
 */
MyTrieMap *NewTrieMap(void);

/**
 * Add a new string to a trie. Returns 1 if the key is new to the trie or 0 if
 * it already existed.
 *
 * If `cb` is given, instead of replacing and freeing the value using `rm_free`,
 * we call the callback with the old and new value, and the function should return the value to set in the
 * node, and take care of freeing any unwanted pointers. The returned value
 * can be NULL and doesn't have to be either the old or new value.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 *  - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
 *  - `str` can be NULL only if `len == 0`. It is not necessarily NULL-terminated.
 *  - `len` can be 0. If so, `str` is regarded as an empty string.
 *  - `value` holds a pointer to the value of the record, which can be NULL
 *  - `cb` must not free the value it returns
 *  - The Redis allocator must be initialized before calling this function,
 *    and `RedisModule_Free` must not get mutated while running this function.
 *
 * C equivalent:
 * ```c
 * int TrieMap_Add(TrieMap *t, const char *str, tm_len_t len, void *value, TrieMapReplaceFunc cb);
 * ```
 */
int TrieMap_Add(MyTrieMap *t,
                const char *str,
                tm_len_t len,
                void *value,
                TrieMapReplaceFunc cb);

/**
 * Find the entry with a given string and length, and return its value, even if
 * that was NULL.
 *
 * Returns the tree root if the key is empty.
 *
 * NOTE: If the key does not exist in the trie, we return the special
 * constant value TRIEMAP_NOTFOUND, so checking if the key exists is done by
 * comparing to it, because NULL can be a valid result.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
 * - `str` can be NULL only if `len == 0`. It is not necessarily NULL-terminated.
 * - `len` can be 0. If so, `str` is regarded as an empty string.
 * - The value behind the returned pointer must not be destroyed by the caller.
 *   Use [`TrieMap_Delete`] to remove it instead.
 * - In case [`TRIE_NOTFOUND`] is returned, the key does not exist in the trie,
 *   and the pointer must not be dereferenced.
 *
 * C equivalent:
 * ```c
 * void *TrieMap_Find(TrieMap *t, const char *str, tm_len_t len);
 * ```
 */
void *TrieMap_Find(MyTrieMap *t, const char *str, tm_len_t len);

/**
 * Find nodes that have a given prefix. Results are placed in an array.
 * The `results` buffer is initialized by this function using the Redis allocator
 * and should be freed by calling [`TrieMapResultBuf_Free`].
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
 * - `str` can be NULL only if `len == 0`. It is not necessarily NULL-terminated.
 * - `len` can be 0. If so, `str` is regarded as an empty string.
 *
 * C equivalent:
 * ```c
 * int TrieMap_FindPrefixes(TrieMap *t, const char *str, tm_len_t len, TrieMapResultBuf *results);
 * ```
 */
int TrieMap_FindPrefixes(MyTrieMap *t,
                         const char *str,
                         tm_len_t len,
                         struct TrieMapResultBuf *results);

/**
 * Mark a node as deleted. It also optimizes the trie by merging nodes if
 * needed. If freeCB is given, it will be used to free the value (not the node)
 * of the deleted node. If it doesn't, we simply call free().
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
 * - `str` can be NULL only if `len == 0`. It is not necessarily NULL-terminated.
 * - `len` can be 0. If so, `str` is regarded as an empty string.
 * - if `func` is not NULL, it must be a valid function pointer of the type [`freeCB`].
 *
 * C equivalent:
 * ```c
 * int TrieMap_Delete(TrieMap *t, const char *str, tm_len_t len, freeCB func);
 * ```
 */
int TrieMap_Delete(MyTrieMap *t, const char *str, tm_len_t len, freeCB func);

/**
 * Free the trie's root and all its children recursively. If freeCB is given, we
 * call it to free individual payload values (not the nodes). If not, free() is used instead.
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
 *
 * C equivalent:
 * ```c
 * void TrieMap_Free(TrieMap *t, freeCB func);
 * ```
 */
void TrieMap_Free(MyTrieMap *t, freeCB func);

/**
 * Determines the amount of memory used by the trie in bytes.
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
 *
 *  C equivalent:
 * ```c
 * size_t TrieMap_MemUsage(TrieMap *t);
 * ```
 */
uintptr_t TrieMap_MemUsage(MyTrieMap *t);

/**
 * Iterate the trie for all the suffixes of a given prefix. This returns an
 * iterator object even if the prefix was not found, and subsequent calls to
 * TrieMapIterator_Next are needed to get the results from the iteration. If the
 * prefix is not found, the first call to next will return 0.
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
 *
 * C equivalent:
 * ```c
 * TrieMapIterator *TrieMap_Iterate(TrieMap *t, const char *prefix, tm_len_t prefixLen);
 * ```
 */
struct TrieMapIterator *TrieMap_Iterate(MyTrieMap *t,
                                        const char *prefix,
                                        tm_len_t prefix_len,
                                        enum tm_iter_mode iter_mode);

/**
 * Set timeout limit used for affix queries. This timeout is checked in
 * [`TrieMapIterator_Next`], [`TrieMapIterator_NextContains`], and [`TrieMapIterator_NextWildcard`],
 * which will return `0` if the timeout is reached.
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `it` must point to a valid TrieMapIterator obtained from [`TrieMap_Iterate`] and cannot be NULL.
 *
 * C equivalent:
 * ```c
 * void TrieMapIterator_SetTimeout(TrieMapIterator *it, struct timespec timeout);
 * ```
 */
void TrieMapIterator_SetTimeout(struct TrieMapIterator *it,
                                timespec timeout);

/**
 * Free a trie iterator
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `it` must point to a valid TrieMapIterator obtained from [`TrieMap_Iterate`] and cannot be NULL.
 *
 *  C equivalent:
 * ```c
 * void TrieMapIterator_Free(TrieMapIterator *it);
 * ```
 */
void TrieMapIterator_Free(struct TrieMapIterator *it);

/**
 * Iterate to the next matching entry in the trie. Returns 1 if we can continue,
 * or 0 if we're done and should exit
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `it` must point to a valid TrieMapIterator obtained from [`TrieMap_Iterate`] and cannot be NULL.
 * - `ptr` must point to a valid pointer to a C string, which will be set to the current key.
 * - `len` must point to a valid `tm_len_t` which will be set to the length of the current key.
 * - `value` must point to a valid pointer, which will be set to the value of the current key.
 *
 * C equivalent:
 * ```c
 * int TrieMapIterator_Next(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value);
 * ```
 */
int TrieMapIterator_Next(struct TrieMapIterator *it,
                         char **ptr,
                         tm_len_t *len,
                         void **value);

/**
 * Iterate to the next matching entry in the trie. Returns 1 if we can continue,
 * or 0 if we're done and should exit.
 * Used by Contains and Suffix queries.
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `it` must point to a valid TrieMapIterator obtained from [`TrieMap_Iterate`] and cannot be NULL.
 * - `ptr` must point to a valid pointer to a C string, which will be set to the current key.
 * - `len` must point to a valid `tm_len_t` which will be set to the length of the current key.
 * - `value` must point to a valid pointer, which will be set to the value of the current key.
 *
 *  C equivalent:
 * ```c
 * int TrieMapIterator_NextContains(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value);
 * ```
 */
int TrieMapIterator_NextContains(struct TrieMapIterator *it,
                                 char **ptr,
                                 tm_len_t *len,
                                 void **value);

/**
 * Iterate to the next matching entry in the trie. Returns 1 if we can continue,
 * or 0 if we're done and should exit.
 * Used by Wildcard queries.
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `it` must point to a valid TrieMapIterator obtained from [`TrieMap_Iterate`] and cannot be NULL.
 * - `ptr` must point to a valid pointer to a C string, which will be set to the current key.
 * - `len` must point to a valid `tm_len_t` which will be set to the length of the current key.
 * - `value` must point to a valid pointer, which will be set to the value of the current key.
 *
 * C equivalent:
 * ```c
 * int TrieMapIterator_NextWildcard(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value);
 * ```
 */
int TrieMapIterator_NextWildcard(struct TrieMapIterator *it,
                                 char **ptr,
                                 tm_len_t *len,
                                 void **value);

/**
 * Iterate the trie for all the suffixes of a given prefix. This returns an
 * iterator object even if the prefix was not found, and subsequent calls to
 * TrieMapIterator_Next are needed to get the results from the iteration. If the
 * prefix is not found, the first call to next will return 0.
 *
 * If `minLen` is 0, `min` is regarded as an empty string. It `minlen` is -1, the itaration starts from the beginning of the trie.
 * If `maxLen` is 0, `max` is regarded as an empty string. If `maxlen` is -1, the iteration goes to the end of the trie.
 * `includeMin` and `includeMax` determine whether the min and max values are included in the iteration.
 *
 * The passed [`TrieMapRangeCallback`] function is called for each key found,
 * passing the key and its length, the value, and the `ctx` pointer passed to this
 * function.
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `trie` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
 * - `min` can be NULL only if `minlen == 0` or `minlen == -1`. It is not necessarily NULL-terminated.
 * - `minlen` can be 0. If so, `min` is regarded as an empty string.
 * - `max` can be NULL only if `maxlen == 0` or `maxlen == -1`. It is not necessarily NULL-terminated.
 * - `maxlen` can be 0. If so, `max` is regarded as an empty string.
 *
 * C equivalent:
 * ```c
 * void TrieMap_IterateRange(TrieMap *trie, const char *min, int minlen, bool includeMin,
 *   const char *max, int maxlen, bool includeMax,
 *   TrieMapRangeCallback callback, void *ctx);
 * ```
 */
void TrieMap_IterateRange(TrieMap *trie,
                          const char *min,
                          int minlen,
                          bool includeMin,
                          const char *max,
                          int maxlen,
                          bool includeMax,
                          TrieMapRangeCallback callback,
                          void *ctx);

/**
 * Returns a random value for a key that has a given prefix.
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
 * - `prefix` can be NULL only if `pflen == 0`. It is not necessarily NULL-terminated.
 *
 *  C equivalent:
 * ```c
 * void *TrieMap_RandomValueByPrefix(TrieMap *t, const char *prefix, tm_len_t pflen);
 * ```
 */
void *TrieMap_RandomValueByPrefix(TrieMap *t, const char *prefix, tm_len_t pflen);
