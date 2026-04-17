#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include "redisearch.h"
#include "score_explain.h"

typedef SearchResult SearchResult;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Returns a newly created [`SearchResult`].
 */
SearchResult SearchResult_New(void);

/**
 * Overrides the contents of `dst` with those from `src` taking ownership of `src`.
 * Ensures proper cleanup of any existing data in `dst`.
 *
 * # Safety
 *
 * 1. `dst` must be a [valid], non-null pointer to a [`SearchResult`].
 * 2. `src` must be a [valid], non-null pointer to a [`SearchResult`].
 * 3. `src` must not be used again.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void SearchResult_Override(SearchResult *dst, SearchResult *src);

/**
 * Clears the [`SearchResult`] pointed to by `res`, removing all values from its [`RLookupRow`][ffi::RLookupRow].
 * This has no effect on the allocated capacity of the lookup row.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void SearchResult_Clear(SearchResult *res);

/**
 * Destroys the [`SearchResult`] pointed to by `res` releasing any resources owned by it.
 * This method takes ownership of the search result, therefore the pointer must **must not** be used again after this function is called.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 * 2. `res` **must not** be used again after this function is called.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void SearchResult_Destroy(SearchResult *res);

/**
 * Moves the contents the [`SearchResult`] pointed to by `res` into a new heap allocation.
 * This method takes ownership of the search result, therefore the pointer must **must not** be used again after this function is called.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 * 2. `res` **must not** be used again after this function is called.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
SearchResult *SearchResult_AllocateMove(SearchResult *res);

/**
 * Destroys the [`SearchResult`] pointed to by `res` releasing any resources owned by it.
 * This method takes ownership of the search result, therefore the pointer must **must not** be used again after this function is called.
 *
 * # Safety
 *
 * 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
 * 2. `res` **must not** be used again after this function is called.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void SearchResult_DeallocateDestroy(SearchResult *res);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
