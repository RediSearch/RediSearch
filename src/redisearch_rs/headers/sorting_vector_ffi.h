#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include "sorting_vector.h"

/**
 * An actual [`RsValue`] object
 */
typedef struct RSValue RSValue;

#define RS_SORTABLES_MAX 1024

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Initializes an empty `RSSortingVector`.
 *
 * No heap allocation is performed.
 */
RSSortingVector RSSortingVector_Empty(void);

/**
 * Returns the memory size of the sorting vector.
 *
 * # Safety
 *
 * 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`] or equivalent.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
size_t RSSortingVector_GetMemorySize(const RSSortingVector *vec);

/**
 * Puts a number (double) at the given index in the sorting vector. If a out of bounds occurs it returns silently.
 *
 * # Panics
 *
 * Panics if the `idx` is out of bounds for the vector.
 *
 * # Safety
 *
 * 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`] or equivalent.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void RSSortingVector_PutNum(RSSortingVector *vec, size_t idx, double num);

/**
 * Puts a string at the given index in the sorting vector.
 *
 * # Panics
 *
 * Panics if the `idx` is out of bounds for the vector.
 *
 * # Safety
 *
 * 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`] or equivalent.
 * 2. `str` must be a [valid], non-null pointer to a C string (null-terminated).
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void RSSortingVector_PutStr(RSSortingVector *vec, size_t idx, const char *str);

/**
 * Puts a string at the given index in the sorting vector, the string is normalized before being set.
 *
 * # Panics
 *
 * - Panics if the provided string is invalid UTF-8
 * - Panics if the `idx` is out of bounds for the vector.
 *
 * # Safety
 *
 * 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`] or equivalent.
 * 2. `str` must be a [valid], non-null pointer to a C string (null-terminated).
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void RSSortingVector_PutStrNormalize(RSSortingVector *vec, size_t idx, const char *str);

/**
 * Puts a value at the given index in the sorting vector. If a out of bounds occurs it returns silently.
 *
 * # Panics
 *
 * Panics if the `idx` is out of bounds for the vector.
 *
 * # Safety
 *
 * 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`] or equivalent.
 * 2. `val` must be a [valid], non-null pointer must point to a `RSValue`.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void RSSortingVector_PutRSVal(RSSortingVector *vec, size_t idx, struct RSValue *val);

/**
 * Puts a null at the given index in the sorting vector.  If a out of bounds occurs it returns silently.
 *
 * # Panics
 *
 * Panics if the `idx` is out of bounds for the vector.
 *
 * # Safety
 *
 * 1. The pointer must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`] or equivalent.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void RSSortingVector_PutNull(RSSortingVector *vec, size_t idx);

/**
 * Creates a new `RSSortingVector` with the given length, returned by value.
 *
 * # Panics
 *
 * Panics if `len` is greater than [`RS_SORTABLES_MAX`].
 */
RSSortingVector RSSortingVector_New(size_t len);

/**
 * Deallocates the inner values buffer of an [`RSSortingVector`] and zeros the struct.
 *
 * Each [`SharedRsValue`] element is dropped (decrementing its refcount) and the heap buffer is freed.
 * After this call the pointed-to struct is in the same state as [`RSSortingVector::empty()`].
 * Passing a null pointer is a no-op.
 *
 * # Safety
 *
 * 1. `vec` must be either null or a [valid] pointer to an [`RSSortingVector`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void RSSortingVector_ClearAndDeAlloc(RSSortingVector *vec);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
