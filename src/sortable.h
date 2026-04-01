/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "value.h"

#ifdef __cplusplus
extern "C" {
#endif

// Maximum number of sortables
#define RS_SORTABLES_MAX 1024 // aligned with SPEC_MAX_FIELDS

#pragma pack(2)
typedef struct RSSortingVector {
  uint16_t len;      // Should be able to hold RS_SORTABLES_MAX-1 (requires 10 bits today)
  RSValue *values[];
} RSSortingVector;
#pragma pack()

/**
 * Creates a new `RSSortingVector` with the given length.
 *
 * # Panics
 *
 * Panics if `len` is greater than [`RS_SORTABLES_MAX`].
 */
RSSortingVector *RSSortingVector_New(size_t len);

/**
 * Reduces the refcount of every `RSValue` and frees the memory allocated for an `RSSortingVector`.
 * Called by the C code to deallocate the vector.
 *
 * # Safety
 *
 * 1. `vec` must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
 * 2. `vec` **must not** be used again after this function is called.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void RSSortingVector_Free(RSSortingVector *vec);

/**
 * Gets a RSValue from the sorting vector at the given index.
 *
 * # Panics
 *
 * Panics if the `idx` is out of bounds for the vector.
 *
 * # Safety
 *
 * 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
RSValue *RSSortingVector_Get(const RSSortingVector *vec, size_t idx);

/**
 * Returns the length of the sorting vector.
 *
 * # Safety
 *
 * 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
static inline size_t RSSortingVector_Length(const RSSortingVector *vec) {
    return vec->len;
}

/**
 * Puts a number (double) at the given index in the sorting vector. If a out of bounds occurs it returns silently.
 *
 * # Panics
 *
 * Panics if the `idx` is out of bounds for the vector.
 *
 * # Safety
 *
 * 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
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
 * 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
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
 * 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
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
 * 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
 * 2. `val` must be a [valid], non-null pointer must point to a `RSValue`.
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void RSSortingVector_PutRSVal(RSSortingVector *vec, size_t idx, RSValue *val);

/**
 * Puts a null at the given index in the sorting vector.  If a out of bounds occurs it returns silently.
 *
 * # Panics
 *
 * Panics if the `idx` is out of bounds for the vector.
 *
 * # Safety
 *
 * 1. The pointer must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
void RSSortingVector_PutNull(RSSortingVector *vec, size_t idx);

/**
 * Returns the memory size of the sorting vector.
 *
 * # Safety
 *
 * 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
 *
 * [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
 */
size_t RSSortingVector_GetMemorySize(const RSSortingVector *vec);

/* Load a sorting vector from RDB. Used by legacy RDB load only */
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb);

/* Normalize sorting string for storage. This folds everything to unicode equivalent strings. The
 * allocated return string needs to be freed later */
char *normalizeStr(const char *str);

#ifdef __cplusplus
}
#endif
