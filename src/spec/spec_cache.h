/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef SPEC_CACHE_H
#define SPEC_CACHE_H

#include "field_spec.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * IndexSpecCache is an immutable, refcounted snapshot of an index's field
 * definitions. It is used by query threads for thread-safe access to field
 * metadata without holding the index lock.
 *
 * If the index schema changes, this object is simply recreated rather
 * than modified, making it immutable.
 *
 * It is freed when its reference count hits 0
 */
typedef struct IndexSpecCache {
  FieldSpec *fields;
  size_t nfields;
  size_t refcount;
} IndexSpecCache;

struct IndexSpec;

/**
 * Retrieves the current spec cache from the index, incrementing its
 * reference count by 1. Use IndexSpecCache_Decref to free
 */
IndexSpecCache *IndexSpec_GetSpecCache(const struct IndexSpec *spec);

/**
 * Decrement the reference count of the spec cache. Should be matched
 * with a previous call of GetSpecCache()
 * Can handle NULL
 */
void IndexSpecCache_Decref(IndexSpecCache *cache);

/**
 * Build a new spec cache from the given index spec. The caller takes
 * ownership of the returned cache (refcount starts at 1).
 * Assumes the spec is properly locked before calling.
 */
IndexSpecCache *IndexSpec_BuildSpecCache(const struct IndexSpec *spec);

#ifdef __cplusplus
}
#endif

#endif // SPEC_CACHE_H
