/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include <stddef.h>
#include "redisearch.h"
#include "iterators/iterator_api.h"
#include "geometry_types.h"

#ifdef __cplusplus
extern "C" {
#endif

GeometryIndex *GeometryIndexFactory(GEOMETRY_COORDS tag);
const GeometryApi *GeometryApi_Get(const GeometryIndex *index);
const char *GeometryCoordsToName(GEOMETRY_COORDS tag);

// Benchmark/test-only constructor. Builds a GeoShape query iterator directly
// from a caller-provided list of document IDs, bypassing the R-tree spatial
// query, so the iterator's read/skip_to machinery can be micro-benchmarked
// against the Rust re-implementation (`NewGeometryQueryIterator`). The IDs are
// copied into the iterator's own storage (the original `ids` buffer is left
// untouched and is not adopted). `allocated` must point to a `size_t` that
// outlives the returned iterator: the iterator's allocator holds a reference to
// it for memory accounting. Mirrors the production construction in
// `RTree::query` (timeout checks skipped when `sctx->time.skipTimeoutChecks`).
QueryIterator *NewGeometryQueryIterator_Bench(const RedisSearchCtx *sctx,
                                              const FieldFilterContext *filterCtx, t_docId *ids,
                                              size_t num, size_t *allocated);

struct GeometryApi {
  void (*freeIndex)(GeometryIndex *index);
  int (*addGeomStr)(GeometryIndex *index, GEOMETRY_FORMAT format, const char *str, size_t len,
                    t_docId docId, RedisModuleString **err_msg);
  int (*delGeom)(GeometryIndex *index, t_docId docId);
  QueryIterator *(*query)(const RedisSearchCtx *sctx, const FieldFilterContext*,
                          const GeometryIndex *index, QueryType queryType, GEOMETRY_FORMAT format,
                          const char *str, size_t len, RedisModuleString **err_msg);
  void (*dump)(const GeometryIndex *index, RedisModuleCtx *ctx);
  size_t (*report)(const GeometryIndex *index);
};

#ifdef __cplusplus
}
#endif
