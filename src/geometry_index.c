/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "geometry_index.h"
#include "rmalloc.h"

void GeometryQuery_Free(GeometryQuery *geomq) {
    rm_free(geomq);
}

IndexIterator* NewGeometryIterator(RedisSearchCtx *ctx, GeometryQuery *geomq) {
  // TODO: GEOMETRY
  return NULL;
}