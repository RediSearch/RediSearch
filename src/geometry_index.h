/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "geometry/geometry_api.h"
#include "search_ctx.h"
#include "index_iterator.h"

typedef struct GeometryQuery {
    GEOMETRY_FORMAT format;
    const char *attr;
    const char *str;
    size_t len;
} GeometryQuery;

void GeometryQuery_Free(GeometryQuery *geomq);

IndexIterator* NewGeometryIterator(RedisSearchCtx *ctx, GeometryQuery *geomq);
