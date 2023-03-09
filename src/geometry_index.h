/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "geometry/geometry_types.h"
#include "search_ctx.h"
#include "index_iterator.h"

typedef void* GeometryIndex;

typedef struct GeometryQuery {
    GEOMETRY_FORMAT format;
    const char *attr;
    const char *str;
    size_t str_len;
    enum QueryType query_type;
    
} GeometryQuery;

void GeometryQuery_Free(GeometryQuery *geomq);

GeometryIndex OpenGeometryIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                 RedisModuleKey **idxKey, const FieldSpec *fs);

RedisModuleString *fmtRedisGeometryIndexKey(RedisSearchCtx *ctx, const char *field);