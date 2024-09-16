/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "geometry/geometry_types.h"
#include "search_ctx.h"
#include "index_iterator.h"

typedef struct GeometryQuery {
    GEOMETRY_FORMAT format;
    QueryType query_type;
    const char *attr;
    const char *str;
    size_t str_len;
} GeometryQuery;

void GeometryQuery_Free(GeometryQuery *geomq);

GeometryIndex *OpenGeometryIndex(RedisModuleCtx *redisCtx, IndexSpec *spec,
                                 RedisModuleKey **idxKey, const FieldSpec *fs);

RedisModuleString *fmtRedisGeometryIndexKey(const RedisSearchCtx *ctx, const char *field);

// Remove indexed data for the given document ID
void GeometryIndex_RemoveId(RedisModuleCtx *ctx, IndexSpec *spec, t_docId id);
