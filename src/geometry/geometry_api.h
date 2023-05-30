/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "redismodule.h"
#include "geometry.h"
#include "geometry_index.h"

typedef struct {
    struct GeometryIndex* (*createIndex)();
    void (*freeIndex)(GeometryIndex *index);
    int (*addGeomStr)(GeometryIndex *index, GEOMETRY_FORMAT format, const char *str, size_t len, t_docId docId, RedisModuleString **err_msg);
    int (*delGeom)(GeometryIndex *index, t_docId docId);
    IndexIterator* (*query)(GeometryIndex *index, enum QueryType queryType, GEOMETRY_FORMAT format, const char *str, size_t len, RedisModuleString **err_msg);
    void (*dump)(GeometryIndex *index, RedisModuleCtx *ctx);
} GeometryApi; // TODO: GEOMETRY Rename to GeometryIndex

#ifdef __cplusplus
extern "C" {
#endif

GeometryApi* GeometryApi_GetOrCreate(GEOMETRY_LIB_TYPE type, void *);
void GeometryApi_Free();

// Return the total memory usage of all Geometry indices
size_t GeometryTotalMemUsage();

#ifdef __cplusplus
} // extrern "C"
#endif
