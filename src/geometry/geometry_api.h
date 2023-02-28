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
    GEOMETRY (*createGeom)(GEOMETRY_FORMAT format, const char *str, size_t len, RedisModuleString **err_msg);
    struct GeometryIndex* (*createIndex)();
    void (*freeIndex)(GeometryIndex index);
    int (*addGeomStr)(struct GeometryIndex *index, GEOMETRY_FORMAT format, const char *str, size_t len, RedisModuleString **err_msg);
    int (*addGeom)(struct GeometryIndex *index, GEOMETRY geom);
    int (*delGeom)(struct GeometryIndex *index, GEOMETRY geom, void *data);
    IndexIterator* (*query)(struct GeometryIndex *index, enum QueryType queryType, GEOMETRY_FORMAT format, const char *str, size_t len);
} GeometryApi; // TODO: GEOMETRY Rename to GeometryIndex

GeometryApi* GeometryApi_GetOrCreate(GEOMETRY_LIB_TYPE type, void *);
void GeometryApi_Free();
