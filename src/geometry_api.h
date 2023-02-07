/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "redismodule.h"
#include "geometry/geometry.h"

typedef void* GEOMETRY;

typedef enum {
  GEOMETRY_FORMAT_NONE = 0,
  GEOMETRY_FORMAT_WKT = 1,
  GEOMETRY_FORMAT_GEOJSON = 2,
} GEOMETRY_FORMAT;

typedef enum {
  GEOMETRY_LIB_TYPE_BOOST_GEOMETRY = 0,
  GEOMETRY_LIB_TYPE_S2 = 1,
} GEOMETRY_LIB_TYPE;

typedef enum {
  GEOMETRY_QUERY_TYPE_NONE = 0,
  GEOMETRY_QUERY_TYPE_WITHIN = 1,
  GEOMETRY_QUERY_TYPE_CONTAINS = 2,
  GEOMETRY_QUERY_TYPE_DISTANCE = 2,

} GEOMETRY_QUERY_TYPE;

typedef struct {
    GEOMETRY (*createGeom)(GEOMETRY_FORMAT format, const char *str, size_t len, RedisModuleString **err_msg);
    void (*freeGeom)(GEOMETRY);
    //IndexIterator* (*query)(GEOMETRY geom, predicate t, void* params);
    //char *(*geomAsCStr)(GEOMETRY geom, GEOMETRY_FORMAT format);
    //...
} GeometryApi;

#ifdef __cplusplus
extern "C" {
#endif

GeometryApi* GeometryApi_Create(GEOMETRY_LIB_TYPE type, void *pdata);

#ifdef __cplusplus
} // extern "C"
#endif
