/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "../redismodule.h"
#include "geometry.h"

typedef void* GEOMETRY;

typedef enum {
  GEOMETRY_FORMAT_NONE = 0,
  GEOMETRY_FORMAT_WKT = 1,
  GEOMETRY_FORMAT_GEOJSON = 2,
} GEOMETRY_FORMAT;

typedef enum {
  GEOMETRY_LIB_TYPE_NONE = 0,
  GEOMETRY_LIB_TYPE_BOOST_GEOMETRY = 1,
  GEOMETRY_LIB_TYPE_S2 = 2,
  GEOMETRY_LIB_TYPE__NUM,
} GEOMETRY_LIB_TYPE;

typedef enum {
  GEOMETRY_QUERY_TYPE_WITHIN = 0,
  GEOMETRY_QUERY_TYPE_CONTAINS = 1,
  GEOMETRY_QUERY_TYPE_DISTANCE = 2,
  GEOMETRY_QUERY_TYPE_NONE = 3,
} GEOMETRY_QUERY_TYPE;

typedef struct {
    GEOMETRY (*createGeom)(GEOMETRY_FORMAT format, const char *str, size_t len, RedisModuleString **err_msg);
    void (*freeGeom)(GEOMETRY);
    //IndexIterator* (*query)(GEOMETRY geom, predicate t, void* params);
    //char *(*geomAsCStr)(GEOMETRY geom, GEOMETRY_FORMAT format);
    //...
} GeometryApi;

GeometryApi* GeometryApi_GetOrCreate(GEOMETRY_LIB_TYPE type, void *);
void GeometryApi_Free();
