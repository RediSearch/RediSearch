/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

typedef void* GEOMETRY; // TODO: GEOMETRY Not uppercase

typedef enum {
  GEOMETRY_LIB_TYPE_NONE = 0,
  GEOMETRY_LIB_TYPE_BOOST_GEOMETRY = 1,
  GEOMETRY_LIB_TYPE_S2 = 2,
  GEOMETRY_LIB_TYPE__NUM,
} GEOMETRY_LIB_TYPE; // TODO: GEOMETRY Not uppercase

typedef enum {
  GEOMETRY_FORMAT_NONE = 0,
  GEOMETRY_FORMAT_WKT = 1,
  GEOMETRY_FORMAT_GEOJSON = 2,
} GEOMETRY_FORMAT; // TODO: GEOMETRY Not uppercase

enum QueryType {
  CONTAINS,
  WITHIN,
};