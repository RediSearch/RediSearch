/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#define GEO_VARIANTS(X) X(Cartesian) X(Geographic)

typedef struct GeometryIndex GeometryIndex;
typedef struct GeometryApi GeometryApi;

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

#define X(variant) \
  GEOMETRY_COORDS_##variant,
typedef enum {
  GEO_VARIANTS(X)
  GEOMETRY_COORDS__NUM,
} GEOMETRY_COORDS;
#undef X

typedef enum QueryType {
  UNKNOWN_QUERY,
  CONTAINS,
  WITHIN,
  DISJOINT,
  INTERSECTS,
} QueryType;
