/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "geometry_api.h"
#include "rtree.hpp"

#include <array>   // std::array
#include <variant> // std::variant, std::monostate, std::get

#define X(variant) , RTree<variant>
struct GeometryIndex {
  std::variant<std::monostate GEO_VARIANTS(X)> index;
  const GeometryApi *api;
};
#undef X

#define X(variant)                                                                          \
void Index_##variant##_Free(GeometryIndex *idx) {                                           \
  delete idx;                                                                               \
}                                                                                           \
int Index_##variant##_Insert(GeometryIndex *idx, GEOMETRY_FORMAT format, const char *str,   \
                             size_t len, t_docId id, RedisModuleString **err_msg) {         \
  switch (format) {                                                                         \
   case GEOMETRY_FORMAT_WKT:                                                                \
    return !std::get<RTree<variant>>(idx->index).insertWKT(str, len, id, err_msg);          \
   case GEOMETRY_FORMAT_GEOJSON:                                                            \
   default:                                                                                 \
    return 1;                                                                               \
  }                                                                                         \
}                                                                                           \
int Index_##variant##_Remove(GeometryIndex *idx, t_docId id) {                              \
  return std::get<RTree<variant>>(idx->index).remove(id);                                   \
}                                                                                           \
IndexIterator *Index_##variant##_Query(const GeometryIndex *idx, QueryType queryType,       \
                                       GEOMETRY_FORMAT format, const char *str, size_t len, \
                                       RedisModuleString **err_msg) {                       \
  switch (format) {                                                                         \
   case GEOMETRY_FORMAT_WKT:                                                                \
    return std::get<RTree<variant>>(idx->index).query(str, len, queryType, err_msg);        \
   case GEOMETRY_FORMAT_GEOJSON:                                                            \
   default:                                                                                 \
    return nullptr;                                                                         \
  }                                                                                         \
}                                                                                           \
void Index_##variant##_Dump(const GeometryIndex *idx, RedisModuleCtx *ctx) {                \
  std::get<RTree<variant>>(idx->index).dump(ctx);                                           \
}
GEO_VARIANTS(X)
#undef X

const GeometryApi *GeometryApi_Get(const GeometryIndex *idx) {
  return idx->api;
}

#define X(variant)                              \
constexpr GeometryApi GeometryApi_##variant = { \
  .freeIndex = Index_##variant##_Free,          \
  .addGeomStr = Index_##variant##_Insert,       \
  .delGeom = Index_##variant##_Remove,          \
  .query = Index_##variant##_Query,             \
  .dump = Index_##variant##_Dump,               \
};
GEO_VARIANTS(X)
#undef X

#define X(variant)                       \
GeometryIndex *Index_##variant##_New() { \
  return new GeometryIndex{              \
    .index = RTree<variant>{},           \
    .api = &GeometryApi_##variant,       \
  };                                     \
}
GEO_VARIANTS(X)
#undef X

using GeometryConstructor_t = GeometryIndex *(*)();
#define X(variant) \
  [GEOMETRY_COORDS_variant] = Index_##variant##_New,
constexpr std::array<GeometryConstructor_t, GEOMETRY_COORDS__NUM> geometry_ctors_g {{
  GEO_VARIANTS(X)
}};
#undef X

GeometryIndex *GeometryIndexFactory(GEOMETRY_COORDS tag) {
  return geometry_ctors_g[tag]();
}


constexpr std::array<const char *, GEOMETRY_COORDS__NUM> tag_names_g {{
  [GEOMETRY_COORDS_CartesianPoint] = "FLAT",
  [GEOMETRY_COORDS_GeographicPoint] = "SPHERICAL",
  [GEOMETRY_COORDS_CartesianPolygon] = "FLAT",
  [GEOMETRY_COORDS_GeographicPolygon] = "SPHERICAL",
}};

const char *GeometryCoordsToName(GEOMETRY_COORDS tag) {
  return tag_names_g[tag];
}

#define X(variant) \
  RTree<variant>::reportTotal() + 
size_t GeometryTotalMemUsage() {
  return GEO_VARIANTS(X) +0;
}
#undef X
