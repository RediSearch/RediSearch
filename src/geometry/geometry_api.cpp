/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "geometry_api.h"
#include "rtree.hpp"

#include <array>    // std::array
#include <variant>  // std::variant, std::monostate, std::get

using Cartesian = RediSearch::GeoShape::Cartesian;
using Geographic = RediSearch::GeoShape::Geographic;

#define X(variant) , RediSearch::GeoShape::RTree<variant>
struct GeometryIndex {
  const GeometryApi *api;
  std::size_t allocated;
  std::variant<std::monostate GEO_VARIANTS(X)> index;
};
#undef X

const GeometryApi *GeometryApi_Get(const GeometryIndex *idx) {
  return idx->api;
}

namespace {
#define X(variant)                                                                                 \
  void Index_##variant##_Free(GeometryIndex *idx) {                                                \
    auto a = RediSearch::Allocator::Allocator<GeometryIndex>{};                                    \
    std::destroy_at(idx);                                                                          \
    a.deallocate(idx, 1);                                                                          \
  }                                                                                                \
  int Index_##variant##_Insert(GeometryIndex *idx, GEOMETRY_FORMAT format, const char *str,        \
                               std::size_t len, t_docId id, RedisModuleString **err_msg) {         \
    switch (format) {                                                                              \
      case GEOMETRY_FORMAT_WKT:                                                                    \
        return !std::get<RediSearch::GeoShape::RTree<variant>>(idx->index)                         \
                    .insertWKT(str, len, id, err_msg);                                             \
      case GEOMETRY_FORMAT_GEOJSON:                                                                \
      default:                                                                                     \
        return 1;                                                                                  \
    }                                                                                              \
  }                                                                                                \
  int Index_##variant##_Remove(GeometryIndex *idx, t_docId id) {                                   \
    return std::get<RediSearch::GeoShape::RTree<variant>>(idx->index).remove(id);                  \
  }                                                                                                \
  IndexIterator *Index_##variant##_Query(const GeometryIndex *idx, QueryType queryType,            \
                                         GEOMETRY_FORMAT format, const char *str, std::size_t len, \
                                         RedisModuleString **err_msg) {                            \
    switch (format) {                                                                              \
      case GEOMETRY_FORMAT_WKT:                                                                    \
        return std::get<RediSearch::GeoShape::RTree<variant>>(idx->index)                          \
            .query(str, len, queryType, err_msg);                                                  \
      case GEOMETRY_FORMAT_GEOJSON:                                                                \
      default:                                                                                     \
        return nullptr;                                                                            \
    }                                                                                              \
  }                                                                                                \
  void Index_##variant##_Dump(const GeometryIndex *idx, RedisModuleCtx *ctx) {                     \
    std::get<RediSearch::GeoShape::RTree<variant>>(idx->index).dump(ctx);                          \
  }                                                                                                \
  constexpr GeometryApi GeometryApi_##variant = {                                                  \
      .freeIndex = Index_##variant##_Free,                                                         \
      .addGeomStr = Index_##variant##_Insert,                                                      \
      .delGeom = Index_##variant##_Remove,                                                         \
      .query = Index_##variant##_Query,                                                            \
      .dump = Index_##variant##_Dump,                                                              \
  };                                                                                               \
  GeometryIndex *Index_##variant##_New() {                                                         \
    auto p = RediSearch::Allocator::Allocator<GeometryIndex>{}.allocate(1);                        \
    return std::construct_at(p, &GeometryApi_##variant, sizeof(GeometryIndex),                     \
                             RediSearch::GeoShape::RTree<variant>{p->allocated});                  \
  }
GEO_VARIANTS(X)
#undef X
}  // anonymous namespace

#define X(variant) [GEOMETRY_COORDS_##variant] = Index_##variant##_New,
GeometryIndex *GeometryIndexFactory(GEOMETRY_COORDS tag) {
  using GeometryConstructor_t = GeometryIndex *(*)();
  static constexpr std::array<GeometryConstructor_t, GEOMETRY_COORDS__NUM> geometry_ctors{
      {GEO_VARIANTS(X)}};
  return geometry_ctors[tag]();
}
#undef X

const char *GeometryCoordsToName(GEOMETRY_COORDS tag) {
  static constexpr std::array<const char *, GEOMETRY_COORDS__NUM> tag_names{{
      [GEOMETRY_COORDS_Cartesian] = "FLAT",
      [GEOMETRY_COORDS_Geographic] = "SPHERICAL",
  }};
  return tag_names[tag];
}

std::size_t GeometryTotalMemUsage() {
  return 0;  // TODO
}
