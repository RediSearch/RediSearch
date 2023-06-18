/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "geometry_api.h"
#include "rtree.hpp"
#include <array>
#include <variant>

struct GeometryIndex {
  std::variant<
#define X(variant) RTree<variant>,
      GEO_VARIANTS(X)
#undef X
          std::monostate>
      index;
  const GeometryApi *api;
};

#define X(variant)                                \
  constexpr GeometryApi GeometryApi_##variant = { \
      .freeIndex = Index_##variant##_Free,        \
      .addGeomStr = Index_##variant##_Insert,     \
      .delGeom = Index_##variant##_Remove,        \
      .query = Index_##variant##_Query,           \
      .dump = Index_##variant##_Dump,             \
  };
GEO_VARIANTS(X)
#undef X

using GeometryCtor = GeometryIndex *(*)();
constexpr std::array<GeometryCtor, GEOMETRY_COORDS__NUM> geometry_ctors_g{
#define X(variant) /* [GEOMETRY_COORDS_variant] = */ Index_##variant##_New,
    GEO_VARIANTS(X)
#undef X
};

constexpr std::array<const GeometryApi *, GEOMETRY_COORDS__NUM> geometry_apis_g{
#define X(variant) /*[GEOMETRY_COORDS_variant] = */ &GeometryApi_##variant,
    GEO_VARIANTS(X)
#undef X
};

const GeometryApi *GeometryApi_Get(GEOMETRY_COORDS tag, [[maybe_unused]] void *ctx) {
  return geometry_apis_g[tag];
}

GeometryIndex *GeometryIndexFactory(GEOMETRY_COORDS tag) {
  return geometry_ctors_g[tag]();
}

#define X(variant)                                                                            \
  GeometryIndex *Index_##variant##_New() {                                                    \
    return new GeometryIndex{                                                                 \
        .index = RTree<variant>{},                                                            \
        .api = GeometryApi_Get(GEOMETRY_COORDS_##variant, nullptr),                           \
    };                                                                                        \
  }                                                                                           \
  void Index_##variant##_Free(GeometryIndex *idx) {                                           \
    delete idx;                                                                               \
  }                                                                                           \
  int Index_##variant##_Insert(GeometryIndex *idx, GEOMETRY_FORMAT format, const char *str,   \
                               size_t len, t_docId id, RedisModuleString **err_msg) {         \
    switch (format) {                                                                         \
      case GEOMETRY_FORMAT_WKT:                                                               \
        return !std::get<RTree<variant>>(idx->index).insertWKT(str, len, id, err_msg);        \
      case GEOMETRY_FORMAT_GEOJSON:                                                           \
      default:                                                                                \
        return 1;                                                                             \
    }                                                                                         \
  }                                                                                           \
  int Index_##variant##_Remove(GeometryIndex *idx, t_docId id) {                              \
    return std::get<RTree<variant>>(idx->index).remove(id);                                   \
  }                                                                                           \
  IndexIterator *Index_##variant##_Query(GeometryIndex *idx, QueryType queryType,             \
                                         GEOMETRY_FORMAT format, const char *str, size_t len, \
                                         RedisModuleString **err_msg) {                       \
    switch (format) {                                                                         \
      case GEOMETRY_FORMAT_WKT:                                                               \
        return std::get<RTree<variant>>(idx->index).query(str, len, queryType, err_msg);      \
      case GEOMETRY_FORMAT_GEOJSON:                                                           \
      default:                                                                                \
        return nullptr;                                                                       \
    }                                                                                         \
  }                                                                                           \
  void Index_##variant##_Dump(GeometryIndex *idx, RedisModuleCtx *ctx) {                      \
    std::get<RTree<variant>>(idx->index).dump(ctx);                                           \
  }

GEO_VARIANTS(X)
#undef X

size_t GeometryTotalMemUsage() {
  return RTree<Cartesian>::reportTotal() + RTree<Geographic>::reportTotal();
}
