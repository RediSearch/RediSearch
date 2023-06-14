/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "geometry_api.h"
#include "rtree.hpp"

#define X(variant)                                \
  constexpr GeometryApi GeometryApi_##variant = { \
      .createIndex = Index_##variant##_New,       \
      .freeIndex = Index_##variant##_Free,        \
      .addGeomStr = Index_##variant##_Insert,     \
      .delGeom = Index_##variant##_Remove,        \
      .query = Index_##variant##_Query,           \
      .dump = Index_##variant##_Dump,             \
  };
GEO_VARIANTS(X)
#undef X

constexpr GeometryApi geometry_apis_g[GEOMETRY_TAG__NUM] = {
    [GEOMETRY_TAG_Cartesian] = GeometryApi_Cartesian,
    [GEOMETRY_TAG_Geographic] = GeometryApi_Geographic,
};

#define X(variant)                                                                            \
  GeometryIndex *Index_##variant##_New() {                                                    \
    return new GeometryIndex{                                                                 \
        .tag = GEOMETRY_TAG_##variant,                                                        \
        .index = static_cast<void *>(new RTree_##variant{}),                                  \
        .vptr = &geometry_apis_g[GEOMETRY_TAG_##variant],                                     \
    };                                                                                        \
  }                                                                                           \
  void Index_##variant##_Free(GeometryIndex *idx) {                                           \
    delete static_cast<RTree<variant> *>(idx->index);                                         \
    delete idx;                                                                               \
  }                                                                                           \
  int Index_##variant##_Insert(GeometryIndex *idx, GEOMETRY_FORMAT format, const char *str,   \
                               size_t len, t_docId id, RedisModuleString **err_msg) {         \
    switch (format) {                                                                         \
      case GEOMETRY_FORMAT_WKT:                                                               \
        return !static_cast<RTree<variant> *>(idx->index)->insertWKT(str, len, id, err_msg);  \
      case GEOMETRY_FORMAT_GEOJSON:                                                           \
      default:                                                                                \
        return 1;                                                                             \
    }                                                                                         \
  }                                                                                           \
  int Index_##variant##_Remove(GeometryIndex *idx, t_docId id) {                              \
    return static_cast<RTree<variant> *>(idx->index)->remove(id);                             \
  }                                                                                           \
  IndexIterator *Index_##variant##_Query(GeometryIndex *index, QueryType queryType,           \
                                         GEOMETRY_FORMAT format, const char *str, size_t len, \
                                         RedisModuleString **err_msg) {                       \
    switch (format) {                                                                         \
      case GEOMETRY_FORMAT_WKT:                                                               \
        return static_cast<RTree<variant> const *>(idx->index)                                \
            ->query(str, len, queryType, err_msg);                                            \
      case GEOMETRY_FORMAT_GEOJSON:                                                           \
      default:                                                                                \
        return nullptr;                                                                       \
    }                                                                                         \
  }                                                                                           \
  void Index_##variant##_Dump(GeometryIndex *idx, RedisModuleCtx *ctx) {                      \
    static_cast<RTree<variant> *>(idx->index)->dump(ctx);                                     \
  }

GEO_VARIANTS(X)
#undef X

size_t RTree_TotalMemUsage() {
  return RTree<Cartesian>::reportTotal() + RTree<Geographic>::reportTotal();
}
