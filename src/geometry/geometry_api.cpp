/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "geometry_api.h"
#include "rtree.hpp"

#include <array>                                // std::array
#include <variant>                              // std::variant, std::monostate, std::get
#include <boost/smart_ptr/allocate_unique.hpp>  // boost::allocate_unique

using namespace RediSearch::GeoShape;
using namespace RediSearch::Allocator;

// using boost::allocate_unique in order to make_unique explicitly using the Redis Allocator
template <typename cs>
using rtree_ptr = std::unique_ptr<RTree<cs>, boost::alloc_deleter<RTree<cs>, Allocator<RTree<cs>>>>;

#define X(variant) , rtree_ptr<variant>
struct GeometryIndex {
  const GeometryApi *api;
  std::variant<std::monostate GEO_VARIANTS(X)> index;
};
#undef X

auto GeometryApi_Get(const GeometryIndex *idx) -> const GeometryApi * {
  return idx->api;
}

namespace {
#define X(variant)                                                                          \
  void Index_##variant##_Free(GeometryIndex *idx) {                                         \
    using alloc_type = Allocator<GeometryIndex>;                                            \
    alloc_type alloc;                                                                       \
    std::allocator_traits<alloc_type>::destroy(alloc, idx);                                 \
    std::allocator_traits<alloc_type>::deallocate(alloc, idx, 1);                           \
  }                                                                                         \
  int Index_##variant##_Insert(GeometryIndex *idx, GEOMETRY_FORMAT format, const char *str, \
                               std::size_t len, t_docId id, RedisModuleString **err_msg) {  \
    switch (format) {                                                                       \
      case GEOMETRY_FORMAT_WKT:                                                             \
        return !std::get<rtree_ptr<variant>>(idx->index)                                    \
                    ->insertWKT(std::string_view{str, len}, id, err_msg);                   \
      case GEOMETRY_FORMAT_GEOJSON:                                                         \
      default:                                                                              \
        return 1;                                                                           \
    }                                                                                       \
  }                                                                                         \
  int Index_##variant##_Remove(GeometryIndex *idx, t_docId id) {                            \
    return std::get<rtree_ptr<variant>>(idx->index)->remove(id);                            \
  }                                                                                         \
  auto Index_##variant##_Query(const RedisSearchCtx *sctx, const FieldIndexFilterContext* filterCtx, \
                               const GeometryIndex *idx, QueryType query_type,              \
                               GEOMETRY_FORMAT format, const char *str, std::size_t len,    \
                               RedisModuleString **err_msg) -> IndexIterator * {            \
    switch (format) {                                                                       \
      case GEOMETRY_FORMAT_WKT:                                                             \
        return std::get<rtree_ptr<variant>>(idx->index)                                     \
            ->query(sctx, filterCtx, std::string_view{str, len}, query_type, err_msg);      \
      case GEOMETRY_FORMAT_GEOJSON:                                                         \
      default:                                                                              \
        return nullptr;                                                                     \
    }                                                                                       \
  }                                                                                         \
  void Index_##variant##_Dump(const GeometryIndex *idx, RedisModuleCtx *ctx) {              \
    std::get<rtree_ptr<variant>>(idx->index)->dump(ctx);                                    \
  }                                                                                         \
  std::size_t Index_##variant##_Report(const GeometryIndex *idx) {                          \
    return std::get<rtree_ptr<variant>>(idx->index)->report();                              \
  }                                                                                         \
  constexpr GeometryApi GeometryApi_##variant = {                                           \
      .freeIndex = Index_##variant##_Free,                                                  \
      .addGeomStr = Index_##variant##_Insert,                                               \
      .delGeom = Index_##variant##_Remove,                                                  \
      .query = Index_##variant##_Query,                                                     \
      .dump = Index_##variant##_Dump,                                                       \
      .report = Index_##variant##_Report,                                                   \
  };                                                                                        \
  auto Index_##variant##_New() -> GeometryIndex * {                                         \
    using alloc_type = Allocator<GeometryIndex>;                                            \
    alloc_type alloc;                                                                       \
    const auto idx = std::allocator_traits<alloc_type>::allocate(alloc, 1);                 \
    std::allocator_traits<alloc_type>::construct(                                           \
        alloc, idx, &GeometryApi_##variant,                                                 \
        boost::allocate_unique<RTree<variant>>(Allocator<RTree<variant>>{}));               \
    return idx;                                                                             \
  }
GEO_VARIANTS(X)
#undef X
}  // anonymous namespace

#define X(variant) Index_##variant##_New,
auto GeometryIndexFactory(GEOMETRY_COORDS tag) -> GeometryIndex * {
  static constexpr auto geometry_ctors = std::array{GEO_VARIANTS(X)};
  return geometry_ctors[tag]();
}
#undef X

auto GeometryCoordsToName(GEOMETRY_COORDS tag) -> const char * {
  static_assert(GEOMETRY_COORDS_Cartesian == 0 && GEOMETRY_COORDS_Geographic == 1);
  using namespace std::literals;
  static constexpr auto tag_names = std::array{"FLAT"sv, "SPHERICAL"sv};
  return tag_names[tag].data();
}
