/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "geometry_api.h"
#include "rtree.hpp"

#include <array>    // std::array
#include <variant>  // std::variant, std::monostate, std::get

using namespace RediSearch::GeoShape;

template <typename cs>
using rtree_ptr = std::unique_ptr<RTree<cs>>;

#define X(variant) , rtree_ptr<variant>
struct GeometryIndex {
  const GeometryApi *api;
  std::variant<std::monostate GEO_VARIANTS(X)> index;

  void *operator new(std::size_t) noexcept {
    using alloc_type = RediSearch::Allocator::Allocator<GeometryIndex>;
    return static_cast<void *>(alloc_type::allocate(1));
  }
  void operator delete(void *p) noexcept {
    using alloc_type = RediSearch::Allocator::Allocator<GeometryIndex>;
    alloc_type::deallocate(static_cast<GeometryIndex *>(p), 1);
  }
};
#undef X

auto GeometryApi_Get(const GeometryIndex *idx) -> const GeometryApi * {
  return idx->api;
}

namespace {
#define X(variant)                                                                          \
  void Index_##variant##_Free(GeometryIndex *idx) {                                         \
    delete idx;                                                                             \
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
  auto Index_##variant##_Query(const GeometryIndex *idx, QueryType query_type,              \
                               GEOMETRY_FORMAT format, const char *str, std::size_t len,    \
                               RedisModuleString **err_msg)                                 \
      ->IndexIterator * {                                                                   \
    switch (format) {                                                                       \
      case GEOMETRY_FORMAT_WKT:                                                             \
        return std::get<rtree_ptr<variant>>(idx->index)                                     \
            ->query(std::string_view{str, len}, query_type, err_msg);                       \
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
  auto Index_##variant##_New()->GeometryIndex * {                                           \
    return new GeometryIndex{&GeometryApi_##variant, std::make_unique<RTree<variant>>()};   \
  }
GEO_VARIANTS(X)
#undef X
}  // anonymous namespace

#define X(variant) /*[GEOMETRY_COORDS_##variant] =*/ Index_##variant##_New,
auto GeometryIndexFactory(GEOMETRY_COORDS tag) -> GeometryIndex * {
  // using GeometryConstructor_t = auto (*)() -> GeometryIndex *;
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
