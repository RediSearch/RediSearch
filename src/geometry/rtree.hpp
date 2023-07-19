/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "allocator/allocator.hpp"
#include "allocator/stateful_allocator.hpp"
#include "allocator/tracking_allocator.hpp"
#include "query_iterator.hpp"
#include "geometry_types.h"

#include <vector>                       // std::vector
#include <variant>                      // std::variant
#include <utility>                      // std::pair
#include <functional>                   // std::hash, std::equal_to
#include <string_view>                  // std::string_view
#include <unordered_map>                // std::unordered_map
#include <boost/geometry/geometry.hpp>  // duh...
#include <boost/optional/optional.hpp>  // boost::optional<T const&>
// #include <boost/unordered/unordered_map.hpp>
// #include <boost/unordered/unordered_node_map.hpp>
// #include <boost/unordered/unordered_flat_map.hpp>  // is faster than std::unordered_map?

namespace RediSearch {
namespace GeoShape {

namespace bg = boost::geometry;
namespace bgm = bg::model;
namespace bgi = bg::index;

using Cartesian = bg::cs::cartesian;
using Geographic = bg::cs::geographic<bg::degree>;

template <typename CoordSystem>
class RTree {
 public:
  // TODO: GEOMETRY - dimension template param (2 or 3)
  using point_type = bgm::point<double, 2, CoordSystem>;
  // bgm::polygon requires default constructible allocators, allocations must be tracked by hand.
  using poly_type = bgm::polygon<point_type, true, true, std::vector, std::vector,
                                 Allocator::StatefulAllocator, Allocator::StatefulAllocator>;
  using geom_type = std::variant<point_type, poly_type>;

  using rect_type = bgm::box<point_type>;
  using doc_type = std::pair<rect_type, t_docId>;
  using doc_alloc = Allocator::TrackingAllocator<doc_type>;
  using rtree_type = bgi::rtree<doc_type, bgi::quadratic<16>, bgi::indexable<doc_type>,
                                bgi::equal_to<doc_type>, doc_alloc>;

  using lookup_type = std::pair<t_docId const, geom_type>;
  using lookup_alloc = Allocator::TrackingAllocator<lookup_type>;
  // `boost::unordered_flat_map` is preferable if we can deal with the issue of reference stability - only relevant because of MT.
  //     this can be avoided by lookup() returning `std::optional<geom_type>` instead of `boost::optional<geom_type const&>`.
  // `boost::unordered_node_map` failed to provide reference stability. important to find out why, as that is it's singular advantage.
  // `boost::unordered_map` requires a default constructable allocator as it calls `allocator<node_type<KV, void*>> a{}.allocate()`
  using LUT_type = std::unordered_map<t_docId, geom_type, std::hash<t_docId>,
                                      std::equal_to<t_docId>, lookup_alloc>;

  using query_results = std::vector<doc_type, Allocator::TrackingAllocator<doc_type>>;

 private:
  mutable std::size_t allocated_;
  rtree_type rtree_;
  LUT_type docLookup_;

 public:
  explicit RTree();

  int insertWKT(std::string_view wkt, t_docId id, RedisModuleString** err_msg);
  bool remove(t_docId id);
  [[nodiscard]] auto query(std::string_view wkt, QueryType query_type,
                           RedisModuleString** err_msg) const -> IndexIterator*;

  void dump(RedisModuleCtx* ctx) const;
  [[nodiscard]] std::size_t report() const noexcept;

 private:
  [[nodiscard]] auto lookup(t_docId id) const -> boost::optional<geom_type const&>;
  [[nodiscard]] auto lookup(doc_type const& doc) const -> boost::optional<geom_type const&>;
  void insert(geom_type const& geom, t_docId id);

  template <typename Predicate, typename Filter>
  [[nodiscard]] auto apply_predicate(Predicate&& p, Filter&& f) const -> query_results;
  [[nodiscard]] auto contains(doc_type const& query_doc, geom_type const& query_geom) const
      -> query_results;
  [[nodiscard]] auto within(doc_type const& query_doc, geom_type const& query_geom) const
      -> query_results;
  [[nodiscard]] auto generate_predicate(doc_type const& query_doc, QueryType query_type,
                                        geom_type const& query_geom) const -> query_results;
};

}  // namespace GeoShape
}  // namespace RediSearch
