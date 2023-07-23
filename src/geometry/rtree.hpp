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

#include <string>       // std::string, std::char_traits
#include <vector>       // std::vector, std::erase_if
#include <variant>      // std::variant, std::visit
#include <utility>      // std::pair
#include <sstream>      // std::stringstream
#include <iterator>     // std::back_inserter
#include <optional>     // std::optional
#include <algorithm>    // ranges::for_each, views::transform
#include <exception>    // std::exception
#include <functional>   // std::hash, std::equal_to, std::reference_wrapper
#include <string_view>  // std::string_view
#include <boost/geometry/geometry.hpp>
#include <boost/unordered/unordered_flat_map.hpp>  // is faster than std::unordered_map?

namespace RediSearch {
namespace GeoShape {

namespace bg = boost::geometry;
namespace bgm = bg::model;
namespace bgi = bg::index;

using Cartesian = bg::cs::cartesian;
using Geographic = bg::cs::geographic<bg::degree>;

using string = std::basic_string<char, std::char_traits<char>, Allocator::Allocator<char>>;

template <typename CoordSystem>
struct RTree {
  using point_type =
      bgm::point<double, 2, CoordSystem>;  // TODO: GEOMETRY - dimension template param (2 or 3)
  // bgm::polygon requires default constructible allocators, allocations must be tracked by hand.
  using poly_type = bgm::polygon<point_type, true, true, std::vector, std::vector,
                                 Allocator::StatefulAllocator, Allocator::StatefulAllocator>;
  using geom_type = std::variant<point_type, poly_type>;

  using rect_type = bgm::box<point_type>;
  using doc_type = std::pair<rect_type, t_docId>;
  using rtree_type = bgi::rtree<doc_type, bgi::quadratic<16>, bgi::indexable<doc_type>,
                                bgi::equal_to<doc_type>, Allocator::TrackingAllocator<doc_type>>;

  using LUT_value_type = std::pair<t_docId const, geom_type>;
  using LUT_type =
      boost::unordered_flat_map<t_docId, geom_type, std::hash<t_docId>, std::equal_to<t_docId>,
                                Allocator::TrackingAllocator<LUT_value_type>>;

  using query_results = std::vector<doc_type, Allocator::TrackingAllocator<doc_type>>;

  Allocator::TrackingAllocator<void> alloc_;
  rtree_type rtree_;
  LUT_type docLookup_;

  RTree() = delete;
  explicit RTree(size_t& alloc_ref);

  [[nodiscard]] auto lookup(t_docId id) const
      -> std::optional<std::reference_wrapper<const geom_type>>;
  [[nodiscard]] auto lookup(doc_type const& doc) const
      -> std::optional<std::reference_wrapper<const geom_type>>;

  [[nodiscard]] auto from_wkt(std::string_view wkt) const -> geom_type;
  void insert(geom_type const& geom, t_docId id);
  int insertWKT(std::string_view wkt, t_docId id, RedisModuleString** err_msg);
  bool remove(t_docId id);

  [[nodiscard]] static auto geometry_to_string(geom_type const& geom) -> string;
  [[nodiscard]] static auto doc_to_string(doc_type const& doc) -> string;
  void dump(RedisModuleCtx* ctx) const;
  [[nodiscard]] size_t report() const noexcept;

  [[nodiscard]] static auto generate_query_iterator(query_results&& results, auto&& a)
      -> IndexIterator*;
  template <typename Predicate>
  [[nodiscard]] auto apply_predicate(Predicate p) const -> query_results;
  [[nodiscard]] auto contains(doc_type const& query_doc, geom_type const& query_geom) const
      -> query_results;
  [[nodiscard]] auto within(doc_type const& query_doc, geom_type const& query_geom) const
      -> query_results;
  [[nodiscard]] auto generate_predicate(doc_type const& query_doc, QueryType query_type,
                                        geom_type const& query_geom) const -> query_results;
  [[nodiscard]] auto query(std::string_view wkt, QueryType query_type,
                           RedisModuleString** err_msg) const -> IndexIterator*;

  [[nodiscard]] static constexpr auto make_mbr(geom_type const& geom) -> rect_type;
  [[nodiscard]] static constexpr auto make_doc(geom_type const& geom, t_docId id = 0) -> doc_type;
  [[nodiscard]] static constexpr auto get_rect(doc_type const& doc) -> rect_type;
  [[nodiscard]] static constexpr auto get_id(doc_type const& doc) -> t_docId;
};

}  // namespace GeoShape
}  // namespace RediSearch
