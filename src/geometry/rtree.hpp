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

using RediSearch::Allocator::Allocator;
using RediSearch::Allocator::StatefulAllocator;
using RediSearch::Allocator::TrackingAllocator;

using Cartesian = bg::cs::cartesian;
using Geographic = bg::cs::geographic<bg::degree>;

using string = std::basic_string<char, std::char_traits<char>, Allocator<char>>;

template <typename coord_system>
struct RTree {
  using point_type =
      bgm::point<double, 2, coord_system>;  // TODO: GEOMETRY - dimension template param (2 or 3)
  using poly_type = bgm::polygon<point_type, true, true, std::vector, std::vector,
                                 StatefulAllocator, StatefulAllocator>;
  using geom_type = std::variant<point_type, poly_type>;

  using rect_type = bgm::box<point_type>;
  using doc_type = std::pair<rect_type, t_docId>;
  using rtree_type = bgi::rtree<doc_type, bgi::quadratic<16>, bgi::indexable<doc_type>,
                                bgi::equal_to<doc_type>, TrackingAllocator<doc_type>>;

  using LUT_value_type = std::pair<t_docId const, geom_type>;
  using LUT_type =
      boost::unordered_flat_map<t_docId, geom_type, std::hash<t_docId>, std::equal_to<t_docId>,
                                TrackingAllocator<LUT_value_type>>;

  using ResultsVec = std::vector<doc_type, TrackingAllocator<doc_type>>;

  TrackingAllocator<void> alloc_;
  rtree_type rtree_;
  LUT_type docLookup_;

  RTree() = delete;
  explicit RTree(std::size_t& alloc_ref);

  [[nodiscard]] auto lookup(t_docId id) const
      -> std::optional<std::reference_wrapper<const geom_type>>;
  [[nodiscard]] auto lookup(doc_type const& doc) const
      -> std::optional<std::reference_wrapper<const geom_type>>;

  [[nodiscard]] auto from_wkt(std::string_view wkt) const -> geom_type;
  void insert(geom_type const& geom, t_docId id);
  int insertWKT(const char* wkt, std::size_t len, t_docId id, RedisModuleString** err_msg);
  bool remove(t_docId id);

  [[nodiscard]] static auto geometry_to_string(geom_type const& geom) -> string;
  [[nodiscard]] static auto doc_to_string(doc_type const& doc) -> string;
  void dump(RedisModuleCtx* ctx) const;
  [[nodiscard]] std::size_t report() const;

  [[nodiscard]] static auto generate_query_iterator(ResultsVec&& results,
                                                    TrackingAllocator<QueryIterator>&& a)
      -> IndexIterator*;
  template <typename Predicate>
  [[nodiscard]] auto query(Predicate p) const -> ResultsVec;
  [[nodiscard]] auto contains(doc_type const& queryDoc, geom_type const& queryGeom) const
      -> ResultsVec;
  [[nodiscard]] auto within(doc_type const& queryDoc, geom_type const& queryGeom) const
      -> ResultsVec;
  [[nodiscard]] auto query(doc_type const& queryDoc, QueryType queryType,
                           geom_type const& queryGeom) const -> ResultsVec;
  [[nodiscard]] auto query(const char* wkt, std::size_t len, QueryType query_type,
                           RedisModuleString** err_msg) const -> IndexIterator*;

  [[nodiscard]] static constexpr auto make_mbr(geom_type const& geom) -> rect_type;
  [[nodiscard]] static constexpr auto make_doc(geom_type const& geom, t_docId id = 0) -> doc_type;
  [[nodiscard]] static constexpr auto get_rect(doc_type const& doc) -> rect_type;
  [[nodiscard]] static constexpr auto get_id(doc_type const& doc) -> t_docId;
};

}  // namespace GeoShape
}  // namespace RediSearch
