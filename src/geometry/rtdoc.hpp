/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#define BOOST_ALLOW_DEPRECATED_HEADERS
#include <boost/geometry.hpp>
#undef BOOST_ALLOW_DEPRECATED_HEADERS
#include "allocator.hpp"
#include "geometry_types.h"

#include <ranges>
#include <iostream>

namespace bg = boost::geometry;
namespace bgm = bg::model;

using Cartesian = bg::cs::cartesian;
using Geographic = bg::cs::geographic<bg::degree>;

using CartesianPoint = bgm::point<double, 2, Cartesian>;
using GeographicPoint = bgm::point<double, 2, Geographic>;

using CartesianPolygon = bgm::polygon<
    /* point_type       */ CartesianPoint,
    /* is_clockwise     */ true,  // TODO: GEOMETRY - (when) do we need to call bg::correct(poly) ?
    /* is_closed        */ true,
    /* points container */ std::vector,
    /* rings_container  */ std::vector,
    /* points_allocator */ rm_allocator,
    /* rings_allocator  */ rm_allocator>;
using GeographicPolygon =
    bgm::polygon<GeographicPoint, true, true, std::vector, std::vector, rm_allocator, rm_allocator>;

using string = std::basic_string<char, std::char_traits<char>, rm_allocator<char>>;

template <typename geom_type>
struct RTDoc {
  using point_type = geom_type::point_type;
  using rect_type = bgm::box<point_type>;

  rect_type rect_;
  t_docId id_;

  explicit RTDoc() = default;
  explicit RTDoc(rect_type const& rect) noexcept : rect_{rect}, id_{0} {
  }
  explicit RTDoc(geom_type const& poly, t_docId id = 0) : rect_{to_rect(poly)}, id_{id} {
  }

  [[nodiscard]] t_docId id() const noexcept {
    return id_;
  }

  [[nodiscard]] static rect_type to_rect(geom_type const& poly) {
    const auto& points = poly.outer();
    if (points.empty()) {
      return rect_type{};
    }
    auto xs = std::ranges::transform_view(points, [](const auto& p) { return bg::get<0>(p); });
    auto [min_x, max_x] = std::ranges::minmax(xs);
    auto ys = std::ranges::transform_view(points, [](const auto& p) { return bg::get<1>(p); });
    auto [min_y, max_y] = std::ranges::minmax(ys);
    return rect_type{point_type{min_x, min_y}, point_type{max_x, max_y}};
  }

  // [[nodiscard]] static geom_type to_poly(rect_type const& rect) noexcept {
  //   auto p_min = rect.min_corner();
  //   auto p_max = rect.max_corner();
  //   auto x_min = bg::get<0>(p_min);
  //   auto y_min = bg::get<1>(p_min);
  //   auto x_max = bg::get<0>(p_max);
  //   auto y_max = bg::get<1>(p_max);

  //   using ring_type = geom_type::ring_type;
  //   return geom_type{
  //       ring_type{p_min, point_type{x_max, y_min}, p_max, point_type{x_min, y_max}, p_min}};
  // }

  [[nodiscard]] string rect_to_string() const {
    using sstream = std::basic_stringstream<char, std::char_traits<char>, rm_allocator<char>>;
    sstream ss{};
    ss << bg::wkt(rect_);
    return ss.str();
  }

  // [[nodiscard]] RedisModuleString* to_RMString() const {
  //   if (RedisModule_CreateString) {
  //     string s = rect_to_string();
  //     return RedisModule_CreateString(nullptr, s.c_str(), s.length());
  //   } else {
  //     return nullptr;
  //   }
  // }

  using Self = RTDoc;
  [[nodiscard]] void* operator new(std::size_t) {
    return rm_allocator<Self>().allocate(1);
  }
  void operator delete(void* p) noexcept {
    rm_allocator<Self>().deallocate(static_cast<Self*>(p), 1);
  }
  [[nodiscard]] void* operator new(std::size_t, void* pos) {
    return pos;
  }
};

template <typename geom_type>
std::ostream& operator<<(std::ostream& os, RTDoc<geom_type> const& doc) {
  os << bg::wkt(doc.rect_);
  return os;
}

template <typename geom_type>
[[nodiscard]] bool operator==(RTDoc<geom_type> const& lhs, RTDoc<geom_type> const& rhs) noexcept {
  return lhs.id_ == rhs.id_ && bg::equals(lhs.rect_, rhs.rect_);
}

template <typename geom_type>
struct RTDoc_Indexable {
  using result_type = RTDoc<geom_type>::rect_type;
  [[nodiscard]] constexpr result_type operator()(RTDoc<geom_type> const& doc) const noexcept {
    return doc.rect_;
  }
};

template <typename geom_type>
struct RTDoc_EqualTo {
  [[nodiscard]] bool operator()(RTDoc<geom_type> const& lhs,
                                RTDoc<geom_type> const& rhs) const noexcept {
    return lhs == rhs;
  }
};

#define X(variant) template class RTDoc<variant>;
GEO_VARIANTS(X)
#undef X
