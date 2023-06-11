/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "polygon.hpp"
#include "rtdoc.h"

#include <ranges>
#include <iostream>

namespace bg = boost::geometry;
namespace bgm = bg::model;
using string = std::basic_string<char, std::char_traits<char>, rm_allocator<char>>;

template <typename coord_system>
struct RTDoc {
  using poly_type = Polygon<coord_system>::polygon_internal;
  using point_type = Point<coord_system>::point_internal;
  using rect_internal = bgm::box<point_type>;

  rect_internal rect_;
  t_docId id_;

  explicit RTDoc() = default;
  explicit RTDoc(rect_internal const& rect) noexcept : rect_{rect}, id_{0} {
  }
  explicit RTDoc(poly_type const& poly, t_docId id = 0) : rect_{to_rect(poly)}, id_{id} {
  }

  [[nodiscard]] t_docId id() const noexcept {
    return id_;
  }

  static RTDoc *from_wkt(const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg) {
    try {
      auto geometry = Polygon<coord_system>::from_wkt(std::string_view{wkt, len});
      return new RTDoc{geometry, id};
    } catch (const std::exception &e) {
      if (err_msg)
        *err_msg = RedisModule_CreateString(nullptr, e.what(), strlen(e.what()));
      return nullptr;
    }
  }

  [[nodiscard]] static rect_internal to_rect(poly_type const& poly) {
    const auto& points = poly.outer();
    if (points.empty()) {
      return rect_internal{};
    }
    auto xs = std::ranges::transform_view(points, [](const auto& p) { return bg::get<0>(p); });
    auto [min_x, max_x] = std::ranges::minmax(xs);
    auto ys = std::ranges::transform_view(points, [](const auto& p) { return bg::get<1>(p); });
    auto [min_y, max_y] = std::ranges::minmax(ys);
    return rect_internal{point_type{min_x, min_y}, point_type{max_x, max_y}};
  }

  [[nodiscard]] static poly_type to_poly(rect_internal const& rect) noexcept {
    auto p_min = rect.min_corner();
    auto p_max = rect.max_corner();
    auto x_min = p_min.get<0>();
    auto y_min = p_min.get<1>();
    auto x_max = p_max.get<0>();
    auto y_max = p_max.get<1>();

    return poly_type{poly_type::ring_type{p_min, point_type{x_max, y_min}, p_max,
                                          point_type{x_min, y_max}, p_min}};
  }

  [[nodiscard]] string rect_to_string() const {
    using sstream = std::basic_stringstream<char, std::char_traits<char>, rm_allocator<char>>;
    sstream ss{};
    ss << bg::wkt(rect_);
    return ss.str();
  }

  [[nodiscard]] RedisModuleString *to_RMString() const {
    if (RedisModule_CreateString) {
      string s = doc->rect_to_string();
      return RedisModule_CreateString(nullptr, s.c_str(), s.length());
    } else {
      return nullptr;
    }
  }

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

template class RTDoc<bg::cs::cartesian>;
using RTDoc_Cartesian = RTDoc<bg::cs::cartesian>;
template class RTDoc<bg::cs::geographic<bg::degree>>;
using RTDoc_Geographic = RTDoc<bg::cs::geographic<bg::degree>>;

template <typename coord_system>
std::ostream& operator<<(std::ostream& os, RTDoc<coord_system> const& doc) {
  os << bg::wkt(doc.rect_);
  return os;
}


template <typename coord_system>
[[nodiscard]] bool operator==(RTDoc<coord_system> const& lhs,
                                     RTDoc<coord_system> const& rhs) noexcept {
  return lhs.id_ == rhs.id_ && bg::equals(lhs.rect_, rhs.rect_);
}

template <typename coord_system>
struct RTDoc_Indexable {
  using result_type = RTDoc<coord_system>::rect_internal;
  [[nodiscard]] constexpr result_type operator()(RTDoc<coord_system> const& doc) const noexcept {
    return doc.rect_;
  }
};

template <typename coord_system>
struct RTDoc_EqualTo {
  [[nodiscard]] bool operator()(RTDoc<coord_system> const& lhs,
                                       RTDoc<coord_system> const& rhs) const noexcept {
    return lhs == rhs;
  }
};
