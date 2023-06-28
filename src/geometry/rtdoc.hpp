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

#include <array>    // std::array
#include <vector>   // std::vector
#include <variant>  // std::variant
#include <sstream>  // std::stringstream

namespace bg = boost::geometry;
namespace bgm = bg::model;
namespace bgi = bg::index;

using Cartesian = bg::cs::cartesian;
using Geographic = bg::cs::geographic<bg::degree>;

using string = std::basic_string<char, std::char_traits<char>, rm_allocator<char>>;
using sstream = std::basic_stringstream<char, std::char_traits<char>, rm_allocator<char>>;

template <typename coord_system>
struct RTDoc {
  using point_type = bgm::point<double, 2, coord_system>;
  using rect_type = bgm::box<point_type>;
  using poly_type = bgm::polygon<
    /* point_type       */ point_type,
    /* is_clockwise     */ true,  // TODO: GEOMETRY - (when) do we need to call bg::correct(poly) ?
    /* is_closed        */ true,
    /* points container */ std::vector,
    /* rings_container  */ std::vector,
    /* points_allocator */ rm_allocator,
    /* rings_allocator  */ rm_allocator>;
  
  using geom_type = std::variant<point_type, poly_type>;

  rect_type rect_;
  t_docId id_;

  explicit RTDoc() = default;
  explicit RTDoc(rect_type const& rect) noexcept : rect_{rect}, id_{0} {
  }
  explicit RTDoc(geom_type const& geom, t_docId id = 0)
      : rect_{std::visit([](auto&& geom) { return bg::return_envelope<rect_type>(geom); }, geom)}, id_{id} {
  }

  [[nodiscard]] t_docId id() const noexcept {
    return id_;
  }

  // template <typename Geometry>
  // [[nodiscard]] static auto get_points(Geometry const& geom) {
  //   if constexpr (std::experimental::is_detected_v<outer_t, Geometry>) {
  //     return geom.outer();
  //   } else if constexpr (std::is_same_v<Geometry, point_type>) {
  //     return std::array{geom};
  //   }
  // }

  // [[nodiscard]] static rect_type to_rect(point_type const& p1, point_type const& p2) {
  //   if (p1 == p2) {
  //     return p1;
  //   } else {
  //     return box_type{p1, p2};
  //   }
  // }

  // template <typename Geometry>
  // [[nodiscard]] static rect_type to_rect(Geometry const& geom) {
  //   const auto points = get_points(geom);
  //   auto [min_x, max_x] = std::ranges::minmax(
  //       points | std::views::transform([](auto const& p) { return bg::get<0>(p); }));
  //   auto [min_y, max_y] = std::ranges::minmax(
  //       points | std::views::transform([](auto const& p) { return bg::get<1>(p); }));
  //   return to_rect(point_type{min_x, min_y}, point_type{max_x, max_y});
  // }

  [[nodiscard]] string rect_to_string() const {
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
  using result_type = RTDoc<coord_system>::rect_type;
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

#define X(variant) template class RTDoc<variant>;
GEO_VARIANTS(X)
#undef X
