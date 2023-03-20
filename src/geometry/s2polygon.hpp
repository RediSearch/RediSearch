/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include <cstdarg>
#include <vector>
#include <memory>
#include "s2geometry/src/s2/s2polygon.h"
#define BOOST_ALLOW_DEPRECATED_HEADERS
#include <boost/geometry.hpp>
#undef BOOST_ALLOW_DEPRECATED_HEADERS
#include "s2point.hpp"

struct Polygon {
	using point_type = Point::point_internal;
	using polygon_internal = S2Polygon;
	polygon_internal poly_;

  [[nodiscard]] explicit Polygon() = default;
  [[nodiscard]] explicit Polygon(std::string_view wkt) : poly_{from_wkt(wkt)} {}

  [[nodiscard]] static polygon_internal from_wkt(std::string_view wkt) {
    namespace bg = boost::geometry;
    namespace bgm = bg::model;
    auto pg = bgm::polygon<bgm::point<double, 2, bg::cs::polar>>{};
    bg::read_wkt(wkt.data(), pg);

    auto change_point_type = [](const auto& bg_points) {
      auto s2_points = std::vector<S2Point>{};
      s2_points.reserve(bg_points.size());

      for (const auto& bgpoint : bg_points) {
        auto lat = S1Angle::Degrees{bgpoint.get<0>()};
        auto lng = S1Angle::Degrees{bgpoint.get<1>()};
        auto s2point = S2LatLng{lat, lng}.ToPoint();
        s2_points.push_back(s2point);
      }

      auto ret_ptr = std::make_unique<S2Loop>(absl::Span(s2_points));
      return ret_ptr;
    };

    auto loops = std::vector<std::unique_ptr<S2Loop>>{};
    loops.emplace_back(change_point_type(bg.outer()));
    for (const auto& inner : bg.inner()) {
      loops.emplace_back(change_point_type(inner));
    }
    auto s2pg = polygon_internal{};
    s2pg.InitNested(loops);

    return s2pg;
  }

  using Self = Polygon;
  [[nodiscard]] void* operator new(std::size_t) {
    return rm_allocator<Self>().allocate(1);
  }
  void operator delete(void* p) noexcept {
    rm_allocator<Self>().deallocate(static_cast<Self*>(p), 1);
  }
};
