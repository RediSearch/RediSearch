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

namespace bg = boost::geometry;
namespace bgm = bg::model;

struct Point {
  using coord_system = bg::cs::cartesian;
  using point_internal = bgm::point<double, 2, coord_system>;

  point_internal point_;

  explicit Point(double x, double y) noexcept : point_{x, y} {
  }
  explicit Point(point_internal const& other) noexcept : point_{other} {
  }

  using Self = Point;
  [[nodiscard]] void* operator new(std::size_t) {
    return rm_allocator<Self>().allocate(1);
  }
  void operator delete(void* p) noexcept {
    rm_allocator<Self>().deallocate(static_cast<Self*>(p), 1);
  }
};
