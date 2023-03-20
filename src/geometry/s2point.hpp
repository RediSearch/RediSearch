/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "deps/s2geometry/src/s2/s1angle.h"
#include "deps/s2geometry/src/s2/s2latlng.h"
#include "deps/s2geometry/src/s2/s2point.h"
#include "allocator.hpp"

struct Point {
  using point_internal = S2Point;
  point_internal point_;

  [[nodiscard]] explicit Point(double x, double y) noexcept
    : point_{S2LatLng{S1Angle::Degrees(x), S1Angle::Degrees(y)}.ToPoint()}
  {}
  [[nodiscard]] explicit Point(point_internal const& other) noexcept
    : point_{other}
  {}

  using Self = Point;
  [[nodiscard]] void* operator new(std::size_t) {
    return rm_allocator<Self>().allocate(1);
  }
  void operator delete(void* p) noexcept {
    rm_allocator<Self>().deallocate(static_cast<Self*>(p), 1);
  }
};
