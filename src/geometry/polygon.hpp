/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "point.hpp"

#include <cstdarg>
#include <ranges>

namespace bg = boost::geometry;
namespace bgm = bg::model;

template <typename coord_system>
struct Polygon {
	using point_type = Point<coord_system>::point_internal;
	using polygon_internal = bgm::polygon<
		/* point_type       */ point_type,
		/* is_clockwise     */ true,		// TODO: GEOMETRY - (when) do we need to call bg::correct(poly) ?
		/* is_closed        */ true,
		/* points container */ std::vector,
		/* rings_container  */ std::vector,
		/* points_allocator */ rm_allocator,
		/* rings_allocator  */ rm_allocator
	>;
	polygon_internal poly_;

  [[nodiscard]] explicit Polygon() = default;
  [[nodiscard]] explicit Polygon(int num_points, ...) {
    std::va_list ap;
    va_start(ap, num_points);
    for ([[maybe_unused]] auto&& _ : std::views::iota(0, num_points)) {
      double x = va_arg(ap, double);
      double y = va_arg(ap, double);
      bg::append(poly_.outer(), point_type{x, y});
    }
    va_end(ap);
  }

  [[nodiscard]] static polygon_internal from_wkt(std::string_view wkt) {
    polygon_internal pg{};
    bg::read_wkt(wkt.data(), pg);
    return pg;
  }

  using Self = Polygon;
  [[nodiscard]] void* operator new(std::size_t) {
    return rm_allocator<Self>().allocate(1);
  }
  void operator delete(void* p) noexcept {
    rm_allocator<Self>().deallocate(static_cast<Self*>(p), 1);
  }
};
