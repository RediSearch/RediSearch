#pragma once

#define BOOST_ALLOW_DEPRECATED_HEADERS
#include <boost/geometry.hpp>
#undef BOOST_ALLOW_DEPRECATED_HEADERS
#include <cstdarg>
#include "allocator.hpp"
#include "point.hpp"

namespace bg = boost::geometry;
namespace bgm = bg::model;

struct Polygon {
	using polygon_internal = bgm::polygon<
		/* point_type       */ Point::point_internal,
		/* is_clockwise     */ true,
		/* is_closed        */ true,
		/* points container */ std::vector,
		/* rings_container  */ std::vector,
		/* points_allocator */ rm_allocator,
		/* rings_allocator  */ rm_allocator
	>;
	polygon_internal poly_;
	
  [[nodiscard]] void* operator new(std::size_t sz) { return rm_malloc(sz); }
  void operator delete(void *p) noexcept { rm_free(p); }

	[[nodiscard]] explicit Polygon() = default;
	[[nodiscard]] explicit Polygon(int num_points, ...) {
		std::va_list ap;
		va_start(ap, num_points);
		for ([[maybe_unused]] auto&& _ : std::views::iota(0, num_points)) {
			double x = va_arg(ap, double);
			double y = va_arg(ap, double);
			bg::append(poly_.outer(), Point::point_internal{x, y});
		}
		va_end(ap);
	}
};

[[nodiscard]] inline bool operator==(Polygon const& lhs, Polygon const& rhs) noexcept {
	return boost::geometry::equals(lhs.poly_, rhs.poly_);
}

