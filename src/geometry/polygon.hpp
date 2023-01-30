#pragma once

#define BOOST_ALLOW_DEPRECATED_HEADERS
#include <boost/geometry.hpp>
#undef BOOST_ALLOW_DEPRECATED_HEADERS
#include "allocator.hpp"
#include "point.hpp"
#include "polygon.h"

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
};

[[nodiscard]] inline bool operator==(Polygon const& lhs, Polygon const& rhs) noexcept {
	return boost::geometry::equals(lhs.poly_, rhs.poly_);
}
