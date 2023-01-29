#pragma once

#define BOOST_ALLOW_DEPRECATED_HEADERS
#include <boost/geometry.hpp>
#undef BOOST_ALLOW_DEPRECATED_HEADERS
#include "allocator.hpp"
#include "point.h"

namespace bg = boost::geometry;
namespace bgm = bg::model;

struct Point {
	using coord_system = bg::cs::cartesian;
	using point_internal = bgm::point<double, 2, coord_system>;

	point_internal point_;

	Point(double x, double y) : point_{x, y} {}
  Point(point_internal const& other) : point_{other} {}
	
  void* operator new(std::size_t sz) { return rm_malloc(sz); }
  void operator delete(void *p) { rm_free(p); }
};

inline bool operator==(Point const& lhs, Point const& rhs) {
	return bg::equals(lhs.point_, rhs.point_);
}
