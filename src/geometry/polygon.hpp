#pragma once

#include <boost/geometry.hpp>
#include "allocator.hpp"
#include "point.hpp"
#include "polygon.h"

namespace bg = boost::geometry;
namespace bgm = bg::model;

struct Polygon {
	using polygon_internal = bgm::polygon<Point::point_internal>;
	polygon_internal poly_;
	
  void* operator new(std::size_t sz) { return rm_malloc(sz); }
  void operator delete(void *p) { rm_free(p); }
};

inline bool operator==(Polygon const& lhs, Polygon const& rhs) {
	return boost::geometry::equals(lhs.poly_, rhs.poly_);
}
