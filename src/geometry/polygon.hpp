#pragma once

#include <boost/geometry.hpp>
#include "point.hpp"
#include "polygon.h"

namespace bg = boost::geometry;
namespace bgm = bg::model;

struct Polygon {
	using polygon_internal = bgm::polygon<Point::point_internal>;
	polygon_internal poly_;
};

inline bool operator==(Polygon const& lhs, Polygon const& rhs) {
	return boost::geometry::equals(lhs.poly_, rhs.poly_);
}
