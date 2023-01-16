#pragma once

#include <boost/geometry.hpp>
#include "point.h"

namespace bg = boost::geometry;
namespace bgm = bg::model;

struct Point {
	using coord_system = bg::cs::cartesian;
	using point_internal = bgm::point<double, 2, coord_system>;

	point_internal point_;

	Point(double x, double y) : point_{x, y} {}
    Point(point_internal const& other) : point_{other} {}
};

inline bool operator==(Point const& lhs, Point const& rhs) {
	return bg::equals(lhs.point_, rhs.point_);
}
