#pragma once

#include <boost/geometry.hpp>
#include <ranges>
#include "point.hpp"
#include "polygon.hpp"
#include "rtdoc.h"

namespace bg = boost::geometry;
namespace bgm = bg::model;

struct RTDoc {
	using rect_internal = bgm::box<Point::point_internal>;
	rect_internal rect_;

	RTDoc() : rect_{{0, 0}, {0, 0}} {}
	RTDoc(rect_internal const& other) : rect_{other} {}
	RTDoc(Polygon::polygon_internal const& polygon) : rect_{to_rect(polygon)} {}

private:
	rect_internal to_rect(Polygon::polygon_internal const& polygon) {
		const auto& points = polygon.outer();
		auto xs = std::ranges::transform_view(points, [] (const auto& p) { return bg::get<0>(p); });
		auto [min_x, max_x] = std::ranges::minmax(xs);
		auto ys = std::ranges::transform_view(points, [] (const auto& p) { return bg::get<1>(p); });
		auto [min_y, max_y] = std::ranges::minmax(ys);
		return {{min_x, min_y}, {max_x, max_y}};
	}
};

inline bool operator==(RTDoc const& lhs, RTDoc const& rhs) {
	return boost::geometry::equals(lhs.rect_, rhs.rect_);
}

struct RTDoc_Indexable {
	using result_type = RTDoc::rect_internal;
	result_type operator()(RTDoc const& doc) const {
		return doc.rect_;
	}
};

struct RTDoc_EqualTo {
	bool operator()(RTDoc const& lhs, RTDoc const& rhs) const {
		return lhs == rhs;
	}
};
