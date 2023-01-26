#pragma once

#include <boost/geometry.hpp>
#include <ranges>
#include "allocator.hpp"
#include "point.hpp"
#include "polygon.hpp"
#include "rtdoc.h"

namespace bg = boost::geometry;
namespace bgm = bg::model;

struct RTDoc {
	using rect_internal = bgm::box<Point::point_internal>;
	Polygon::polygon_internal poly_;
	rect_internal rect_;

	RTDoc() : poly_{}, rect_{{0, 0}, {0, 0}} {}
	RTDoc(rect_internal const& rect) : poly_{to_poly(rect)}, rect_{rect} {}
	RTDoc(Polygon::polygon_internal const& poly) : poly_{poly}, rect_{to_rect(poly)} {}

	static rect_internal to_rect(Polygon::polygon_internal const& poly) {
		const auto& points = poly.outer();
		auto xs = std::ranges::transform_view(points, [] (const auto& p) { return bg::get<0>(p); });
		auto [min_x, max_x] = std::ranges::minmax(xs);
		auto ys = std::ranges::transform_view(points, [] (const auto& p) { return bg::get<1>(p); });
		auto [min_y, max_y] = std::ranges::minmax(ys);
		return {{min_x, min_y}, {max_x, max_y}};
	}

	static Polygon::polygon_internal to_poly(rect_internal const& rect) {
		auto p_min = rect.min_corner();
		auto p_max = rect.max_corner();
		auto x_min = p_min.get<0>();
		auto y_min = p_min.get<1>();
		auto x_max = p_max.get<0>();
		auto y_max = p_max.get<1>();

		return {{p_min, {x_max, y_min}, p_max, {x_min, y_max}, p_min}};
	}
	
  void* operator new(std::size_t sz) { return rm_malloc(sz); }
  void* operator new(std::size_t, void* pos) { return pos; }
  void operator delete(void *p) { rm_free(p); }
};

inline bool operator==(RTDoc const& lhs, RTDoc const& rhs) {
	return bg::equals(lhs.rect_, rhs.rect_) && 
		  	 bg::equals(lhs.poly_, rhs.poly_);
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
