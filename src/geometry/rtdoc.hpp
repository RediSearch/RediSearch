#pragma once

#define BOOST_ALLOW_DEPRECATED_HEADERS
#include <boost/geometry.hpp>
#undef BOOST_ALLOW_DEPRECATED_HEADERS
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
	docID_t id_;

	explicit RTDoc() = default;
	explicit RTDoc(rect_internal const& rect) noexcept : poly_{to_poly(rect)}, rect_{rect}, id_{0} {}
	explicit RTDoc(Polygon::polygon_internal const& poly, docID_t id = 0) : poly_{poly}, rect_{to_rect(poly)}, id_{id} {}

	[[nodiscard]] static rect_internal to_rect(Polygon::polygon_internal const& poly) {
		const auto& points = poly.outer();
		auto xs = std::ranges::transform_view(points, [] (const auto& p) { return bg::get<0>(p); });
		auto [min_x, max_x] = std::ranges::minmax(xs);
		auto ys = std::ranges::transform_view(points, [] (const auto& p) { return bg::get<1>(p); });
		auto [min_y, max_y] = std::ranges::minmax(ys);
		return {{min_x, min_y}, {max_x, max_y}};
	}

	[[nodiscard]] static Polygon::polygon_internal to_poly(rect_internal const& rect) noexcept {
		auto p_min = rect.min_corner();
		auto p_max = rect.max_corner();
		auto x_min = p_min.get<0>();
		auto y_min = p_min.get<1>();
		auto x_max = p_max.get<0>();
		auto y_max = p_max.get<1>();

		return {{p_min, {x_max, y_min}, p_max, {x_min, y_max}, p_min}};
	}
	
  [[nodiscard]] void* operator new(std::size_t sz) { return rm_malloc(sz); }
  [[nodiscard]] void* operator new(std::size_t, void* pos) { return pos; }
  void operator delete(void *p) noexcept { rm_free(p); }
};

[[nodiscard]] inline bool operator==(RTDoc const& lhs, RTDoc const& rhs) noexcept {
	return lhs.id_ == rhs.id_ &&
				 bg::equals(lhs.rect_, rhs.rect_) && 
		  	 bg::equals(lhs.poly_, rhs.poly_);
}

struct RTDoc_Indexable {
	using result_type = RTDoc::rect_internal;
	[[nodiscard]] constexpr result_type operator()(RTDoc const& doc) const noexcept {
		return doc.rect_;
	}
};

struct RTDoc_EqualTo {
	[[nodiscard]] inline bool operator()(RTDoc const& lhs, RTDoc const& rhs) const noexcept {
		return lhs == rhs;
	}
};
