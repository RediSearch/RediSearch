#pragma once

#include <ranges>
#include <iostream>
#include "polygon.hpp"
#include "rtdoc.h"

namespace bg = boost::geometry;
namespace bgm = bg::model;
using string = std::basic_string<char, std::char_traits<char>, rm_allocator<char>>;

struct RTDoc {
	using point_type = Point::point_internal;
	using poly_type = Polygon::polygon_internal;
	using rect_internal = bgm::box<point_type>;
	poly_type poly_;
	rect_internal rect_;
	docID_t id_;

	explicit RTDoc() = default;
	explicit RTDoc(rect_internal const& rect) noexcept : poly_{to_poly(rect)}, rect_{rect}, id_{0} {}
	explicit RTDoc(poly_type const& poly, docID_t id = 0) : poly_{poly}, rect_{to_rect(poly)}, id_{id} {}
	explicit RTDoc(std::string_view wkt, docID_t id = 0) : poly_{Polygon::from_wkt(wkt)} , rect_{to_rect(poly_)}, id_{id} {}

	[[nodiscard]] docID_t id() const noexcept {
		return id_;
	}

	[[nodiscard]] static rect_internal to_rect(poly_type const& poly) {
		const auto& points = poly.outer();
		auto xs = std::ranges::transform_view(points, [] (const auto& p) { return bg::get<0>(p); });
		auto [min_x, max_x] = std::ranges::minmax(xs);
		auto ys = std::ranges::transform_view(points, [] (const auto& p) { return bg::get<1>(p); });
		auto [min_y, max_y] = std::ranges::minmax(ys);
		return rect_internal{
			point_type{min_x, min_y}, point_type{max_x, max_y}
		};
	}

	[[nodiscard]] static poly_type to_poly(rect_internal const& rect) noexcept {
		auto p_min = rect.min_corner();
		auto p_max = rect.max_corner();
		auto x_min = p_min.get<0>();
		auto y_min = p_min.get<1>();
		auto x_max = p_max.get<0>();
		auto y_max = p_max.get<1>();

		return poly_type{
			poly_type::ring_type{
				p_min, point_type{x_max, y_min}, p_max, point_type{x_min, y_max}, p_min
			}
		};
	}

	[[nodiscard]] string to_string() const {
		using sstream = std::basic_stringstream<char, std::char_traits<char>, rm_allocator<char>>;
		sstream ss{};
		ss << bg::wkt(poly_);
		return ss.str();
	}
	
	using Self = RTDoc;
  [[nodiscard]] void* operator new(std::size_t sz) { return rm_allocator<Self>().allocate(sz); }
  void operator delete(void *p) noexcept { rm_allocator<Self>().deallocate(static_cast<Self*>(p), sizeof(Self)); }
  [[nodiscard]] void* operator new(std::size_t, void* pos) { return pos; }
};

inline std::ostream& operator<<(std::ostream& os, RTDoc const& doc) {
	os << bg::wkt(doc.poly_);
	return os;
}

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
