
#define BOOST_ALLOW_DEPRECATED_HEADERS
#include <boost/geometry.hpp>
#undef BOOST_ALLOW_DEPRECATED_HEADERS
#include <fstream>
#include "polygon.hpp"
#include "rtree.hpp"
#include "wkt.h"

namespace bg = boost::geometry;

[[nodiscard]] Polygon::polygon_internal from_wkt(std::string_view wkt) {
	Polygon::polygon_internal pg{};
	bg::read_wkt(wkt.data(), pg);
	return pg;
}

[[nodiscard]] RTDoc *From_WKT(const char *wkt, size_t len, docID_t id) {
	return new RTDoc{from_wkt({wkt, len}), id};
}

RTree *Load_WKT_File(RTree *rtree, const char *path) {
	using string = std::basic_string<char, std::char_traits<char>, rm_allocator<char>>;

	auto file = std::ifstream{path};
	auto& rt = rtree->rtree_;
	
	for (string wkt{}; std::getline(file, wkt, '\n'); ) {
		rt.insert(RTDoc{from_wkt(wkt)});
	}

	return rtree;
}
