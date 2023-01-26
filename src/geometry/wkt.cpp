
#include <boost/geometry.hpp>
#include <fstream>
#include "polygon.hpp"
#include "rtree.hpp"
#include "wkt.h"

namespace bg = boost::geometry;

Polygon::polygon_internal from_wkt(std::string_view wkt) {
    Polygon::polygon_internal pg{};
    bg::read_wkt(wkt.data(), pg);
    return pg;
}

RTDoc *From_WKT(const char *wkt) {
	return new RTDoc{from_wkt(wkt)};
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
