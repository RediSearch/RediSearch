
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

Polygon *From_WKT(const char *wkt) {
	return new Polygon{from_wkt(wkt)};
}

RTree *Load_WKT_File(RTree *rtree, const char *path) {
    auto file = std::ifstream{path};
    auto& rt = rtree->rtree_;
    
    for (std::string wkt{}; std::getline(file, wkt, '\n'); ) {
        rt.insert(RTDoc{from_wkt(wkt)});
    }

    return rtree;
}
