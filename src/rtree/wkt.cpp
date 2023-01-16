
#include <boost/geometry.hpp>
#include "polygon.hpp"
#include "wkt.h"

namespace bg = boost::geometry;

struct Polygon *From_WKT(const char *wkt) {
    Polygon *pg = Polygon_NewByPoints(0);
    bg::read_wkt(wkt, pg->poly_);
	return pg;
}
