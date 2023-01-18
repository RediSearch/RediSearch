#pragma once

#include "polygon.h"
#include "rtree.h"

#ifdef __cplusplus
extern "C" {
#endif

struct Polygon *From_WKT(const char *wkt);
struct RTree *Load_WKT_File(const char *path);

#ifdef __cplusplus
}
#endif

