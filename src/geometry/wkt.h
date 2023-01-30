#pragma once

#include "rtdoc.h"
#include "rtree.h"

#ifdef __cplusplus
extern "C" {
#endif

NODISCARD struct RTDoc *From_WKT(const char *wkt, docID_t id);
struct RTree *Load_WKT_File(struct RTree *rtree, const char *path);

#ifdef __cplusplus
}
#endif

