#pragma once

#include <stddef.h>
#include <stdbool.h>
#include "rtdoc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct RTree;

struct RTree *RTree_New();
void RTree_Free(struct RTree *rtree);
void RTree_Insert(struct RTree *rtree, struct RTDoc const *doc);
bool RTree_Remove(struct RTree *rtree, struct RTDoc const *doc);
size_t RTree_Size(struct RTree const *rtree);
bool RTree_IsEmpty(struct RTree const *rtree);
void RTree_Clear(struct RTree *rtree);
void RTree_Query_Free(struct RTDoc *query);
size_t RTree_Query_Contains(struct RTree const *rtree, struct Point const *point, struct RTDoc **results);
struct RTDoc *RTree_Bounds(struct RTree const *rtree);

#ifdef __cplusplus
}
#endif

