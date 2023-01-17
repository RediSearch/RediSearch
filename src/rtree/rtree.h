#pragma once

#include <stddef.h>
#include <stdbool.h>
#include "rtdoc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct RTree;
struct RTree_QueryIterator;

struct RTree *RTree_New();
void RTree_Free(struct RTree *rtree);
void RTree_Insert(struct RTree *rtree, struct RTDoc const *doc);
bool RTree_Remove(struct RTree *rtree, struct RTDoc const *doc);
size_t RTree_Size(struct RTree const *rtree);
bool RTree_IsEmpty(struct RTree const *rtree);
void RTree_Clear(struct RTree *rtree);
struct RTDoc *RTree_Bounds(struct RTree const *rtree);

struct RTree_QueryIterator *RTree_Query_Contains(struct RTree const *rtree, struct Polygon const *query_poly, size_t *num_results);
void RTree_QIter_Free(struct RTree_QueryIterator *iter);
struct RTDoc *RTree_QIter_Next(struct RTree_QueryIterator *iter);

#ifdef __cplusplus
}
#endif

