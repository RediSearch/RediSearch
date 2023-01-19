#pragma once

#include <stddef.h>
#include <stdbool.h>
#include "rtdoc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct RTree;
struct QueryIterator;

struct RTree *RTree_New();
void RTree_Free(struct RTree *rtree);
void RTree_Insert(struct RTree *rtree, struct RTDoc const *doc);
bool RTree_Remove(struct RTree *rtree, struct RTDoc const *doc);
size_t RTree_Size(struct RTree const *rtree);
bool RTree_IsEmpty(struct RTree const *rtree);
void RTree_Clear(struct RTree *rtree);
struct RTDoc *RTree_Bounds(struct RTree const *rtree);

struct QueryIterator *RTree_Query_Contains(struct RTree const *rtree, struct Polygon const *query_poly);
struct QueryIterator *RTree_Query_Within(struct RTree const *rtree, struct Polygon const *query_poly);
void QIter_Free(struct QueryIterator *iter);
struct RTDoc *QIter_Next(struct QueryIterator *iter);
size_t QIter_Remaining(struct QueryIterator const *iter);

#ifdef __cplusplus
}
#endif

