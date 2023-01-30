#pragma once

#include <stddef.h>
#include <stdbool.h>
#include "query_iterator.h"
#include "rtdoc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct RTree;

NODISCARD struct RTree *RTree_New();
void RTree_Free(struct RTree *rtree) NOEXCEPT;
void RTree_Insert(struct RTree *rtree, struct RTDoc const *doc);
bool RTree_Remove(struct RTree *rtree, struct RTDoc const *doc);
NODISCARD size_t RTree_Size(struct RTree const *rtree) NOEXCEPT;
NODISCARD bool RTree_IsEmpty(struct RTree const *rtree) NOEXCEPT;
void RTree_Clear(struct RTree *rtree) NOEXCEPT;
NODISCARD struct RTDoc *RTree_Bounds(struct RTree const *rtree);

NODISCARD struct QueryIterator *RTree_Query_Contains(struct RTree const *rtree, struct RTDoc const *query);
NODISCARD struct QueryIterator *RTree_Query_Within(struct RTree const *rtree, struct RTDoc const *query);

NODISCARD size_t RTree_MemUsage(struct RTree const *rtree);
#ifdef __cplusplus
}
#endif

