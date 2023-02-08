#pragma once

#include <stddef.h>
#include <stdbool.h>
#include "query_iterator.h"
#include "rtdoc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct RTree;

enum QueryType {
  CONTAINS,
  WITHIN,
};

NODISCARD struct RTree *RTree_New();
struct RTree *Load_WKT_File(struct RTree *rtree, const char *path);
void RTree_Free(struct RTree *rtree) NOEXCEPT;
void RTree_Insert(struct RTree *rtree, struct RTDoc const *doc);
int RTree_Insert_WKT(struct RTree *rtree, const char *wkt, size_t len, docID_t id);
bool RTree_Remove(struct RTree *rtree, struct RTDoc const *doc);
int RTree_Remove_WKT(struct RTree *rtree, const char *wkt, size_t len, docID_t id);
NODISCARD size_t RTree_Size(struct RTree const *rtree) NOEXCEPT;
NODISCARD bool RTree_IsEmpty(struct RTree const *rtree) NOEXCEPT;
void RTree_Clear(struct RTree *rtree) NOEXCEPT;
NODISCARD struct RTDoc *RTree_Bounds(struct RTree const *rtree);

NODISCARD struct QueryIterator *RTree_Query(struct RTree const *rtree, struct RTDoc const *queryDoc,
                                            enum QueryType queryType);
NODISCARD struct QueryIterator *RTree_Query_WKT(struct RTree const *rtree, const char *wkt,
                                                size_t len, docID_t id, enum QueryType queryType);

NODISCARD size_t RTree_MemUsage(struct RTree const *rtree);
#ifdef __cplusplus
}
#endif
