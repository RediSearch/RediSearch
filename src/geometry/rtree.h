/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include <stddef.h>
#include <stdbool.h>
#include "index_iterator.h"
#include "rtdoc.h"
#include "geometry_types.h"

#ifdef __cplusplus
extern "C" {
#endif

struct RTree;
struct RedisModuleCtx;

NODISCARD struct RTree *RTree_New();
struct RTree *Load_WKT_File(struct RTree *rtree, const char *path);
void RTree_Free(struct RTree *rtree) NOEXCEPT;
int RTree_Insert_WKT(struct RTree *rtree, const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg);
bool RTree_Remove(struct RTree *rtree, struct RTDoc const *doc);
bool RTree_RemoveByDocId(struct RTree *rtree, t_docId);
int RTree_Remove_WKT(struct RTree *rtree, const char *wkt, size_t len, t_docId id);
void RTree_Dump(struct RTree* rtree, RedisModuleCtx *ctx);
NODISCARD size_t RTree_Size(struct RTree const *rtree) NOEXCEPT;
NODISCARD bool RTree_IsEmpty(struct RTree const *rtree) NOEXCEPT;
void RTree_Clear(struct RTree *rtree) NOEXCEPT;
NODISCARD struct RTDoc *RTree_Bounds(struct RTree const *rtree);

NODISCARD IndexIterator *RTree_Query(struct RTree const *rtree, struct RTDoc const *queryDoc, enum QueryType queryType);

// Caller should free the returned err_msg
NODISCARD IndexIterator *RTree_Query_WKT(struct RTree const *rtree, const char *wkt, size_t len, enum QueryType queryType, RedisModuleString **err_msg);

NODISCARD size_t RTree_MemUsage(struct RTree const *rtree);
#ifdef __cplusplus
}
#endif
