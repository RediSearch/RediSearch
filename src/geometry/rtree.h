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

struct RTree_Cartesian;
struct RTree_Geographic;
struct RedisModuleCtx;

NODISCARD struct RTree_Cartesian *RTree_Cartesian_New();
NODISCARD struct RTree_Geographic *RTree_Geographic_New();
// struct RTree *Load_WKT_File(struct RTree *rtree, const char *path);
void RTree_Cartesian_Free(struct RTree_Cartesian *rtree) NOEXCEPT;
void RTree_Geographic_Free(struct RTree_Geographic *rtree) NOEXCEPT;
int RTree_Cartesian_Insert_WKT(struct RTree_Cartesian *rtree, const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg);
int RTree_Geographic_Insert_WKT(struct RTree_Geographic *rtree, const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg);
bool RTree_Cartesian_Remove(struct RTree_Cartesian *rtree, struct RTDoc_Cartesian const *doc);
bool RTree_Geographic_Remove(struct RTree_Geographic *rtree, struct RTDoc_Geographic const *doc);
bool RTree_Cartesian_RemoveByDocId(struct RTree_Cartesian *rtree, t_docId);
bool RTree_Geographic_RemoveByDocId(struct RTree_Geographic *rtree, t_docId);
int RTree_Cartesian_Remove_WKT(struct RTree_Cartesian *rtree, const char *wkt, size_t len, t_docId id);
int RTree_Geographic_Remove_WKT(struct RTree_Geographic *rtree, const char *wkt, size_t len, t_docId id);
void RTree_Cartesian_Dump(struct RTree_Cartesian *rtree, RedisModuleCtx *ctx);
void RTree_Geographic_Dump(struct RTree_Geographic *rtree, RedisModuleCtx *ctx);
NODISCARD size_t RTree_Cartesian_Size(struct RTree_Cartesian const *rtree) NOEXCEPT;
NODISCARD size_t RTree_Geographic_Size(struct RTree_Geographic const *rtree) NOEXCEPT;
NODISCARD bool RTree_Cartesian_IsEmpty(struct RTree_Cartesian const *rtree) NOEXCEPT;
NODISCARD bool RTree_Geographic_IsEmpty(struct RTree_Geographic const *rtree) NOEXCEPT;
void RTree_Cartesian_Clear(struct RTree_Cartesian *rtree) NOEXCEPT;
void RTree_Geographic_Clear(struct RTree_Geographic *rtree) NOEXCEPT;
NODISCARD struct RTDoc_Cartesian *RTree_Cartesian_Bounds(struct RTree_Cartesian const *rtree);
NODISCARD struct RTDoc_Geographic *RTree_Geographic_Bounds(struct RTree_Geographic const *rtree);

NODISCARD IndexIterator *RTree_Cartesian_Query(struct RTree_Cartesian const *rtree, struct RTDoc_Cartesian const *queryDoc, enum QueryType queryType);
NODISCARD IndexIterator *RTree_Geographic_Query(struct RTree_Geographic const *rtree, struct RTDoc_Geographic const *queryDoc, enum QueryType queryType);

// Caller should free the returned err_msg
NODISCARD IndexIterator *RTree_Cartesian_Query_WKT(struct RTree_Cartesian const *rtree, const char *wkt, size_t len, enum QueryType queryType, RedisModuleString **err_msg);
NODISCARD IndexIterator *RTree_Geographic_Query_WKT(struct RTree_Geographic const *rtree, const char *wkt, size_t len, enum QueryType queryType, RedisModuleString **err_msg);

NODISCARD size_t RTree_Cartesian_MemUsage(struct RTree_Cartesian const *rtree);
NODISCARD size_t RTree_Geographic_MemUsage(struct RTree_Geographic const *rtree);

// Return the total memory usage of all RTree instances
NODISCARD size_t RTree_TotalMemUsage();

#ifdef __cplusplus
}
#endif
