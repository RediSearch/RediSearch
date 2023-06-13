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

struct RedisModuleCtx;

#define X(variant)                                                                                 \
  struct RTree_##variant;                                                                          \
  NODISCARD struct RTree_##variant *RTree_##variant##_New();                                       \
  void RTree_##variant##_Free(struct RTree_##variant *rtree) NOEXCEPT;                             \
  int RTree_##variant##_Insert_WKT(                                                                \
    struct RTree_##variant *rtree, const char *wkt, size_t len,                                    \
    t_docId id, RedisModuleString **err_msg);                                                      \
  bool RTree_##variant##_Remove(struct RTree_##variant *rtree, struct RTDoc_##variant const *doc); \
  bool RTree_##variant##_RemoveByDocId(struct RTree_##variant *rtree, t_docId);                    \
  int RTree_##variant##_Remove_WKT(                                                                \
    struct RTree_##variant *rtree, const char *wkt, size_t len, t_docId id);                       \
  void RTree_##variant##_Dump(struct RTree_##variant *rtree, RedisModuleCtx *ctx);                 \
  NODISCARD size_t RTree_##variant##_Size(struct RTree_##variant const *rtree) NOEXCEPT;           \
  NODISCARD bool RTree_##variant##_IsEmpty(struct RTree_##variant const *rtree) NOEXCEPT;          \
  void RTree_##variant##_Clear(struct RTree_##variant *rtree) NOEXCEPT;                            \
  NODISCARD struct RTDoc_##variant *RTree_##variant##_Bounds(struct RTree_##variant const *rtree); \
  NODISCARD IndexIterator *RTree_##variant##_Query(                                                \
    struct RTree_##variant const *rtree, struct RTDoc_##variant const *queryDoc,                   \
    enum QueryType queryType);                                                                     \
  NODISCARD IndexIterator *RTree_##variant##_Query_WKT(                                            \
    struct RTree_##variant const *rtree, const char *wkt, size_t len,                              \
    enum QueryType queryType, RedisModuleString **err_msg);                                        \
  NODISCARD size_t RTree_##variant##_MemUsage(struct RTree_##variant const *rtree);

GEO_VARIANTS(X)
#undef X

// Return the total memory usage of all RTree instances
NODISCARD size_t RTree_TotalMemUsage();

#ifdef __cplusplus
}
#endif
