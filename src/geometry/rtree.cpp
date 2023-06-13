/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rtree.hpp"

#define X(variant)                                                                               \
  RTree_##variant *RTree_##variant##_New() {                                                     \
    return new RTree_##variant{};                                                                \
  }                                                                                              \
  void RTree_##variant##_Free(RTree_##variant *rtree) noexcept {                                 \
    delete rtree;                                                                                \
  }                                                                                              \
  int RTree_##variant##_Insert_WKT(RTree_##variant *rtree, const char *wkt, size_t len,          \
                                   t_docId id, RedisModuleString **err_msg) {                    \
    return rtree->insert(wkt, len, id, err_msg);                                                 \
  }                                                                                              \
  bool RTree_##variant##_Remove(RTree_##variant *rtree, RTDoc_##variant const *doc) {            \
    return rtree->remove(*doc);                                                                  \
  }                                                                                              \
  bool RTree_##variant##_RemoveByDocId(RTree_##variant *rtree, t_docId id) {                     \
    return rtree->remove(id);                                                                    \
  }                                                                                              \
  int RTree_##variant##_Remove_WKT(RTree_##variant *rtree, const char *wkt, size_t len,          \
                                   t_docId id) {                                                 \
    return rtree->remove(wkt, len, id);                                                          \
  }                                                                                              \
  void RTree_##variant##_Dump(RTree_##variant *rtree, RedisModuleCtx *ctx) {                     \
    rtree->dump(ctx);                                                                            \
  }                                                                                              \
  IndexIterator *RTree_##variant##_Query(RTree_##variant const *rtree,                           \
                                         RTDoc_##variant const *queryDoc, QueryType queryType) { \
    return rtree->query(*queryDoc, queryType);                                                   \
  }                                                                                              \
  IndexIterator *RTree_##variant##_Query_WKT(RTree_##variant const *rtree, const char *wkt,      \
                                             size_t len, enum QueryType queryType,               \
                                             RedisModuleString **err_msg) {                      \
    return rtree->query(wkt, len, queryType, err_msg);                                           \
  }                                                                                              \
  RTDoc_##variant *RTree_##variant##_Bounds(RTree_##variant const *rtree) {                      \
    return new RTDoc_##variant{rtree->rtree_.bounds()};                                          \
  }                                                                                              \
  size_t RTree_##variant##_Size(RTree_##variant const *rtree) noexcept {                         \
    return rtree->size();                                                                        \
  }                                                                                              \
  bool RTree_##variant##_IsEmpty(RTree_##variant const *rtree) noexcept {                        \
    return rtree->is_empty();                                                                    \
  }                                                                                              \
  void RTree_##variant##_Clear(RTree_##variant *rtree) noexcept {                                \
    rtree->clear();                                                                              \
  }                                                                                              \
  size_t RTree_##variant##_MemUsage(RTree_##variant const *rtree) {                              \
    return rtree->report();                                                                      \
  }

GEO_VARIANTS(X)
#undef X

size_t RTree_TotalMemUsage() {
  return RTree_Cartesian::reportTotal() + RTree_Geographic::reportTotal();
}
