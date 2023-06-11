/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rtree.hpp"

// #include <fstream>


RTree_Cartesian *RTree_Cartesian_New() {
  return new RTree_Cartesian{};
}
RTree_Geographic *RTree_Geographic_New() {
  return new RTree_Geographic{};
}

// RTree *Load_WKT_File(RTree *rtree, const char *path) {
//   if (nullptr == rtree) {
//     rtree = RTree_New();
//   }

//   auto file = std::ifstream{path};
//   for (string wkt{}; std::getline(file, wkt, '\n');) {
//     auto geometry = Polygon::from_wkt(wkt);
//     rtree->insert(geometry, 0);
//   }

//   return rtree;
// }

void RTree_Cartesian_Free(RTree_Cartesian *rtree) noexcept {
  delete rtree;
}
void RTree_Geographic_Free(RTree_Geographic *rtree) noexcept {
  delete rtree;
}

int RTree_Cartesian_Insert_WKT(RTree_Cartesian *rtree, const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg) {
  return rtree->insert(wkt, len, id, err_msg);
}
int RTree_Geographic_Insert_WKT(RTree_Cartesian *rtree, const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg) {
  return rtree->insert(wkt, len, id, err_msg);
}

bool RTree_Cartesian_Remove(RTree_Cartesian *rtree, RTDoc_Cartesian const *doc) {
  return rtree->remove(*doc);
}
bool RTree_Geographic_Remove(RTree_Geographic *rtree, RTDoc_Geographic const *doc) {
  return rtree->remove(*doc);
}

bool RTree_Cartesian_RemoveByDocId(RTree_Cartesian *rtree, t_docId id) {
  return rtree->remove(id);
}
bool RTree_Geographic_RemoveByDocId(RTree_Geographic *rtree, t_docId id) {
  return rtree->remove(id);
}

int RTree_Cartesian_Remove_WKT(RTree_Cartesian *rtree, const char *wkt, size_t len, t_docId id) {
  return rtree->remove(wkt, len, id);
}
int RTree_Geographic_Remove_WKT(RTree_Geographic *rtree, const char *wkt, size_t len, t_docId id) {
  return rtree->remove(wkt, len, id);
}

void RTree_Cartesian_Dump(RTree_Cartesian *rtree, RedisModuleCtx *ctx) {
  rtree->dump(ctx);
}
void RTree_Geographic_Dump(RTree_Geographic *rtree, RedisModuleCtx *ctx) {
  rtree->dump(ctx);
}

IndexIterator *RTree_Cartesian_Query(RTree_Cartesian const *rtree, RTDoc_Cartesian const *queryDoc, QueryType queryType) {
  return rtree->query(*queryDoc, queryType);
}
IndexIterator *RTree_Geographic_Query(RTree_Geographic const *rtree, RTDoc_Geographic const *queryDoc, QueryType queryType) {
  return rtree->query(*queryDoc, queryType);
}

IndexIterator *RTree_Cartesian_Query_WKT(RTree_Cartesian const *rtree, const char *wkt, size_t len, enum QueryType queryType, RedisModuleString **err_msg) {
  return rtree->query(wkt, len, queryType, err_msg);
}
IndexIterator *RTree_Geographic_Query_WKT(RTree_Geographic const *rtree, const char *wkt, size_t len, enum QueryType queryType, RedisModuleString **err_msg) {
  return rtree->query(wkt, len, queryType, err_msg);
}

RTDoc_Cartesian *RTree_Cartesian_Bounds(RTree_Cartesian const *rtree) {
  return new RTDoc_Cartesian{rtree->rtree_.bounds()};
}
RTDoc_Geographic *RTree_Geographic_Bounds(RTree_Geographic const *rtree) {
  return new RTDoc_Geographic{rtree->rtree_.bounds()};
}

size_t RTree_Cartesian_Size(RTree_Cartesian const *rtree) noexcept {
  return rtree->size();
}
size_t RTree_Geographic_Size(RTree_Geographic const *rtree) noexcept {
  return rtree->size();
}

bool RTree_Cartesian_IsEmpty(RTree_Cartesian const *rtree) noexcept {
  return rtree->is_empty();
}
bool RTree_Geographic_IsEmpty(RTree_Geographic const *rtree) noexcept {
  return rtree->is_empty();
}

void RTree_Cartesian_Clear(RTree_Cartesian *rtree) noexcept {
  rtree->clear();
}
void RTree_Geographic_Clear(RTree_Geographic *rtree) noexcept {
  rtree->clear();
}

size_t RTree_Cartesian_MemUsage(RTree_Cartesian const *rtree) {
  return rtree->report();
}
size_t RTree_Geographic_MemUsage(RTree_Geographic const *rtree) {
  return rtree->report();
}

size_t RTree_TotalMemUsage() {
  return RTree_Cartesian::reportTotal() + RTree_Geographic::reportTotal();
}
