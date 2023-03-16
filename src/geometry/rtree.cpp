/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rtree.hpp"

#include <fstream>

RTree *RTree_New() {
  return new RTree{};
}

RTree *Load_WKT_File(RTree *rtree, const char *path) {
  if (nullptr == rtree) {
    rtree = RTree_New();
  }

  auto file = std::ifstream{path};
  for (string wkt{}; std::getline(file, wkt, '\n');) {
    rtree->insert(RTDoc{wkt});
  }

  return rtree;
}

void RTree_Free(RTree *rtree) noexcept {
  delete rtree;
}

void RTree_Insert(RTree *rtree, RTDoc const *doc) {
  rtree->insert(*doc);
}

int RTree_Insert_WKT(RTree *rtree, const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg) {
  try {
    rtree->insert(RTDoc{std::string_view{wkt, len}, id});
    return 0;
  } catch (const std::exception &e) {
    if (err_msg)
      *err_msg = RedisModule_CreateString(nullptr, e.what(), strlen(e.what()));
    return 1;
  }
}

bool RTree_Remove(RTree *rtree, RTDoc const *doc) {
  return rtree->remove(*doc);
}

int RTree_Remove_WKT(RTree *rtree, const char *wkt, size_t len, t_docId id) {
  try {
    return rtree->remove(RTDoc{std::string_view{wkt, len}, id});
  } catch (...) {
    return -1;
  }
}

void RTree_Dump(RTree* rtree, RedisModuleCtx *ctx) {
  rtree->dump(ctx);
}

IndexIterator *generate_query_iterator(RTree::ResultsVec&& results) {
  auto ids = GeometryQueryIterator::container(results.size());
  std::ranges::transform(results, ids.begin(), [](auto && doc) { return doc.id(); });
  auto gqi = new GeometryQueryIterator(std::move(ids));
  return gqi->base();
}

IndexIterator *RTree_Query(RTree const *rtree, RTDoc const *queryDoc, QueryType queryType) {
  return generate_query_iterator(rtree->query(*queryDoc, queryType));
}

IndexIterator *RTree_Query_WKT(RTree const *rtree, const char *wkt, size_t len, enum QueryType queryType, RedisModuleString **err_msg) {
  try
  {
    auto res = rtree->query(RTDoc{std::string_view{wkt, len}, 0}, queryType);
    return generate_query_iterator(std::move(res));
  }
  catch(const std::exception& e)
  {
    if (err_msg)
      *err_msg = RedisModule_CreateString(nullptr, e.what(), strlen(e.what()));
    return nullptr;
  }
}

RTDoc *RTree_Bounds(RTree const *rtree) {
  return new RTDoc{rtree->rtree_.bounds()};
}

size_t RTree_Size(RTree const *rtree) noexcept {
  return rtree->size();
}

bool RTree_IsEmpty(RTree const *rtree) noexcept {
  return rtree->is_empty();
}

void RTree_Clear(RTree *rtree) noexcept {
  rtree->clear();
}

size_t RTree_MemUsage(RTree const *rtree) {
  return rtree->report();
}
