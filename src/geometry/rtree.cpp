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
    auto geometry = Polygon::from_wkt(wkt);
    rtree->insert(geometry, 0);
  }

  return rtree;
}

void RTree_Free(RTree *rtree) noexcept {
  delete rtree;
}

int RTree_Insert_WKT(RTree *rtree, const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg) {
  try {
    auto geometry = Polygon::from_wkt(std::string_view{wkt, len});
    rtree->insert(geometry, id);
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

bool RTree_RemoveByDocId(RTree *rtree, t_docId id) {
  return rtree->remove(id);
}

int RTree_Remove_WKT(RTree *rtree, const char *wkt, size_t len, t_docId id) {
  try {
    auto geometry = rtree->lookup(id);
    if (geometry.has_value()) {
      rtree->removeId(id);
      return rtree->remove(RTDoc{geometry.value(), id});
    } else {
      auto geometry = Polygon::from_wkt(std::string_view{wkt,len});
      return rtree->remove(RTDoc{geometry, id});
    }
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
  auto geometry = rtree->lookup(queryDoc->id());
  return generate_query_iterator(rtree->query(*queryDoc, queryType, geometry.value()));
}

IndexIterator *RTree_Query_WKT(RTree const *rtree, const char *wkt, size_t len, enum QueryType queryType, RedisModuleString **err_msg) {
  try
  {
    auto geometry = Polygon::from_wkt(wkt);
    auto res = rtree->query(RTDoc{geometry, 0}, queryType, geometry);
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

size_t RTree_TotalMemUsage() {
  return RTree::reportTotal();
}
