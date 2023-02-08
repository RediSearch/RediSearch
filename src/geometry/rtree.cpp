
#include <fstream>
#include "rtree.hpp"

[[nodiscard]] RTree *RTree_New() {
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

int RTree_Insert_WKT(RTree *rtree, const char *wkt, size_t len, docID_t id) {
  try {
    rtree->insert(RTDoc{std::string_view{wkt, len}, id});
    return 0;
  } catch (...) {
    return 1;
  }
}

bool RTree_Remove(RTree *rtree, RTDoc const *doc) {
  return rtree->remove(*doc);
}

int RTree_Remove_WKT(RTree *rtree, const char *wkt, size_t len, docID_t id) {
  try {
    return rtree->remove(RTDoc{std::string_view{wkt, len}, id});
  } catch (...) {
    return -1;
  }
}

[[nodiscard]] GeometryQueryIterator *RTree_Query(RTree const *rtree, RTDoc const *queryDoc,
                                         QueryType queryType) {
  return new GeometryQueryIterator{rtree->query(*queryDoc, queryType)};
}

[[nodiscard]] GeometryQueryIterator *RTree_Query_WKT(struct RTree const *rtree, const char *wkt, size_t len,
                                             docID_t id, enum QueryType queryType) {
  return new GeometryQueryIterator{rtree->query(RTDoc{std::string_view{wkt, len}, id}, queryType)};
}

[[nodiscard]] RTDoc *RTree_Bounds(RTree const *rtree) {
  return new RTDoc{rtree->rtree_.bounds()};
}

[[nodiscard]] size_t RTree_Size(RTree const *rtree) noexcept {
  return rtree->size();
}

[[nodiscard]] bool RTree_IsEmpty(RTree const *rtree) noexcept {
  return rtree->is_empty();
}

void RTree_Clear(RTree *rtree) noexcept {
  rtree->clear();
}

[[nodiscard]] size_t RTree_MemUsage(RTree const *rtree) {
  return rtree->report();
}
