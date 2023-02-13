
#include <fstream>
#include "rtree.hpp"

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

int RTree_Insert_WKT(RTree *rtree, const char *wkt, size_t len, t_docId id) {
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

int RTree_Remove_WKT(RTree *rtree, const char *wkt, size_t len, t_docId id) {
  try {
    return rtree->remove(RTDoc{std::string_view{wkt, len}, id});
  } catch (...) {
    return -1;
  }
}

IndexIterator *RTree_Query(RTree const *rtree, RTDoc const *queryDoc, QueryType queryType) {
  return (new GeometryQueryIterator {
    std::ranges::transform_view(
      rtree->query(
        *queryDoc,
        queryType
      ),
      [](auto && doc) {
        return doc.id();
      }
    )
  })->base();
}

IndexIterator *RTree_Query_WKT(RTree const *rtree, const char *wkt, size_t len, enum QueryType queryType) {
  return (new GeometryQueryIterator {
    std::ranges::transform_view(
      rtree->query(
        RTDoc{std::string_view{wkt, len}, 0},
        queryType
      ),
      [](auto && doc) {
        return doc.id();
      }
    )
  })->base();
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
