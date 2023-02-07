
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
	for (string wkt{}; std::getline(file, wkt, '\n'); ) {
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

bool RTree_Remove(RTree *rtree, RTDoc const *doc) {
	return rtree->remove(*doc);
}

[[nodiscard]] GeometryQueryIterator *RTree_Query(RTree const *rtree, RTDoc const *queryDoc, QueryType queryType) {
	auto qi = new GeometryQueryIterator{};
	switch (queryType) {
		case QueryType::CONTAINS: qi->iter_ = std::move(rtree->contains(queryDoc)); break;
		case QueryType::WITHIN  : qi->iter_ = std::move(rtree->within  (queryDoc)); break;
		default: __builtin_unreachable();
	}
	return qi;
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
