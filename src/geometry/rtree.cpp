
#include "rtree.hpp"

[[nodiscard]] RTree *RTree_New() {
	return new RTree{};
}

void RTree_Free(RTree *rtree) noexcept {
	delete rtree;
}

void RTree_Insert(RTree *rtree, RTDoc const *doc) {
	rtree->rtree_.insert(*doc);
}

bool RTree_Remove(RTree *rtree, RTDoc const *doc) {
	return rtree->rtree_.remove(*doc);
}

[[nodiscard]] QueryIterator *RTree_Query(RTree const *rtree, RTDoc const *queryDoc, QueryType queryType) {
	switch (queryType) {
		case QueryType::CONTAINS: return rtree->contains(queryDoc);
		case QueryType::WITHIN  : return rtree->within  (queryDoc);
		default: __builtin_unreachable();
	}
}

[[nodiscard]] RTDoc *RTree_Bounds(RTree const *rtree) {
	return new RTDoc{rtree->rtree_.bounds()};
}

[[nodiscard]] size_t RTree_Size(RTree const *rtree) noexcept {
	return rtree->rtree_.size();
}

[[nodiscard]] bool RTree_IsEmpty(RTree const *rtree) noexcept {
	return rtree->rtree_.empty();
}

void RTree_Clear(RTree *rtree) noexcept {
	rtree->rtree_.clear();
}



[[nodiscard]] size_t RTree_MemUsage(RTree const *rtree) {
	return rtree->rtree_.get_allocator().report();
}
